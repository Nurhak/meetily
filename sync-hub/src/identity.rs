//! The hub's identity: its ID and its TLS key, whose public-key fingerprint phones pin at pairing.
//!
//! Spec: sync-protocol.md, "Transport and security". The key is created once and kept in `identity/` under the
//! hub's data directory. TODO(desktop integration): move the private key into the OS key store (Keychain on
//! macOS, DPAPI on Windows) through a `KeyStorage` hook; until then the file is readable by the owner only.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct HubIdentity {
    /// The hub's deviceId; also its `hubId`.
    pub hub_id: String,
    pub cert_der: Vec<u8>,
    key_der: Vec<u8>,
    /// SHA-256 of the DER SubjectPublicKeyInfo, base64url without padding (43 characters).
    pub fingerprint: String,
}

#[derive(Serialize, Deserialize)]
struct IdentityFile {
    hub_id: String,
}

impl HubIdentity {
    /// Loads the identity from `dir`, creating it on first use.
    pub fn load_or_create(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let (id_path, key_path, cert_path) = paths(dir);
        if id_path.exists() && key_path.exists() && cert_path.exists() {
            let id: IdentityFile = serde_json::from_slice(&std::fs::read(&id_path)?)?;
            let key_pem = std::fs::read_to_string(&key_path)?;
            let key = KeyPair::from_pem(&key_pem).context("reading hub key")?;
            let cert_der = std::fs::read(&cert_path)?;
            return Ok(Self::from_parts(id.hub_id, &key, cert_der));
        }
        let hub_id = uuid::Uuid::new_v4().to_string();
        let key = KeyPair::generate().context("generating hub key")?;
        let cert_der = self_signed_certificate(&hub_id, &key)?;
        write_private(&key_path, key.serialize_pem().as_bytes())?;
        std::fs::write(&cert_path, &cert_der)?;
        std::fs::write(
            &id_path,
            serde_json::to_vec_pretty(&IdentityFile {
                hub_id: hub_id.clone(),
            })?,
        )?;
        Ok(Self::from_parts(hub_id, &key, cert_der))
    }

    /// Deletes the identity: every paired device must pair again afterwards.
    pub fn reset(dir: &Path) -> Result<()> {
        let (id_path, key_path, cert_path) = paths(dir);
        for p in [id_path, key_path, cert_path] {
            if p.exists() {
                std::fs::remove_file(p)?;
            }
        }
        Ok(())
    }

    fn from_parts(hub_id: String, key: &KeyPair, cert_der: Vec<u8>) -> Self {
        let fingerprint = spki_fingerprint(&key.public_key_der());
        Self {
            hub_id,
            cert_der,
            key_der: key.serialize_der(),
            fingerprint,
        }
    }

    pub fn rustls_server_config(&self) -> Result<Arc<rustls::ServerConfig>> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(self.cert_der.clone())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.clone())),
            )?;
        Ok(Arc::new(config))
    }
}

/// SHA-256 of a DER SubjectPublicKeyInfo, base64url without padding.
pub fn spki_fingerprint(spki_der: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(spki_der))
}

/// The SubjectPublicKeyInfo fingerprint of a DER certificate (what recorders compare).
pub fn certificate_fingerprint(cert_der: &[u8]) -> Result<String> {
    let (_, cert) = x509_parser::parse_x509_certificate(cert_der)
        .map_err(|e| anyhow::anyhow!("invalid certificate: {e}"))?;
    Ok(spki_fingerprint(cert.tbs_certificate.subject_pki.raw))
}

fn paths(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
    (
        dir.join("hub.json"),
        dir.join("key.pem"),
        dir.join("cert.der"),
    )
}

fn self_signed_certificate(hub_id: &str, key: &KeyPair) -> Result<Vec<u8>> {
    let mut params = CertificateParams::new(vec!["salescopilot-hub.local".to_string()])?;
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, format!("Sales Copilot hub {hub_id}"));
    params.distinguished_name = name;
    let now = chrono::Utc::now();
    use chrono::Datelike;
    // Recorders ignore validity dates; ten years keeps ordinary TLS tooling happy.
    params.not_before = rcgen::date_time_ymd(now.year() - 1, 1, 1);
    params.not_after = rcgen::date_time_ymd(now.year() + 10, 1, 1);
    Ok(params.self_signed(key)?.der().to_vec())
}

fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(contents)?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, contents)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_matches_certificate_and_survives_reload() {
        let dir = tempfile::tempdir().unwrap();
        let a = HubIdentity::load_or_create(dir.path()).unwrap();
        assert_eq!(a.fingerprint.len(), 43);
        assert_eq!(certificate_fingerprint(&a.cert_der).unwrap(), a.fingerprint);
        let b = HubIdentity::load_or_create(dir.path()).unwrap();
        assert_eq!(a.hub_id, b.hub_id);
        assert_eq!(a.fingerprint, b.fingerprint);
        HubIdentity::reset(dir.path()).unwrap();
        let c = HubIdentity::load_or_create(dir.path()).unwrap();
        assert_ne!(a.fingerprint, c.fingerprint);
        assert_ne!(a.hub_id, c.hub_id);
    }
}
