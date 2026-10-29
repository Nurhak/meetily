//! Client-side key pinning, as recorders do it (sync-protocol.md, "Transport and security"): accept the server if
//! its public-key fingerprint matches; ignore host names, chains and validity dates. Used by the tests and
//! available to Rust clients.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

use crate::identity::certificate_fingerprint;

#[derive(Debug)]
pub struct PinnedKeyVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl PinnedKeyVerifier {
    pub fn new(fingerprint: impl Into<String>) -> Self {
        Self {
            fingerprint: fingerprint.into(),
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        }
    }

    /// A client configuration that trusts exactly the hub with this fingerprint.
    pub fn client_config(fingerprint: impl Into<String>) -> rustls::ClientConfig {
        let verifier = Arc::new(Self::new(fingerprint));
        rustls::ClientConfig::builder_with_provider(verifier.provider.clone())
            .with_safe_default_protocol_versions()
            .expect("ring supports the default protocol versions")
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth()
    }
}

impl ServerCertVerifier for PinnedKeyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let actual = certificate_fingerprint(end_entity).map_err(|_| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
        })?;
        // Fingerprints are public; a plain comparison is fine here.
        if actual == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
