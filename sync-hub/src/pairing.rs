//! Pairing codes, the verification number for manual pairing, attempt limits and the QR URI.
//! Spec: sync-protocol.md, "Pairing".

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use rand::Rng;
use sha2::{Digest, Sha256};

/// Crockford base32 alphabet: digits and letters without I, L, O, U.
const ALPHABET: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
pub const CODE_LIFETIME: Duration = Duration::from_secs(5 * 60);
pub const MAX_FAILURES_PER_ADDRESS: u32 = 5;
pub const MAX_FAILURES_TOTAL: u32 = 20;

/// Canonical form: upper case, no dashes or spaces, I and L → 1, O → 0.
pub fn canonical_code(typed: &str) -> String {
    typed
        .chars()
        .filter(|c| *c != '-' && !c.is_whitespace())
        .map(|c| match c.to_ascii_uppercase() {
            'I' | 'L' => '1',
            'O' => '0',
            other => other,
        })
        .collect()
}

/// `uint32_big_endian(SHA-256("salescopilot-pair-v1" ‖ fingerprint ‖ canonical code)[0..4]) mod 1 000 000`.
pub fn verification_number(fingerprint: &str, code: &str) -> u32 {
    let mut h = Sha256::new();
    h.update(b"salescopilot-pair-v1");
    h.update(fingerprint.as_bytes());
    h.update(canonical_code(code).as_bytes());
    let d = h.finalize();
    u32::from_be_bytes([d[0], d[1], d[2], d[3]]) % 1_000_000
}

/// `123456` → `123 456`.
pub fn format_verification(n: u32) -> String {
    let s = format!("{n:06}");
    format!("{} {}", &s[..3], &s[3..])
}

fn new_code() -> String {
    let mut rng = rand::rngs::OsRng;
    (0..8)
        .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
        .collect()
}

/// What the desktop app shows while a phone pairs.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PairingInfo {
    /// For display, e.g. `7KQ2-M9XD`.
    pub code: String,
    /// For display, e.g. `689 088`; equals what the phone shows when pairing without the QR code.
    pub verification: String,
    pub qr_uri: String,
    pub expires_in_secs: u64,
}

pub enum Attempt {
    Accepted,
    Invalid,
    AddressBlocked,
}

pub struct PairingSession {
    code: String,
    expires: Instant,
    failures_by_address: HashMap<IpAddr, u32>,
    failures_total: u32,
}

impl PairingSession {
    pub fn start() -> Self {
        Self {
            code: new_code(),
            expires: Instant::now() + CODE_LIFETIME,
            failures_by_address: HashMap::new(),
            failures_total: 0,
        }
    }

    #[cfg(test)]
    pub fn with_code(code: &str) -> Self {
        let mut s = Self::start();
        s.code = canonical_code(code);
        s
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires || self.failures_total >= MAX_FAILURES_TOTAL
    }

    pub fn remaining(&self) -> Duration {
        self.expires.saturating_duration_since(Instant::now())
    }

    pub fn display_code(&self) -> String {
        format!("{}-{}", &self.code[..4], &self.code[4..])
    }

    /// Checks a code sent by `address`. The caller ends the session after `Accepted`.
    pub fn attempt(&mut self, address: IpAddr, typed: &str) -> Attempt {
        if self.failures_by_address.get(&address).copied().unwrap_or(0) >= MAX_FAILURES_PER_ADDRESS
        {
            return Attempt::AddressBlocked;
        }
        if self.is_expired() {
            return Attempt::Invalid;
        }
        let candidate = canonical_code(typed);
        let ok: bool = candidate.len() == self.code.len()
            && candidate
                .bytes()
                .zip(self.code.bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0;
        if ok {
            return Attempt::Accepted;
        }
        *self.failures_by_address.entry(address).or_default() += 1;
        self.failures_total += 1;
        Attempt::Invalid
    }

    pub fn info(
        &self,
        hub_id: &str,
        hub_name: &str,
        fingerprint: &str,
        addresses: &[SocketAddr],
    ) -> PairingInfo {
        PairingInfo {
            code: self.display_code(),
            verification: format_verification(verification_number(fingerprint, &self.code)),
            qr_uri: qr_uri(hub_id, hub_name, addresses, fingerprint, &self.code),
            expires_in_secs: self.remaining().as_secs(),
        }
    }
}

/// `salescopilot://pair?v=1&hid=…&n=…&a=host:port,…&fp=…&c=…`. IPv6 link-local addresses are left out: their
/// zone means nothing on the phone.
pub fn qr_uri(
    hub_id: &str,
    hub_name: &str,
    addresses: &[SocketAddr],
    fingerprint: &str,
    code: &str,
) -> String {
    let addrs: Vec<String> = addresses
        .iter()
        .filter(|a| match a.ip() {
            IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) != 0xfe80,
            IpAddr::V4(_) => true,
        })
        .map(|a| match a.ip() {
            IpAddr::V4(ip) => format!("{ip}:{}", a.port()),
            IpAddr::V6(ip) => format!("[{ip}]:{}", a.port()),
        })
        .collect();
    format!(
        "salescopilot://pair?v=1&hid={hub_id}&n={}&a={}&fp={fingerprint}&c={code}",
        percent_encode(hub_name),
        percent_encode(&addrs.join(",")).replace("%2C", ","),
    )
}

fn percent_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_test_vector() {
        // sync-protocol.md, "Without a camera".
        assert_eq!(canonical_code("7kq2-m9xd"), "7KQ2M9XD");
        assert_eq!(
            verification_number("U8XDRA0Y33XOTCMNDjhDJQqqhc5DhzLBNCl1tHKMtAE", "7kq2-m9xd"),
            689088
        );
        assert_eq!(format_verification(42917), "042 917");
        assert_eq!(canonical_code("io1l 0o"), "101100");
    }

    #[test]
    fn codes_use_the_crockford_alphabet() {
        for _ in 0..200 {
            let c = new_code();
            assert_eq!(c.len(), 8);
            assert_eq!(
                canonical_code(&c),
                c,
                "canonicalising a generated code must not change it"
            );
        }
    }

    #[test]
    fn failures_are_limited_per_address_and_in_total() {
        let mut s = PairingSession::with_code("7KQ2M9XD");
        let attacker: IpAddr = "192.168.1.66".parse().unwrap();
        let phone: IpAddr = "192.168.1.20".parse().unwrap();
        for _ in 0..MAX_FAILURES_PER_ADDRESS {
            assert!(matches!(s.attempt(attacker, "AAAAAAAA"), Attempt::Invalid));
        }
        assert!(matches!(
            s.attempt(attacker, "7KQ2M9XD"),
            Attempt::AddressBlocked
        ));
        // Another address can still pair, also with a typed lower-case code with a dash and an O for 0.
        assert!(matches!(s.attempt(phone, "7kq2-m9xd"), Attempt::Accepted));

        let mut s = PairingSession::with_code("7KQ2M9XD");
        for i in 0..MAX_FAILURES_TOTAL {
            let ip: IpAddr = format!("10.0.0.{}", i + 1).parse().unwrap();
            s.attempt(ip, "AAAAAAAA");
        }
        assert!(s.is_expired());
        assert!(matches!(s.attempt(phone, "7KQ2M9XD"), Attempt::Invalid));
    }

    #[test]
    fn qr_uri_lists_usable_addresses() {
        let addrs: Vec<SocketAddr> = vec![
            "192.168.1.10:47821".parse().unwrap(),
            "[fd12::5]:47821".parse().unwrap(),
            "[fe80::1]:47821".parse().unwrap(),
        ];
        let uri = qr_uri(
            "6f1d2c7e-4b8a-4f1e-9c3d-2a5b7e9f0c11",
            "Nurhak's MacBook",
            &addrs,
            "FP",
            "7KQ2M9XD",
        );
        assert_eq!(
            uri,
            "salescopilot://pair?v=1&hid=6f1d2c7e-4b8a-4f1e-9c3d-2a5b7e9f0c11&n=Nurhak%27s%20MacBook\
             &a=192.168.1.10%3A47821,%5Bfd12%3A%3A5%5D%3A47821&fp=FP&c=7KQ2M9XD"
        );
    }
}
