//! Only clients on the local network may talk to the hub (sync-protocol.md, "Transport and security").

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::RwLock;

/// A network the hub has an interface on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LocalNet {
    pub addr: IpAddr,
    pub prefix: u8,
}

#[derive(Default)]
pub struct NetFilter {
    local: RwLock<Vec<LocalNet>>,
}

impl NetFilter {
    pub fn new() -> Self {
        let f = Self::default();
        f.refresh();
        f
    }

    /// Re-reads the interfaces (networks change when a laptop moves).
    pub fn refresh(&self) {
        let nets = local_nets();
        *self.local.write().unwrap() = nets;
    }

    pub fn allows(&self, peer: IpAddr) -> bool {
        allowed(peer, &self.local.read().unwrap())
    }
}

pub fn local_nets() -> Vec<LocalNet> {
    if_addrs::get_if_addrs()
        .map(|ifs| {
            ifs.into_iter()
                .map(|i| match i.addr {
                    if_addrs::IfAddr::V4(a) => LocalNet {
                        addr: IpAddr::V4(a.ip),
                        prefix: a.prefixlen,
                    },
                    if_addrs::IfAddr::V6(a) => LocalNet {
                        addr: IpAddr::V6(a.ip),
                        prefix: a.prefixlen,
                    },
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn allowed(peer: IpAddr, local: &[LocalNet]) -> bool {
    let peer = match peer {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    };
    let private = match peer {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private() || ip.is_link_local(),
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || (ip.segments()[0] & 0xfe00) == 0xfc00
                || (ip.segments()[0] & 0xffc0) == 0xfe80
        }
    };
    private || local.iter().any(|net| same_subnet(peer, net))
}

fn same_subnet(peer: IpAddr, net: &LocalNet) -> bool {
    match (peer, net.addr) {
        (IpAddr::V4(a), IpAddr::V4(b)) => mask_v4(a, net.prefix) == mask_v4(b, net.prefix),
        (IpAddr::V6(a), IpAddr::V6(b)) => mask_v6(a, net.prefix) == mask_v6(b, net.prefix),
        _ => false,
    }
}

fn mask_v4(ip: Ipv4Addr, prefix: u8) -> u32 {
    let bits = u32::from(ip);
    if prefix == 0 {
        0
    } else {
        bits & (u32::MAX << (32 - prefix.min(32)))
    }
}

fn mask_v6(ip: Ipv6Addr, prefix: u8) -> u128 {
    let bits = u128::from(ip);
    if prefix == 0 {
        0
    } else {
        bits & (u128::MAX << (128 - prefix.min(128)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn private_and_link_local_are_allowed() {
        for a in [
            "127.0.0.1",
            "10.1.2.3",
            "172.20.0.5",
            "192.168.1.20",
            "169.254.3.4",
            "::1",
            "fd12::5",
            "fe80::1",
            "::ffff:192.168.1.20",
        ] {
            assert!(allowed(ip(a), &[]), "{a}");
        }
        for a in ["8.8.8.8", "2001:db8::1", "::ffff:8.8.8.8", "100.64.0.1"] {
            assert!(!allowed(ip(a), &[]), "{a}");
        }
    }

    #[test]
    fn global_addresses_on_a_local_subnet_are_allowed() {
        let local = [
            LocalNet {
                addr: ip("2a02:a46a:1234:5678::10"),
                prefix: 64,
            },
            LocalNet {
                addr: ip("100.64.12.7"),
                prefix: 24,
            },
        ];
        assert!(allowed(ip("2a02:a46a:1234:5678:abcd::99"), &local));
        assert!(!allowed(ip("2a02:a46a:1234:9999::99"), &local));
        assert!(allowed(ip("100.64.12.200"), &local));
        assert!(!allowed(ip("100.64.13.1"), &local));
    }
}
