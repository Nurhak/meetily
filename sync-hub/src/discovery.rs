//! DNS-SD advertisement of the hub: `_salescopilot._tcp` with TXT `v`, `hid`, `fp` (sync-protocol.md, "Discovery").
//! TXT records are unauthenticated; phones only use them to find addresses.

use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceInfo};

pub const SERVICE_TYPE: &str = "_salescopilot._tcp.local.";

pub struct Advertisement {
    daemon: ServiceDaemon,
    fullname: String,
}

pub fn txt_records(hub_id: &str, fingerprint: &str) -> Vec<(&'static str, String)> {
    vec![
        ("v", "1".to_string()),
        ("hid", hub_id.to_string()),
        ("fp", fingerprint.to_string()),
    ]
}

/// DNS-SD instance names are at most 63 bytes.
pub fn instance_name(hub_name: &str) -> String {
    let mut name: String = hub_name
        .chars()
        .filter(|c| !c.is_control() && *c != '.')
        .collect();
    while name.len() > 63 {
        name.pop();
    }
    if name.is_empty() {
        "Sales Copilot".to_string()
    } else {
        name
    }
}

impl Advertisement {
    pub fn start(hub_name: &str, hub_id: &str, fingerprint: &str, port: u16) -> Result<Self> {
        let daemon = ServiceDaemon::new()?;
        let host = format!("salescopilot-{}.local.", &hub_id[..8]);
        let props: Vec<(&str, String)> = txt_records(hub_id, fingerprint);
        let info = ServiceInfo::new(
            SERVICE_TYPE,
            &instance_name(hub_name),
            &host,
            "",
            port,
            &props[..],
        )?
        .enable_addr_auto();
        let fullname = info.get_fullname().to_string();
        daemon.register(info)?;
        Ok(Self { daemon, fullname })
    }
}

impl Drop for Advertisement {
    fn drop(&mut self) {
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_txt() {
        assert_eq!(instance_name("Nurhak's MacBook"), "Nurhak's MacBook");
        assert_eq!(instance_name("a.b"), "ab");
        assert_eq!(instance_name(&"x".repeat(80)).len(), 63);
        let txt = txt_records("6f1d2c7e-4b8a-4f1e-9c3d-2a5b7e9f0c11", "FP");
        assert_eq!(txt[0], ("v", "1".to_string()));
    }
}
