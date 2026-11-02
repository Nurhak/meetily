//! DNS-SD advertisement of the hub: `_salescopilot._tcp` with TXT `v`, `hid`, `fp` (sync-protocol.md, "Discovery").
//! TXT records are unauthenticated; phones only use them to find addresses.

use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceInfo};

pub const SERVICE_TYPE: &str = "_salescopilot._tcp.local.";

pub struct Advertisement {
    daemon: ServiceDaemon,
    fullname: String,
    /// The same service on Apple's peer-to-peer Wi-Fi (macOS only), through the system's DNS-SD.
    #[cfg(target_os = "macos")]
    peer_to_peer: Option<apple::Registration>,
}

/// TXT record data in DNS wire format: length-prefixed `key=value` strings.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub fn txt_wire(hub_id: &str, fingerprint: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for (k, v) in txt_records(hub_id, fingerprint) {
        let entry = format!("{k}={v}");
        out.push(entry.len().min(255) as u8);
        out.extend_from_slice(&entry.as_bytes()[..entry.len().min(255)]);
    }
    out
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
        #[cfg(target_os = "macos")]
        let peer_to_peer = apple::Registration::peer_to_peer(&instance_name(hub_name), port, &txt_wire(hub_id, fingerprint))
            .map_err(|e| tracing::warn!(target: "sync_hub", "peer-to-peer Wi-Fi advertisement failed: {e}"))
            .ok();
        Ok(Self {
            daemon,
            fullname,
            #[cfg(target_os = "macos")]
            peer_to_peer,
        })
    }

    /// Whether iPhones and iPads can find the hub over Apple's peer-to-peer Wi-Fi.
    pub fn peer_to_peer(&self) -> bool {
        #[cfg(target_os = "macos")]
        return self.peer_to_peer.is_some();
        #[cfg(not(target_os = "macos"))]
        false
    }
}

/// The system's DNS-SD (mDNSResponder), for what a Rust mDNS implementation can't do: advertising on Apple's
/// peer-to-peer Wi-Fi (AWDL), so an iPhone finds the Mac without a shared network. The regular interfaces stay
/// with `mdns-sd`; this registration covers only the peer-to-peer ones.
#[cfg(target_os = "macos")]
mod apple {
    use std::ffi::{c_char, c_void, CString};

    type DnsServiceRef = *mut c_void;
    type RegisterReply = Option<
        unsafe extern "C" fn(DnsServiceRef, u32, i32, *const c_char, *const c_char, *const c_char, *mut c_void),
    >;

    /// `kDNSServiceInterfaceIndexP2P`: `(uint32_t)-3`.
    const INTERFACE_INDEX_P2P: u32 = u32::MAX - 2;

    extern "C" {
        // dns_sd.h, in libSystem.
        fn DNSServiceRegister(
            sd_ref: *mut DnsServiceRef,
            flags: u32,
            interface_index: u32,
            name: *const c_char,
            regtype: *const c_char,
            domain: *const c_char,
            host: *const c_char,
            port_network_order: u16,
            txt_len: u16,
            txt_record: *const c_void,
            callback: RegisterReply,
            context: *mut c_void,
        ) -> i32;
        fn DNSServiceRefDeallocate(sd_ref: DnsServiceRef);
    }

    pub struct Registration(DnsServiceRef);

    // The reference is only used to deallocate it, once.
    unsafe impl Send for Registration {}
    unsafe impl Sync for Registration {}

    impl Registration {
        pub fn peer_to_peer(name: &str, port: u16, txt: &[u8]) -> Result<Self, String> {
            let name = CString::new(name).map_err(|e| e.to_string())?;
            let regtype = CString::new("_salescopilot._tcp").expect("no NUL");
            let mut sd_ref: DnsServiceRef = std::ptr::null_mut();
            // No callback: the registration stays while the reference lives; conflicts are renamed by the system.
            let err = unsafe {
                DNSServiceRegister(
                    &mut sd_ref,
                    0,
                    INTERFACE_INDEX_P2P,
                    name.as_ptr(),
                    regtype.as_ptr(),
                    std::ptr::null(),
                    std::ptr::null(),
                    port.to_be(),
                    txt.len() as u16,
                    txt.as_ptr() as *const c_void,
                    None,
                    std::ptr::null_mut(),
                )
            };
            if err != 0 || sd_ref.is_null() {
                return Err(format!("DNSServiceRegister failed ({err})"));
            }
            Ok(Self(sd_ref))
        }
    }

    impl Drop for Registration {
        fn drop(&mut self) {
            unsafe { DNSServiceRefDeallocate(self.0) };
        }
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
        let wire = txt_wire("6f1d2c7e-4b8a-4f1e-9c3d-2a5b7e9f0c11", "FP");
        assert_eq!(&wire[..4], b"\x03v=1");
        assert_eq!(wire[4] as usize, "hid=6f1d2c7e-4b8a-4f1e-9c3d-2a5b7e9f0c11".len());
        assert_eq!(&wire[wire.len() - 6..], b"\x05fp=FP");
    }
}
