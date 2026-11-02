//! The hub's side of the Bluetooth LE link (sync-protocol.md, "Bluetooth LE"), without the platform's Bluetooth
//! API: GATT chunks in, GATT chunks out. Each chunk starts with a channel byte; each channel is a byte stream that
//! carries TLS and HTTP/1.1 exactly like a TCP connection ([`Hub::serve_stream`]).
//!
//! The desktop app's GATT server (CoreBluetooth on macOS, `GattServiceProvider` on Windows) creates one
//! [`BtSession`] per phone that subscribes to `fromHub`, passes every write to `toHub` to
//! [`BtSession::received`], and sends what [`BtSession`]'s receiver yields as notifications on `fromHub`.

use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::hub::Hub;

pub const SERVICE_UUID: &str = "2e6bd214-4421-469e-b114-e9b8b7f1456f";
/// Read: `v=1;hid=<hubId>`.
pub const INFO_UUID: &str = "c3b580af-e181-4874-9534-a8b25aafd385";
/// Write without response: phone → hub chunks.
pub const TO_HUB_UUID: &str = "22f7722e-05ff-476a-af1c-7e321c54ed07";
/// Notify: hub → phone chunks.
pub const FROM_HUB_UUID: &str = "cebf8b4f-2a4c-41e9-b75f-effb7ac08cdf";
/// Channels 0–3.
pub const CHANNELS: usize = 4;
/// Notifications waiting for the radio, per session; beyond this the hub's side of the stream waits.
const NOTIFY_QUEUE: usize = 32;
const STREAM_BUFFER: usize = 64 * 1024;
/// Phone → hub bytes waiting for the hub to read them, per channel; a phone that sends more is disconnected.
const MAX_QUEUED: usize = 256 * 1024;
/// GATT allows attribute values of at most 512 bytes, whatever the MTU.
const MAX_ATTRIBUTE: usize = 512;
/// Phones connected over Bluetooth at once.
pub const MAX_SESSIONS: usize = 8;

/// The value of the `info` characteristic.
pub fn info_value(hub_id: &str) -> Vec<u8> {
    format!("v=1;hid={hub_id}").into_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BtError {
    /// A chunk without a channel byte or for a channel above 3: disconnect the phone.
    BadChunk,
    /// Data on a channel the phone closed and the hub hasn't finished closing: disconnect the phone.
    ChannelClosing,
    /// The phone sent more than the hub has read yet ([`MAX_QUEUED`]): disconnect the phone.
    Overloaded,
    /// Already [`MAX_SESSIONS`] phones on Bluetooth: refuse this one.
    TooManySessions,
}

/// Largest payload for this ATT MTU.
fn payload_for(att_mtu: usize) -> usize {
    (att_mtu.max(23) - 3).min(MAX_ATTRIBUTE)
}

#[derive(Default)]
struct ChannelState {
    phone_closed: bool,
    hub_closed: bool,
}

struct Channel {
    to_stream: mpsc::UnboundedSender<Option<Vec<u8>>>,
    /// Bytes sent to `to_stream` and not yet written to the hub's side.
    queued: Arc<AtomicUsize>,
    state: Arc<StdMutex<ChannelState>>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Channel {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// One connected phone.
pub struct BtSession {
    hub: Hub,
    peer: [u8; 6],
    /// Largest notification payload: ATT MTU − 3.
    max_payload: Arc<AtomicUsize>,
    notify: mpsc::Sender<Vec<u8>>,
    channels: [Option<Channel>; CHANNELS],
}

impl BtSession {
    /// `peer_key` identifies the phone as far as the platform can (e.g. the central's identifier), so limits per
    /// client address apply to it across reconnections. `att_mtu` is the negotiated ATT MTU (23 at least).
    pub fn new(hub: Hub, peer_key: &str, att_mtu: usize) -> Result<(Self, mpsc::Receiver<Vec<u8>>), BtError> {
        if !hub.bt_session_opened(MAX_SESSIONS) {
            return Err(BtError::TooManySessions);
        }
        let (notify, notifications) = mpsc::channel(NOTIFY_QUEUE);
        let digest = Sha256::digest(peer_key.as_bytes());
        let mut peer = [0u8; 6];
        peer.copy_from_slice(&digest[..6]);
        let session = Self {
            hub,
            peer,
            max_payload: Arc::new(AtomicUsize::new(payload_for(att_mtu))),
            notify,
            channels: Default::default(),
        };
        Ok((session, notifications))
    }

    /// The MTU changed (some platforms report it after the subscription).
    pub fn set_mtu(&self, att_mtu: usize) {
        self.max_payload.store(payload_for(att_mtu), Ordering::Relaxed);
    }

    /// The address a channel's requests appear to come from: link-local, so it passes the local-network check,
    /// derived from the phone and the channel.
    pub fn peer_address(&self, channel: u8) -> SocketAddr {
        let p = self.peer;
        let ip = Ipv6Addr::new(
            0xfe80,
            0,
            0,
            0,
            0xb7b7,
            u16::from_be_bytes([p[0], p[1]]),
            u16::from_be_bytes([p[2], p[3]]),
            u16::from_be_bytes([p[4], p[5]]),
        );
        SocketAddr::V6(SocketAddrV6::new(ip, channel as u16, 0, 0))
    }

    /// A write to `toHub`.
    pub fn received(&mut self, chunk: &[u8]) -> Result<(), BtError> {
        let (&channel, data) = chunk.split_first().ok_or(BtError::BadChunk)?;
        let index = channel as usize;
        if index >= CHANNELS {
            return Err(BtError::BadChunk);
        }
        // A finished channel can be opened again.
        let finished = self.channels[index].as_ref().is_some_and(|c| {
            let s = c.state.lock().unwrap();
            s.phone_closed && s.hub_closed
        });
        if finished {
            self.channels[index] = None;
        }
        if data.is_empty() {
            // The phone closed its side.
            if let Some(c) = &self.channels[index] {
                let mut s = c.state.lock().unwrap();
                if !s.phone_closed {
                    s.phone_closed = true;
                    let _ = c.to_stream.send(None);
                }
            }
            return Ok(());
        }
        if self.channels[index].is_none() {
            self.channels[index] = Some(self.open(channel));
        }
        let c = self.channels[index].as_ref().expect("opened above");
        if c.state.lock().unwrap().phone_closed {
            return Err(BtError::ChannelClosing);
        }
        if c.queued.fetch_add(data.len(), Ordering::SeqCst) + data.len() > MAX_QUEUED {
            return Err(BtError::Overloaded);
        }
        let _ = c.to_stream.send(Some(data.to_vec()));
        Ok(())
    }

    fn open(&self, channel: u8) -> Channel {
        let (hub_end, link_end) = tokio::io::duplex(STREAM_BUFFER);
        let (mut link_read, mut link_write) = tokio::io::split(link_end);
        let state = Arc::new(StdMutex::new(ChannelState::default()));
        let hub = self.hub.clone();
        let peer = self.peer_address(channel);
        let serve = tokio::spawn(async move {
            if let Err(e) = hub.serve_stream(hub_end, peer).await {
                tracing::debug!(target: "sync_hub", "bluetooth channel {channel}: {e:#}");
            }
        });
        let (to_stream, mut from_phone) = mpsc::unbounded_channel::<Option<Vec<u8>>>();
        let queued = Arc::new(AtomicUsize::new(0));
        let written = queued.clone();
        let write = tokio::spawn(async move {
            while let Some(item) = from_phone.recv().await {
                match item {
                    Some(bytes) => {
                        let ok = link_write.write_all(&bytes).await.is_ok();
                        written.fetch_sub(bytes.len(), Ordering::SeqCst);
                        if !ok {
                            break;
                        }
                    }
                    None => {
                        let _ = link_write.shutdown().await;
                        break;
                    }
                }
            }
        });
        let notify = self.notify.clone();
        let max_payload = self.max_payload.clone();
        let read_state = state.clone();
        let read = tokio::spawn(async move {
            let mut buf = vec![0u8; STREAM_BUFFER];
            loop {
                let room = max_payload.load(Ordering::Relaxed).saturating_sub(1).max(1);
                // An error ends the channel like the end of the stream.
                let n = link_read.read(&mut buf[..room.min(STREAM_BUFFER)]).await.unwrap_or(0);
                let mut chunk = Vec::with_capacity(n + 1);
                chunk.push(channel);
                chunk.extend_from_slice(&buf[..n]);
                if n == 0 {
                    // Queue the close before marking the channel finished, so a reopened channel can't overtake it.
                    let _ = notify.send(chunk).await;
                    read_state.lock().unwrap().hub_closed = true;
                    return;
                }
                if notify.send(chunk).await.is_err() {
                    return;
                }
            }
        });
        Channel { to_stream, queued, state, tasks: vec![serve, write, read] }
    }
}

impl Drop for BtSession {
    fn drop(&mut self) {
        self.hub.bt_session_closed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_sizes() {
        assert_eq!(payload_for(23), 20);
        assert_eq!(payload_for(185), 182);
        assert_eq!(payload_for(517), 512);
        assert_eq!(payload_for(0), 20);
    }

    #[test]
    fn info_and_addresses() {
        assert_eq!(info_value("6f1d2c7e-4b8a-4f1e-9c3d-2a5b7e9f0c11"), b"v=1;hid=6f1d2c7e-4b8a-4f1e-9c3d-2a5b7e9f0c11");
        let local = crate::netfilter::allowed(
            "fe80::b7b7:1:2:3".parse().unwrap(),
            &[],
        );
        assert!(local, "Bluetooth peers must pass the local-network check");
    }
}
