//! Hub side of the Sales Copilot local-network sync protocol, version 1.
//!
//! The desktop app (this Meetily fork) is the hub: phones pair with it by QR code and sync meetings, transcripts and
//! recordings with it directly over the local network. The protocol is specified in the SalesMeetingCopilot
//! repository under `spec/` (meeting-format.md, sync-protocol.md, sync-api.openapi.yaml); the JSON Schemas are
//! vendored in `spec/schemas/`.
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! use sync_hub::{Hub, HubConfig, HubEvent};
//! let config = HubConfig::new("/path/to/data", "Nurhak's MacBook", "macos", "0.4.1");
//! let (hub, mut events) = Hub::open(config).await?;
//! let server = hub.serve().await?;
//! let pairing = hub.start_pairing(); // show pairing.qr_uri as a QR code, with pairing.code and pairing.verification
//! while let Some(event) = events.recv().await {
//!     if let HubEvent::TranscriptionWanted { meeting_id } = event {
//!         let ticket = hub.begin_transcription(&meeting_id).await?;
//!         // … transcribe ticket.audio_path, then:
//!         // hub.finish_transcription(ticket, transcript_json).await?;
//!     }
//! }
//! server.shutdown().await;
//! # Ok(()) }
//! ```

pub mod api;
mod discovery;
pub mod docs;
pub mod error;
mod hub;
pub mod identity;
pub mod netfilter;
pub mod pairing;
pub mod pinning;
pub mod probe;
mod schema;
mod store;

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use axum_server::tls_rustls::RustlsConfig;

pub use hub::{
    audio_etag, meeting_etag, sha256_file, transcript_etag, AudioFile, Caller, FinishOutcome, Hub,
    HubConfig, HubEvent, Precondition, TranscriptionTicket, Write, DEFAULT_PORT, PROTOCOL_VERSION,
};
pub use pairing::PairingInfo;
pub use store::DeviceRecord;

/// A running server. Dropping it does not stop the server; call [`RunningHub::shutdown`].
pub struct RunningHub {
    /// The addresses actually bound (with the port in use).
    pub addresses: Vec<SocketAddr>,
    handle: axum_server::Handle,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    _advertisement: Option<discovery::Advertisement>,
}

impl RunningHub {
    pub fn port(&self) -> u16 {
        self.addresses.first().map(|a| a.port()).unwrap_or_default()
    }

    pub async fn shutdown(self) {
        self.handle.graceful_shutdown(Some(Duration::from_secs(5)));
        for t in &self.tasks {
            t.abort();
        }
    }
}

fn bind(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    if addr.is_ipv6() {
        // A separate IPv4 listener handles IPv4; Windows is IPv6-only by default, Linux is not.
        socket.set_only_v6(true)?;
    }
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    socket.bind(&addr.into())?;
    socket.listen(128)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

impl Hub {
    /// Starts the HTTPS server (and DNS-SD advertisement when configured).
    pub async fn serve(&self) -> anyhow::Result<RunningHub> {
        let config = self.config().clone();
        anyhow::ensure!(!config.listen.is_empty(), "no listen addresses");
        let mut listeners = Vec::new();
        let first = config.listen[0];
        let first_listener = match bind(first) {
            Ok(l) => l,
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && first.port() != 0 => {
                tracing::warn!(target: "sync_hub", "port {} is taken, using another one", first.port());
                bind(SocketAddr::new(first.ip(), 0))?
            }
            Err(e) => return Err(e).with_context(|| format!("listening on {first}")),
        };
        let port = first_listener.local_addr()?.port();
        listeners.push(first_listener);
        for addr in config.listen.iter().skip(1) {
            match bind(SocketAddr::new(addr.ip(), port)) {
                Ok(l) => listeners.push(l),
                Err(e) => tracing::warn!(target: "sync_hub", "not listening on {addr}: {e}"),
            }
        }
        let addresses: Vec<SocketAddr> = listeners
            .iter()
            .filter_map(|l| l.local_addr().ok())
            .collect();
        self.set_listening(addresses.clone());

        let tls = RustlsConfig::from_config(self.identity().rustls_server_config()?);
        let app = api::router(self.clone());
        let handle = axum_server::Handle::new();
        let mut tasks = Vec::new();
        for listener in listeners {
            let server = axum_server::from_tcp_rustls(listener, tls.clone()).handle(handle.clone());
            let service = app
                .clone()
                .into_make_service_with_connect_info::<SocketAddr>();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = server.serve(service).await {
                    tracing::error!(target: "sync_hub", "server stopped: {e}");
                }
            }));
        }

        let hub = self.clone();
        tasks.push(tokio::spawn(async move {
            let mut maintenance = tokio::time::interval(Duration::from_secs(3600));
            let mut networks = tokio::time::interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = maintenance.tick() => {
                        if let Err(e) = hub.run_maintenance().await {
                            tracing::warn!(target: "sync_hub", "maintenance failed: {e:#}");
                        }
                    }
                    _ = networks.tick() => hub.netfilter().refresh(),
                }
            }
        }));

        let advertisement = if config.advertise {
            discovery::Advertisement::start(&config.name, self.hub_id(), self.fingerprint(), port)
                .map_err(
                    |e| tracing::warn!(target: "sync_hub", "DNS-SD advertisement failed: {e:#}"),
                )
                .ok()
        } else {
            None
        };
        tracing::info!(target: "sync_hub", "listening on {addresses:?}");
        Ok(RunningHub {
            addresses,
            handle,
            tasks,
            _advertisement: advertisement,
        })
    }
}
