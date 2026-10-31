//! The connection interface the transfer layer is written against.
//!
//! A connection carries one or more bidirectional streams and any number of unidirectional ones,
//! between two mutually authenticated peers. QUIC provides this natively; other transports (TLS
//! over TCP with a stream multiplexer) emulate it. Nothing here knows about Xet.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use rustls::pki_types::CertificateDer;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Result;

/// Write half of a stream. `shutdown()` finishes it (the peer then reads end-of-stream).
pub type SendStream = Box<dyn AsyncWrite + Send + Unpin>;
/// Read half of a stream.
pub type RecvStream = Box<dyn AsyncRead + Send + Unpin>;

/// Which wire protocol a connection uses. Reported to users: only `Quic` is direct QUIC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportKind {
    /// QUIC over UDP.
    Quic,
    /// TLS 1.3 over TCP, streams multiplexed with yamux. Possibly relayed by a provider tunnel.
    TlsTcp,
}

impl fmt::Display for TransportKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TransportKind::Quic => "QUIC (UDP)",
            TransportKind::TlsTcp => "TLS over TCP (not QUIC)",
        })
    }
}

/// Bytes the transport put on, and took off, the wire for this connection (all overhead included).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WireStats {
    pub tx: u64,
    pub rx: u64,
}

#[async_trait::async_trait]
pub trait Connection: Send + Sync {
    async fn open_bi(&self) -> Result<(SendStream, RecvStream)>;
    async fn accept_bi(&self) -> Result<(SendStream, RecvStream)>;
    async fn open_uni(&self) -> Result<SendStream>;
    async fn accept_uni(&self) -> Result<RecvStream>;

    /// Closes the connection immediately; pending streams fail.
    fn close(&self, code: u32, reason: &[u8]);
    /// Resolves once the connection is closed, by either side.
    async fn closed(&self);

    /// The certificate the peer authenticated with.
    fn peer_certificate(&self) -> Option<CertificateDer<'static>>;
    fn remote_address(&self) -> SocketAddr;
    fn kind(&self) -> TransportKind;

    /// Monotonic counter that advances when application data arrives from the peer. Keep-alives
    /// do not advance it, so it detects a peer that is connected but not sending.
    fn rx_progress(&self) -> u64;
    fn wire_stats(&self) -> WireStats;
}

/// A shared handle to a connection.
pub type Conn = Arc<dyn Connection>;

/// A connection attempt accepted by a listener; awaiting `handshake` completes authentication.
pub struct Incoming {
    pub remote: SocketAddr,
    pub handshake: BoxFuture<'static, Result<Conn>>,
}

#[async_trait::async_trait]
pub trait Listener: Send + Sync {
    /// The next incoming attempt, or `None` once the listener is closed.
    async fn accept(&self) -> Option<Incoming>;
    fn local_addr(&self) -> Result<SocketAddr>;
    fn kind(&self) -> TransportKind;
}
