//! Quinn endpoints with pinned mutual TLS, exposed through the `transport` traits.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{ClientConfig, Endpoint, ServerConfig, TransportConfig, VarInt};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::CertificateDer;
use transport::verify::{PinnedClientVerifier, PinnedServerVerifier};
use transport::{
    Conn, Connection, Error, Identity, Incoming, Listener, RecvStream, Result, SERVER_NAME,
    SendStream, TransportKind, WireStats,
};

/// ALPN protocol identifier; bumped with incompatible wire changes.
pub const ALPN: &[u8] = b"xet-p2p/2";

/// Tunables shared by both endpoints.
#[derive(Debug, Clone)]
pub struct EndpointOptions {
    /// Maximum concurrent incoming unidirectional (data) streams a peer may open.
    pub max_data_streams: u32,
    pub idle_timeout: Duration,
    pub keep_alive: Duration,
}

impl Default for EndpointOptions {
    fn default() -> Self {
        Self {
            max_data_streams: 4,
            idle_timeout: Duration::from_secs(30),
            keep_alive: Duration::from_secs(5),
        }
    }
}

fn provider() -> Arc<CryptoProvider> {
    // Explicit, so the choice does not depend on which rustls features other crates enable.
    Arc::new(rustls::crypto::ring::default_provider())
}

fn transport(opts: &EndpointOptions, incoming_uni: u32) -> Result<Arc<TransportConfig>> {
    let mut t = TransportConfig::default();
    t.max_concurrent_bidi_streams(VarInt::from_u32(1))
        .max_concurrent_uni_streams(VarInt::from_u32(incoming_uni))
        .keep_alive_interval(Some(opts.keep_alive))
        .max_idle_timeout(Some(
            opts.idle_timeout
                .try_into()
                .map_err(|_| Error::Config("idle timeout out of range".into()))?,
        ));
    Ok(Arc::new(t))
}

/// A listening endpoint that accepts only clients presenting one of `allowed_clients`.
pub fn server_endpoint(
    listen: SocketAddr,
    identity: Identity,
    allowed_clients: Vec<CertificateDer<'static>>,
    opts: &EndpointOptions,
) -> Result<QuicListener> {
    if allowed_clients.is_empty() {
        return Err(Error::Config(
            "at least one allowed client certificate is required".into(),
        ));
    }
    let provider = provider();
    let mut tls = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(PinnedClientVerifier::new(allowed_clients, &provider))
        .with_single_cert(vec![identity.cert], identity.key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];

    let quic = QuicServerConfig::try_from(tls).map_err(|e| Error::Config(e.to_string()))?;
    let mut config = ServerConfig::with_crypto(Arc::new(quic));
    config.transport_config(transport(opts, opts.max_data_streams)?);
    Ok(QuicListener {
        endpoint: Endpoint::server(config, listen)?,
    })
}

/// A client endpoint that authenticates with `identity` and accepts only `pinned_servers`.
pub fn client_endpoint(
    identity: Identity,
    pinned_servers: Vec<CertificateDer<'static>>,
    opts: &EndpointOptions,
    bind: SocketAddr,
) -> Result<QuicClient> {
    if pinned_servers.is_empty() {
        return Err(Error::Config(
            "at least one pinned server certificate is required".into(),
        ));
    }
    let provider = provider();
    let mut tls = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous() // Not a bypass: installs a strict pinning verifier in place of WebPKI.
        .with_custom_certificate_verifier(PinnedServerVerifier::new(pinned_servers, &provider))
        .with_client_auth_cert(vec![identity.cert], identity.key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];

    let quic = QuicClientConfig::try_from(tls).map_err(|e| Error::Config(e.to_string()))?;
    let mut config = ClientConfig::new(Arc::new(quic));
    // The server opens data streams toward the client when it is the sender (pull).
    config.transport_config(transport(opts, opts.max_data_streams)?);

    let mut endpoint = Endpoint::client(bind)?;
    endpoint.set_default_client_config(config);
    Ok(QuicClient { endpoint })
}

/// Connects to `addr`. Fails unless the server presents a pinned certificate and accepts ours.
pub async fn connect(client: &QuicClient, addr: SocketAddr) -> Result<Conn> {
    client.connect(addr).await
}

/// A listening QUIC endpoint.
pub struct QuicListener {
    endpoint: Endpoint,
}

impl QuicListener {
    pub fn close(&self) {
        self.endpoint.close(0u32.into(), b"shutdown");
    }
}

#[async_trait::async_trait]
impl Listener for QuicListener {
    async fn accept(&self) -> Option<Incoming> {
        let incoming = self.endpoint.accept().await?;
        let remote = incoming.remote_address();
        Some(Incoming {
            remote,
            handshake: Box::pin(async move {
                let conn = incoming.await.map_err(conn_err)?;
                Ok(Arc::new(QuicConn(conn)) as Conn)
            }),
        })
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.endpoint.local_addr()?)
    }

    fn kind(&self) -> TransportKind {
        TransportKind::Quic
    }
}

/// A client QUIC endpoint (one local UDP socket for any number of connections).
pub struct QuicClient {
    endpoint: Endpoint,
}

impl QuicClient {
    pub async fn connect(&self, addr: SocketAddr) -> Result<Conn> {
        let conn = self
            .endpoint
            .connect(addr, SERVER_NAME)
            .map_err(|e| Error::Connection(e.to_string()))?
            .await
            .map_err(conn_err)?;
        Ok(Arc::new(QuicConn(conn)))
    }

    /// Waits until every connection has finished closing (lets final frames reach the peer).
    pub async fn wait_idle(&self) {
        self.endpoint.wait_idle().await;
    }
}

struct QuicConn(quinn::Connection);

fn conn_err(e: quinn::ConnectionError) -> Error {
    Error::Connection(e.to_string())
}

#[async_trait::async_trait]
impl Connection for QuicConn {
    async fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        let (s, r) = self.0.open_bi().await.map_err(conn_err)?;
        Ok((Box::new(s), Box::new(r)))
    }

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream)> {
        let (s, r) = self.0.accept_bi().await.map_err(conn_err)?;
        Ok((Box::new(s), Box::new(r)))
    }

    async fn open_uni(&self) -> Result<SendStream> {
        Ok(Box::new(self.0.open_uni().await.map_err(conn_err)?))
    }

    async fn accept_uni(&self) -> Result<RecvStream> {
        Ok(Box::new(self.0.accept_uni().await.map_err(conn_err)?))
    }

    fn close(&self, code: u32, reason: &[u8]) {
        self.0.close(code.into(), reason);
    }

    async fn closed(&self) {
        self.0.closed().await;
    }

    fn peer_certificate(&self) -> Option<CertificateDer<'static>> {
        self.0
            .peer_identity()?
            .downcast::<Vec<CertificateDer<'static>>>()
            .ok()?
            .into_iter()
            .next()
    }

    fn remote_address(&self) -> SocketAddr {
        self.0.remote_address()
    }

    fn kind(&self) -> TransportKind {
        TransportKind::Quic
    }

    fn rx_progress(&self) -> u64 {
        // STREAM frames only: keep-alive PINGs do not count as progress.
        self.0.stats().frame_rx.stream
    }

    fn wire_stats(&self) -> WireStats {
        let s = self.0.stats();
        WireStats {
            tx: s.udp_tx.bytes,
            rx: s.udp_rx.bytes,
        }
    }
}

/// An unspecified local address of the same family as `remote`, for binding a client endpoint.
pub fn bind_addr_for(remote: SocketAddr) -> SocketAddr {
    match remote {
        SocketAddr::V4(_) => (std::net::Ipv4Addr::UNSPECIFIED, 0).into(),
        SocketAddr::V6(_) => (std::net::Ipv6Addr::UNSPECIFIED, 0).into(),
    }
}
