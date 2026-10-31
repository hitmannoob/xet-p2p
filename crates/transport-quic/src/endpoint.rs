//! Quinn endpoint construction with pinned mutual TLS.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{ClientConfig, Connection, Endpoint, ServerConfig, TransportConfig, VarInt};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::CertificateDer;

use crate::error::{Error, Result};
use crate::identity::{Identity, SERVER_NAME};
use crate::verify::{PinnedClientVerifier, PinnedServerVerifier};

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
) -> Result<Endpoint> {
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
    Ok(Endpoint::server(config, listen)?)
}

/// A client endpoint that authenticates with `identity` and accepts only `pinned_servers`.
pub fn client_endpoint(
    identity: Identity,
    pinned_servers: Vec<CertificateDer<'static>>,
    opts: &EndpointOptions,
    bind: SocketAddr,
) -> Result<Endpoint> {
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
    // The server never opens streams toward the client.
    config.transport_config(transport(opts, 0)?);

    let mut endpoint = Endpoint::client(bind)?;
    endpoint.set_default_client_config(config);
    Ok(endpoint)
}

/// Connects to `addr`. Fails unless the server presents a pinned certificate and accepts ours.
pub async fn connect(endpoint: &Endpoint, addr: SocketAddr) -> Result<Connection> {
    Ok(endpoint.connect(addr, SERVER_NAME)?.await?)
}

/// An unspecified local address of the same family as `remote`, for binding a client endpoint.
pub fn bind_addr_for(remote: SocketAddr) -> SocketAddr {
    match remote {
        SocketAddr::V4(_) => (std::net::Ipv4Addr::UNSPECIFIED, 0).into(),
        SocketAddr::V6(_) => (std::net::Ipv6Addr::UNSPECIFIED, 0).into(),
    }
}

/// The certificate the peer authenticated with on `conn` (pinned mutual TLS guarantees one).
pub fn peer_certificate(conn: &Connection) -> Option<CertificateDer<'static>> {
    conn.peer_identity()?
        .downcast::<Vec<CertificateDer<'static>>>()
        .ok()?
        .into_iter()
        .next()
}
