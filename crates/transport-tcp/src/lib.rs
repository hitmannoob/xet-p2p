//! TLS-over-TCP transport for `xet-p2p`, for paths where UDP (and so QUIC) is unavailable, such as
//! provider TCP tunnels.
//!
//! Peers authenticate exactly as over QUIC: TLS 1.3 with both certificates pinned
//! ([`transport::verify`]). QUIC's streams are emulated with yamux over the TLS session: every
//! stream starts with a one-byte tag saying whether it is bidirectional or unidirectional, and a
//! single driver task owns the multiplexer. This transport is always reported as
//! [`TransportKind::TlsTcp`], never as QUIC.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::poll_fn;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use transport::verify::{PinnedClientVerifier, PinnedServerVerifier};
use transport::{
    Conn, Connection, Error, Identity, Incoming, Listener, RecvStream, Result, SERVER_NAME,
    SendStream, TransportKind, WireStats,
};

/// ALPN for the TCP transport; distinct from QUIC's so the two can never be confused.
pub const ALPN: &[u8] = b"xet-p2p-tcp/3";

const TAG_BI: u8 = b'B';
const TAG_UNI: u8 = b'U';
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const KEEPALIVE: Duration = Duration::from_secs(15);

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn yamux_config() -> yamux::Config {
    let mut c = yamux::Config::default();
    // Generous windows: a long-RTT tunnel should not throttle a single xorb stream.
    c.set_max_connection_receive_window(Some(256 * 1024 * 1024));
    c.set_max_num_streams(256);
    c
}

/// A TCP listener that accepts only clients presenting one of `allowed_clients`.
pub async fn tcp_listen(
    listen: SocketAddr,
    identity: Identity,
    allowed_clients: Vec<CertificateDer<'static>>,
) -> Result<TcpTlsListener> {
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
    Ok(TcpTlsListener {
        tcp: TcpListener::bind(listen).await?,
        acceptor: TlsAcceptor::from(Arc::new(tls)),
    })
}

/// Connects to `addr`, authenticating with `identity` and accepting only `pinned_servers`.
pub async fn tcp_connect(
    addr: SocketAddr,
    identity: Identity,
    pinned_servers: Vec<CertificateDer<'static>>,
) -> Result<Conn> {
    if pinned_servers.is_empty() {
        return Err(Error::Config(
            "at least one pinned server certificate is required".into(),
        ));
    }
    let provider = provider();
    let mut tls = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous() // Not a bypass: a strict pinning verifier replaces WebPKI.
        .with_custom_certificate_verifier(PinnedServerVerifier::new(pinned_servers, &provider))
        .with_client_auth_cert(vec![identity.cert], identity.key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];

    let stream = TcpStream::connect(addr).await?;
    tune(&stream)?;
    let counted = Counted::new(stream);
    let stats = counted.stats.clone();
    let name = ServerName::try_from(SERVER_NAME)
        .map_err(|e| Error::Config(e.to_string()))?
        .to_owned();
    let tls = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        TlsConnector::from(Arc::new(tls)).connect(name, counted),
    )
    .await
    .map_err(|_| Error::Connection("TLS handshake timed out".into()))?
    .map_err(|e| Error::Connection(e.to_string()))?;
    let peer = tls
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|c| c.first().cloned());
    let mut conn = start(tls, yamux::Mode::Client, addr, peer, stats);
    // TLS 1.3 lets the client finish before the server has checked its certificate; make one
    // round trip so a rejected client fails here, not at its first stream.
    probe(&mut conn).await?;
    Ok(Arc::new(conn))
}

fn tune(stream: &TcpStream) -> Result<()> {
    stream.set_nodelay(true)?;
    let ka = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE)
        .with_interval(KEEPALIVE);
    socket2::SockRef::from(stream).set_tcp_keepalive(&ka)?;
    Ok(())
}

pub struct TcpTlsListener {
    tcp: TcpListener,
    acceptor: TlsAcceptor,
}

#[async_trait::async_trait]
impl Listener for TcpTlsListener {
    async fn accept(&self) -> Option<Incoming> {
        loop {
            let (stream, remote) = match self.tcp.accept().await {
                Ok(s) => s,
                Err(_) => continue, // transient accept errors (e.g. EMFILE) must not stop serving
            };
            let acceptor = self.acceptor.clone();
            return Some(Incoming {
                remote,
                handshake: Box::pin(async move {
                    tune(&stream)?;
                    let counted = Counted::new(stream);
                    let stats = counted.stats.clone();
                    let tls = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(counted))
                        .await
                        .map_err(|_| Error::Connection("TLS handshake timed out".into()))?
                        .map_err(|e| Error::Connection(e.to_string()))?;
                    let peer = tls
                        .get_ref()
                        .1
                        .peer_certificates()
                        .and_then(|c| c.first().cloned());
                    let mut conn = start(tls, yamux::Mode::Server, remote, peer, stats);
                    answer_probe(&mut conn).await?;
                    Ok(Arc::new(conn) as Conn)
                }),
            });
        }
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.tcp.local_addr()?)
    }

    fn kind(&self) -> TransportKind {
        TransportKind::TlsTcp
    }
}

// ---------------------------------------------------------------------------------------------
// Multiplexed connection

type YStream = tokio_util::compat::Compat<yamux::Stream>;

enum Cmd {
    Open(oneshot::Sender<std::result::Result<yamux::Stream, yamux::ConnectionError>>),
    Close,
}

pub struct TcpTlsConn {
    cmds: mpsc::UnboundedSender<Cmd>,
    bi: Mutex<mpsc::UnboundedReceiver<YStream>>,
    uni: Mutex<mpsc::UnboundedReceiver<YStream>>,
    closed: watch::Receiver<bool>,
    remote: SocketAddr,
    peer: Option<CertificateDer<'static>>,
    wire: Arc<ByteStats>,
    progress: Arc<AtomicU64>,
}

fn start<S>(
    tls: S,
    mode: yamux::Mode,
    remote: SocketAddr,
    peer: Option<CertificateDer<'static>>,
    wire: Arc<ByteStats>,
) -> TcpTlsConn
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    let (bi_tx, bi_rx) = mpsc::unbounded_channel();
    let (uni_tx, uni_rx) = mpsc::unbounded_channel();
    let (closed_tx, closed_rx) = watch::channel(false);

    let conn = yamux::Connection::new(tls.compat(), yamux_config(), mode);
    tokio::spawn(async move {
        drive(conn, cmd_rx, in_tx).await;
        let _ = closed_tx.send(true);
    });
    // Route each inbound stream by its tag, without letting one slow opener block the others.
    tokio::spawn(route(in_rx, bi_tx, uni_tx));

    TcpTlsConn {
        cmds: cmd_tx,
        bi: Mutex::new(bi_rx),
        uni: Mutex::new(uni_rx),
        closed: closed_rx,
        remote,
        peer: peer.map(|c| c.into_owned()),
        wire,
        progress: Arc::new(AtomicU64::new(0)),
    }
}

/// Owns the yamux connection: serves open/close commands and hands out inbound streams. Polling
/// `poll_next_inbound` is what drives all I/O, so this runs for the connection's lifetime.
///
/// Closing is abrupt, like QUIC's `close`: the multiplexer and socket are dropped at once, so the
/// peer sees the connection end immediately. A graceful yamux close would first try to flush
/// buffered stream data, which never completes if the peer has stopped reading.
async fn drive<T>(
    mut conn: yamux::Connection<T>,
    mut cmds: mpsc::UnboundedReceiver<Cmd>,
    inbound: mpsc::UnboundedSender<yamux::Stream>,
) where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let mut pending: std::collections::VecDeque<oneshot::Sender<_>> = Default::default();
    poll_fn(|cx| {
        loop {
            while let Poll::Ready(cmd) = cmds.poll_recv(cx) {
                match cmd {
                    Some(Cmd::Open(tx)) => pending.push_back(tx),
                    Some(Cmd::Close) | None => return Poll::Ready(()),
                }
            }
            if !pending.is_empty()
                && let Poll::Ready(r) = conn.poll_new_outbound(cx)
            {
                let _ = pending.pop_front().unwrap().send(r);
                continue;
            }
            match conn.poll_next_inbound(cx) {
                Poll::Ready(Some(Ok(s))) => {
                    let _ = inbound.send(s);
                }
                Poll::Ready(Some(Err(_)) | None) => return Poll::Ready(()),
                Poll::Pending => return Poll::Pending,
            }
        }
    })
    .await;
}

async fn route(
    mut inbound: mpsc::UnboundedReceiver<yamux::Stream>,
    bi: mpsc::UnboundedSender<YStream>,
    uni: mpsc::UnboundedSender<YStream>,
) {
    while let Some(s) = inbound.recv().await {
        let (bi, uni) = (bi.clone(), uni.clone());
        tokio::spawn(async move {
            let mut s = s.compat();
            let mut tag = [0u8; 1];
            if tokio::time::timeout(HANDSHAKE_TIMEOUT, s.read_exact(&mut tag))
                .await
                .is_ok_and(|r| r.is_ok())
            {
                let _ = match tag[0] {
                    TAG_BI => bi.send(s),
                    TAG_UNI => uni.send(s),
                    _ => Ok(()), // unknown stream type: drop it
                };
            }
        });
    }
}

impl TcpTlsConn {
    async fn open(&self, tag: u8) -> Result<YStream> {
        let (tx, rx) = oneshot::channel();
        self.cmds
            .send(Cmd::Open(tx))
            .map_err(|_| Error::Connection("connection closed".into()))?;
        let s = rx
            .await
            .map_err(|_| Error::Connection("connection closed".into()))?
            .map_err(|e| Error::Connection(e.to_string()))?;
        let mut s = s.compat();
        s.write_all(&[tag]).await?;
        s.flush().await?;
        Ok(s)
    }

    fn counted(&self, s: tokio::io::ReadHalf<YStream>) -> RecvStream {
        Box::new(ProgressReader {
            inner: s,
            progress: self.progress.clone(),
        })
    }
}

#[async_trait::async_trait]
impl Connection for TcpTlsConn {
    async fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        let (r, w) = tokio::io::split(self.open(TAG_BI).await?);
        Ok((Box::new(w), self.counted(r)))
    }

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream)> {
        let s = self
            .bi
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| Error::Connection("connection closed".into()))?;
        let (r, w) = tokio::io::split(s);
        Ok((Box::new(w), self.counted(r)))
    }

    async fn open_uni(&self) -> Result<SendStream> {
        Ok(Box::new(self.open(TAG_UNI).await?))
    }

    async fn accept_uni(&self) -> Result<RecvStream> {
        let s = self
            .uni
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| Error::Connection("connection closed".into()))?;
        let (r, _w) = tokio::io::split(s);
        Ok(self.counted(r))
    }

    fn close(&self, _code: u32, _reason: &[u8]) {
        let _ = self.cmds.send(Cmd::Close);
    }

    async fn closed(&self) {
        let mut rx = self.closed.clone();
        let _ = rx.wait_for(|c| *c).await;
    }

    fn peer_certificate(&self) -> Option<CertificateDer<'static>> {
        self.peer.clone()
    }

    fn remote_address(&self) -> SocketAddr {
        self.remote
    }

    fn kind(&self) -> TransportKind {
        TransportKind::TlsTcp
    }

    fn rx_progress(&self) -> u64 {
        self.progress.load(Ordering::Relaxed)
    }

    fn wire_stats(&self) -> WireStats {
        WireStats {
            tx: self.wire.tx.load(Ordering::Relaxed),
            rx: self.wire.rx.load(Ordering::Relaxed),
        }
    }
}

/// Client side of the post-handshake round trip: open a stream, expect one byte back.
async fn probe(conn: &mut TcpTlsConn) -> Result<()> {
    let mut s = conn.open(TAG_UNI).await.map_err(rejected)?;
    let mut ok = [0u8; 1];
    tokio::time::timeout(HANDSHAKE_TIMEOUT, s.read_exact(&mut ok))
        .await
        .map_err(|_| Error::Connection("server did not confirm the session".into()))?
        .map_err(rejected)?;
    let _ = s.shutdown().await;
    Ok(())
}

async fn answer_probe(conn: &mut TcpTlsConn) -> Result<()> {
    let mut s = tokio::time::timeout(HANDSHAKE_TIMEOUT, conn.uni.get_mut().recv())
        .await
        .map_err(|_| Error::Connection("client did not complete the session".into()))?
        .ok_or_else(|| Error::Connection("client closed during setup".into()))?;
    s.write_all(&[1]).await?;
    s.shutdown().await?;
    Ok(())
}

fn rejected(e: impl std::fmt::Display) -> Error {
    Error::Connection(format!("peer rejected the session: {e}"))
}

// ---------------------------------------------------------------------------------------------
// Byte counting

#[derive(Default)]
struct ByteStats {
    tx: AtomicU64,
    rx: AtomicU64,
}

/// Counts bytes crossing the TCP socket (TLS records included).
struct Counted {
    inner: TcpStream,
    stats: Arc<ByteStats>,
}

impl Counted {
    fn new(inner: TcpStream) -> Self {
        Self {
            inner,
            stats: Arc::default(),
        }
    }
}

impl AsyncRead for Counted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);
        let n = (buf.filled().len() - before) as u64;
        self.stats.rx.fetch_add(n, Ordering::Relaxed);
        r
    }
}

impl AsyncWrite for Counted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &r {
            self.stats.tx.fetch_add(*n as u64, Ordering::Relaxed);
        }
        r
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Advances the connection's progress counter as stream data is read.
struct ProgressReader<R> {
    inner: R,
    progress: Arc<AtomicU64>,
}

impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);
        let n = (buf.filled().len() - before) as u64;
        if n > 0 {
            self.progress.fetch_add(n, Ordering::Relaxed);
        }
        r
    }
}
