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

/// ALPN for the TCP transport; distinct from QUIC's so the two can never be confused. Version 4
/// adds lanes (several TCP connections per session); version 3 peers get a single connection.
pub const ALPN: &[u8] = b"xet-p2p-tcp/4";
const ALPN_V3: &[u8] = b"xet-p2p-tcp/3";

const TAG_BI: u8 = b'B';
const TAG_UNI: u8 = b'U';
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const KEEPALIVE: Duration = Duration::from_secs(15);
/// TCP connections per session by default (`XET_P2P_TCP_LANES` overrides; 1 disables lanes).
/// One TCP connection's congestion window caps throughput on long or relayed paths (a Modal
/// tunnel measured 107 MiB/s on one connection, 291 on four, 399 on eight).
pub const DEFAULT_LANES: usize = 4;
/// Most lanes a server lets one session add.
const MAX_LANES: usize = 16;
/// Lane handshake: after the probe tag, `kind` (0 = new session, 1 = join) and the session id.
const LANE_NEW: u8 = 0;
const LANE_JOIN: u8 = 1;

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

fn lanes_wanted() -> usize {
    std::env::var("XET_P2P_TCP_LANES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_LANES)
        .clamp(1, MAX_LANES)
}

/// A TCP listener that accepts only clients presenting one of `allowed_clients`.
///
/// TLS and the session handshake run in the background for every TCP connection; [`accept`]
/// yields only connections that start a session. Lanes joining an existing session attach to it.
///
/// [`accept`]: Listener::accept
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
    tls.alpn_protocols = vec![ALPN.to_vec(), ALPN_V3.to_vec()];
    let tcp = TcpListener::bind(listen).await?;
    let local = tcp.local_addr()?;
    let (tx, rx) = mpsc::unbounded_channel();
    let acceptor = tokio::spawn(accept_loop(tcp, TlsAcceptor::from(Arc::new(tls)), tx));
    Ok(TcpTlsListener {
        local,
        sessions: Mutex::new(rx),
        acceptor,
    })
}

/// Connects to `addr`, authenticating with `identity` and accepting only `pinned_servers`. Extra
/// lanes (see [`DEFAULT_LANES`]) are added in the background once the server has confirmed the
/// session; streams use whichever lanes are up.
pub async fn tcp_connect(
    addr: SocketAddr,
    identity: Identity,
    pinned_servers: Vec<CertificateDer<'static>>,
) -> Result<Conn> {
    tcp_connect_lanes(addr, identity, pinned_servers, lanes_wanted()).await
}

/// [`tcp_connect`] with an explicit number of lanes (1 = a single TCP connection).
pub async fn tcp_connect_lanes(
    addr: SocketAddr,
    identity: Identity,
    pinned_servers: Vec<CertificateDer<'static>>,
    lanes: usize,
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
    tls.alpn_protocols = vec![ALPN.to_vec(), ALPN_V3.to_vec()];
    let tls = Arc::new(tls);

    let (stream, peer, wire, v4) = connect_lane(addr, &tls).await?;
    let conn = Arc::new(TcpTlsConn::new(addr, peer));
    conn.add_lane(start_lane(
        stream,
        yamux::Mode::Client,
        wire,
        &conn.inbound,
        None,
    ));
    let session: [u8; 16] = rand::random();
    let mut probe = conn.open(TAG_UNI).await.map_err(rejected)?;
    if v4 {
        probe.write_all(&[LANE_NEW]).await?;
        probe.write_all(&session).await?;
        probe.flush().await?;
    }
    let (tx, rx) = watch::channel(None);
    let weak = Arc::downgrade(&conn);
    tokio::spawn(async move {
        let answer = read_answer(probe).await;
        let confirmed = answer.is_ok();
        let _ = tx.send(Some(answer));
        if confirmed && v4 && lanes > 1 {
            add_lanes(weak, addr, tls, session, lanes - 1).await;
        }
    });
    *conn.confirmed.lock().unwrap() = rx;
    Ok(conn)
}

/// TCP connect + TLS handshake for one lane. Returns the stream, the server's certificate, the
/// lane's byte counters, and whether the server speaks the lane protocol.
async fn connect_lane(
    addr: SocketAddr,
    tls: &Arc<rustls::ClientConfig>,
) -> Result<(
    tokio_rustls::client::TlsStream<Counted>,
    Option<CertificateDer<'static>>,
    Arc<ByteStats>,
    bool,
)> {
    let t = std::time::Instant::now();
    let stream = TcpStream::connect(addr).await?;
    tracing::debug!(?addr, ms = t.elapsed().as_millis() as u64, "tcp connected");
    tune(&stream)?;
    let counted = Counted::new(stream);
    let wire = counted.stats.clone();
    let name = ServerName::try_from(SERVER_NAME)
        .map_err(|e| Error::Config(e.to_string()))?
        .to_owned();
    let stream = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        TlsConnector::from(tls.clone()).connect(name, counted),
    )
    .await
    .map_err(|_| Error::Connection("TLS handshake timed out".into()))?
    .map_err(|e| Error::Connection(e.to_string()))?;
    let session = stream.get_ref().1;
    let peer = session
        .peer_certificates()
        .and_then(|c| c.first().cloned())
        .map(|c| c.into_owned());
    let v4 = session.alpn_protocol() == Some(ALPN);
    tracing::debug!(ms = t.elapsed().as_millis() as u64, "tls handshake done");
    Ok((stream, peer, wire, v4))
}

/// Opens `n` more lanes into the session, in parallel; any that fail are simply not used.
async fn add_lanes(
    conn: std::sync::Weak<TcpTlsConn>,
    addr: SocketAddr,
    tls: Arc<rustls::ClientConfig>,
    session: [u8; 16],
    n: usize,
) {
    let joins = (0..n).map(|_| {
        let (conn, tls) = (conn.clone(), tls.clone());
        async move {
            let (stream, peer, wire, _) = connect_lane(addr, &tls).await?;
            let Some(owner) = conn.upgrade() else {
                return Ok(());
            };
            if peer != owner.peer {
                return Err(Error::Connection("lane reached a different server".into()));
            }
            let lane = start_lane(stream, yamux::Mode::Client, wire, &owner.inbound, None);
            let mut probe = lane_open(&lane.cmds, TAG_UNI).await?;
            probe.write_all(&[LANE_JOIN]).await?;
            probe.write_all(&session).await?;
            probe.flush().await?;
            // The lane only carries streams once the server has attached it to the session.
            read_answer(probe).await.map_err(Error::Connection)?;
            owner.add_lane(lane);
            Ok::<_, Error>(())
        }
    });
    for r in futures::future::join_all(joins).await {
        if let Err(e) = r {
            tracing::debug!("extra lane not added: {e}");
        }
    }
}

async fn read_answer(mut probe: YStream) -> std::result::Result<(), String> {
    let mut ok = [0u8; 1];
    let answer = match tokio::time::timeout(HANDSHAKE_TIMEOUT, probe.read_exact(&mut ok)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("peer rejected the session: {e}")),
        Err(_) => Err("server did not confirm the session".into()),
    };
    let _ = probe.shutdown().await;
    answer
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
    local: SocketAddr,
    sessions: Mutex<mpsc::UnboundedReceiver<Incoming>>,
    acceptor: tokio::task::JoinHandle<()>,
}

impl Drop for TcpTlsListener {
    fn drop(&mut self) {
        self.acceptor.abort();
    }
}

type Sessions =
    Arc<std::sync::Mutex<std::collections::HashMap<[u8; 16], std::sync::Weak<TcpTlsConn>>>>;

async fn accept_loop(
    tcp: TcpListener,
    acceptor: TlsAcceptor,
    out: mpsc::UnboundedSender<Incoming>,
) {
    let sessions: Sessions = Arc::default();
    loop {
        let (stream, remote) = match tcp.accept().await {
            Ok(s) => s,
            Err(_) => continue, // transient accept errors (e.g. EMFILE) must not stop serving
        };
        let (acceptor, sessions, out) = (acceptor.clone(), sessions.clone(), out.clone());
        tokio::spawn(async move {
            match server_handshake(stream, remote, acceptor, &sessions).await {
                Ok(Some(conn)) => {
                    let _ = out.send(Incoming {
                        remote,
                        handshake: Box::pin(async move { Ok(conn as Conn) }),
                    });
                }
                Ok(None) => {} // a lane joined an existing session
                Err(e) => tracing::warn!(%remote, "handshake failed: {e}"),
            }
        });
    }
}

/// TLS, then the client's probe: a new session (returned) or a lane for an existing one.
async fn server_handshake(
    stream: TcpStream,
    remote: SocketAddr,
    acceptor: TlsAcceptor,
    sessions: &Sessions,
) -> Result<Option<Arc<TcpTlsConn>>> {
    tune(&stream)?;
    let counted = Counted::new(stream);
    let wire = counted.stats.clone();
    let tls = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(counted))
        .await
        .map_err(|_| Error::Connection("TLS handshake timed out".into()))?
        .map_err(|e| Error::Connection(e.to_string()))?;
    let session = tls.get_ref().1;
    let peer = session
        .peer_certificates()
        .and_then(|c| c.first().cloned())
        .map(|c| c.into_owned());
    let v4 = session.alpn_protocol() == Some(ALPN);

    // The client's first stream is the probe; it is not routed with the session's streams.
    let (probe_tx, probe_rx) = oneshot::channel();
    let fresh = TcpTlsConn::new(remote, peer.clone());
    let lane = start_lane(
        tls,
        yamux::Mode::Server,
        wire,
        &fresh.inbound,
        Some(probe_tx),
    );
    let mut probe = tokio::time::timeout(HANDSHAKE_TIMEOUT, probe_rx)
        .await
        .map_err(|_| Error::Connection("client did not complete the session".into()))?
        .map_err(|_| Error::Connection("client closed during setup".into()))?;
    let (kind, id) = if v4 {
        let mut head = [0u8; 17];
        tokio::time::timeout(HANDSHAKE_TIMEOUT, probe.read_exact(&mut head))
            .await
            .map_err(|_| Error::Connection("client did not complete the session".into()))??;
        (head[0], <[u8; 16]>::try_from(&head[1..]).unwrap())
    } else {
        (LANE_NEW, rand::random())
    };
    match kind {
        LANE_NEW => {
            let conn = Arc::new(fresh);
            conn.add_lane(lane);
            {
                let mut map = sessions.lock().unwrap();
                map.retain(|_, w| w.strong_count() > 0);
                map.insert(id, Arc::downgrade(&conn));
            }
            probe.write_all(&[1]).await?;
            probe.shutdown().await?;
            Ok(Some(conn))
        }
        LANE_JOIN => {
            let owner = sessions.lock().unwrap().get(&id).and_then(|w| w.upgrade());
            let owner =
                owner.ok_or_else(|| Error::Connection("lane for an unknown session".into()))?;
            if owner.peer != peer {
                return Err(Error::Connection("lane from a different client".into()));
            }
            if owner.lane_count() >= MAX_LANES {
                return Err(Error::Connection("too many lanes".into()));
            }
            // Route the lane's streams into the session it joins.
            let lane = lane.reroute(&owner.inbound);
            owner.add_lane(lane);
            probe.write_all(&[1]).await?;
            probe.shutdown().await?;
            Ok(None)
        }
        _ => Err(Error::Connection("unknown lane kind".into())),
    }
}

#[async_trait::async_trait]
impl Listener for TcpTlsListener {
    async fn accept(&self) -> Option<Incoming> {
        self.sessions.lock().await.recv().await
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.local)
    }

    fn kind(&self) -> TransportKind {
        TransportKind::TlsTcp
    }
}

// ---------------------------------------------------------------------------------------------
// Lanes and the session

type YStream = tokio_util::compat::Compat<yamux::Stream>;

enum Cmd {
    Open(oneshot::Sender<std::result::Result<yamux::Stream, yamux::ConnectionError>>),
    Close,
}

/// Where a lane delivers the streams its peer opens.
#[derive(Clone)]
struct Inbound {
    bi: mpsc::UnboundedSender<YStream>,
    uni: mpsc::UnboundedSender<YStream>,
}

/// One TLS-over-TCP connection with its yamux multiplexer.
struct Lane {
    cmds: mpsc::UnboundedSender<Cmd>,
    closed: watch::Receiver<bool>,
    wire: Arc<ByteStats>,
    /// Where `route` sends this lane's inbound streams; replaceable until the lane is in use.
    target: Arc<std::sync::Mutex<Inbound>>,
}

impl Lane {
    fn reroute(self, to: &Inbound) -> Self {
        *self.target.lock().unwrap() = to.clone();
        self
    }
}

/// A session: one or more lanes between the same two peers. Streams are opened round-robin over
/// the live lanes; inbound streams from every lane are merged.
pub struct TcpTlsConn {
    lanes: std::sync::Mutex<Vec<Lane>>,
    next: std::sync::atomic::AtomicUsize,
    inbound: Inbound,
    bi: Mutex<mpsc::UnboundedReceiver<YStream>>,
    uni: Mutex<mpsc::UnboundedReceiver<YStream>>,
    remote: SocketAddr,
    peer: Option<CertificateDer<'static>>,
    progress: Arc<AtomicU64>,
    /// The server's answer to the client's probe: `None` until it arrives (always `Ok` on the
    /// server side).
    confirmed: std::sync::Mutex<watch::Receiver<Option<std::result::Result<(), String>>>>,
}

/// Starts the driver for one lane. A server lane hands its first inbound stream to `probe`.
fn start_lane<S>(
    tls: S,
    mode: yamux::Mode,
    wire: Arc<ByteStats>,
    to: &Inbound,
    probe: Option<oneshot::Sender<YStream>>,
) -> Lane
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    let (closed_tx, closed_rx) = watch::channel(false);
    let target = Arc::new(std::sync::Mutex::new(to.clone()));

    let conn = yamux::Connection::new(tls.compat(), yamux_config(), mode);
    tokio::spawn(async move {
        drive(conn, cmd_rx, in_tx).await;
        let _ = closed_tx.send(true);
    });
    tokio::spawn(route(in_rx, target.clone(), probe));
    Lane {
        cmds: cmd_tx,
        closed: closed_rx,
        wire,
        target,
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

/// Routes each inbound stream by its tag, without letting one slow opener block the others. The
/// first stream of a server lane is its probe (yamux delivers streams in opening order).
async fn route(
    mut inbound: mpsc::UnboundedReceiver<yamux::Stream>,
    target: Arc<std::sync::Mutex<Inbound>>,
    mut probe: Option<oneshot::Sender<YStream>>,
) {
    while let Some(s) = inbound.recv().await {
        let mut s = s.compat();
        if let Some(p) = probe.take() {
            let mut tag = [0u8; 1];
            if let Ok(Ok(_)) = tokio::time::timeout(HANDSHAKE_TIMEOUT, s.read_exact(&mut tag)).await
                && tag[0] == TAG_UNI
            {
                let _ = p.send(s);
            }
            continue;
        }
        let target = target.clone();
        tokio::spawn(async move {
            let mut tag = [0u8; 1];
            if tokio::time::timeout(HANDSHAKE_TIMEOUT, s.read_exact(&mut tag))
                .await
                .is_ok_and(|r| r.is_ok())
            {
                let to = target.lock().unwrap().clone();
                let _ = match tag[0] {
                    TAG_BI => to.bi.send(s),
                    TAG_UNI => to.uni.send(s),
                    _ => Ok(()), // unknown stream type: drop it
                };
            }
        });
    }
}

async fn lane_open(cmds: &mpsc::UnboundedSender<Cmd>, tag: u8) -> Result<YStream> {
    let (tx, rx) = oneshot::channel();
    cmds.send(Cmd::Open(tx))
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

impl TcpTlsConn {
    fn new(remote: SocketAddr, peer: Option<CertificateDer<'static>>) -> Self {
        let (bi_tx, bi_rx) = mpsc::unbounded_channel();
        let (uni_tx, uni_rx) = mpsc::unbounded_channel();
        Self {
            lanes: Default::default(),
            next: Default::default(),
            inbound: Inbound {
                bi: bi_tx,
                uni: uni_tx,
            },
            bi: Mutex::new(bi_rx),
            uni: Mutex::new(uni_rx),
            remote,
            peer,
            progress: Arc::new(AtomicU64::new(0)),
            confirmed: std::sync::Mutex::new(watch::channel(Some(Ok(()))).1),
        }
    }

    fn add_lane(&self, lane: Lane) {
        self.lanes.lock().unwrap().push(lane);
    }

    fn lane_count(&self) -> usize {
        self.lanes.lock().unwrap().len()
    }

    /// Opens a stream on the next live lane (the first lane carries the session: when it closes,
    /// the session is closed).
    async fn open(&self, tag: u8) -> Result<YStream> {
        let cmds = {
            let lanes = self.lanes.lock().unwrap();
            let live: Vec<_> = lanes.iter().filter(|l| !*l.closed.borrow()).collect();
            if live.is_empty() || *lanes[0].closed.borrow() {
                return Err(Error::Connection("connection closed".into()));
            }
            let i = self.next.fetch_add(1, Ordering::Relaxed) % live.len();
            live[i].cmds.clone()
        };
        lane_open(&cmds, tag).await
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
    async fn rejection(&self) -> Option<String> {
        let mut answer = self.confirmed.lock().unwrap().clone();
        let _ =
            tokio::time::timeout(Duration::from_secs(2), answer.wait_for(Option::is_some)).await;
        let why = answer.borrow().clone();
        why.and_then(|r| r.err())
    }

    async fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        let (r, w) = tokio::io::split(self.open(TAG_BI).await?);
        Ok((Box::new(w), self.counted(r)))
    }

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream)> {
        let s = tokio::select! {
            s = async { self.bi.lock().await.recv().await } => s,
            _ = self.closed() => None,
        }
        .ok_or_else(|| Error::Connection("connection closed".into()))?;
        let (r, w) = tokio::io::split(s);
        Ok((Box::new(w), self.counted(r)))
    }

    async fn open_uni(&self) -> Result<SendStream> {
        Ok(Box::new(self.open(TAG_UNI).await?))
    }

    async fn accept_uni(&self) -> Result<RecvStream> {
        let s = tokio::select! {
            s = async { self.uni.lock().await.recv().await } => s,
            _ = self.closed() => None,
        }
        .ok_or_else(|| Error::Connection("connection closed".into()))?;
        let (r, _w) = tokio::io::split(s);
        Ok(self.counted(r))
    }

    fn close(&self, _code: u32, _reason: &[u8]) {
        for lane in self.lanes.lock().unwrap().iter() {
            let _ = lane.cmds.send(Cmd::Close);
        }
    }

    async fn closed(&self) {
        let first = self.lanes.lock().unwrap().first().map(|l| l.closed.clone());
        if let Some(mut rx) = first {
            let _ = rx.wait_for(|c| *c).await;
        }
        self.close(0, b"");
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
        let lanes = self.lanes.lock().unwrap();
        WireStats {
            tx: lanes
                .iter()
                .map(|l| l.wire.tx.load(Ordering::Relaxed))
                .sum(),
            rx: lanes
                .iter()
                .map(|l| l.wire.rx.load(Ordering::Relaxed))
                .sum(),
        }
    }
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
