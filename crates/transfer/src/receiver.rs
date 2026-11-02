//! Receiver side: decide what is missing locally, request it, verify it, reconstruct, install.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tracing::{info, warn};
use transport::{Conn, Listener, RecvStream, SendStream};
use transport::{fingerprint, read_frame, write_frame};
use xet_local::manifest::validate_rel_path;
use xet_local::{LocalCas, create_confined_dir, resolve_confined};

use crate::budget::MemoryBudget;
use crate::error::{Error, Result};
use crate::fetch::{FetchOptions, Fetcher};
use crate::locks::DestLocks;
use crate::protocol::{
    MAX_CONTROL_FRAME, Message, Operation, PROTOCOL_VERSION, ReceiverReport, TransferMode,
    decode_manifest,
};
use crate::sender::{SendStats, SendTuning, file_hashes, serve_requests};
use crate::session::SessionStore;

const CLOSE_GRACE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct ReceiverOptions {
    /// Objects received and verified concurrently (each may hold one xorb in memory).
    pub parallel_objects: usize,
    /// Durable session state for resuming interrupted transfers.
    pub session_dir: PathBuf,
    /// How long a transfer waits for another into an overlapping destination to finish. Longer
    /// than the QUIC idle timeout, so a resume can outwait a dead predecessor's connection.
    pub dest_lock_wait: Duration,
    /// Upper bound on xorb bytes held in memory at once, across all concurrent transfers
    /// (at least one xorb always fits).
    pub max_memory_bytes: u64,
    /// Per-sender destination scopes, keyed by certificate SHA-256 fingerprint. A sender may write
    /// only within one of its scopes (root-relative paths; an empty path means the whole root).
    /// When empty, every authenticated sender may write anywhere under the root.
    pub client_scopes: HashMap<String, Vec<PathBuf>>,
    /// Abort a transfer when the sender delivers no stream data for this long while the receiver
    /// is waiting on it (keep-alive pings do not count). Frees the destination lock.
    pub stall_timeout: Duration,
    /// Test hook: pause after committing each xorb, to make interruption timing deterministic.
    pub commit_delay: Option<Duration>,
    /// Where no copy-on-write clone is possible, hard-link files identical to ones already
    /// installed (and verified) under the root, like rsync --link-dest: the copies then share one
    /// file. Off by default.
    pub link_identical: bool,
}

impl ReceiverOptions {
    /// Defaults, keeping session state alongside the CAS.
    pub fn new(cas: &LocalCas) -> Self {
        Self {
            parallel_objects: 4,
            session_dir: cas.root().join("sessions"),
            dest_lock_wait: Duration::from_secs(60),
            max_memory_bytes: crate::sender::DEFAULT_MAX_MEMORY_BYTES,
            client_scopes: HashMap::new(),
            stall_timeout: Duration::from_secs(120),
            commit_delay: None,
            link_identical: false,
        }
    }
}

pub struct Receiver {
    root: PathBuf,
    fetcher: Fetcher,
    locks: Arc<DestLocks>,
    opts: ReceiverOptions,
    activity: Arc<Activity>,
}

/// Connection activity, for agents that exit when idle.
#[derive(Debug)]
pub struct Activity {
    active: std::sync::atomic::AtomicUsize,
    last: std::sync::Mutex<std::time::Instant>,
}

impl Default for Activity {
    fn default() -> Self {
        Self {
            active: Default::default(),
            last: std::sync::Mutex::new(std::time::Instant::now()),
        }
    }
}

impl Activity {
    fn begin(&self) {
        self.active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.last.lock().unwrap() = std::time::Instant::now();
    }

    fn end(&self) {
        self.active
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        *self.last.lock().unwrap() = std::time::Instant::now();
    }

    /// How long nothing has been connected, or `None` while a connection is open.
    pub fn idle_for(&self) -> Option<Duration> {
        if self.active.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            return None;
        }
        Some(self.last.lock().unwrap().elapsed())
    }
}

impl Receiver {
    /// `root` must exist; every destination is confined to it. The CAS must not overlap any
    /// destination (checked per transfer).
    pub fn new(root: &Path, cas: Arc<LocalCas>, opts: ReceiverOptions) -> Result<Self> {
        let fetcher = Fetcher {
            sessions: SessionStore::open(&opts.session_dir)?,
            cas,
            budget: MemoryBudget::new(opts.max_memory_bytes),
            opts: FetchOptions {
                parallel_objects: opts.parallel_objects.max(1),
                stall_timeout: opts.stall_timeout,
                commit_delay: opts.commit_delay,
                link_identical: opts.link_identical,
            },
        };
        Ok(Self {
            root: std::fs::canonicalize(root).map_err(xet_local::Error::from)?,
            fetcher,
            locks: Arc::default(),
            opts,
            activity: Arc::default(),
        })
    }

    /// Accepts connections until the endpoint closes. Per-connection failures are logged.
    pub async fn serve(self: Arc<Self>, listener: Arc<dyn Listener>) {
        while let Some(incoming) = listener.accept().await {
            let this = self.clone();
            tokio::spawn(async move {
                let remote = incoming.remote;
                match incoming.handshake.await {
                    Ok(conn) => {
                        this.activity.begin();
                        match this.handle(conn).await {
                            Ok(r) => info!(%remote, ?r, "transfer complete"),
                            Err(e) => warn!(%remote, "transfer failed: {e}"),
                        }
                        this.activity.end();
                    }
                    Err(e) => warn!(%remote, "handshake failed: {e}"),
                }
            });
        }
    }

    /// Connection activity across all listeners.
    pub fn activity(&self) -> Arc<Activity> {
        self.activity.clone()
    }

    /// Runs one transfer on an established, authenticated connection.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn handle(&self, conn: Conn) -> Result<ReceiverReport> {
        let (mut send, mut recv) = self
            .fetcher
            .stall(&conn, async { Ok(conn.accept_bi().await?) })
            .await?;
        let result = self.run(&conn, &mut send, &mut recv).await;
        match &result {
            Ok(report) => {
                write_frame(
                    &mut send,
                    &Message::Result {
                        report: report.clone(),
                    },
                )
                .await?;
            }
            Err(e) => {
                let _ = write_frame(
                    &mut send,
                    &Message::Error {
                        message: e.to_string(),
                    },
                )
                .await;
            }
        }
        let _ = send.shutdown().await;
        // Give the sender a moment to read the final frame and close first, then close regardless:
        // its keep-alives would otherwise hold the connection open indefinitely.
        let _ = tokio::time::timeout(CLOSE_GRACE, conn.closed()).await;
        conn.close(0, b"done");
        result
    }

    async fn run(
        &self,
        conn: &Conn,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> Result<ReceiverReport> {
        let op = match self.fetcher.stall(conn, next(recv)).await? {
            Message::Hello {
                protocol_version,
                session_id,
                op,
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    return Err(Error::Protocol(format!(
                        "unsupported protocol version {protocol_version} (receiver speaks {PROTOCOL_VERSION})"
                    )));
                }
                info!(connection = %session_id, ?op, "HELLO");
                op
            }
            other => return Err(unexpected(other)),
        };
        match op {
            Operation::Push { dest, delete, mode } => {
                self.receive_push(conn, send, recv, &dest, delete, mode)
                    .await
            }
            Operation::Pull { src, mode } => self.serve_pull(conn, send, recv, &src, mode).await,
        }
    }

    /// The client pushes a tree into `dest`.
    async fn receive_push(
        &self,
        conn: &Conn,
        send: &mut SendStream,
        recv: &mut RecvStream,
        dest: &str,
        delete: bool,
        mode: TransferMode,
    ) -> Result<ReceiverReport> {
        self.check_dest(dest)?;
        self.check_scope(conn, dest)?;
        // Held until this transfer ends; overlapping destinations wait their turn.
        let _dest_guard = self
            .locks
            .acquire(&validate_rel_path(dest)?, self.opts.dest_lock_wait)
            .await?;
        write_frame(
            send,
            &Message::HelloOk {
                protocol_version: PROTOCOL_VERSION,
            },
        )
        .await?;

        let manifest = match self.fetcher.stall(conn, next(recv)).await? {
            Message::Manifest { encoded } => decode_manifest(&encoded)?,
            other => return Err(unexpected(other)),
        };
        manifest.validate()?;

        let dest_dir = create_confined_dir(&self.root, dest)?;
        self.fetcher
            .fetch_and_install(conn, send, &manifest, dest, &dest_dir, mode, delete)
            .await
    }

    /// The client pulls the tree at `src`: this side ingests it and serves the client's requests.
    /// Returns the client's report of what it installed.
    async fn serve_pull(
        &self,
        conn: &Conn,
        send: &mut SendStream,
        recv: &mut RecvStream,
        src: &str,
        mode: TransferMode,
    ) -> Result<ReceiverReport> {
        self.check_dest(src)?;
        self.check_scope(conn, src)?;
        // Serialize with pushes into the same tree, so the client sees one consistent version.
        let _guard = self
            .locks
            .acquire(&validate_rel_path(src)?, self.opts.dest_lock_wait)
            .await?;
        let path = resolve_confined(&self.root, src)?;
        write_frame(
            send,
            &Message::HelloOk {
                protocol_version: PROTOCOL_VERSION,
            },
        )
        .await?;

        let cas = &self.fetcher.cas;
        let (manifest, _) = cas.ingest(&path).await?;
        let files = file_hashes(&manifest)?;
        let referenced = cas.referenced_xorbs(&files).await?;
        cas.touch_xorbs(&referenced);
        info!(%src, files = manifest.files().count(), ?mode, "serving pull");
        write_frame(send, &Message::manifest(&manifest)?).await?;

        let tuning = SendTuning {
            parallel_streams: self.opts.parallel_objects,
            max_memory_bytes: self.opts.max_memory_bytes,
        };
        let mut stats = SendStats::default();
        let report = self
            .fetcher
            .stall(
                conn,
                serve_requests(
                    cas,
                    conn,
                    recv,
                    &files,
                    &referenced,
                    &tuning,
                    &None,
                    &mut stats,
                ),
            )
            .await?;
        info!(%src, ?stats, "pull served");
        Ok(report)
    }

    /// Rejects destinations that escape the root or overlap the CAS or session directories.
    fn check_dest(&self, dest: &str) -> Result<()> {
        let rel = validate_rel_path(dest)?;
        let lexical = self.root.join(rel);
        for (dir, what) in [
            (
                self.fetcher.cas.root(),
                "destination overlaps the receiver's CAS directory",
            ),
            (
                self.fetcher.sessions.dir(),
                "destination overlaps the receiver's session directory",
            ),
        ] {
            if lexical.starts_with(dir) || dir.starts_with(&lexical) {
                return Err(Error::Local(xet_local::Error::UnsafePath {
                    path: dest.to_owned(),
                    reason: what,
                }));
            }
        }
        Ok(())
    }

    /// Enforces the sender's configured destination scopes, if any.
    fn check_scope(&self, conn: &Conn, dest: &str) -> Result<()> {
        if self.opts.client_scopes.is_empty() {
            return Ok(());
        }
        let cert = conn
            .peer_certificate()
            .ok_or_else(|| Error::Protocol("peer presented no certificate".into()))?;
        let fp = fingerprint(&cert);
        let scopes = self
            .opts
            .client_scopes
            .get(&fp)
            .ok_or_else(|| Error::Protocol(format!("sender {fp} has no destination scope")))?;
        let rel = validate_rel_path(dest)?;
        if scopes.iter().any(|s| rel.starts_with(s)) {
            Ok(())
        } else {
            Err(Error::Local(xet_local::Error::UnsafePath {
                path: dest.to_owned(),
                reason: "outside the destinations this sender is allowed to write",
            }))
        }
    }
}

async fn next(recv: &mut RecvStream) -> Result<Message> {
    match read_frame::<Message>(recv, MAX_CONTROL_FRAME).await? {
        Some(Message::Error { message }) => Err(Error::Remote(message)),
        Some(m) => Ok(m),
        None => Err(Error::Protocol("sender closed the control stream".into())),
    }
}

fn unexpected(m: Message) -> Error {
    Error::Protocol(format!("unexpected {} from sender", m.name()))
}
