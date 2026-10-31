//! Receiver side: decide what is missing locally, request it, verify it, reconstruct, install.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info, warn};
use transport_quic::quinn::{Connection, Endpoint, RecvStream, SendStream};
use transport_quic::{fingerprint, peer_certificate, read_frame, read_payload, write_frame};
use xet_local::chunks::{VerifiedChunk, XorbPacker, decode_chunk_lists, parse_chunk_run};
use xet_local::manifest::validate_rel_path;
use xet_local::{
    LocalCas, Manifest, MerkleHash, MissingContent, PendingShard, ReconstructOptions,
    create_confined_dir,
};

use crate::budget::MemoryBudget;
use crate::error::{Error, Result};
use crate::locks::DestLocks;
use crate::protocol::{
    ChunkRequest, MAX_CHUNK_LISTS_BYTES, MAX_CHUNK_RUN_BYTES, MAX_CONTROL_FRAME, MAX_OBJECT_HEADER,
    MAX_SHARD_BYTES, MAX_XORB_BYTES, Message, ObjectHeader, ObjectKind, PROTOCOL_VERSION,
    ReceiverReport, TransferMode, parse_hash, to_hex,
};
use crate::session::{Session, SessionStore};

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
        }
    }
}

pub struct Receiver {
    root: PathBuf,
    cas: Arc<LocalCas>,
    sessions: SessionStore,
    locks: Arc<DestLocks>,
    budget: MemoryBudget,
    opts: ReceiverOptions,
}

impl Receiver {
    /// `root` must exist; every destination is confined to it. The CAS must not overlap any
    /// destination (checked per transfer).
    pub fn new(root: &Path, cas: Arc<LocalCas>, opts: ReceiverOptions) -> Result<Self> {
        Ok(Self {
            root: std::fs::canonicalize(root).map_err(xet_local::Error::from)?,
            sessions: SessionStore::open(&opts.session_dir)?,
            cas,
            locks: Arc::default(),
            budget: MemoryBudget::new(opts.max_memory_bytes),
            opts: ReceiverOptions {
                parallel_objects: opts.parallel_objects.max(1),
                ..opts
            },
        })
    }

    /// Accepts connections until the endpoint closes. Per-connection failures are logged.
    pub async fn serve(self: Arc<Self>, endpoint: Endpoint) {
        while let Some(incoming) = endpoint.accept().await {
            let this = self.clone();
            tokio::spawn(async move {
                let remote = incoming.remote_address();
                match incoming.await {
                    Ok(conn) => match this.handle(conn).await {
                        Ok(r) => info!(%remote, ?r, "transfer complete"),
                        Err(e) => warn!(%remote, "transfer failed: {e}"),
                    },
                    Err(e) => warn!(%remote, "handshake failed: {e}"),
                }
            });
        }
    }

    /// Runs one transfer on an established, authenticated connection.
    pub async fn handle(&self, conn: Connection) -> Result<ReceiverReport> {
        let (mut send, mut recv) = self
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
        let _ = send.finish();
        // Give the sender a moment to read the final frame and close first, then close regardless:
        // its keep-alives would otherwise hold the connection open indefinitely.
        let _ = tokio::time::timeout(CLOSE_GRACE, conn.closed()).await;
        conn.close(0u32.into(), b"done");
        result
    }

    async fn run(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> Result<ReceiverReport> {
        let (dest, delete, mode) = match self.stall(conn, next(recv)).await? {
            Message::Hello {
                protocol_version,
                session_id,
                dest,
                delete,
                mode,
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    return Err(Error::Protocol(format!(
                        "unsupported protocol version {protocol_version} (receiver speaks {PROTOCOL_VERSION})"
                    )));
                }
                info!(connection = %session_id, %dest, delete, ?mode, "HELLO");
                self.check_dest(&dest)?;
                self.check_scope(conn, &dest)?;
                (dest, delete, mode)
            }
            other => return Err(unexpected(other)),
        };
        // Held until this transfer ends; overlapping destinations wait their turn.
        let _dest_guard = self
            .locks
            .acquire(&validate_rel_path(&dest)?, self.opts.dest_lock_wait)
            .await?;
        write_frame(
            send,
            &Message::HelloOk {
                protocol_version: PROTOCOL_VERSION,
            },
        )
        .await?;

        let manifest = match self.stall(conn, next(recv)).await? {
            Message::Manifest { manifest } => manifest,
            other => return Err(unexpected(other)),
        };
        manifest.validate()?;

        let mut session = self.sessions.begin(&dest, &manifest)?;
        let mut report = ReceiverReport {
            resumed: session.is_resumed(),
            attempts: session.record.attempts,
            prior_xorbs_committed: session.prior.xorbs_committed,
            prior_xorb_bytes_committed: session.prior.xorb_bytes_committed,
            ..Default::default()
        };
        if report.resumed {
            info!(%dest, attempt = report.attempts, prior_xorbs = report.prior_xorbs_committed, "resuming session");
        }

        let missing = self.cas.missing_content(&manifest).await?;
        report.mode = mode;
        match mode {
            TransferMode::Xorb => {
                self.fetch_whole_xorbs(conn, send, &missing, &mut session, &mut report)
                    .await?
            }
            TransferMode::Chunk => {
                self.fetch_chunks(conn, send, &manifest, &missing, &mut session, &mut report)
                    .await?
            }
        }

        let dest_dir = create_confined_dir(&self.root, &dest)?;
        let r = self
            .cas
            .reconstruct_with(
                &manifest,
                &dest_dir,
                ReconstructOptions {
                    delete_extraneous: delete,
                },
            )
            .await?;
        report.files_deleted = r.files_deleted;
        report.dirs_deleted = r.dirs_deleted;
        report.files_installed = r.files;
        report.files_unchanged = r.files_unchanged;
        report.dirs_created = r.dirs;
        report.bytes_reconstructed = r.bytes;
        session.finish()?;
        Ok(report)
    }

    /// Whole-xorb mode: the sender's native shard entries and xorbs, as stored.
    async fn fetch_whole_xorbs(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        missing: &MissingContent,
        session: &mut Session<'_>,
        report: &mut ReceiverReport,
    ) -> Result<()> {
        let mut xorbs = missing.xorbs.clone();
        let mut present = missing.present_xorbs.clone();

        // Round 1: reconstruction metadata for files this CAS has never seen. Entries received by
        // an earlier, interrupted attempt (any session) are reused; only the rest are requested.
        let mut pending = None;
        if !missing.reconstructions.is_empty() {
            let (saved, have) = self.sessions.take_pending(&missing.reconstructions);
            report.reconstructions_reused = have.len();
            let need: BTreeSet<MerkleHash> =
                missing.reconstructions.difference(&have).copied().collect();
            let mut parts = vec![saved];
            if !need.is_empty() {
                write_frame(
                    send,
                    &Message::Need {
                        reconstructions: to_hex(need.iter().copied()),
                        xorbs: vec![],
                    },
                )
                .await?;
                let shard = self
                    .receive_single(conn, ObjectKind::Shard, MAX_SHARD_BYTES)
                    .await?;
                report.shards_received += 1;
                report.payload_bytes_received += shard.len() as u64;
                let p = PendingShard::parse(&shard, &need)?;
                self.sessions.put_pending(&p)?;
                write_frame(
                    send,
                    &Message::Ack {
                        kind: ObjectKind::Shard,
                        hash: None,
                    },
                )
                .await?;
                parts.push(p);
            }
            let p = PendingShard::merge(parts);
            let referenced = p.referenced_xorbs();
            let absent = self.cas.absent_xorbs(&referenced).await?;
            present.extend(referenced.difference(&absent));
            xorbs.extend(absent);
            pending = Some(p);
        }
        report.xorbs_already_present = present.len();

        // Round 2: whole xorbs not present locally.
        if !xorbs.is_empty() {
            write_frame(
                send,
                &Message::Need {
                    reconstructions: vec![],
                    xorbs: to_hex(xorbs.iter().copied()),
                },
            )
            .await?;
            report.payload_bytes_received += self
                .receive_xorbs(conn, send, xorbs.clone(), session)
                .await?;
            report.xorbs_received += xorbs.len();
        }

        // Only now, with every xorb verified locally, are the new file entries checked and kept.
        if let Some(p) = pending {
            let files = p.file_hashes();
            self.cas.commit_shard(p).await?;
            self.sessions.remove_pending(&files);
        }

        Ok(())
    }

    /// Chunk mode: fetch only chunks this CAS does not hold (in any xorb), repack them into native
    /// xorbs, and register fresh reconstructions for the new files.
    async fn fetch_chunks(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        manifest: &Manifest,
        missing: &MissingContent,
        session: &mut Session<'_>,
        report: &mut ReceiverReport,
    ) -> Result<()> {
        report.xorbs_already_present = missing.present_xorbs.len();
        // Files to describe: never seen, or whose stored reconstruction lost a xorb.
        let wanted: BTreeSet<MerkleHash> = missing
            .reconstructions
            .union(&missing.files_missing_xorbs)
            .copied()
            .collect();
        if wanted.is_empty() {
            return Ok(());
        }

        let (mut lists, have) = self.sessions.take_chunk_lists(&wanted);
        report.chunk_lists_reused = have.len();
        let need: BTreeSet<MerkleHash> = wanted.difference(&have).copied().collect();
        if !need.is_empty() {
            write_frame(
                send,
                &Message::NeedChunkLists {
                    files: to_hex(need.iter().copied()),
                },
            )
            .await?;
            let bytes = self
                .receive_single(conn, ObjectKind::ChunkLists, MAX_CHUNK_LISTS_BYTES)
                .await?;
            report.chunk_list_bytes_received += bytes.len() as u64;
            report.payload_bytes_received += bytes.len() as u64;
            let got = tokio::task::spawn_blocking(move || decode_chunk_lists(&bytes, &need))
                .await
                .map_err(|e| Error::Protocol(format!("decode task failed: {e}")))??;
            self.sessions.put_chunk_lists(&got)?;
            write_frame(
                send,
                &Message::Ack {
                    kind: ObjectKind::ChunkLists,
                    hash: None,
                },
            )
            .await?;
            lists.extend(got);
        }

        let plan = self.cas.plan_chunks(&lists)?;
        report.chunks_total = plan.total_chunks;
        report.chunks_present = plan.present_chunks;
        report.chunk_bytes_present = plan.present_bytes;
        if !plan.missing.is_empty() {
            let requests = plan
                .requests
                .iter()
                .map(|(file, ranges)| ChunkRequest {
                    file: file.hex(),
                    ranges: ranges.clone(),
                })
                .collect();
            write_frame(send, &Message::NeedChunks { requests }).await?;
            self.receive_chunk_runs(conn, send, plan.missing.clone(), session, report)
                .await?;
        }

        let sha: HashMap<MerkleHash, String> = manifest
            .files()
            .filter_map(|(_, _, h, sha, _)| Some((MerkleHash::from_hex(h).ok()?, sha.to_owned())))
            .collect();
        let files: Vec<_> = lists
            .into_iter()
            .map(|(f, l)| {
                let sha = sha.get(&f).cloned().ok_or_else(|| {
                    Error::Protocol(format!("chunk list for file {} not in manifest", f.hex()))
                })?;
                Ok((f, l, sha))
            })
            .collect::<Result<_>>()?;
        self.cas.register_files(&files).await?;
        self.sessions.remove_chunk_lists(&wanted);
        Ok(())
    }

    /// Receives chunk runs until every chunk in `outstanding` has arrived and verified. Each
    /// chunk is hashed; unrequested or repeated chunks abort the transfer. Verified native chunk
    /// bytes are packed into new xorbs, committed as each fills.
    async fn receive_chunk_runs_unguarded(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        mut outstanding: HashMap<MerkleHash, u64>,
        session: &mut Session<'_>,
        report: &mut ReceiverReport,
    ) -> Result<()> {
        type Received = (
            Vec<u8>,
            Vec<VerifiedChunk>,
            tokio::sync::OwnedSemaphorePermit,
        );
        let permits = Arc::new(Semaphore::new(self.opts.parallel_objects));
        let mut tasks: JoinSet<Result<Received>> = JoinSet::new();
        let mut packer = XorbPacker::default();

        while !outstanding.is_empty() || !tasks.is_empty() {
            tokio::select! {
                stream = conn.accept_uni(), if !outstanding.is_empty() => {
                    let mut stream = stream?;
                    let (permits, budget) = (permits.clone(), self.budget.clone());
                    tasks.spawn(async move {
                        let _permit = permits.acquire_owned().await.expect("semaphore never closed");
                        let header = read_header(&mut stream).await?;
                        if header.kind != ObjectKind::ChunkRun {
                            return Err(Error::Protocol("expected a chunk run".into()));
                        }
                        if header.len > MAX_CHUNK_RUN_BYTES {
                            return Err(transport_quic::Error::FrameTooLarge { len: header.len, max: MAX_CHUNK_RUN_BYTES }.into());
                        }
                        let memory = budget.reserve(header.len).await;
                        let bytes = read_payload(&mut stream, header.len, MAX_CHUNK_RUN_BYTES).await?;
                        let (bytes, chunks) = tokio::task::spawn_blocking(move || {
                            let chunks = parse_chunk_run(&bytes);
                            (bytes, chunks)
                        })
                        .await
                        .map_err(|e| Error::Protocol(format!("verify task failed: {e}")))?;
                        Ok((bytes, chunks?, memory))
                    });
                }
                Some(done) = tasks.join_next(), if !tasks.is_empty() => {
                    let (bytes, chunks, _memory) =
                        done.map_err(|e| Error::Protocol(format!("receive task failed: {e}")))??;
                    for c in chunks {
                        if outstanding.remove(&c.hash).is_none() {
                            return Err(Error::Protocol(format!("unrequested or duplicate chunk {}", c.hash.hex())));
                        }
                        packer.push(&bytes[c.span.clone()], c.size);
                        report.chunks_received += 1;
                        if packer.is_full() {
                            self.flush_pack(&mut packer, session, report).await?;
                        }
                    }
                    report.chunk_bytes_received += bytes.len() as u64;
                    report.payload_bytes_received += bytes.len() as u64;
                    write_frame(send, &Message::Ack { kind: ObjectKind::ChunkRun, hash: None }).await?;
                }
            }
        }
        if !packer.is_empty() {
            self.flush_pack(&mut packer, session, report).await?;
        }
        Ok(())
    }

    async fn receive_chunk_runs(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        outstanding: HashMap<MerkleHash, u64>,
        session: &mut Session<'_>,
        report: &mut ReceiverReport,
    ) -> Result<()> {
        self.stall(
            conn,
            self.receive_chunk_runs_unguarded(conn, send, outstanding, session, report),
        )
        .await
    }

    async fn receive_xorbs(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        expected: BTreeSet<MerkleHash>,
        session: &mut Session<'_>,
    ) -> Result<u64> {
        self.stall(
            conn,
            self.receive_xorbs_unguarded(conn, send, expected, session),
        )
        .await
    }

    /// Runs `fut` (which waits on the sender) but fails if the connection receives no stream data
    /// for `stall_timeout`. Local work inside `fut` does not stop the sender's data from arriving,
    /// so only a sender that stops sending trips this.
    async fn stall<T>(
        &self,
        conn: &Connection,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let timeout = self.opts.stall_timeout;
        let tick = (timeout / 4).clamp(Duration::from_millis(10), Duration::from_secs(1));
        let mut interval = tokio::time::interval(tick);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = conn.stats().frame_rx.stream;
        let mut since = tokio::time::Instant::now();
        tokio::pin!(fut);
        loop {
            tokio::select! {
                r = &mut fut => return r,
                _ = interval.tick() => {
                    let now = conn.stats().frame_rx.stream;
                    if now != last {
                        last = now;
                        since = tokio::time::Instant::now();
                    } else if since.elapsed() >= timeout {
                        return Err(Error::Stalled(timeout));
                    }
                }
            }
        }
    }

    async fn flush_pack(
        &self,
        packer: &mut XorbPacker,
        session: &mut Session<'_>,
        report: &mut ReceiverReport,
    ) -> Result<()> {
        let body = packer.take();
        let len = body.len() as u64;
        self.cas.commit_packed(body).await?;
        report.xorbs_packed += 1;
        session.xorb_committed(len)?;
        if let Some(d) = self.opts.commit_delay {
            tokio::time::sleep(d).await;
        }
        Ok(())
    }

    /// Rejects destinations that escape the root or overlap the CAS or session directories.
    fn check_dest(&self, dest: &str) -> Result<()> {
        let rel = validate_rel_path(dest)?;
        let lexical = self.root.join(rel);
        for (dir, what) in [
            (
                self.cas.root(),
                "destination overlaps the receiver's CAS directory",
            ),
            (
                self.sessions.dir(),
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
    fn check_scope(&self, conn: &Connection, dest: &str) -> Result<()> {
        if self.opts.client_scopes.is_empty() {
            return Ok(());
        }
        let cert = peer_certificate(conn)
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

    /// Receives one object of `kind` (without a hash) of at most `max` bytes.
    async fn receive_single(
        &self,
        conn: &Connection,
        kind: ObjectKind,
        max: u64,
    ) -> Result<Vec<u8>> {
        self.stall(conn, async {
            let mut stream = conn.accept_uni().await?;
            let header = read_header(&mut stream).await?;
            if header.kind != kind || header.hash.is_some() {
                return Err(Error::Protocol(format!("expected a {kind:?} object")));
            }
            Ok(read_payload(&mut stream, header.len, max).await?)
        })
        .await
    }

    /// Receives exactly the requested xorbs, verifying and committing each, and ACKs each one.
    async fn receive_xorbs_unguarded(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        expected: BTreeSet<MerkleHash>,
        session: &mut Session<'_>,
    ) -> Result<u64> {
        let mut remaining = expected.len();
        let expected = Arc::new(Mutex::new(expected));
        let permits = Arc::new(Semaphore::new(self.opts.parallel_objects));
        let delay = self.opts.commit_delay;
        let mut tasks: JoinSet<Result<(MerkleHash, u64)>> = JoinSet::new();
        let mut total = 0;

        while remaining > 0 || !tasks.is_empty() {
            tokio::select! {
                // Accepting is its own branch so ACKs keep flowing while we wait for the next
                // stream: a sender may legitimately wait for an ACK before sending more. Quinn's
                // stream limit bounds accepted streams; the permit bounds objects held in memory.
                stream = conn.accept_uni(), if remaining > 0 => {
                    let mut stream = stream?;
                    remaining -= 1;
                    let (cas, expected, permits) = (self.cas.clone(), expected.clone(), permits.clone());
                    let budget = self.budget.clone();
                    tasks.spawn(async move {
                        let _permit = permits.acquire_owned().await.expect("semaphore never closed");
                        let header = read_header(&mut stream).await?;
                        let hash = match (&header.kind, &header.hash) {
                            (ObjectKind::Xorb, Some(h)) => parse_hash(h)?,
                            _ => return Err(Error::Protocol("expected a xorb object".into())),
                        };
                        if !expected.lock().unwrap().remove(&hash) {
                            return Err(Error::Protocol(format!("unrequested or duplicate xorb {}", hash.hex())));
                        }
                        if header.len > MAX_XORB_BYTES {
                            return Err(transport_quic::Error::FrameTooLarge { len: header.len, max: MAX_XORB_BYTES }.into());
                        }
                        let _memory = budget.reserve(header.len).await;
                        let bytes = read_payload(&mut stream, header.len, MAX_XORB_BYTES).await?;
                        let len = bytes.len() as u64;
                        cas.commit_xorb(hash, bytes).await?;
                        if let Some(d) = delay {
                            tokio::time::sleep(d).await;
                        }
                        Ok((hash, len))
                    });
                }
                Some(done) = tasks.join_next(), if !tasks.is_empty() => {
                    let (hash, len) = done.map_err(|e| Error::Protocol(format!("receive task failed: {e}")))??;
                    total += len;
                    session.xorb_committed(len)?;
                    write_frame(send, &Message::Ack { kind: ObjectKind::Xorb, hash: Some(hash.hex()) }).await?;
                }
            }
        }
        Ok(total)
    }
}

async fn read_header(stream: &mut RecvStream) -> Result<ObjectHeader> {
    read_frame::<ObjectHeader>(stream, MAX_OBJECT_HEADER)
        .await?
        .ok_or_else(|| Error::Protocol("empty object stream".into()))
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
