//! Fetching side of a transfer: decide what is missing locally, request it, verify it, repack,
//! reconstruct, and install. Used by the receiver of a push and by the client of a pull.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{Instrument, debug, info};
use transport::{Conn, RecvStream, SendStream};
use transport::{read_frame, read_payload, write_frame};
use xet_local::chunks::ChunkList;
use xet_local::chunks::{VerifiedChunk, XorbPacker, decode_chunk_lists, parse_chunk_run};
use xet_local::{
    LocalCas, Manifest, MerkleHash, MissingContent, PendingShard, ReconstructOptions,
    Reconstruction,
};

use crate::budget::MemoryBudget;
use crate::error::{Error, Result};
use crate::protocol::{
    ChunkRequest, MAX_CHUNK_LISTS_BYTES, MAX_CHUNK_RUN_BYTES, MAX_OBJECT_HEADER, MAX_SHARD_BYTES,
    MAX_XORB_BYTES, Message, ObjectHeader, ObjectKind, ReceiverReport, TransferMode, parse_hash,
    to_hex,
};
use crate::session::{Session, SessionStore};

#[derive(Debug, Clone)]
pub(crate) struct FetchOptions {
    pub parallel_objects: usize,
    pub stall_timeout: Duration,
    pub commit_delay: Option<Duration>,
}

pub(crate) struct Fetcher {
    pub cas: Arc<LocalCas>,
    pub sessions: SessionStore,
    pub budget: MemoryBudget,
    pub opts: FetchOptions,
}

/// Packed xorbs committing at once, beside receiving.
const PIPELINED_COMMITS: usize = 2;

type CommitDone = std::result::Result<Result<(u64, Vec<MerkleHash>)>, tokio::task::JoinError>;

/// Books a finished background commit; returns the chunk hashes it made available.
fn committed(
    done: CommitDone,
    session: &mut Session<'_>,
    report: &mut ReceiverReport,
) -> Result<Vec<MerkleHash>> {
    let (len, hashes) = done.map_err(|e| Error::Protocol(format!("commit task failed: {e}")))??;
    report.xorbs_packed += 1;
    session.xorb_committed(len)?;
    Ok(hashes)
}

/// Tracks which files' content is complete in the CAS while chunks arrive, registers each file's
/// reconstruction as soon as it is, and starts staging it (overlapping with the rest of the
/// transfer).
struct Readiness<'a> {
    /// Files not registered yet, with their chunk lists and SHA-256.
    lists: HashMap<MerkleHash, (ChunkList, String)>,
    /// Per file, how many of its distinct missing chunks have not been committed yet.
    waiting: HashMap<MerkleHash, usize>,
    /// Per missing chunk, the files waiting for it.
    needed_by: HashMap<MerkleHash, Vec<MerkleHash>>,
    recon: &'a mut Reconstruction,
}

impl Readiness<'_> {
    async fn chunks_committed(&mut self, cas: &LocalCas, hashes: &[MerkleHash]) -> Result<()> {
        let mut ready = Vec::new();
        for h in hashes {
            for f in self.needed_by.remove(h).unwrap_or_default() {
                if let Some(n) = self.waiting.get_mut(&f) {
                    *n -= 1;
                    if *n == 0 {
                        ready.push(f);
                    }
                }
            }
        }
        self.register(cas, ready).await
    }

    async fn register(&mut self, cas: &LocalCas, files: Vec<MerkleHash>) -> Result<()> {
        let items: Vec<(MerkleHash, ChunkList, String)> = files
            .iter()
            .filter_map(|f| self.lists.remove(f).map(|(l, sha)| (*f, l, sha)))
            .collect();
        if items.is_empty() {
            return Ok(());
        }
        cas.register_files(&items).await?;
        for (f, ..) in &items {
            self.recon.content_ready(f);
        }
        Ok(())
    }
}

impl Fetcher {
    /// Fetches everything `manifest` needs that this CAS lacks, then reconstructs and atomically
    /// installs it into `dest_dir`. `session_key` names the resumable session (the destination).
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn fetch_and_install(
        &self,
        conn: &Conn,
        send: &mut SendStream,
        manifest: &Manifest,
        session_key: &str,
        dest_dir: &Path,
        mode: TransferMode,
        delete: bool,
    ) -> Result<ReceiverReport> {
        let t = std::time::Instant::now();
        let mut session = self.sessions.begin(session_key, manifest)?;
        debug!(ms = t.elapsed().as_millis() as u64, "session begun");
        let mut report = ReceiverReport {
            resumed: session.is_resumed(),
            attempts: session.record.attempts,
            prior_xorbs_committed: session.prior.xorbs_committed,
            prior_xorb_bytes_committed: session.prior.xorb_bytes_committed,
            ..Default::default()
        };
        if report.resumed {
            info!(dest = %session_key, attempt = report.attempts, prior_xorbs = report.prior_xorbs_committed, "resuming session");
        }

        let t = std::time::Instant::now();
        let missing = self.cas.missing_content(manifest).await?;
        debug!(ms = t.elapsed().as_millis() as u64, "missing content found");
        report.mode = mode;

        // Files are staged as soon as their content is here, beside the rest of the transfer;
        // nothing is installed until all of them verify.
        let mut recon = self.cas.begin_reconstruct(
            manifest,
            dest_dir,
            ReconstructOptions {
                delete_extraneous: delete,
            },
        )?;
        let incomplete: BTreeSet<MerkleHash> = missing
            .reconstructions
            .union(&missing.files_missing_xorbs)
            .copied()
            .collect();
        for (_, size, h, ..) in manifest.files() {
            if let Ok(h) = MerkleHash::from_hex(h)
                && size > 0
                && !incomplete.contains(&h)
            {
                recon.content_ready(&h);
            }
        }
        match mode {
            TransferMode::Xorb => {
                self.fetch_whole_xorbs(conn, send, &missing, &mut session, &mut report)
                    .await?
            }
            TransferMode::Chunk => {
                self.fetch_chunks(
                    conn,
                    send,
                    manifest,
                    &missing,
                    &mut session,
                    &mut report,
                    &mut recon,
                )
                .await?
            }
        }

        let t = std::time::Instant::now();
        let r = recon.finish().await?;
        debug!(ms = t.elapsed().as_millis() as u64, "reconstructed");
        report.files_deleted = r.files_deleted;
        report.dirs_deleted = r.dirs_deleted;
        report.files_installed = r.files;
        report.files_unchanged = r.files_unchanged;
        report.files_trusted = r.files_trusted;
        report.dirs_created = r.dirs;
        report.bytes_reconstructed = r.bytes;
        let t = std::time::Instant::now();
        session.finish()?;
        debug!(ms = t.elapsed().as_millis() as u64, "session finished");
        Ok(report)
    }

    /// Whole-xorb mode: the sender's native shard entries and xorbs, as stored.
    async fn fetch_whole_xorbs(
        &self,
        conn: &Conn,
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
    #[tracing::instrument(level = "debug", skip_all)]
    #[allow(clippy::too_many_arguments)]
    async fn fetch_chunks(
        &self,
        conn: &Conn,
        send: &mut SendStream,
        manifest: &Manifest,
        missing: &MissingContent,
        session: &mut Session<'_>,
        report: &mut ReceiverReport,
        recon: &mut Reconstruction,
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

        let sha: HashMap<MerkleHash, String> = manifest
            .files()
            .filter_map(|(_, _, h, sha, _)| Some((MerkleHash::from_hex(h).ok()?, sha.to_owned())))
            .collect();
        let mut ready = Readiness {
            lists: HashMap::new(),
            waiting: HashMap::new(),
            needed_by: HashMap::new(),
            recon,
        };
        for (f, l) in lists {
            let sha = sha.get(&f).cloned().ok_or_else(|| {
                Error::Protocol(format!("chunk list for file {} not in manifest", f.hex()))
            })?;
            let mut distinct = std::collections::HashSet::new();
            for (h, _) in &l {
                if plan.missing.contains_key(h) && distinct.insert(*h) {
                    ready.needed_by.entry(*h).or_default().push(f);
                }
            }
            ready.waiting.insert(f, distinct.len());
            ready.lists.insert(f, (l, sha));
        }
        // Files whose every chunk is already here can start now.
        let complete: Vec<MerkleHash> = ready
            .waiting
            .iter()
            .filter(|(_, n)| **n == 0)
            .map(|(f, _)| *f)
            .collect();
        ready.register(&self.cas, complete).await?;

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
            let t = std::time::Instant::now();
            self.receive_chunk_runs(
                conn,
                send,
                plan.missing.clone(),
                session,
                report,
                &mut ready,
            )
            .await?;
            debug!(ms = t.elapsed().as_millis() as u64, "chunk runs received");
        }
        // Anything left (normally nothing) registers now; a missing chunk fails here.
        let rest: Vec<MerkleHash> = ready.lists.keys().copied().collect();
        ready.register(&self.cas, rest).await?;
        self.sessions.remove_chunk_lists(&wanted);
        Ok(())
    }

    /// Receives chunk runs until every chunk in `outstanding` has arrived and verified. Each
    /// chunk is hashed; unrequested or repeated chunks abort the transfer. Verified native chunk
    /// bytes are packed into new xorbs, committed as each fills.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn receive_chunk_runs_unguarded(
        &self,
        conn: &Conn,
        send: &mut SendStream,
        mut outstanding: HashMap<MerkleHash, u64>,
        session: &mut Session<'_>,
        report: &mut ReceiverReport,
        ready: &mut Readiness<'_>,
    ) -> Result<()> {
        type Received = (
            Vec<u8>,
            Vec<VerifiedChunk>,
            tokio::sync::OwnedSemaphorePermit,
        );
        let permits = Arc::new(Semaphore::new(self.opts.parallel_objects));
        let mut tasks: JoinSet<Result<Received>> = JoinSet::new();
        let mut packer = XorbPacker::default();
        // The chunk hashes in `packer`, in order.
        let mut pack: Vec<MerkleHash> = Vec::new();
        // Packed xorbs commit in the background while chunk runs keep arriving.
        let mut commits: JoinSet<Result<(u64, Vec<MerkleHash>)>> = JoinSet::new();

        while !outstanding.is_empty() || !tasks.is_empty() {
            tokio::select! {
                // Record each commit as soon as it lands, so resume state is never behind.
                Some(done) = commits.join_next(), if !commits.is_empty() => {
                    let hashes = committed(done, session, report)?;
                    ready.chunks_committed(&self.cas, &hashes).await?;
                }
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
                            return Err(transport::Error::FrameTooLarge { len: header.len, max: MAX_CHUNK_RUN_BYTES }.into());
                        }
                        let memory = budget.reserve(header.len).await;
                        let bytes = read_payload(&mut stream, header.len, MAX_CHUNK_RUN_BYTES)
                            .instrument(tracing::debug_span!("read_chunk_run_payload"))
                            .await?;
                        let span = tracing::debug_span!("verify_chunk_run");
                        let (bytes, chunks) = tokio::task::spawn_blocking(move || {
                            let _g = span.entered();
                            let t = std::time::Instant::now();
                            let chunks = parse_chunk_run(&bytes);
                            debug!(ms = t.elapsed().as_millis() as u64, bytes = bytes.len(), "chunk run verified");
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
                        pack.push(c.hash);
                        report.chunks_received += 1;
                        if packer.is_full() {
                            self.commit_pack(&mut commits, &mut packer, &mut pack, session, report, ready)
                                .await?;
                        }
                    }
                    report.chunk_bytes_received += bytes.len() as u64;
                    report.payload_bytes_received += bytes.len() as u64;
                    write_frame(send, &Message::Ack { kind: ObjectKind::ChunkRun, hash: None }).await?;
                }
            }
        }
        if !packer.is_empty() {
            self.commit_pack(&mut commits, &mut packer, &mut pack, session, report, ready)
                .await?;
        }
        while let Some(done) = commits.join_next().await {
            let hashes = committed(done, session, report)?;
            ready.chunks_committed(&self.cas, &hashes).await?;
        }
        Ok(())
    }

    async fn receive_chunk_runs(
        &self,
        conn: &Conn,
        send: &mut SendStream,
        outstanding: HashMap<MerkleHash, u64>,
        session: &mut Session<'_>,
        report: &mut ReceiverReport,
        ready: &mut Readiness<'_>,
    ) -> Result<()> {
        self.stall(
            conn,
            self.receive_chunk_runs_unguarded(conn, send, outstanding, session, report, ready),
        )
        .await
    }

    async fn receive_xorbs(
        &self,
        conn: &Conn,
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
    pub(crate) async fn stall<T>(
        &self,
        conn: &Conn,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let timeout = self.opts.stall_timeout;
        let tick = (timeout / 4).clamp(Duration::from_millis(10), Duration::from_secs(1));
        let mut interval = tokio::time::interval(tick);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = conn.rx_progress();
        let mut since = tokio::time::Instant::now();
        tokio::pin!(fut);
        loop {
            tokio::select! {
                r = &mut fut => return r,
                _ = interval.tick() => {
                    let now = conn.rx_progress();
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

    /// Starts committing the packer's xorb in the background, after waiting for earlier commits
    /// if `PIPELINED_COMMITS` are already in flight. Committing (building the footer, which
    /// re-hashes every chunk, then the durable write) used to stall the receive loop for ~45 ms
    /// per 16 MiB.
    #[allow(clippy::too_many_arguments)]
    async fn commit_pack(
        &self,
        commits: &mut JoinSet<Result<(u64, Vec<MerkleHash>)>>,
        packer: &mut XorbPacker,
        pack: &mut Vec<MerkleHash>,
        session: &mut Session<'_>,
        report: &mut ReceiverReport,
        ready: &mut Readiness<'_>,
    ) -> Result<()> {
        while commits.len() >= PIPELINED_COMMITS {
            let done = commits.join_next().await.expect("non-empty");
            let hashes = committed(done, session, report)?;
            ready.chunks_committed(&self.cas, &hashes).await?;
        }
        while let Some(done) = commits.try_join_next() {
            let hashes = committed(done, session, report)?;
            ready.chunks_committed(&self.cas, &hashes).await?;
        }
        let body = packer.take();
        let hashes = std::mem::take(pack);
        let (cas, delay) = (self.cas.clone(), self.opts.commit_delay);
        commits.spawn(
            async move {
                let len = body.len() as u64;
                let t = std::time::Instant::now();
                cas.commit_packed(body).await?;
                debug!(
                    ms = t.elapsed().as_millis() as u64,
                    bytes = len,
                    "xorb committed"
                );
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }
                Ok((len, hashes))
            }
            .instrument(tracing::debug_span!("commit_xorb")),
        );
        Ok(())
    }

    /// Receives one object of `kind` (without a hash) of at most `max` bytes.
    async fn receive_single(&self, conn: &Conn, kind: ObjectKind, max: u64) -> Result<Vec<u8>> {
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
        conn: &Conn,
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
                            return Err(transport::Error::FrameTooLarge { len: header.len, max: MAX_XORB_BYTES }.into());
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
