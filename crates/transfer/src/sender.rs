//! Sender side: ingest locally, then answer the receiver's requests for native objects.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::debug;
use transport::{Conn, RecvStream, SendStream, TransportKind};
use transport::{read_frame, write_frame};
use xet_local::chunks::{ChunkRun, encode_chunk_lists};
use xet_local::{IngestReport, LocalCas, Manifest, MerkleHash};

use crate::budget::MemoryBudget;
use crate::error::{Error, Result};
use crate::protocol::{
    ChunkRequest, MAX_CONTROL_FRAME, Message, ObjectHeader, ObjectKind, Operation,
    PROTOCOL_VERSION, ReceiverReport, TransferMode, parse_hash,
};
use crate::rate::{RateLimiter, SLICE};

#[derive(Debug, Clone)]
pub struct PushOptions {
    /// Destination directory relative to the receiver's root.
    pub dest: String,
    /// Maximum xorbs in flight at once.
    pub parallel_streams: usize,
    /// Upper bound on xorb bytes held in memory at once (at least one xorb always fits).
    pub max_memory_bytes: u64,
    /// Ask the receiver to remove anything under `dest` that the source does not contain.
    pub delete: bool,
    /// How missing content is moved (chunk-level by default).
    pub mode: TransferMode,
    /// Cap on payload bytes per second across all streams; `None` for unlimited.
    pub bandwidth_limit: Option<f64>,
}

/// Default for [`PushOptions::max_memory_bytes`] and the receiver's equivalent.
pub const DEFAULT_MAX_MEMORY_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct PushReport {
    pub ingest: IngestReport,
    pub logical_bytes: u64,
    pub xorbs_referenced: usize,
    pub xorbs_sent: usize,
    pub xorb_bytes_sent: u64,
    pub shard_bytes_sent: u64,
    /// Chunk mode: native chunk-run bytes, runs, and chunk-list bytes sent.
    pub chunk_bytes_sent: u64,
    pub chunk_runs_sent: usize,
    pub chunk_list_bytes_sent: u64,
    pub receiver: ReceiverReport,
    /// Bytes the transport sent / received for this connection, all overhead included (UDP
    /// datagrams for QUIC, TCP payload for TLS-over-TCP).
    pub udp_bytes_sent: u64,
    pub udp_bytes_received: u64,
    /// The transport actually used.
    pub transport: TransportKind,
    pub elapsed: Duration,
}

impl PushReport {
    pub fn xorbs_reused(&self) -> usize {
        self.xorbs_referenced - self.xorbs_sent
    }

    /// Every object byte sent: native xorbs, shards, chunk runs, and chunk lists.
    pub fn payload_bytes_sent(&self) -> u64 {
        self.xorb_bytes_sent
            + self.shard_bytes_sent
            + self.chunk_bytes_sent
            + self.chunk_list_bytes_sent
    }
}

/// Pushes `src` (a file or directory) over `conn` into `opts.dest` on the receiver.
pub async fn push(
    cas: Arc<LocalCas>,
    src: &Path,
    conn: &Conn,
    opts: &PushOptions,
) -> Result<PushReport> {
    let start = Instant::now();
    let limiter = opts.bandwidth_limit.map(|b| Arc::new(RateLimiter::new(b)));
    let (manifest, ingest) = cas.ingest(src).await?;
    let files = file_hashes(&manifest)?;
    let referenced = cas.referenced_xorbs(&files).await?;
    cas.touch_xorbs(&referenced); // recency for GC on the sender's CAS

    let mut report = PushReport {
        ingest,
        logical_bytes: manifest.total_file_bytes(),
        xorbs_referenced: referenced.len(),
        xorbs_sent: 0,
        xorb_bytes_sent: 0,
        shard_bytes_sent: 0,
        chunk_bytes_sent: 0,
        chunk_runs_sent: 0,
        chunk_list_bytes_sent: 0,
        receiver: ReceiverReport::default(),
        udp_bytes_sent: 0,
        udp_bytes_received: 0,
        transport: conn.kind(),
        elapsed: Duration::ZERO,
    };

    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(
        &mut send,
        &Message::Hello {
            protocol_version: PROTOCOL_VERSION,
            session_id: session_id(),
            op: Operation::Push {
                dest: opts.dest.clone(),
                delete: opts.delete,
                mode: opts.mode,
            },
        },
    )
    .await?;
    // The manifest follows the HELLO without waiting for HELLO_OK: one round trip less. If the
    // receiver refuses the HELLO it never reads the manifest, so its reply is awaited alongside
    // the write and wins over a write that cannot finish.
    let manifest_msg = Message::manifest(&manifest)?;
    {
        let write = write_frame(&mut send, &manifest_msg);
        let reply = next(&mut recv);
        tokio::pin!(write, reply);
        let mut written = None;
        let reply = loop {
            tokio::select! {
                w = &mut write, if written.is_none() => written = Some(w),
                r = &mut reply => break r,
            }
        };
        match reply? {
            Message::HelloOk { protocol_version } if protocol_version == PROTOCOL_VERSION => {}
            Message::HelloOk { protocol_version } => {
                return Err(Error::Protocol(format!(
                    "receiver speaks protocol {protocol_version}"
                )));
            }
            other => return Err(unexpected(other)),
        }
        match written {
            Some(w) => w?,
            None => write.await?,
        }
    }

    let mut stats = SendStats::default();
    report.receiver = serve_requests(
        &cas,
        conn,
        &mut recv,
        &files,
        &referenced,
        &opts.tuning(),
        &limiter,
        &mut stats,
    )
    .await?;
    report.xorbs_sent = stats.xorbs_sent;
    report.xorb_bytes_sent = stats.xorb_bytes_sent;
    report.shard_bytes_sent = stats.shard_bytes_sent;
    report.chunk_bytes_sent = stats.chunk_bytes_sent;
    report.chunk_runs_sent = stats.chunk_runs_sent;
    report.chunk_list_bytes_sent = stats.chunk_list_bytes_sent;

    let _ = send.shutdown().await;
    let stats = conn.wire_stats();
    report.udp_bytes_sent = stats.tx;
    report.udp_bytes_received = stats.rx;
    report.transport = conn.kind();
    report.elapsed = start.elapsed();
    Ok(report)
}

/// What the sending side of a transfer sent.
#[derive(Debug, Clone, Default)]
pub(crate) struct SendStats {
    pub xorbs_sent: usize,
    pub xorb_bytes_sent: u64,
    pub shard_bytes_sent: u64,
    pub chunk_bytes_sent: u64,
    pub chunk_runs_sent: usize,
    pub chunk_list_bytes_sent: u64,
}

/// Concurrency and memory limits for the sending side.
#[derive(Debug, Clone)]
pub(crate) struct SendTuning {
    pub parallel_streams: usize,
    pub max_memory_bytes: u64,
}

impl PushOptions {
    fn tuning(&self) -> SendTuning {
        SendTuning {
            parallel_streams: self.parallel_streams,
            max_memory_bytes: self.max_memory_bytes,
        }
    }
}

/// Answers the fetching peer's requests for objects of this manifest until it reports a result.
/// Used by the client of a push and by the server of a pull.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_requests(
    cas: &Arc<LocalCas>,
    conn: &Conn,
    recv: &mut RecvStream,
    files: &BTreeSet<MerkleHash>,
    referenced: &BTreeSet<MerkleHash>,
    tuning: &SendTuning,
    limiter: &Limiter,
    stats: &mut SendStats,
) -> Result<ReceiverReport> {
    loop {
        match next(recv).await? {
            Message::Need {
                reconstructions,
                xorbs,
            } => {
                let reconstructions = parse_subset(&reconstructions, files, "file")?;
                let xorbs = parse_subset(&xorbs, referenced, "xorb")?;
                debug!(
                    shard_files = reconstructions.len(),
                    xorbs = xorbs.len(),
                    "NEED"
                );
                if !reconstructions.is_empty() {
                    let shard = cas.export_shard(&reconstructions).await?;
                    stats.shard_bytes_sent +=
                        send_object(conn, limiter, ObjectKind::Shard, None, shard).await?;
                }
                stats.xorb_bytes_sent += send_xorbs(cas, conn, &xorbs, tuning, limiter).await?;
                stats.xorbs_sent += xorbs.len();
            }
            Message::NeedChunkLists { files: wanted } => {
                let wanted = parse_subset(&wanted, files, "file")?;
                debug!(files = wanted.len(), "NEED_CHUNK_LISTS");
                let lists = encode_chunk_lists(&cas.chunk_lists(&wanted).await?);
                stats.chunk_list_bytes_sent +=
                    send_object(conn, limiter, ObjectKind::ChunkLists, None, lists).await?;
            }
            Message::NeedChunks { requests } => {
                debug!(files = requests.len(), "NEED_CHUNKS");
                let (bytes, runs) =
                    send_chunk_runs(cas, conn, &requests, files, tuning, limiter).await?;
                stats.chunk_bytes_sent += bytes;
                stats.chunk_runs_sent += runs;
            }
            Message::Ack { .. } => {}
            Message::Result { report } => return Ok(report),
            other => return Err(unexpected(other)),
        }
    }
}

/// The receiver may only request objects this manifest actually references.
fn parse_subset(
    requested: &[String],
    allowed: &BTreeSet<MerkleHash>,
    what: &str,
) -> Result<BTreeSet<MerkleHash>> {
    let mut out = BTreeSet::new();
    for s in requested {
        let h = parse_hash(s)?;
        if !allowed.contains(&h) {
            return Err(Error::Protocol(format!(
                "receiver requested unrelated {what} {s}"
            )));
        }
        out.insert(h);
    }
    Ok(out)
}

async fn send_xorbs(
    cas: &Arc<LocalCas>,
    conn: &Conn,
    xorbs: &BTreeSet<MerkleHash>,
    opts: &SendTuning,
    limiter: &Limiter,
) -> Result<u64> {
    let permits = Arc::new(Semaphore::new(opts.parallel_streams.max(1)));
    let budget = MemoryBudget::new(opts.max_memory_bytes);
    let mut tasks = JoinSet::new();
    for &hash in xorbs {
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore never closed");
        let memory = budget.reserve(cas.xorb_len(&hash).await?).await;
        let (cas, conn, limiter) = (cas.clone(), conn.clone(), limiter.clone());
        tasks.spawn(async move {
            let _permits = (permit, memory);
            let bytes = cas.read_xorb(&hash).await?;
            send_object(&conn, &limiter, ObjectKind::Xorb, Some(hash), bytes).await
        });
    }
    let mut total = 0;
    while let Some(res) = tasks.join_next().await {
        total += res.map_err(|e| Error::Protocol(format!("send task failed: {e}")))??;
    }
    Ok(total)
}

/// Sends the native bytes of every requested chunk range, one stream per run, bounded by the
/// stream and memory limits. Returns (bytes, runs).
async fn send_chunk_runs(
    cas: &Arc<LocalCas>,
    conn: &Conn,
    requests: &[ChunkRequest],
    files: &BTreeSet<MerkleHash>,
    opts: &SendTuning,
    limiter: &Limiter,
) -> Result<(u64, usize)> {
    let permits = Arc::new(Semaphore::new(opts.parallel_streams.max(1)));
    let budget = MemoryBudget::new(opts.max_memory_bytes);
    let mut tasks = JoinSet::new();
    let mut streams = 0;
    let mut total = 0;

    // Consecutive runs (possibly from different xorbs) share one stream up to BATCH bytes: the
    // receiver verifies each chunk by its own hash, so stream boundaries carry no meaning, and
    // thousands of tiny per-file runs would otherwise cost a stream and an ACK each.
    const BATCH: u64 = 4 * 1024 * 1024;
    let mut batch: Vec<ChunkRun> = Vec::new();
    let mut batch_bytes = 0u64;

    let mut flush = async |batch: &mut Vec<ChunkRun>, batch_bytes: &mut u64| -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let runs = std::mem::take(batch);
        let bytes = std::mem::take(batch_bytes);
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore never closed");
        let memory = budget.reserve(bytes).await;
        let (cas, conn, limiter) = (cas.clone(), conn.clone(), limiter.clone());
        streams += 1;
        tasks.spawn(async move {
            let _permits = (permit, memory);
            let mut body = Vec::with_capacity(bytes as usize);
            for mut run in runs {
                cas.read_chunk_run(&mut run).await?;
                body.extend_from_slice(&run.bytes);
            }
            send_object(&conn, &limiter, ObjectKind::ChunkRun, None, body).await
        });
        // Surface failures early rather than after planning everything.
        while let Some(res) = tasks.try_join_next() {
            total += res.map_err(|e| Error::Protocol(format!("send task failed: {e}")))??;
        }
        Ok(())
    };

    for req in requests {
        let file = parse_hash(&req.file)?;
        if !files.contains(&file) {
            return Err(Error::Protocol(format!(
                "receiver requested chunks of unrelated file {}",
                req.file
            )));
        }
        for run in cas.plan_chunk_runs(&file, &req.ranges).await? {
            let len = run.byte_range.end - run.byte_range.start;
            if batch_bytes > 0 && batch_bytes + len > BATCH {
                flush(&mut batch, &mut batch_bytes).await?;
            }
            batch_bytes += len;
            batch.push(run);
        }
    }
    flush(&mut batch, &mut batch_bytes).await?;
    while let Some(res) = tasks.join_next().await {
        total += res.map_err(|e| Error::Protocol(format!("send task failed: {e}")))??;
    }
    Ok((total, streams))
}

pub(crate) type Limiter = Option<Arc<RateLimiter>>;

async fn send_object(
    conn: &Conn,
    limiter: &Limiter,
    kind: ObjectKind,
    hash: Option<MerkleHash>,
    bytes: Vec<u8>,
) -> Result<u64> {
    let mut stream: SendStream = conn.open_uni().await?;
    let len = bytes.len() as u64;
    write_frame(
        &mut stream,
        &ObjectHeader {
            kind,
            hash: hash.map(|h| h.hex()),
            len,
        },
    )
    .await?;
    match limiter {
        None => stream.write_all(&bytes).await?,
        Some(limiter) => {
            for piece in bytes.chunks(SLICE) {
                limiter.take(piece.len()).await;
                stream.write_all(piece).await?;
            }
        }
    }
    let _ = stream.shutdown().await;
    Ok(len)
}

pub(crate) async fn next(recv: &mut RecvStream) -> Result<Message> {
    match read_frame::<Message>(recv, MAX_CONTROL_FRAME).await? {
        Some(Message::Error { message }) => Err(Error::Remote(message)),
        Some(m) => Ok(m),
        None => Err(Error::Protocol("receiver closed the control stream".into())),
    }
}

pub(crate) fn unexpected(m: Message) -> Error {
    Error::Protocol(format!("unexpected {} from receiver", m.name()))
}

pub(crate) fn file_hashes(m: &Manifest) -> Result<BTreeSet<MerkleHash>> {
    m.files()
        .filter(|(_, size, ..)| *size > 0)
        .map(|(_, _, h, ..)| parse_hash(h))
        .collect()
}

pub(crate) fn session_id() -> String {
    hex::encode(rand::random::<[u8; 16]>())
}
