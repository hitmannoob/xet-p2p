//! Sender side: ingest locally, then answer the receiver's requests for native objects.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::debug;
use transport_quic::quinn::{Connection, RecvStream, SendStream};
use transport_quic::{read_frame, write_frame};
use xet_local::{IngestReport, LocalCas, Manifest, MerkleHash};

use crate::budget::MemoryBudget;
use crate::error::{Error, Result};
use crate::protocol::{
    MAX_CONTROL_FRAME, Message, ObjectHeader, ObjectKind, PROTOCOL_VERSION, ReceiverReport,
    parse_hash,
};

#[derive(Debug, Clone)]
pub struct PushOptions {
    /// Destination directory relative to the receiver's root.
    pub dest: String,
    /// Maximum xorbs in flight at once.
    pub parallel_streams: usize,
    /// Upper bound on xorb bytes held in memory at once (at least one xorb always fits).
    pub max_memory_bytes: u64,
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
    pub receiver: ReceiverReport,
    pub udp_bytes_sent: u64,
    pub elapsed: Duration,
}

impl PushReport {
    pub fn xorbs_reused(&self) -> usize {
        self.xorbs_referenced - self.xorbs_sent
    }

    pub fn payload_bytes_sent(&self) -> u64 {
        self.xorb_bytes_sent + self.shard_bytes_sent
    }
}

/// Pushes `src` (a file or directory) over `conn` into `opts.dest` on the receiver.
pub async fn push(
    cas: Arc<LocalCas>,
    src: &Path,
    conn: &Connection,
    opts: &PushOptions,
) -> Result<PushReport> {
    let start = Instant::now();
    let (manifest, ingest) = cas.ingest(src).await?;
    let files = file_hashes(&manifest)?;
    let referenced = cas.referenced_xorbs(&files).await?;

    let mut report = PushReport {
        ingest,
        logical_bytes: manifest.total_file_bytes(),
        xorbs_referenced: referenced.len(),
        xorbs_sent: 0,
        xorb_bytes_sent: 0,
        shard_bytes_sent: 0,
        receiver: ReceiverReport::default(),
        udp_bytes_sent: 0,
        elapsed: Duration::ZERO,
    };

    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(
        &mut send,
        &Message::Hello {
            protocol_version: PROTOCOL_VERSION,
            session_id: session_id(),
            dest: opts.dest.clone(),
        },
    )
    .await?;
    match next(&mut recv).await? {
        Message::HelloOk { protocol_version } if protocol_version == PROTOCOL_VERSION => {}
        Message::HelloOk { protocol_version } => {
            return Err(Error::Protocol(format!(
                "receiver speaks protocol {protocol_version}"
            )));
        }
        other => return Err(unexpected(other)),
    }

    write_frame(&mut send, &Message::Manifest { manifest }).await?;

    loop {
        match next(&mut recv).await? {
            Message::Need {
                reconstructions,
                xorbs,
            } => {
                let reconstructions = parse_subset(&reconstructions, &files, "file")?;
                let xorbs = parse_subset(&xorbs, &referenced, "xorb")?;
                debug!(
                    shard_files = reconstructions.len(),
                    xorbs = xorbs.len(),
                    "NEED"
                );
                if !reconstructions.is_empty() {
                    let shard = cas.export_shard(&reconstructions).await?;
                    report.shard_bytes_sent +=
                        send_object(conn, ObjectKind::Shard, None, shard).await?;
                }
                report.xorb_bytes_sent += send_xorbs(&cas, conn, &xorbs, opts).await?;
                report.xorbs_sent += xorbs.len();
            }
            Message::Ack { .. } => {}
            Message::Result { report: r } => {
                report.receiver = r;
                break;
            }
            other => return Err(unexpected(other)),
        }
    }

    let _ = send.finish();
    report.udp_bytes_sent = conn.stats().udp_tx.bytes;
    report.elapsed = start.elapsed();
    Ok(report)
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
    conn: &Connection,
    xorbs: &BTreeSet<MerkleHash>,
    opts: &PushOptions,
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
        let (cas, conn) = (cas.clone(), conn.clone());
        tasks.spawn(async move {
            let _permits = (permit, memory);
            let bytes = cas.read_xorb(&hash).await?;
            send_object(&conn, ObjectKind::Xorb, Some(hash), bytes).await
        });
    }
    let mut total = 0;
    while let Some(res) = tasks.join_next().await {
        total += res.map_err(|e| Error::Protocol(format!("send task failed: {e}")))??;
    }
    Ok(total)
}

async fn send_object(
    conn: &Connection,
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
    stream.write_all(&bytes).await?;
    let _ = stream.finish();
    Ok(len)
}

async fn next(recv: &mut RecvStream) -> Result<Message> {
    match read_frame::<Message>(recv, MAX_CONTROL_FRAME).await? {
        Some(Message::Error { message }) => Err(Error::Remote(message)),
        Some(m) => Ok(m),
        None => Err(Error::Protocol("receiver closed the control stream".into())),
    }
}

fn unexpected(m: Message) -> Error {
    Error::Protocol(format!("unexpected {} from receiver", m.name()))
}

fn file_hashes(m: &Manifest) -> Result<BTreeSet<MerkleHash>> {
    m.files()
        .filter(|(_, size, ..)| *size > 0)
        .map(|(_, _, h, ..)| parse_hash(h))
        .collect()
}

fn session_id() -> String {
    hex::encode(rand::random::<[u8; 16]>())
}
