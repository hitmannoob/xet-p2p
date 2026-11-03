//! The client side of a pull: ask the server for the tree at `src`, then fetch and install it
//! locally with the same verification, repacking, resume, and atomic install as a push receiver.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use transport::{Conn, TransportKind, write_frame};
use xet_local::LocalCas;

use crate::budget::MemoryBudget;
use crate::error::{Error, Result};
use crate::fetch::{FetchOptions, Fetcher};
use crate::protocol::{
    Message, Operation, PROTOCOL_VERSION, ReceiverReport, TransferMode, decode_manifest,
};
use crate::sender::{next, session_id, unexpected};
use crate::session::SessionStore;

#[derive(Debug, Clone)]
pub struct PullOptions {
    /// Source path, relative to the server's root.
    pub src: String,
    pub mode: TransferMode,
    /// Mirror: remove anything in the local destination the source does not contain.
    pub delete: bool,
    pub parallel_streams: usize,
    pub max_memory_bytes: u64,
    /// Abort if the server stops sending data for this long while we wait on it.
    pub stall_timeout: Duration,
}

impl PullOptions {
    pub fn new(src: impl Into<String>) -> Self {
        Self {
            src: src.into(),
            mode: TransferMode::Chunk,
            delete: false,
            parallel_streams: 4,
            max_memory_bytes: crate::sender::DEFAULT_MAX_MEMORY_BYTES,
            stall_timeout: Duration::from_secs(120),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PullReport {
    pub files: usize,
    pub logical_bytes: u64,
    /// What this side fetched and installed.
    pub local: ReceiverReport,
    pub transport: TransportKind,
    pub wire_bytes_sent: u64,
    pub wire_bytes_received: u64,
    pub elapsed: Duration,
}

/// Pulls `opts.src` from the server over `conn` into the local directory `dest`, using `cas` as
/// this side's store (and its `sessions/` for resume).
pub async fn pull(
    cas: Arc<LocalCas>,
    conn: &Conn,
    dest: &Path,
    opts: &PullOptions,
) -> Result<PullReport> {
    let start = Instant::now();
    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(
        &mut send,
        &Message::Hello {
            protocol_version: PROTOCOL_VERSION,
            session_id: session_id(),
            op: Operation::Pull {
                src: opts.src.clone(),
                mode: opts.mode,
            },
        },
    )
    .await?;
    match next(&mut recv).await? {
        Message::HelloOk {
            protocol_version, ..
        } if protocol_version == PROTOCOL_VERSION => {}
        Message::HelloOk {
            protocol_version, ..
        } => {
            return Err(Error::Protocol(format!(
                "server speaks protocol {protocol_version}"
            )));
        }
        other => return Err(unexpected(other)),
    }

    let fetcher = Fetcher {
        sessions: SessionStore::open(cas.root().join("sessions"))?,
        budget: MemoryBudget::new(opts.max_memory_bytes),
        opts: FetchOptions {
            parallel_objects: opts.parallel_streams.max(1),
            stall_timeout: opts.stall_timeout,
            commit_delay: None,
            link_identical: false,
        },
        cas,
    };
    let manifest = match fetcher.stall(conn, next(&mut recv)).await? {
        Message::Manifest { encoded } => decode_manifest(&encoded)?,
        other => return Err(unexpected(other)),
    };
    manifest.validate()?;

    std::fs::create_dir_all(dest).map_err(xet_local::Error::from)?;
    let dest = std::fs::canonicalize(dest).map_err(xet_local::Error::from)?;
    let key = format!("pull:{}", dest.display());
    let result = fetcher
        .fetch_and_install(
            conn,
            &mut send,
            &manifest,
            &key,
            &dest,
            opts.mode,
            opts.delete,
        )
        .await;

    // Tell the server how it went, then read its closing frame (an echo of the result, or an
    // error) so neither side closes before the other has finished.
    let closing = match &result {
        Ok(r) => Message::Result { report: r.clone() },
        Err(e) => Message::Error {
            message: e.to_string(),
        },
    };
    let _ = write_frame(&mut send, &closing).await;
    let _ = send.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), next(&mut recv)).await;

    let local = result?;
    let wire = conn.wire_stats();
    Ok(PullReport {
        files: manifest.files().count(),
        logical_bytes: manifest.total_file_bytes(),
        local,
        transport: conn.kind(),
        wire_bytes_sent: wire.tx,
        wire_bytes_received: wire.rx,
        elapsed: start.elapsed(),
    })
}
