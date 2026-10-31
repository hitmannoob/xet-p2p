//! The xet-p2p control protocol (version 2).
//!
//! One bidirectional control stream, opened by the sender, carries CBOR [`Message`] frames.
//! Objects travel on unidirectional streams opened by the sender: an [`ObjectHeader`] frame
//! followed by the object's bytes. Xorbs, shards, and chunk runs are native Xet bytes, unchanged.
//!
//! Chunk mode (default):
//! ```text
//! sender                                  receiver
//!   HELLO {version, session, dest, mode} ->
//!                                       <-  HELLO_OK
//!   MANIFEST                             ->
//!                                       <-  NEED_CHUNK_LISTS {files}        (new files only)
//!   [uni] CHUNK_LISTS                    ->  verified against each file hash
//!                                       <-  NEED_CHUNKS {file, ranges}*     (chunks not held)
//!   [uni] CHUNK_RUN ...                  ->  each chunk hashed, repacked into native xorbs
//!                                       <-  ACK per verified run
//!                                       <-  RESULT  |  ERROR
//! ```
//! Xorb mode (v1 behaviour): `NEED {reconstructions, xorbs}` rounds answered with native shard and
//! whole xorb objects.
//!
//! `FINALIZE` from the brief is implicit: once the receiver holds everything the manifest needs it
//! reconstructs, verifies, and installs without further instruction.

use serde::{Deserialize, Serialize};
use xet_local::{Manifest, MerkleHash};

use crate::error::{Error, Result};

pub const PROTOCOL_VERSION: u32 = 2;

const MIB: u64 = 1024 * 1024;

/// Largest control frame (bounded mainly by manifest size).
pub const MAX_CONTROL_FRAME: u32 = 64 * MIB as u32;
/// Largest object header frame.
pub const MAX_OBJECT_HEADER: u32 = 4096;
/// Largest serialized xorb accepted. Upstream caps xorbs at 64 MiB of chunk data; compression
/// headers and the footer add a little.
pub const MAX_XORB_BYTES: u64 = 80 * MIB;
/// Largest shard accepted (reconstruction metadata for one transfer).
pub const MAX_SHARD_BYTES: u64 = 256 * MIB;
/// Largest chunk-list object accepted (36 bytes per chunk: ~460 GiB of data at 64 KiB chunks).
pub const MAX_CHUNK_LISTS_BYTES: u64 = 256 * MIB;
/// Largest chunk run accepted (a run never spans more than one xorb).
pub const MAX_CHUNK_RUN_BYTES: u64 = MAX_XORB_BYTES;

/// How missing content is moved.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferMode {
    /// Only chunks the receiver lacks, repacked into native xorbs on the receiver.
    #[default]
    Chunk,
    /// Whole native xorbs, as stored by the sender.
    Xorb,
}

/// Chunk index ranges `[start, end)` into one file's chunk list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRequest {
    pub file: String,
    pub ranges: Vec<(u32, u32)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Hello {
        protocol_version: u32,
        session_id: String,
        /// Destination directory, relative to the receiver's root.
        dest: String,
        /// Mirror: remove anything under `dest` the manifest does not describe (opt-in).
        #[serde(default)]
        delete: bool,
        #[serde(default)]
        mode: TransferMode,
    },
    HelloOk {
        protocol_version: u32,
    },
    Manifest {
        manifest: Manifest,
    },
    Need {
        /// Xet file hashes whose reconstruction entries (as one native shard) are wanted.
        reconstructions: Vec<String>,
        /// Xorb hashes wanted as whole native xorbs.
        xorbs: Vec<String>,
    },
    NeedChunkLists {
        files: Vec<String>,
    },
    NeedChunks {
        requests: Vec<ChunkRequest>,
    },
    Ack {
        kind: ObjectKind,
        hash: Option<String>,
    },
    Result {
        report: ReceiverReport,
    },
    Error {
        message: String,
    },
}

impl Message {
    pub fn name(&self) -> &'static str {
        match self {
            Message::Hello { .. } => "HELLO",
            Message::HelloOk { .. } => "HELLO_OK",
            Message::Manifest { .. } => "MANIFEST",
            Message::Need { .. } => "NEED",
            Message::NeedChunkLists { .. } => "NEED_CHUNK_LISTS",
            Message::NeedChunks { .. } => "NEED_CHUNKS",
            Message::Ack { .. } => "ACK",
            Message::Result { .. } => "RESULT",
            Message::Error { .. } => "ERROR",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    Shard,
    Xorb,
    ChunkLists,
    ChunkRun,
}

/// First frame of every object stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectHeader {
    pub kind: ObjectKind,
    /// Xorb hash; absent for shards (which the receiver keys by their file entries).
    pub hash: Option<String>,
    /// Exact payload length in bytes.
    pub len: u64,
}

/// What the receiver did, reported back to the sender.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReceiverReport {
    /// Files written into the destination.
    pub files_installed: usize,
    /// Files already identical at the destination, left untouched.
    pub files_unchanged: usize,
    pub dirs_created: usize,
    pub bytes_reconstructed: u64,
    pub shards_received: usize,
    pub xorbs_received: usize,
    pub payload_bytes_received: u64,
    /// Distinct xorbs the manifest needs that were already in the receiver's CAS.
    pub xorbs_already_present: usize,
    /// This transfer continued an interrupted session for the same destination and manifest.
    pub resumed: bool,
    /// Attempts made on this session, including this one.
    pub attempts: u32,
    /// Xorbs committed by earlier, interrupted attempts (resume savings).
    pub prior_xorbs_committed: u64,
    pub prior_xorb_bytes_committed: u64,
    /// With `delete`: entries removed from the destination.
    pub files_deleted: usize,
    pub dirs_deleted: usize,
    pub mode: TransferMode,
    /// Chunk mode: chunks across the new files (with repeats), distinct chunks already held
    /// locally (and their bytes), and distinct chunks received.
    pub chunks_total: usize,
    pub chunks_present: usize,
    pub chunk_bytes_present: u64,
    pub chunks_received: usize,
    /// Native chunk-run bytes and chunk-list bytes received.
    pub chunk_bytes_received: u64,
    pub chunk_list_bytes_received: u64,
    /// Native xorbs built on the receiver from received chunks.
    pub xorbs_packed: usize,
    /// Chunk lists saved by an earlier, interrupted attempt and reused.
    pub chunk_lists_reused: usize,
    /// Reconstruction entries received by an earlier, interrupted attempt and reused instead of
    /// being requested again.
    pub reconstructions_reused: usize,
}

pub(crate) fn to_hex(hashes: impl IntoIterator<Item = MerkleHash>) -> Vec<String> {
    hashes.into_iter().map(|h| h.hex()).collect()
}

pub(crate) fn parse_hash(s: &str) -> Result<MerkleHash> {
    MerkleHash::from_hex(s).map_err(|_| Error::Protocol(format!("invalid hash {s:?}")))
}
