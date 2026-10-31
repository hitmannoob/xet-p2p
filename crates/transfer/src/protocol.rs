//! The xet-p2p control protocol (version 1).
//!
//! One bidirectional control stream, opened by the sender, carries CBOR [`Message`] frames.
//! Objects travel on unidirectional streams opened by the sender: an [`ObjectHeader`] frame
//! followed by the object's native Xet bytes, unchanged.
//!
//! ```text
//! sender                                receiver
//!   HELLO {version, session, dest}  ->
//!                                   <-  HELLO_OK
//!   MANIFEST                        ->
//!                                   <-  NEED {reconstructions, xorbs}   (0..n rounds)
//!   [uni] OBJECT shard / xorb ...   ->
//!                                   <-  ACK per verified, committed object
//!                                       (receiver: verify, reconstruct, atomically install)
//!                                   <-  RESULT  |  ERROR
//! ```
//!
//! `FINALIZE` from the brief is implicit: once the receiver holds everything the manifest needs it
//! reconstructs, verifies, and installs without further instruction.

use serde::{Deserialize, Serialize};
use xet_local::{Manifest, MerkleHash};

use crate::error::{Error, Result};

pub const PROTOCOL_VERSION: u32 = 1;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Hello {
        protocol_version: u32,
        session_id: String,
        /// Destination directory, relative to the receiver's root.
        dest: String,
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
pub struct ReceiverReport {
    pub files_installed: usize,
    pub dirs_created: usize,
    pub bytes_reconstructed: u64,
    pub shards_received: usize,
    pub xorbs_received: usize,
    pub payload_bytes_received: u64,
}

pub(crate) fn to_hex(hashes: impl IntoIterator<Item = MerkleHash>) -> Vec<String> {
    hashes.into_iter().map(|h| h.hex()).collect()
}

pub(crate) fn parse_hash(s: &str) -> Result<MerkleHash> {
    MerkleHash::from_hex(s).map_err(|_| Error::Protocol(format!("invalid hash {s:?}")))
}
