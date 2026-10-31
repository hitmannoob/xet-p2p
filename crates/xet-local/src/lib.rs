//! Offline Xet storage for `xet-p2p`.
//!
//! Wraps upstream `xet-core` (pinned `=1.8.0`) so the rest of the project never touches its
//! `simulation`-module `LocalClient` directly. All chunking, hashing, xorb and shard encoding,
//! deduplication, and reconstruction are upstream's; this crate adds path handling, a file-tree
//! manifest, and verified atomic installation.

mod cas;
pub mod chunks;
mod error;
mod exchange;
pub mod gc;
mod index;
pub mod manifest;
mod walk;

pub use cas::{
    IngestReport, LocalCas, MissingContent, ReconstructOptions, ReconstructReport,
    create_confined_dir,
};
pub use error::{Error, Result};
pub use exchange::PendingShard;
pub use manifest::{Entry, EntryKind, Manifest};
pub use xet_core_structures::merklehash::MerkleHash;
pub use xet_data::deduplication::DeduplicationMetrics;
