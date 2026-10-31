//! Offline Xet storage for `xet-p2p`.
//!
//! Wraps upstream `xet-core` (pinned `=1.8.0`) so the rest of the project never touches its
//! `simulation`-module `LocalClient` directly. All chunking, hashing, xorb and shard encoding,
//! deduplication, and reconstruction are upstream's; this crate adds path handling, a file-tree
//! manifest, and verified atomic installation.

mod cas;
mod error;
pub mod manifest;
mod walk;

pub use cas::{IngestReport, LocalCas, MissingContent, ReconstructReport};
pub use error::{Error, Result};
pub use manifest::{Entry, EntryKind, Manifest};
pub use xet_core_structures::merklehash::MerkleHash;
pub use xet_data::deduplication::DeduplicationMetrics;
