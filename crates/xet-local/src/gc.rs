//! Garbage collection for a local CAS.
//!
//! The CAS is a cache: every byte in it can be fetched again from a peer. GC therefore never
//! breaks correctness, only costs a future re-transfer, and it keeps the CAS self-consistent:
//!
//! 1. **Orphans**: xorbs no registered file reconstruction references (for example packs from an
//!    abandoned transfer) are deleted once older than a grace period, so recent resumable work is
//!    kept.
//! 2. **Size cap** (optional): if the CAS still exceeds it, the least recently used live xorbs are
//!    evicted. Every file whose reconstruction used an evicted xorb has its entry removed, so a later
//!    transfer simply fetches its content again.
//! 3. Shards with no remaining file entries, and chunk-index entries of deleted xorbs, are removed.
//!
//! "Use" is the xorb file's modification time, refreshed on commit and whenever a reconstruction
//! or push uses the xorb ([`LocalCas::touch_xorbs`]). GC requires exclusive access
//! ([`LocalCas::open_exclusive`]).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::time::{Duration, SystemTime};

use xet_client::cas_client::{Client, DeletionControlableClient, DirectAccessClient};
use xet_core_structures::merklehash::MerkleHash;

use crate::cas::LocalCas;
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct GcOptions {
    /// Unreferenced xorbs younger than this are kept (resumable transfers may still need them).
    pub orphan_grace: Duration,
    /// Evict least recently used xorbs until stored xorb bytes are at most this.
    pub max_bytes: Option<u64>,
    /// Report what would be deleted without deleting anything.
    pub dry_run: bool,
}

impl Default for GcOptions {
    fn default() -> Self {
        Self {
            orphan_grace: Duration::from_secs(7 * 24 * 3600),
            max_bytes: None,
            dry_run: false,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    pub files_before: usize,
    pub xorbs_before: usize,
    pub bytes_before: u64,
    pub orphan_xorbs_deleted: usize,
    pub orphan_bytes_deleted: u64,
    pub orphans_kept_in_grace: usize,
    pub xorbs_evicted: usize,
    pub bytes_evicted: u64,
    pub files_invalidated: usize,
    pub shards_deleted: usize,
    pub bytes_after: u64,
    pub dry_run: bool,
}

struct XorbStat {
    hash: MerkleHash,
    bytes: u64,
    used: SystemTime,
}

impl LocalCas {
    /// Collects garbage as described in the module docs. Requires [`LocalCas::open_exclusive`].
    pub async fn gc(&self, opts: &GcOptions) -> Result<GcReport> {
        if !self.is_exclusive() {
            return Err(Error::Busy(
                "garbage collection needs exclusive access; open with LocalCas::open_exclusive"
                    .into(),
            ));
        }
        let mut report = GcReport {
            dry_run: opts.dry_run,
            ..Default::default()
        };

        // Live set: xorb -> files whose registered reconstruction uses it.
        let entries = self
            .client
            .list_file_shard_entries()
            .await
            .map_err(xet_err)?;
        report.files_before = entries.len();
        let mut users: HashMap<MerkleHash, BTreeSet<MerkleHash>> = HashMap::new();
        for (file, _) in &entries {
            if let Some((info, _)) = self
                .client
                .get_file_reconstruction_info(file)
                .await
                .map_err(xet_err)?
            {
                for seg in &info.segments {
                    users.entry(seg.xorb_hash).or_default().insert(*file);
                }
            }
        }

        let mut stats = Vec::new();
        for hash in self.client.list_xorbs().await.map_err(xet_err)? {
            let Ok(meta) = fs::metadata(self.xorb_path(&hash)) else {
                continue;
            };
            stats.push(XorbStat {
                hash,
                bytes: meta.len(),
                used: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            });
        }
        report.xorbs_before = stats.len();
        report.bytes_before = stats.iter().map(|s| s.bytes).sum();

        // 1. Orphans past the grace period.
        let cutoff = SystemTime::now()
            .checked_sub(opts.orphan_grace)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut doomed: Vec<MerkleHash> = Vec::new();
        let mut live: Vec<XorbStat> = Vec::new();
        for s in stats {
            if users.contains_key(&s.hash) {
                live.push(s);
            } else if s.used < cutoff {
                report.orphan_xorbs_deleted += 1;
                report.orphan_bytes_deleted += s.bytes;
                doomed.push(s.hash);
            } else {
                report.orphans_kept_in_grace += 1;
                live.push(s); // kept, counts toward the size cap but is never evicted below
            }
        }

        // 2. Size cap: evict least recently used *referenced* xorbs.
        let mut remaining: u64 = live.iter().map(|s| s.bytes).sum();
        let mut invalidated: BTreeSet<MerkleHash> = BTreeSet::new();
        if let Some(max) = opts.max_bytes {
            let mut candidates: Vec<&XorbStat> = live
                .iter()
                .filter(|s| users.contains_key(&s.hash))
                .collect();
            candidates.sort_by_key(|s| s.used);
            for s in candidates {
                if remaining <= max {
                    break;
                }
                remaining -= s.bytes;
                report.xorbs_evicted += 1;
                report.bytes_evicted += s.bytes;
                doomed.push(s.hash);
                invalidated.extend(&users[&s.hash]);
            }
        }
        report.files_invalidated = invalidated.len();
        report.bytes_after = remaining;

        // Shards left with no file entries once invalidated files are dropped.
        let mut shard_users: HashMap<MerkleHash, usize> = HashMap::new();
        for (file, shard) in &entries {
            if !invalidated.contains(file) {
                *shard_users.entry(*shard).or_default() += 1;
            }
        }
        let dead_shards: Vec<MerkleHash> = self
            .client
            .list_shard_entries()
            .await
            .map_err(xet_err)?
            .into_iter()
            .filter(|s| !shard_users.contains_key(s))
            .collect();
        report.shards_deleted = dead_shards.len();

        if opts.dry_run {
            return Ok(report);
        }

        // Order matters for crash safety: drop file entries before the xorbs they need, so an
        // interrupted GC never leaves a registered reconstruction pointing at a missing xorb.
        for file in &invalidated {
            self.client.delete_file_entry(file).await.map_err(xet_err)?;
        }
        for shard in &dead_shards {
            self.client
                .remove_shard_dedup_entries(shard)
                .await
                .map_err(xet_err)?;
            self.client
                .delete_shard_entry(shard)
                .await
                .map_err(xet_err)?;
        }
        let doomed_set: HashSet<MerkleHash> = doomed.iter().copied().collect();
        for xorb in &doomed {
            if let Ok(footer) = self.client.xorb_footer(xorb).await {
                self.index.remove_xorb(xorb, &footer.info.chunk_hashes)?;
            }
            self.client.delete_xorb(xorb).await;
        }
        debug_assert!(doomed_set.len() == doomed.len());
        Ok(report)
    }

    /// Marks xorbs as recently used, for GC's least-recently-used eviction. Best effort.
    pub fn touch_xorbs<'a>(&self, xorbs: impl IntoIterator<Item = &'a MerkleHash>) {
        let now = SystemTime::now();
        for x in xorbs {
            if let Ok(f) = fs::File::open(self.xorb_path(x)) {
                let _ = f.set_modified(now);
            }
        }
    }
}

fn xet_err(e: impl std::fmt::Display) -> Error {
    Error::Xet(e.to_string())
}
