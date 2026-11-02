//! Ingest cache: lets an unchanged source file skip being read, chunked and hashed again.
//!
//! For every file ingested it records the file's [`Stamp`] (size, mtime, ctime, inode, device)
//! with the Xet file hash and SHA-256 computed from it. On the next ingest a file whose stamp is
//! identical reuses those hashes, provided this CAS still holds the file's content. Like git's
//! index, this trusts that a file whose ctime has not moved has not been written: ctime cannot be
//! set by ordinary tools, and every write updates it. Files changed within `RACY_SECS` of being
//! hashed are not recorded (`RACY_SECS`), so a write landing in the same timestamp tick as the read is not
//! missed.
//!
//! Stored in the CAS's redb database; one read and one write transaction per ingest.

use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use xet_core_structures::merklehash::MerkleHash;

use crate::error::{Error, Result};

const INGEST: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ingest_cache");
/// Xet file hash -> key of a destination file this receiver installed or verified with that
/// content (only receiver-owned files: never a sender's source file). Used to make new copies of
/// it without rebuilding from the CAS; trusted only while that file's stamp still matches.
const INSTALLED: TableDefinition<[u8; 32], &[u8]> = TableDefinition::new("installed_by_content");
const RECORD_LEN: usize = 8 + 12 + 12 + 8 + 8 + 32 + 32;
/// Files modified (mtime or ctime) within this many seconds of being hashed are not cached.
const RACY_SECS: i64 = 2;
/// How long after installing a file it may be recorded, on filesystems with sub-second timestamps.
pub(crate) const INSTALL_SETTLE: std::time::Duration = std::time::Duration::from_millis(50);

/// What identifies one version of a file without reading it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Stamp {
    size: u64,
    mtime: (i64, u32),
    ctime: (i64, u32),
    ino: u64,
    dev: u64,
}

impl Stamp {
    /// `None` where the platform does not expose ctime and inode (the cache is then unused).
    pub fn of(m: &std::fs::Metadata) -> Option<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Some(Self {
                size: m.len(),
                mtime: (m.mtime(), m.mtime_nsec() as u32),
                ctime: (m.ctime(), m.ctime_nsec() as u32),
                ino: m.ino(),
                dev: m.dev(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = m;
            None
        }
    }

    /// Whether a file this process just wrote and verified can be recorded at `at`. On a
    /// filesystem with sub-second timestamps (seen as a non-zero nanosecond part) any later write
    /// moves the stamp once a timestamp tick (milliseconds) has passed, so [`INSTALL_SETTLE`]
    /// suffices; elsewhere the usual [`Stamp::settled_before`] rule applies.
    fn settled_after_install(&self, at: SystemTime) -> bool {
        if self.mtime.1 == 0 && self.ctime.1 == 0 {
            return self.settled_before(at);
        }
        let Ok(at) = at.duration_since(SystemTime::UNIX_EPOCH) else {
            return false;
        };
        let ns = |(s, n): (i64, u32)| s as i128 * 1_000_000_000 + n as i128;
        let latest = ns(self.mtime).max(ns(self.ctime));
        latest + INSTALL_SETTLE.as_nanos() as i128 <= at.as_nanos() as i128
    }

    /// Whether the file was last changed well before `hashed_at` (so recording it is safe).
    fn settled_before(&self, hashed_at: SystemTime) -> bool {
        let Ok(at) = hashed_at.duration_since(SystemTime::UNIX_EPOCH) else {
            return false;
        };
        let latest = self.mtime.0.max(self.ctime.0);
        latest.saturating_add(RACY_SECS) < at.as_secs() as i64
    }

    fn encode(&self, xet: &MerkleHash, sha256: &[u8; 32]) -> [u8; RECORD_LEN] {
        let mut out = [0u8; RECORD_LEN];
        let mut w = 0;
        let mut put = |b: &[u8]| {
            out[w..w + b.len()].copy_from_slice(b);
            w += b.len();
        };
        put(&self.size.to_le_bytes());
        put(&self.mtime.0.to_le_bytes());
        put(&self.mtime.1.to_le_bytes());
        put(&self.ctime.0.to_le_bytes());
        put(&self.ctime.1.to_le_bytes());
        put(&self.ino.to_le_bytes());
        put(&self.dev.to_le_bytes());
        put(xet.as_bytes());
        put(sha256);
        out
    }
}

/// Decodes a record into its stamp, Xet hash and SHA-256 (`None` if malformed).
fn decode(b: &[u8]) -> Option<(Stamp, MerkleHash, [u8; 32])> {
    if b.len() != RECORD_LEN {
        return None;
    }
    let u64_at = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
    let i64_at = |i: usize| i64::from_le_bytes(b[i..i + 8].try_into().unwrap());
    let u32_at = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
    let stamp = Stamp {
        size: u64_at(0),
        mtime: (i64_at(8), u32_at(16)),
        ctime: (i64_at(20), u32_at(28)),
        ino: u64_at(32),
        dev: u64_at(40),
    };
    let xet = MerkleHash::from(&<[u8; 32]>::try_from(&b[48..80]).unwrap());
    let sha = <[u8; 32]>::try_from(&b[80..112]).unwrap();
    Some((stamp, xet, sha))
}

/// A file's Xet hash and SHA-256.
pub(crate) type Hashes = (MerkleHash, [u8; 32]);

/// A file hashed by this ingest, to record.
pub(crate) struct Hashed<'a> {
    pub key: &'a [u8],
    pub stamp: Stamp,
    pub xet: MerkleHash,
    pub sha256: [u8; 32],
}

#[derive(Clone)]
pub(crate) struct IngestCache {
    db: Arc<Database>,
}

impl IngestCache {
    pub fn open(db: Arc<Database>) -> Result<Self> {
        let txn = db.begin_write().map_err(db_err)?;
        txn.open_table(INGEST).map_err(db_err)?;
        txn.open_table(INSTALLED).map_err(db_err)?;
        txn.commit().map_err(db_err)?;
        Ok(Self { db })
    }

    /// The recorded hashes for each `(key, stamp)` whose record has exactly that stamp.
    pub fn lookup(&self, files: &[(&[u8], Stamp)]) -> Result<Vec<Option<Hashes>>> {
        let txn = self.db.begin_read().map_err(db_err)?;
        let table = txn.open_table(INGEST).map_err(db_err)?;
        files
            .iter()
            .map(|(key, stamp)| {
                Ok(table
                    .get(*key)
                    .map_err(db_err)?
                    .and_then(|v| decode(v.value()))
                    .filter(|(s, ..)| s == stamp)
                    .map(|(_, xet, sha)| (xet, sha)))
            })
            .collect()
    }

    /// Records `hashed` (those settled before `hashed_at`) and, if `prune` is given as
    /// `(prefix, keep)`, forgets every key under `prefix` not in `keep`.
    pub fn record(
        &self,
        hashed: &[Hashed<'_>],
        hashed_at: SystemTime,
        prune: Option<(&[u8], &std::collections::HashSet<&[u8]>)>,
    ) -> Result<()> {
        self.record_if(hashed, prune, |s| s.settled_before(hashed_at))
    }

    /// Records files this process just installed (see [`Stamp::settled_after_install`]), also as
    /// copies of their content.
    pub fn record_installed(&self, installed: &[Hashed<'_>]) -> Result<()> {
        let now = SystemTime::now();
        self.record_if(installed, None, |s| s.settled_after_install(now))?;
        self.index_copies(installed, |s| s.settled_after_install(now))
    }

    /// Records destination files this receiver just verified unchanged (see [`Self::record`]),
    /// also as copies of their content.
    pub fn record_verified(&self, verified: &[Hashed<'_>], hashed_at: SystemTime) -> Result<()> {
        self.record(verified, hashed_at, None)?;
        self.index_copies(verified, |s| s.settled_before(hashed_at))
    }

    fn index_copies(&self, files: &[Hashed<'_>], settled: impl Fn(&Stamp) -> bool) -> Result<()> {
        let txn = self.db.begin_write().map_err(db_err)?;
        {
            let mut table = txn.open_table(INSTALLED).map_err(db_err)?;
            for h in files.iter().filter(|h| settled(&h.stamp)) {
                let mut k = [0u8; 32];
                k.copy_from_slice(h.xet.as_bytes());
                table.insert(k, h.key).map_err(db_err)?;
            }
        }
        txn.commit().map_err(db_err)
    }

    /// A receiver-owned file recorded with content `xet` and SHA-256 `sha256`, if its stamp still
    /// matches the record (so it has not been written since it was verified): its path and size.
    pub fn installed_copy(
        &self,
        xet: &MerkleHash,
        sha256: &[u8; 32],
    ) -> Result<Option<(std::path::PathBuf, std::fs::Metadata)>> {
        let mut k = [0u8; 32];
        k.copy_from_slice(xet.as_bytes());
        let key = {
            let txn = self.db.begin_read().map_err(db_err)?;
            let table = txn.open_table(INSTALLED).map_err(db_err)?;
            match table.get(k).map_err(db_err)? {
                Some(v) => v.value().to_vec(),
                None => return Ok(None),
            }
        };
        let path = path_of(&key);
        let Ok(m) = std::fs::symlink_metadata(&path) else {
            return Ok(None);
        };
        let Some(stamp) = Stamp::of(&m).filter(|_| m.is_file()) else {
            return Ok(None);
        };
        Ok(self
            .lookup(&[(key.as_slice(), stamp)])?
            .into_iter()
            .next()
            .flatten()
            .filter(|(x, sha)| x == xet && sha == sha256)
            .map(|_| (path, m)))
    }

    fn record_if(
        &self,
        hashed: &[Hashed<'_>],
        prune: Option<(&[u8], &std::collections::HashSet<&[u8]>)>,
        settled: impl Fn(&Stamp) -> bool,
    ) -> Result<()> {
        let txn = self.db.begin_write().map_err(db_err)?;
        {
            let mut table = txn.open_table(INGEST).map_err(db_err)?;
            if let Some((prefix, keep)) = prune {
                let mut stale = Vec::new();
                for item in table.range(prefix..).map_err(db_err)? {
                    let (k, _) = item.map_err(db_err)?;
                    let k = k.value();
                    if !k.starts_with(prefix) {
                        break;
                    }
                    if !keep.contains(k) {
                        stale.push(k.to_vec());
                    }
                }
                for k in stale {
                    table.remove(k.as_slice()).map_err(db_err)?;
                }
            }
            for h in hashed {
                if settled(&h.stamp) {
                    table
                        .insert(h.key, h.stamp.encode(&h.xet, &h.sha256).as_slice())
                        .map_err(db_err)?;
                } else {
                    table.remove(h.key).map_err(db_err)?;
                }
            }
        }
        txn.commit().map_err(db_err)
    }
}

/// Inverse of [`key`].
fn path_of(key: &[u8]) -> std::path::PathBuf {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        std::path::PathBuf::from(std::ffi::OsStr::from_bytes(key))
    }
    #[cfg(not(unix))]
    {
        std::path::PathBuf::from(String::from_utf8_lossy(key).into_owned())
    }
}

/// The cache key for a file: its absolute path's bytes.
pub(crate) fn key(path: &Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().to_vec()
    }
    #[cfg(not(unix))]
    {
        path.to_string_lossy().into_owned().into_bytes()
    }
}

fn db_err(e: impl std::fmt::Display) -> Error {
    Error::Xet(format!("ingest cache: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_and_racy_files_are_not_settled() {
        let s = Stamp {
            size: 7,
            mtime: (100, 5),
            ctime: (101, 6),
            ino: 9,
            dev: 3,
        };
        let xet = MerkleHash::from(&[4u8; 32]);
        let (d, x, sha) = decode(&s.encode(&xet, &[8u8; 32])).unwrap();
        assert_eq!((d, x, sha), (s, xet, [8u8; 32]));
        let at = |secs| SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs);
        assert!(s.settled_before(at(104)));
        assert!(!s.settled_before(at(103)));
        assert!(decode(&[0u8; 5]).is_none());
    }
}
