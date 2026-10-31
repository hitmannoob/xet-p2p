//! Durable receiver-side state for resuming interrupted pushes.
//!
//! Two kinds of state live under the session directory:
//!
//! - **Sessions** (`<key>.json`), keyed by destination plus a digest of the manifest, so retrying
//!   the same push finds its progress record (attempts, xorbs committed). Deleted on success.
//! - **Pending reconstruction entries** (`pending/<file-hash>.shard`): single-file native shards
//!   received from a peer but not yet registered in the CAS (registration waits for their xorbs).
//!   Keyed by file hash, not session, so a push whose source changed after an interruption still
//!   reuses the entries of files that did not change. Removed once registered.
//!
//! Committed xorbs need no session state: they are in the CAS and found by lookup.
//! Everything idle for [`STATE_MAX_AGE`] is discarded when the store opens.

use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use xet_local::{Manifest, MerkleHash, PendingShard};

use crate::error::{Error, Result};

const SESSION_FORMAT_VERSION: u32 = 1;

pub const STATE_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub format_version: u32,
    pub dest: String,
    pub created_unix: u64,
    pub updated_unix: u64,
    /// Number of transfer attempts that used this session, including the current one.
    pub attempts: u32,
    /// Xorbs verified and committed to the CAS across all attempts.
    pub xorbs_committed: u64,
    pub xorb_bytes_committed: u64,
}

pub struct SessionStore {
    dir: PathBuf,
    pending: PathBuf,
}

/// An open session for one transfer.
pub struct Session<'a> {
    store: &'a SessionStore,
    key: String,
    pub record: SessionRecord,
    /// Progress committed by earlier attempts (zero for a fresh session).
    pub prior: SessionRecord,
}

impl SessionStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let pending = dir.as_ref().join("pending");
        fs::create_dir_all(&pending).map_err(io)?;
        let dir = fs::canonicalize(dir.as_ref()).map_err(io)?;
        let pending = dir.join("pending");
        let store = Self { dir, pending };
        store.purge_older_than(STATE_MAX_AGE);
        Ok(store)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Opens the session for (`dest`, `manifest`), creating it if absent.
    pub fn begin(&self, dest: &str, manifest: &Manifest) -> Result<Session<'_>> {
        let key = session_key(dest, manifest)?;
        let now = now_unix();
        let prior = self
            .load_record(&key)
            .filter(|r| r.dest == dest)
            .unwrap_or_default();
        let record = SessionRecord {
            format_version: SESSION_FORMAT_VERSION,
            dest: dest.to_owned(),
            created_unix: if prior.created_unix == 0 {
                now
            } else {
                prior.created_unix
            },
            updated_unix: now,
            attempts: prior.attempts + 1,
            xorbs_committed: prior.xorbs_committed,
            xorb_bytes_committed: prior.xorb_bytes_committed,
        };
        let session = Session {
            store: self,
            key,
            record,
            prior,
        };
        session.save()?;
        Ok(session)
    }

    /// Loads saved reconstruction entries for any of `wanted`. Unreadable entries are ignored
    /// (they will be requested again). Entries stay untrusted until `commit_shard` verifies them.
    pub fn take_pending(
        &self,
        wanted: &BTreeSet<MerkleHash>,
    ) -> (PendingShard, BTreeSet<MerkleHash>) {
        let mut parts = Vec::new();
        let mut found = BTreeSet::new();
        for hash in wanted {
            let one = BTreeSet::from([*hash]);
            if let Some(p) = fs::read(self.pending_path(hash))
                .ok()
                .and_then(|b| PendingShard::parse_containing(&b, &one).ok())
            {
                parts.push(p);
                found.insert(*hash);
            }
        }
        (PendingShard::merge(parts), found)
    }

    /// Saves received entries, one file each, so a later attempt need not request them again.
    pub fn put_pending(&self, shard: &PendingShard) -> Result<()> {
        for (hash, bytes) in shard.split()? {
            write_atomic(&self.pending_path(&hash), &bytes)?;
        }
        Ok(())
    }

    /// Drops entries now registered in the CAS.
    pub fn remove_pending(&self, hashes: &BTreeSet<MerkleHash>) {
        for hash in hashes {
            let _ = fs::remove_file(self.pending_path(hash));
        }
    }

    fn pending_path(&self, hash: &MerkleHash) -> PathBuf {
        self.pending.join(format!("{}.shard", hash.hex()))
    }

    fn path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{key}.json"))
    }

    fn load_record(&self, key: &str) -> Option<SessionRecord> {
        let bytes = fs::read(self.path(key)).ok()?;
        let record: SessionRecord = serde_json::from_slice(&bytes).ok()?;
        (record.format_version == SESSION_FORMAT_VERSION).then_some(record)
    }

    fn purge_older_than(&self, age: Duration) {
        let cutoff = now_unix().saturating_sub(age.as_secs());
        for entry in fs::read_dir(&self.dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "json") {
                let stale = fs::read(&path)
                    .ok()
                    .and_then(|b| serde_json::from_slice::<SessionRecord>(&b).ok())
                    .is_none_or(|r| r.updated_unix < cutoff);
                if stale {
                    let _ = fs::remove_file(&path);
                }
            }
        }
        for entry in fs::read_dir(&self.pending).into_iter().flatten().flatten() {
            let stale = entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .is_none_or(|t| t.as_secs() < cutoff);
            if stale {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

impl Session<'_> {
    pub fn is_resumed(&self) -> bool {
        self.prior.attempts > 0
    }

    pub fn xorb_committed(&mut self, bytes: u64) -> Result<()> {
        self.record.xorbs_committed += 1;
        self.record.xorb_bytes_committed += bytes;
        self.record.updated_unix = now_unix();
        self.save()
    }

    /// The transfer completed: nothing left to resume.
    pub fn finish(self) -> Result<()> {
        fs::remove_file(self.store.path(&self.key)).map_err(io)
    }

    fn save(&self) -> Result<()> {
        let json = serde_json::to_vec_pretty(&self.record)
            .map_err(|e| Error::Protocol(format!("encoding session: {e}")))?;
        write_atomic(&self.store.path(&self.key), &json)
    }
}

/// `sha256(dest || 0x00 || manifest-json)`, hex. Manifests serialize deterministically.
fn session_key(dest: &str, manifest: &Manifest) -> Result<String> {
    let json = serde_json::to_vec(manifest)
        .map_err(|e| Error::Protocol(format!("encoding manifest: {e}")))?;
    let mut h = Sha256::new();
    h.update(dest.as_bytes());
    h.update([0]);
    h.update(&json);
    Ok(hex::encode(h.finalize()))
}

/// Write to a uniquely named temp file, fsync, rename over `path`, then fsync the directory.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = path.parent().expect("state paths have a parent");
    let tmp = dir.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        fs::rename(&tmp, path)?;
        #[cfg(unix)]
        fs::File::open(dir)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result.map_err(io)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

fn io(e: std::io::Error) -> Error {
    Error::Local(xet_local::Error::Io(e))
}
