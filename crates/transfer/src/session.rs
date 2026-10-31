//! Durable receiver-side transfer sessions, for resuming interrupted pushes.
//!
//! A session is keyed by the destination plus a digest of the manifest, so retrying the same push
//! (same files, same destination) finds it again whatever the sender's connection id. It records
//! progress and keeps the reconstruction shard already received, so a resumed transfer neither
//! re-requests that metadata nor re-sends xorbs (those are in the CAS and found by lookup).
//! A session is deleted when its transfer completes.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use xet_local::Manifest;

use crate::error::{Error, Result};

const SESSION_FORMAT_VERSION: u32 = 1;

/// Sessions untouched for this long are discarded when the store opens.
pub const SESSION_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

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
}

/// An open session for one transfer.
pub struct Session<'a> {
    store: &'a SessionStore,
    key: String,
    pub record: SessionRecord,
    /// Progress committed by earlier attempts (zero for a fresh session).
    pub prior: SessionRecord,
    pub saved_shard: Option<Vec<u8>>,
}

impl SessionStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_owned();
        fs::create_dir_all(&dir).map_err(io)?;
        let store = Self { dir };
        store.purge_older_than(SESSION_MAX_AGE);
        Ok(store)
    }

    /// Opens the session for (`dest`, `manifest`), creating it if absent.
    pub fn begin(&self, dest: &str, manifest: &Manifest) -> Result<Session<'_>> {
        let key = session_key(dest, manifest)?;
        let now = now_unix();
        let prior = self.load_record(&key).filter(|r| r.dest == dest);
        let saved_shard = prior
            .as_ref()
            .and_then(|_| fs::read(self.path(&key, "shard")).ok());
        let prior = prior.unwrap_or_default();
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
            saved_shard,
        };
        session.save()?;
        Ok(session)
    }

    fn path(&self, key: &str, ext: &str) -> PathBuf {
        self.dir.join(format!("{key}.{ext}"))
    }

    fn load_record(&self, key: &str) -> Option<SessionRecord> {
        let bytes = fs::read(self.path(key, "json")).ok()?;
        let record: SessionRecord = serde_json::from_slice(&bytes).ok()?;
        (record.format_version == SESSION_FORMAT_VERSION).then_some(record)
    }

    fn purge_older_than(&self, age: Duration) {
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };
        let cutoff = now_unix().saturating_sub(age.as_secs());
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "json") {
                let stale = fs::read(&path)
                    .ok()
                    .and_then(|b| serde_json::from_slice::<SessionRecord>(&b).ok())
                    .is_none_or(|r| r.updated_unix < cutoff);
                if stale {
                    let _ = fs::remove_file(path.with_extension("shard"));
                    let _ = fs::remove_file(&path);
                }
            }
        }
    }
}

impl Session<'_> {
    pub fn is_resumed(&self) -> bool {
        self.prior.attempts > 0
    }

    pub fn save_shard(&self, bytes: &[u8]) -> Result<()> {
        write_atomic(&self.store.path(&self.key, "shard"), bytes)
    }

    pub fn xorb_committed(&mut self, bytes: u64) -> Result<()> {
        self.record.xorbs_committed += 1;
        self.record.xorb_bytes_committed += bytes;
        self.record.updated_unix = now_unix();
        self.save()
    }

    /// The transfer completed: nothing left to resume.
    pub fn finish(self) -> Result<()> {
        let _ = fs::remove_file(self.store.path(&self.key, "shard"));
        fs::remove_file(self.store.path(&self.key, "json")).map_err(io)
    }

    fn save(&self) -> Result<()> {
        let json = serde_json::to_vec_pretty(&self.record)
            .map_err(|e| Error::Protocol(format!("encoding session: {e}")))?;
        write_atomic(&self.store.path(&self.key, "json"), &json)
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

/// Write to a temp file, fsync, rename over `path`, then fsync the directory.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().expect("session paths have a parent");
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    {
        let mut f = fs::File::create(&tmp).map_err(io)?;
        f.write_all(bytes).map_err(io)?;
        f.sync_all().map_err(io)?;
    }
    fs::rename(&tmp, path).map_err(io)?;
    #[cfg(unix)]
    fs::File::open(dir).and_then(|d| d.sync_all()).map_err(io)?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
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
