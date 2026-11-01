//! Persistent chunk index: chunk hash → (xorb hash, chunk index) for every xorb in a local CAS.
//!
//! Upstream's local CAS indexes only global-dedup-eligible chunks (about 1 in 1024), which cannot
//! answer "do I already hold this chunk?". This index can. It lives beside upstream's state in an
//! embedded redb database, is updated whenever this crate commits a xorb, and is reconciled with
//! the xorbs on disk (e.g. ones created by ingest) by [`ChunkIndex::sync`].

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use xet_core_structures::merklehash::MerkleHash;

use crate::error::{Error, Result};

const CHUNKS: TableDefinition<[u8; 32], ([u8; 32], u32)> = TableDefinition::new("chunks");
const INDEXED_XORBS: TableDefinition<[u8; 32], u32> = TableDefinition::new("indexed_xorbs");

/// Where a chunk is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkLocation {
    pub xorb: MerkleHash,
    pub index: u32,
}

pub(crate) struct ChunkIndex {
    db: Arc<Database>,
}

impl ChunkIndex {
    pub fn open(path: &Path) -> Result<Self> {
        let db = shared_db(path)?;
        let txn = db.begin_write().map_err(db_err)?;
        txn.open_table(CHUNKS).map_err(db_err)?;
        txn.open_table(INDEXED_XORBS).map_err(db_err)?;
        txn.commit().map_err(db_err)?;
        Ok(Self { db })
    }

    pub fn is_indexed(&self, xorb: &MerkleHash) -> Result<bool> {
        let txn = self.db.begin_read().map_err(db_err)?;
        let table = txn.open_table(INDEXED_XORBS).map_err(db_err)?;
        Ok(table.get(key(xorb)).map_err(db_err)?.is_some())
    }

    /// Records every chunk of `xorb` (in order) in one durable transaction.
    pub fn insert_xorb(&self, xorb: &MerkleHash, chunks: &[MerkleHash]) -> Result<()> {
        let txn = self.db.begin_write().map_err(db_err)?;
        {
            let mut table = txn.open_table(CHUNKS).map_err(db_err)?;
            for (i, chunk) in chunks.iter().enumerate() {
                table
                    .insert(key(chunk), (key(xorb), i as u32))
                    .map_err(db_err)?;
            }
            let mut indexed = txn.open_table(INDEXED_XORBS).map_err(db_err)?;
            indexed
                .insert(key(xorb), chunks.len() as u32)
                .map_err(db_err)?;
        }
        txn.commit().map_err(db_err)
    }

    /// Forgets `xorb`: drops its chunks' entries that still point at it, and its indexed marker.
    pub fn remove_xorb(&self, xorb: &MerkleHash, chunks: &[MerkleHash]) -> Result<()> {
        let txn = self.db.begin_write().map_err(db_err)?;
        {
            let mut table = txn.open_table(CHUNKS).map_err(db_err)?;
            for chunk in chunks {
                let points_here = table
                    .get(key(chunk))
                    .map_err(db_err)?
                    .is_some_and(|v| v.value().0 == key(xorb));
                if points_here {
                    table.remove(key(chunk)).map_err(db_err)?;
                }
            }
            let mut indexed = txn.open_table(INDEXED_XORBS).map_err(db_err)?;
            indexed.remove(key(xorb)).map_err(db_err)?;
        }
        txn.commit().map_err(db_err)
    }

    /// Locations of whichever of `chunks` are indexed.
    pub fn lookup<'a>(
        &self,
        chunks: impl IntoIterator<Item = &'a MerkleHash>,
    ) -> Result<HashMap<MerkleHash, ChunkLocation>> {
        let txn = self.db.begin_read().map_err(db_err)?;
        let table = txn.open_table(CHUNKS).map_err(db_err)?;
        let mut out = HashMap::new();
        let mut seen = HashSet::new();
        for chunk in chunks {
            if !seen.insert(*chunk) {
                continue;
            }
            if let Some(v) = table.get(key(chunk)).map_err(db_err)? {
                let (xorb, index) = v.value();
                out.insert(
                    *chunk,
                    ChunkLocation {
                        xorb: MerkleHash::from(&xorb),
                        index,
                    },
                );
            }
        }
        Ok(out)
    }
}

fn key(h: &MerkleHash) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(h.as_bytes());
    out
}

/// One `Database` per path per process (redb refuses a second open of the same file).
pub(crate) fn shared_db(path: &Path) -> Result<Arc<Database>> {
    static CACHE: Mutex<Vec<(PathBuf, Weak<Database>)>> = Mutex::new(Vec::new());
    let mut cache = CACHE.lock().unwrap();
    cache.retain(|(_, w)| w.strong_count() > 0);
    if let Some(db) = cache
        .iter()
        .find(|(p, _)| p == path)
        .and_then(|(_, w)| w.upgrade())
    {
        return Ok(db);
    }
    let db = Arc::new(Database::create(path).map_err(db_err)?);
    cache.push((path.to_owned(), Arc::downgrade(&db)));
    Ok(db)
}

fn db_err(e: impl std::fmt::Display) -> Error {
    Error::Xet(format!("chunk index: {e}"))
}
