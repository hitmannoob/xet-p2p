//! Moving native Xet objects between two local CAS instances.
//!
//! The sender exports reconstruction metadata as a native shard and reads xorbs as their stored
//! bytes. The receiver treats everything it gets as untrusted: xorbs are fully re-hashed before
//! storage, and a shard's file entries are registered only after each file hash has been recomputed
//! from the chunk hashes of xorbs already verified in the local CAS.

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::io::Cursor;

use xet_client::cas_client::{Client, DirectAccessClient};
use xet_core_structures::merklehash::{MerkleHash, file_hash};
use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;
use xet_core_structures::metadata_shard::shard_in_memory::MDBInMemoryShard;
use xet_core_structures::xorb_object::{SerializedXorbObject, XorbObject};

use crate::cas::LocalCas;
use crate::error::{Error, Result};

/// Reconstruction metadata received from a peer: parsed, but neither trusted nor registered.
#[derive(Debug, Clone)]
pub struct PendingShard {
    files: Vec<MDBFileInfo>,
}

impl PendingShard {
    /// Parses a native shard and keeps its file entries, which must be exactly `expected`.
    ///
    /// Any xorb/chunk-index section in the shard is ignored: the receiver derives that from xorbs
    /// it has verified itself, never from a peer's claims.
    pub fn parse(bytes: &[u8], expected: &BTreeSet<MerkleHash>) -> Result<Self> {
        let shard = MDBInMemoryShard::from_reader(&mut Cursor::new(bytes)).map_err(|e| {
            Error::Verification {
                path: "<shard>".into(),
                reason: format!("malformed shard: {e}"),
            }
        })?;
        let got: BTreeSet<MerkleHash> = shard.file_content.keys().copied().collect();
        if &got != expected {
            return Err(Error::Verification {
                path: "<shard>".into(),
                reason: format!(
                    "shard describes {} file(s), {} requested; sets differ",
                    got.len(),
                    expected.len()
                ),
            });
        }
        Ok(Self {
            files: shard.file_content.into_values().collect(),
        })
    }

    /// Parses a shard saved by an earlier, interrupted session and keeps only the entries in
    /// `expected`, all of which must be present. Entries are still fully verified at commit.
    pub fn parse_containing(bytes: &[u8], expected: &BTreeSet<MerkleHash>) -> Result<Self> {
        let mut shard = MDBInMemoryShard::from_reader(&mut Cursor::new(bytes)).map_err(|e| {
            Error::Verification {
                path: "<saved shard>".into(),
                reason: format!("malformed shard: {e}"),
            }
        })?;
        let mut files = Vec::with_capacity(expected.len());
        for hash in expected {
            files.push(
                shard
                    .file_content
                    .remove(hash)
                    .ok_or_else(|| Error::Verification {
                        path: "<saved shard>".into(),
                        reason: format!("no entry for file {}", hash.hex()),
                    })?,
            );
        }
        Ok(Self { files })
    }

    pub fn num_files(&self) -> usize {
        self.files.len()
    }

    /// Every xorb any file in this shard references.
    pub fn referenced_xorbs(&self) -> BTreeSet<MerkleHash> {
        self.files
            .iter()
            .flat_map(|f| f.segments.iter().map(|s| s.xorb_hash))
            .collect()
    }
}

impl LocalCas {
    /// Every xorb referenced by the reconstructions of `files` in this CAS.
    pub async fn referenced_xorbs(
        &self,
        files: &BTreeSet<MerkleHash>,
    ) -> Result<BTreeSet<MerkleHash>> {
        let mut out = BTreeSet::new();
        for info in self.file_infos(files).await? {
            out.extend(info.segments.iter().map(|s| s.xorb_hash));
        }
        Ok(out)
    }

    /// Builds a native shard holding the reconstruction entries for `files`.
    pub async fn export_shard(&self, files: &BTreeSet<MerkleHash>) -> Result<Vec<u8>> {
        let mut shard = MDBInMemoryShard::default();
        for info in self.file_infos(files).await? {
            shard.add_file_reconstruction_info(info).map_err(xet_err)?;
        }
        shard.to_bytes().map_err(xet_err)
    }

    /// The stored, serialized bytes of a xorb, exactly as they go on the wire.
    pub async fn read_xorb(&self, hash: &MerkleHash) -> Result<Vec<u8>> {
        Ok(self
            .client
            .get_xorb_raw_bytes(hash, None)
            .await
            .map_err(xet_err)?
            .into())
    }

    /// The subset of `xorbs` not present in this CAS.
    pub async fn absent_xorbs<'a>(
        &self,
        xorbs: impl IntoIterator<Item = &'a MerkleHash>,
    ) -> Result<BTreeSet<MerkleHash>> {
        let mut out = BTreeSet::new();
        for hash in xorbs {
            if !self.client.xorb_exists(hash).await.map_err(xet_err)? {
                out.insert(*hash);
            }
        }
        Ok(out)
    }

    /// Verifies a serialized xorb against `hash` (every chunk and the xorb hash are recomputed)
    /// and stores it atomically. Rejects anything that does not verify.
    pub async fn commit_xorb(&self, hash: MerkleHash, bytes: Vec<u8>) -> Result<()> {
        let (xorb, bytes) = tokio::task::spawn_blocking(move || {
            let xorb = XorbObject::validate_xorb_object(&mut Cursor::new(&bytes), &hash);
            (xorb, bytes)
        })
        .await
        .map_err(|e| Error::Xet(e.to_string()))?;

        let xorb = xorb.ok().flatten().ok_or_else(|| Error::Verification {
            path: format!("xorb {}", hash.hex()),
            reason: "content does not match its hash or is malformed".into(),
        })?;

        let footer_start = (bytes.len() as u64)
            .checked_sub(size_of::<u32>() as u64 + xorb.info_length as u64)
            .ok_or_else(|| Error::Verification {
                path: format!("xorb {}", hash.hex()),
                reason: "footer length exceeds object".into(),
            })?;
        let num_chunks = xorb.info.num_chunks;
        let serialized = SerializedXorbObject {
            raw_num_bytes: xorb.uncompressed_range_length(0, num_chunks).unwrap_or(0) as u64,
            num_chunks: num_chunks as usize,
            footer_start: Some(footer_start),
            serialized_data: bytes,
            hash,
        };

        let permit = self.client.acquire_upload_permit().await.map_err(xet_err)?;
        self.client
            .upload_xorb("default", serialized, None, permit)
            .await
            .map_err(xet_err)?;
        Ok(())
    }

    /// Registers a peer's reconstruction entries after checking each against local, verified xorbs:
    /// every referenced chunk range must exist, segment sizes must match, and the file hash
    /// recomputed from the chunk hashes must equal the entry's claimed hash.
    pub async fn commit_shard(&self, pending: PendingShard) -> Result<()> {
        let mut footers: HashMap<MerkleHash, XorbObject> = HashMap::new();

        for info in &pending.files {
            let claimed = info.metadata.file_hash;
            let fail = |reason: String| Error::Verification {
                path: format!("file {}", claimed.hex()),
                reason,
            };

            let mut chunks = Vec::new();
            for seg in &info.segments {
                let footer = match footers.entry(seg.xorb_hash) {
                    Entry::Occupied(e) => e.into_mut(),
                    Entry::Vacant(e) => {
                        if !self
                            .client
                            .xorb_exists(&seg.xorb_hash)
                            .await
                            .map_err(xet_err)?
                        {
                            return Err(fail(format!(
                                "references absent xorb {}",
                                seg.xorb_hash.hex()
                            )));
                        }
                        e.insert(
                            self.client
                                .xorb_footer(&seg.xorb_hash)
                                .await
                                .map_err(xet_err)?,
                        )
                    }
                };
                let range = footer
                    .chunk_hash_sizes(seg.chunk_index_start, seg.chunk_index_end)
                    .map_err(|e| {
                        fail(format!(
                            "invalid chunk range in xorb {}: {e}",
                            seg.xorb_hash.hex()
                        ))
                    })?;
                let seg_bytes: u64 = range.iter().map(|(_, size)| size).sum();
                if seg_bytes != seg.unpacked_segment_bytes as u64 {
                    return Err(fail(format!(
                        "segment claims {} bytes, chunks hold {seg_bytes}",
                        seg.unpacked_segment_bytes
                    )));
                }
                chunks.extend(range);
            }

            let computed = file_hash(&chunks);
            if computed != claimed {
                return Err(fail(format!("recomputed file hash {}", computed.hex())));
            }
        }

        let mut shard = MDBInMemoryShard::default();
        for info in pending.files {
            shard.add_file_reconstruction_info(info).map_err(xet_err)?;
        }
        let bytes = shard.to_bytes().map_err(xet_err)?;
        let permit = self.client.acquire_upload_permit().await.map_err(xet_err)?;
        self.client
            .upload_shard(bytes.into(), permit, None)
            .await
            .map_err(xet_err)?;
        Ok(())
    }

    async fn file_infos(&self, files: &BTreeSet<MerkleHash>) -> Result<Vec<MDBFileInfo>> {
        let mut out = Vec::with_capacity(files.len());
        for hash in files {
            let (info, _shard) = self
                .client
                .get_file_reconstruction_info(hash)
                .await
                .map_err(xet_err)?
                .ok_or(Error::MissingContent { files: 1, xorbs: 0 })?;
            out.push(info);
        }
        Ok(out)
    }
}

fn xet_err(e: impl std::fmt::Display) -> Error {
    Error::Xet(e.to_string())
}
