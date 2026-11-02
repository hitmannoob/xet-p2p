//! Chunk-level exchange: move only the chunks a receiver lacks, then repack them into native xorbs.
//!
//! The sender describes each new file by its ordered chunk list, which the receiver checks against
//! the file's Xet hash. The receiver requests the chunks its index does not hold; the sender answers
//! with the chunks' native serialized bytes (header + compressed data, exactly as stored in its
//! xorbs). The receiver decompresses and hashes every chunk, concatenates the native bytes into new
//! xorbs with upstream's footer builder (no recompression), and writes a fresh native reconstruction
//! for each file against its own xorbs. The file hash depends only on chunk hashes and sizes, so it
//! is unchanged, and the new reconstruction is verified like any peer shard before it is registered.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Range;

use xet_client::cas_client::Client;
use xet_core_structures::merklehash::{MerkleHash, compute_data_hash, file_hash};
use xet_core_structures::metadata_shard::Sha256;
use xet_core_structures::metadata_shard::chunk_verification::range_hash_from_chunks;
use xet_core_structures::metadata_shard::file_structs::{
    FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, FileVerificationEntry,
    MDBFileInfo,
};
use xet_core_structures::xorb_object::constants::MAX_CHUNK_SIZE;
use xet_core_structures::xorb_object::{
    XORB_CHUNK_HEADER_LENGTH, XorbObject, parse_chunk_header, reconstruct_xorb_with_footer,
};

use crate::cas::LocalCas;
use crate::error::{Error, Result};
use crate::exchange::PendingShard;
pub use crate::index::ChunkLocation;

/// A file's ordered chunks: (chunk hash, uncompressed size).
pub type ChunkList = Vec<(MerkleHash, u64)>;

/// Encoded size of one chunk-list entry: hash + u32 size.
const ENTRY_BYTES: usize = 32 + 4;

/// Flush a repacked xorb once it holds this much uncompressed data or this many chunks.
pub const PACK_TARGET_BYTES: u64 = 16 * 1024 * 1024;
pub const PACK_MAX_CHUNKS: usize = 8 * 1024;

/// Encodes chunk lists as `{ file_hash[32], n: u32 LE, n × (chunk_hash[32], size: u32 LE) }*`.
pub fn encode_chunk_lists(lists: &[(MerkleHash, ChunkList)]) -> Vec<u8> {
    let total: usize = lists.iter().map(|(_, l)| 36 + l.len() * ENTRY_BYTES).sum();
    let mut out = Vec::with_capacity(total);
    for (file, list) in lists {
        out.extend_from_slice(file.as_bytes());
        out.extend_from_slice(&(list.len() as u32).to_le_bytes());
        for (hash, size) in list {
            out.extend_from_slice(hash.as_bytes());
            out.extend_from_slice(&(*size as u32).to_le_bytes());
        }
    }
    out
}

/// Decodes chunk lists for exactly the files in `expected`, verifying each list against its file
/// hash. A list that does not hash to its file is rejected, so a peer cannot misdescribe a file.
pub fn decode_chunk_lists(
    bytes: &[u8],
    expected: &BTreeSet<MerkleHash>,
) -> Result<Vec<(MerkleHash, ChunkList)>> {
    let fail = |reason: String| Error::Verification {
        path: "<chunk lists>".into(),
        reason,
    };
    let mut pos = 0;
    let mut take = |n: usize| -> Result<&[u8]> {
        let s = bytes
            .get(pos..pos + n)
            .ok_or_else(|| fail("truncated".into()))?;
        pos += n;
        Ok(s)
    };
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    while seen.len() < expected.len() {
        let file = MerkleHash::from(&<[u8; 32]>::try_from(take(32)?).unwrap());
        let n = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        if !expected.contains(&file) || !seen.insert(file) {
            return Err(fail(format!(
                "unrequested or duplicate file {}",
                file.hex()
            )));
        }
        let mut list = Vec::with_capacity(n.min(bytes.len() / ENTRY_BYTES));
        for _ in 0..n {
            let hash = MerkleHash::from(&<[u8; 32]>::try_from(take(32)?).unwrap());
            let size = u32::from_le_bytes(take(4)?.try_into().unwrap()) as u64;
            if size == 0 || size > *MAX_CHUNK_SIZE as u64 {
                return Err(fail(format!("chunk size {size} out of range")));
            }
            list.push((hash, size));
        }
        if file_hash(&list) != file {
            return Err(fail(format!(
                "chunk list does not hash to file {}",
                file.hex()
            )));
        }
        out.push((file, list));
    }
    if take(1).is_ok() {
        return Err(fail("trailing bytes".into()));
    }
    Ok(out)
}

/// Native serialized chunks `[start, end)` of one sender xorb, at bytes `byte_range` of it.
#[derive(Debug, Clone)]
pub struct ChunkRun {
    pub xorb: MerkleHash,
    pub start: u32,
    pub end: u32,
    pub byte_range: Range<u64>,
    /// Filled by [`LocalCas::read_chunk_run`].
    pub bytes: Vec<u8>,
}

/// One chunk parsed and verified from a run.
#[derive(Debug, Clone)]
pub struct VerifiedChunk {
    pub hash: MerkleHash,
    pub size: u32,
    /// Byte span of the chunk's native serialization (header + compressed data) in the run.
    pub span: Range<usize>,
}

/// Parses native serialized chunks, decompressing and hashing each one. CPU-bound.
pub fn parse_chunk_run(bytes: &[u8]) -> Result<Vec<VerifiedChunk>> {
    let mut out = Vec::new();
    for_each_chunk(bytes, |hash, plain, span| {
        out.push(VerifiedChunk {
            hash,
            size: plain.len() as u32,
            span,
        });
        Ok(())
    })?;
    Ok(out)
}

/// [`parse_chunk_run`], also returning the uncompressed bytes of each chunk for which `keep` is
/// true (for writing them out directly).
pub fn parse_chunk_run_keeping(
    bytes: &[u8],
    keep: impl Fn(&MerkleHash) -> bool,
) -> Result<Vec<(VerifiedChunk, Option<Vec<u8>>)>> {
    let mut out = Vec::new();
    for_each_chunk(bytes, |hash, plain, span| {
        let kept = keep(&hash).then(|| plain.to_vec());
        out.push((
            VerifiedChunk {
                hash,
                size: plain.len() as u32,
                span,
            },
            kept,
        ));
        Ok(())
    })?;
    Ok(out)
}

/// Decodes native serialized chunks, calling `f(hash, uncompressed, span)` for each in order.
/// Sizes are bounded by `MAX_CHUNK_SIZE` before decompression.
pub(crate) fn for_each_chunk(
    bytes: &[u8],
    mut f: impl FnMut(MerkleHash, &[u8], Range<usize>) -> Result<()>,
) -> Result<()> {
    for_each_plain_chunk(bytes, |plain, span| {
        f(compute_data_hash(plain), plain, span)
    })
}

/// Like [`for_each_chunk`] without hashing: bounds-checks and decompresses each chunk.
pub(crate) fn for_each_plain_chunk(
    bytes: &[u8],
    mut f: impl FnMut(&[u8], Range<usize>) -> Result<()>,
) -> Result<()> {
    let fail = |reason: String| Error::Verification {
        path: "<chunk run>".into(),
        reason,
    };
    let max = *MAX_CHUNK_SIZE;
    let mut pos = 0;
    while pos < bytes.len() {
        let header_bytes: [u8; XORB_CHUNK_HEADER_LENGTH] = bytes
            .get(pos..pos + XORB_CHUNK_HEADER_LENGTH)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| fail("truncated chunk header".into()))?;
        let header =
            parse_chunk_header(header_bytes).map_err(|e| fail(format!("bad chunk header: {e}")))?;
        let compressed = header.get_compressed_length() as usize;
        let size = header.get_uncompressed_length() as usize;
        if size == 0 || size > max || compressed > max + 4096 {
            return Err(fail(format!(
                "chunk sizes out of range ({compressed}/{size})"
            )));
        }
        let data_start = pos + XORB_CHUNK_HEADER_LENGTH;
        let data = bytes
            .get(data_start..data_start + compressed)
            .ok_or_else(|| fail("truncated chunk data".into()))?;
        let plain = header
            .get_compression_scheme()
            .and_then(|s| s.decompress_from_slice(data))
            .map_err(|e| fail(format!("cannot decompress chunk: {e}")))?;
        if plain.len() != size {
            return Err(fail("decompressed size differs from header".into()));
        }
        f(&plain, pos..data_start + compressed)?;
        pos = data_start + compressed;
    }
    Ok(())
}

/// Accumulates verified native chunks into the body of a new xorb.
#[derive(Default)]
pub struct XorbPacker {
    buf: Vec<u8>,
    chunks: usize,
    uncompressed: u64,
}

impl XorbPacker {
    pub fn push(&mut self, native_chunk: &[u8], size: u32) {
        self.buf.extend_from_slice(native_chunk);
        self.chunks += 1;
        self.uncompressed += size as u64;
    }

    pub fn is_full(&self) -> bool {
        self.uncompressed >= PACK_TARGET_BYTES || self.chunks >= PACK_MAX_CHUNKS
    }

    pub fn is_empty(&self) -> bool {
        self.chunks == 0
    }

    pub fn take(&mut self) -> Vec<u8> {
        self.chunks = 0;
        self.uncompressed = 0;
        std::mem::take(&mut self.buf)
    }
}

/// What a receiver must fetch to hold every chunk of some files.
#[derive(Debug, Clone, Default)]
pub struct ChunkPlan {
    /// Per file, ranges of chunk indices (into its chunk list) to request. Each missing chunk hash
    /// is requested exactly once, at its first occurrence.
    pub requests: Vec<(MerkleHash, Vec<(u32, u32)>)>,
    /// The distinct missing chunks, each with its uncompressed size.
    pub missing: HashMap<MerkleHash, u64>,
    /// Distinct chunks (and their uncompressed bytes) already held locally.
    pub present_chunks: usize,
    pub present_bytes: u64,
    pub total_chunks: usize,
}

impl LocalCas {
    // ---- sender ----

    /// The ordered chunk list of each file, from its local reconstruction.
    pub async fn chunk_lists(
        &self,
        files: &BTreeSet<MerkleHash>,
    ) -> Result<Vec<(MerkleHash, ChunkList)>> {
        let mut footers = HashMap::new();
        let mut out = Vec::with_capacity(files.len());
        for file in files {
            let info = self.file_info(file).await?;
            let mut list = Vec::new();
            for seg in &info.segments {
                let footer = self.footer_cached(&mut footers, &seg.xorb_hash).await?;
                list.extend(
                    footer
                        .chunk_hash_sizes(seg.chunk_index_start, seg.chunk_index_end)
                        .map_err(xet_err)?,
                );
            }
            out.push((*file, list));
        }
        Ok(out)
    }

    /// Native bytes of chunks `ranges` (indices into `file`'s chunk list), as runs of contiguous
    /// chunks from the xorbs that hold them.
    pub async fn read_chunk_ranges(
        &self,
        file: &MerkleHash,
        ranges: &[(u32, u32)],
    ) -> Result<Vec<ChunkRun>> {
        let mut runs = self.plan_chunk_runs(file, ranges).await?;
        for run in &mut runs {
            self.read_chunk_run(run).await?;
        }
        Ok(runs)
    }

    /// Maps `ranges` of `file`'s chunk list to runs in this CAS's xorbs, without reading data.
    pub async fn plan_chunk_runs(
        &self,
        file: &MerkleHash,
        ranges: &[(u32, u32)],
    ) -> Result<Vec<ChunkRun>> {
        let info = self.file_info(file).await?;
        let mut footers = HashMap::new();
        let mut runs: Vec<ChunkRun> = Vec::new();
        let total: u32 = info
            .segments
            .iter()
            .map(|s| s.chunk_index_end - s.chunk_index_start)
            .sum();
        for &(a, b) in ranges {
            if a >= b || b > total {
                return Err(Error::InvalidManifest(format!(
                    "chunk range {a}..{b} outside file {} ({total} chunks)",
                    file.hex()
                )));
            }
            let mut pos = 0u32; // file chunk index at the start of `seg`
            for seg in &info.segments {
                let len = seg.chunk_index_end - seg.chunk_index_start;
                let (lo, hi) = (a.max(pos), b.min(pos + len));
                if lo < hi {
                    let start = seg.chunk_index_start + (lo - pos);
                    let end = seg.chunk_index_start + (hi - pos);
                    // Extend the previous run when it continues in the same xorb.
                    if let Some(last) = runs.last_mut()
                        && last.xorb == seg.xorb_hash
                        && last.end == start
                    {
                        last.end = end;
                    } else {
                        runs.push(ChunkRun {
                            xorb: seg.xorb_hash,
                            start,
                            end,
                            byte_range: 0..0,
                            bytes: Vec::new(),
                        });
                    }
                }
                pos += len;
            }
        }
        for run in &mut runs {
            let footer = self.footer_cached(&mut footers, &run.xorb).await?;
            let (lo, hi) = footer
                .get_byte_offset(run.start, run.end)
                .map_err(xet_err)?;
            run.byte_range = lo as u64..hi as u64;
        }
        Ok(runs)
    }

    /// Reads a planned run's native bytes.
    /// (Upstream's `get_xorb_raw_bytes` reads the whole xorb before slicing; read only the range.)
    pub async fn read_chunk_run(&self, run: &mut ChunkRun) -> Result<()> {
        let path = self.xorb_path(&run.xorb);
        let (lo, hi) = (run.byte_range.start, run.byte_range.end);
        run.bytes = tokio::task::spawn_blocking(move || read_range(&path, lo, hi))
            .await
            .map_err(xet_err)??;
        Ok(())
    }

    // ---- receiver ----

    /// Which chunks of `lists` this CAS lacks, and the ranges to request for them.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn plan_chunks(&self, lists: &[(MerkleHash, ChunkList)]) -> Result<ChunkPlan> {
        let present =
            self.locate_chunks(lists.iter().flat_map(|(_, l)| l.iter().map(|(h, _)| h)))?;
        let mut plan = ChunkPlan::default();
        let mut seen = HashSet::new();
        for (file, list) in lists {
            let mut ranges: Vec<(u32, u32)> = Vec::new();
            for (i, (hash, size)) in list.iter().enumerate() {
                plan.total_chunks += 1;
                if !seen.insert(*hash) {
                    continue;
                }
                if present.contains_key(hash) {
                    plan.present_chunks += 1;
                    plan.present_bytes += size;
                    continue;
                }
                plan.missing.insert(*hash, *size);
                let i = i as u32;
                match ranges.last_mut() {
                    Some(r) if r.1 == i => r.1 = i + 1,
                    _ => ranges.push((i, i + 1)),
                }
            }
            if !ranges.is_empty() {
                plan.requests.push((*file, ranges));
            }
        }
        Ok(plan)
    }

    /// Locations of whichever chunks are held in a xorb present in this CAS.
    pub fn locate_chunks<'a>(
        &self,
        chunks: impl IntoIterator<Item = &'a MerkleHash>,
    ) -> Result<HashMap<MerkleHash, ChunkLocation>> {
        let mut found = self.index.lookup(chunks)?;
        let xorbs: HashSet<MerkleHash> = found.values().map(|l| l.xorb).collect();
        let gone: HashSet<MerkleHash> = xorbs
            .into_iter()
            .filter(|x| !self.xorb_path(x).is_file())
            .collect();
        found.retain(|_, l| !gone.contains(&l.xorb));
        Ok(found)
    }

    /// Builds a native xorb from verified native chunk bytes (no recompression), stores it
    /// durably and indexes it. Returns its hash and chunk count.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn commit_packed(&self, body: Vec<u8>) -> Result<(MerkleHash, usize)> {
        let t = std::time::Instant::now();
        let (xorb, hash, bytes) = tokio::task::spawn_blocking(move || {
            let mut out = Vec::with_capacity(body.len() + 4096);
            reconstruct_xorb_with_footer(&mut out, &body).map(|(x, h)| (x, h, out))
        })
        .await
        .map_err(|e| Error::Xet(e.to_string()))?
        .map_err(xet_err)?;
        let footer = t.elapsed();
        let n = xorb.info.num_chunks as usize;
        self.store_verified_xorb(hash, bytes, &xorb).await?;
        tracing::debug!(
            footer_ms = footer.as_millis() as u64,
            store_ms = (t.elapsed() - footer).as_millis() as u64,
            "packed xorb built and stored"
        );
        Ok((hash, n))
    }

    /// Writes and registers a native reconstruction for each file against chunks held locally.
    /// The result goes through the same verification as a peer's shard.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn register_files(&self, files: &[(MerkleHash, ChunkList, String)]) -> Result<()> {
        let mut infos = Vec::with_capacity(files.len());
        for (file, list, sha256) in files {
            let locs = self.locate_chunks(list.iter().map(|(h, _)| h))?;
            let mut segments: Vec<(MerkleHash, u32, u32, u64, Vec<MerkleHash>)> = Vec::new();
            for (hash, size) in list {
                let loc = locs
                    .get(hash)
                    .ok_or(Error::MissingContent { files: 1, xorbs: 0 })?;
                match segments.last_mut() {
                    Some(s) if s.0 == loc.xorb && s.2 == loc.index => {
                        s.2 += 1;
                        s.3 += size;
                        s.4.push(*hash);
                    }
                    _ => segments.push((loc.xorb, loc.index, loc.index + 1, *size, vec![*hash])),
                }
            }
            let sha = Sha256::from_hex(sha256).map_err(|e| {
                Error::InvalidManifest(format!("bad sha256 for {}: {e}", file.hex()))
            })?;
            infos.push(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(*file, segments.len(), true, true),
                verification: segments
                    .iter()
                    .map(|s| FileVerificationEntry::new(range_hash_from_chunks(&s.4)))
                    .collect(),
                segments: segments
                    .iter()
                    .map(|s| FileDataSequenceEntry::new(s.0, s.3 as u32, s.1, s.2))
                    .collect(),
                metadata_ext: Some(FileMetadataExt::new(sha)),
            });
        }
        self.commit_shard(PendingShard::from_infos(infos)).await
    }

    /// Writes `file`'s bytes to `out` from this CAS: each segment's native chunks are read in one
    /// range read (footers cached), decoded, and checked against the chunk hashes in the xorb's
    /// footer. Returns (bytes written, SHA-256 hex) computed in the same pass.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn write_file_from_cas(
        &self,
        file: &MerkleHash,
        out: &std::path::Path,
    ) -> Result<(u64, String)> {
        // Resolve every segment's byte range first (footers are cached). Then one thread reads and
        // decompresses while another writes and hashes, so the file's SHA-256, the end-to-end
        // check, is the only serial stage. Chunks are not re-hashed here: each was verified when
        // it was stored, and the caller checks this SHA-256 against the manifest before install.
        let info = self.file_info(file).await?;
        let mut segments = Vec::with_capacity(info.segments.len());
        for seg in &info.segments {
            let footer = self.footer(&seg.xorb_hash).await?;
            let (lo, hi) = footer
                .get_byte_offset(seg.chunk_index_start, seg.chunk_index_end)
                .map_err(xet_err)?;
            let chunks = (seg.chunk_index_end - seg.chunk_index_start) as usize;
            segments.push((self.xorb_path(&seg.xorb_hash), lo as u64, hi as u64, chunks));
        }
        let out = out.to_owned();
        let total: u64 = segments.iter().map(|(_, lo, hi, _)| hi - lo).sum();
        tokio::task::spawn_blocking(move || -> Result<(u64, String)> {
            if total < PIPELINE_MIN_BYTES {
                // Small files are decoded inline: a second thread only pays off for large ones.
                let mut decoded = Vec::new();
                decode_segments(segments, &mut |d| {
                    let ok = d.is_ok();
                    decoded.push(d);
                    ok
                });
                return finish_file(&out, decoded);
            }
            let (tx, rx) = std::sync::mpsc::sync_channel::<Result<Vec<u8>>>(4);
            let reader = std::thread::spawn(move || {
                decode_segments(segments, &mut |d| {
                    let ok = d.is_ok();
                    tx.send(d).is_ok() && ok
                })
            });
            let r = finish_file(&out, rx);
            let _ = reader.join();
            r
        })
        .await
        .map_err(xet_err)?
    }

    /// [`Self::write_file_from_cas`] for many small files at once: each xorb they come from is read
    /// once (when the span covering them is not much larger than what they need), not once per
    /// file. Files whose content spans several xorbs are written one by one. Returns each file's
    /// (size, SHA-256), in the order given.
    pub(crate) async fn write_small_files(
        &self,
        files: &[(MerkleHash, std::path::PathBuf)],
    ) -> Result<Vec<(u64, String)>> {
        let mut out: Vec<Option<(u64, String)>> = vec![None; files.len()];
        // xorb -> [(index into files, segment ranges)]
        let mut groups: HashMap<MerkleHash, Vec<(usize, Vec<(u64, u64, usize)>)>> = HashMap::new();
        for (i, (file, tmp)) in files.iter().enumerate() {
            let info = self.file_info(file).await?;
            let first = info.segments.first().map(|s| s.xorb_hash);
            if first.is_none() || info.segments.iter().any(|s| Some(s.xorb_hash) != first) {
                out[i] = Some(self.write_file_from_cas(file, tmp).await?);
                continue;
            }
            let xorb = first.unwrap();
            let footer = self.footer(&xorb).await?;
            let mut ranges = Vec::with_capacity(info.segments.len());
            for seg in &info.segments {
                let (lo, hi) = footer
                    .get_byte_offset(seg.chunk_index_start, seg.chunk_index_end)
                    .map_err(xet_err)?;
                ranges.push((
                    lo as u64,
                    hi as u64,
                    (seg.chunk_index_end - seg.chunk_index_start) as usize,
                ));
            }
            groups.entry(xorb).or_default().push((i, ranges));
        }
        let groups: Vec<_> = groups
            .into_iter()
            .map(|(x, g)| (self.xorb_path(&x), g))
            .collect();
        let tmps: Vec<std::path::PathBuf> = files.iter().map(|(_, t)| t.clone()).collect();
        let written =
            tokio::task::spawn_blocking(move || -> Result<Vec<(usize, (u64, String))>> {
                let (mut done, mut span, mut one) = (Vec::new(), Vec::new(), Vec::new());
                for (path, group) in groups {
                    let lo = group
                        .iter()
                        .flat_map(|(_, r)| r.iter().map(|x| x.0))
                        .min()
                        .unwrap_or(0);
                    let hi = group
                        .iter()
                        .flat_map(|(_, r)| r.iter().map(|x| x.1))
                        .max()
                        .unwrap_or(0);
                    let needed: u64 = group
                        .iter()
                        .flat_map(|(_, r)| r.iter().map(|x| x.1 - x.0))
                        .sum();
                    let whole = hi - lo <= 4 * needed + (1 << 20);
                    if whole {
                        read_range_into(&path, lo, hi, &mut span)?;
                    }
                    for (i, ranges) in group {
                        let mut decoded = Vec::with_capacity(ranges.len());
                        for (a, b, chunks) in ranges {
                            let bytes: &[u8] = if whole {
                                &span[(a - lo) as usize..(b - lo) as usize]
                            } else {
                                read_range_into(&path, a, b, &mut one)?;
                                &one
                            };
                            let mut plain = Vec::with_capacity(bytes.len() * 2);
                            let mut n = 0;
                            for_each_plain_chunk(bytes, |p, _| {
                                plain.extend_from_slice(p);
                                n += 1;
                                Ok(())
                            })?;
                            if n != chunks {
                                return Err(xet_err("segment chunk count differs from its xorb"));
                            }
                            decoded.push(Ok(plain));
                        }
                        done.push((i, finish_file(&tmps[i], decoded)?));
                    }
                }
                Ok(done)
            })
            .await
            .map_err(xet_err)??;
        for (i, r) in written {
            out[i] = Some(r);
        }
        out.into_iter()
            .map(|r| r.ok_or_else(|| xet_err("small file not written")))
            .collect()
    }

    async fn file_info(&self, file: &MerkleHash) -> Result<MDBFileInfo> {
        Ok(self
            .client
            .get_file_reconstruction_info(file)
            .await
            .map_err(xet_err)?
            .ok_or(Error::MissingContent { files: 1, xorbs: 0 })?
            .0)
    }

    async fn footer_cached<'a>(
        &self,
        cache: &'a mut HashMap<MerkleHash, std::sync::Arc<XorbObject>>,
        xorb: &MerkleHash,
    ) -> Result<&'a XorbObject> {
        if !cache.contains_key(xorb) {
            cache.insert(*xorb, self.footer(xorb).await?);
        }
        Ok(&cache[xorb])
    }
}

/// Reads `[lo, hi)` of `path` into `buf` (reused across calls; not zeroed first).
/// A file segment to decode: (xorb path, byte range, number of chunks).
type Segment = (std::path::PathBuf, u64, u64, usize);

/// Reads and decompresses each segment in order, handing each result to `emit` until it returns
/// false (after an error, or when the consumer has gone).
fn decode_segments(segments: Vec<Segment>, emit: &mut dyn FnMut(Result<Vec<u8>>) -> bool) {
    let mut buf = Vec::new();
    for (path, lo, hi, chunks) in segments {
        let decoded = (|| -> Result<Vec<u8>> {
            read_range_into(&path, lo, hi, &mut buf)?;
            let mut plain = Vec::with_capacity(buf.len() * 2);
            let mut n = 0;
            for_each_plain_chunk(&buf, |p, _| {
                plain.extend_from_slice(p);
                n += 1;
                Ok(())
            })?;
            if n != chunks {
                return Err(xet_err("segment chunk count differs from its xorb"));
            }
            Ok(plain)
        })();
        if !emit(decoded) {
            break;
        }
    }
}

/// Files at least this large are decoded on one thread while another writes and hashes.
const PIPELINE_MIN_BYTES: u64 = 8 << 20;

/// Writes the decoded segments arriving on `rx` to `out`, hashing them; returns (size, SHA-256).
fn finish_file(
    out: &std::path::Path,
    decoded: impl IntoIterator<Item = Result<Vec<u8>>>,
) -> Result<(u64, String)> {
    use sha2::Digest;
    use std::io::Write;
    let mut writer = std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(out)?);
    let mut sha = sha2::Sha256::new();
    let mut written = 0u64;
    for plain in decoded {
        let plain = plain?;
        writer.write_all(&plain)?;
        sha.update(&plain);
        written += plain.len() as u64;
    }
    writer.flush()?;
    let file = writer.into_inner().map_err(|e| e.into_error())?;
    crate::durable::sync_staged(&file)?;
    Ok((written, hex::encode(sha.finalize())))
}

fn read_range_into(path: &std::path::Path, lo: u64, hi: u64, buf: &mut Vec<u8>) -> Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::Start(lo))?;
    buf.clear();
    f.take(hi - lo).read_to_end(buf)?;
    if buf.len() as u64 != hi - lo {
        return Err(xet_err("xorb shorter than its footer says"));
    }
    Ok(())
}

fn read_range(path: &std::path::Path, lo: u64, hi: u64) -> Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::Start(lo))?;
    let mut buf = vec![0u8; (hi - lo) as usize];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

fn xet_err(e: impl std::fmt::Display) -> Error {
    Error::Xet(e.to_string())
}
