//! Embedded local CAS backed by upstream `LocalClient`, plus tree ingest and reconstruction.

use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt;
use sha2::{Digest, Sha256};
use xet_client::cas_client::{Client, DirectAccessClient, LocalClient};
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::xorb_object::XorbObject;
use xet_data::deduplication::DeduplicationMetrics;
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{FileUploadSession, Sha256Policy};
use xet_runtime::core::XetContext;

use crate::error::{Error, Result};
use crate::index::{ChunkIndex, shared_db};
use crate::ingest_cache::{self, Hashed, IngestCache};
use crate::manifest::{
    Entry, EntryKind, MANIFEST_FORMAT_VERSION, Manifest, STAGING_PREFIX, validate_rel_path,
};
use crate::walk::{WalkedEntry, WalkedKind, walk};
use tracing::Instrument as _;

/// A persistent, on-disk Xet content store. No network access is ever made.
///
/// Layout under `root` is owned by upstream (`xet/`, containing the `LocalClient` xorb and
/// shard directories and its redb index); treat it as opaque.
pub struct LocalCas {
    root: PathBuf,
    config: Arc<TranslatorConfig>,
    /// The same store with xorb compression off, for ingesting files that do not compress.
    config_raw: Arc<TranslatorConfig>,
    /// The same store upstream sessions open for `config` (redb handles are shared per path).
    pub(crate) client: Arc<LocalClient>,
    pub(crate) index: ChunkIndex,
    ingest_cache: IngestCache,
    /// Advisory lock on `<root>/.lock`: shared for normal use, exclusive for GC. Held for the
    /// lifetime of this value.
    _lock: fs::File,
    exclusive: bool,
    /// Parsed xorb footers. Xorbs are immutable, so entries stay valid until GC deletes the xorb.
    footers: std::sync::Mutex<std::collections::HashMap<MerkleHash, Arc<XorbObject>>>,
}

/// Footers kept in memory (each is a few KiB to a few hundred KiB).
const FOOTER_CACHE_ENTRIES: usize = 4096;

/// SHA-256 of zero bytes. Upstream 1.8.0 reports all zeros for empty files.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Content a manifest references that this CAS cannot yet reconstruct.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MissingContent {
    /// Xet file hashes whose reconstruction metadata (shard entry) is absent.
    pub reconstructions: BTreeSet<MerkleHash>,
    /// Xorbs referenced by known reconstructions but not present locally.
    pub xorbs: BTreeSet<MerkleHash>,
    /// Xorbs referenced by known reconstructions and already present locally.
    pub present_xorbs: BTreeSet<MerkleHash>,
    /// Files with a known reconstruction that references an absent xorb (in `xorbs`).
    pub files_missing_xorbs: BTreeSet<MerkleHash>,
}

impl MissingContent {
    pub fn is_empty(&self) -> bool {
        self.reconstructions.is_empty() && self.xorbs.is_empty()
    }
}

#[derive(Debug, Clone, Default)]
pub struct IngestReport {
    pub files: usize,
    pub dirs: usize,
    /// Of `files`, how many (and their bytes) were unchanged since an earlier ingest and reused
    /// its hashes without being read. `dedup` covers only the files that were read.
    pub cached_files: usize,
    pub cached_bytes: u64,
    /// Of the files read, how many were ingested without compression (samples did not compress).
    pub raw_files: usize,
    pub dedup: DeduplicationMetrics,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconstructReport {
    /// Files written (reconstructed, verified, installed).
    pub files: usize,
    /// Files already present at the destination with identical content; left untouched.
    pub files_unchanged: usize,
    /// Of `files_unchanged`, those trusted from their recorded stamp without being read.
    pub files_trusted: usize,
    pub dirs: usize,
    /// Bytes reconstructed (excludes unchanged files).
    pub bytes: u64,
    /// With [`ReconstructOptions::delete_extraneous`]: non-directory entries removed (regular
    /// files, symlinks, special files) and directories removed.
    pub files_deleted: usize,
    pub dirs_deleted: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconstructOptions {
    /// Make `dest` mirror the manifest: after every file is verified and before any is installed,
    /// remove whatever under `dest` the manifest does not describe, including entries whose type
    /// conflicts with it. Symlinks are removed, never followed. Off by default.
    pub delete_extraneous: bool,
}

impl LocalCas {
    /// Opens (creating if needed) a local CAS rooted at `root`.
    ///
    /// When called inside a multi-threaded tokio runtime, upstream reuses that runtime.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(root.as_ref(), false).await
    }

    /// Opens the CAS for exclusive use (required by [`LocalCas::gc`]). Fails while any other
    /// handle, in this or another process, has it open.
    pub async fn open_exclusive(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(root.as_ref(), true).await
    }

    pub fn is_exclusive(&self) -> bool {
        self.exclusive
    }

    async fn open_with(root: &Path, exclusive: bool) -> Result<Self> {
        fs::create_dir_all(root)?;
        let root = fs::canonicalize(root)?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join(".lock"))?;
        let locked = if exclusive {
            lock.try_lock()
        } else {
            lock.try_lock_shared()
        };
        if let Err(e) = locked {
            return Err(Error::Busy(match (exclusive, e) {
                (true, fs::TryLockError::WouldBlock) => {
                    "in use by another process (stop serve/push first)".into()
                }
                (false, fs::TryLockError::WouldBlock) => {
                    "garbage collection is running on this CAS".into()
                }
                (_, fs::TryLockError::Error(e)) => e.to_string(),
            }));
        }
        let ctx = XetContext::default()?;
        let config = Arc::new(TranslatorConfig::local_config(&ctx, &root)?);
        let raw_ctx = XetContext::with_config(
            xet_runtime::config::XetConfig::new()
                .with_config("xorb.compression_policy", "none")
                .map_err(|e| Error::Xet(e.to_string()))?,
        )?;
        let config_raw = Arc::new(TranslatorConfig::local_config(&raw_ctx, &root)?);
        // Mirrors `xet_data::processing::create_remote_client` for `local://` endpoints.
        let client = LocalClient::new(ctx.clone(), root.join("xet").join("xorbs"))
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        let index = ChunkIndex::open(&root.join("chunk_index.redb"))?;
        let ingest_cache = IngestCache::open(shared_db(&root.join("chunk_index.redb"))?)?;
        let cas = Self {
            root,
            config,
            config_raw,
            client,
            index,
            ingest_cache,
            _lock: lock,
            exclusive,
            footers: Default::default(),
        };
        cas.sync_index().await?;
        Ok(cas)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Where upstream `LocalClient` stores a xorb. Coupled to the pinned 1.8.0 layout; a mismatch
    /// surfaces as a sync error at commit, never as silent non-durability.
    pub(crate) fn xorb_path(&self, hash: &MerkleHash) -> PathBuf {
        self.client_dir()
            .join("xorbs")
            .join(format!("default.{}", hash.hex()))
    }

    /// Where upstream `LocalClient` stores a shard (same coupling as [`Self::xorb_path`]).
    pub(crate) fn shard_path(&self, hash: &MerkleHash) -> PathBuf {
        self.client_dir().join("shards").join(
            xet_core_structures::metadata_shard::utils::shard_file_name(hash),
        )
    }

    /// A xorb's parsed footer, read once with buffered I/O and then cached.
    pub(crate) async fn footer(&self, xorb: &MerkleHash) -> Result<Arc<XorbObject>> {
        if let Some(f) = self.footers.lock().unwrap().get(xorb) {
            return Ok(f.clone());
        }
        let path = self.xorb_path(xorb);
        let footer = tokio::task::spawn_blocking(move || {
            let mut r = std::io::BufReader::new(fs::File::open(&path)?);
            XorbObject::deserialize(&mut r).map_err(|e| Error::Xet(e.to_string()))
        })
        .await
        .map_err(|e| Error::Xet(e.to_string()))??;
        let footer = Arc::new(footer);
        let mut cache = self.footers.lock().unwrap();
        if cache.len() >= FOOTER_CACHE_ENTRIES {
            cache.clear();
        }
        cache.insert(*xorb, footer.clone());
        Ok(footer)
    }

    pub(crate) fn forget_footer(&self, xorb: &MerkleHash) {
        self.footers.lock().unwrap().remove(xorb);
    }

    fn client_dir(&self) -> PathBuf {
        self.root.join("xet").join("xorbs")
    }

    /// Chunks, hashes, and stores `src` (a file or directory) as native Xet xorbs and shards.
    /// Returns the manifest needed to reconstruct it.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn ingest(&self, src: impl AsRef<Path>) -> Result<(Manifest, IngestReport)> {
        let src = src.as_ref();
        let started = std::time::SystemTime::now();
        let walked = walk(src)?;
        let root = fs::canonicalize(src)?;
        let single_file = fs::symlink_metadata(src)?.is_file();
        let key_of = |w: &WalkedEntry| {
            ingest_cache::key(&if single_file {
                root.clone()
            } else {
                root.join(&w.rel_path)
            })
        };
        let keys: Vec<Vec<u8>> = walked.iter().map(key_of).collect();

        // Files unchanged since an earlier ingest reuse its hashes (if this CAS still holds
        // their content); the rest are read, chunked and hashed.
        let mut known: Vec<Option<(String, String)>> = vec![None; walked.len()];
        let probe: Vec<(usize, ingest_cache::Stamp)> = walked
            .iter()
            .enumerate()
            .filter_map(|(i, w)| match w.kind {
                WalkedKind::File {
                    size,
                    stamp: Some(stamp),
                    ..
                } if size > 0 => Some((i, stamp)),
                _ => None,
            })
            .collect();
        let lookups: Vec<(&[u8], ingest_cache::Stamp)> = probe
            .iter()
            .map(|(i, stamp)| (keys[*i].as_slice(), *stamp))
            .collect();
        let mut present_xorbs = std::collections::HashSet::new();
        for ((i, _), hit) in probe.iter().zip(self.ingest_cache.lookup(&lookups)?) {
            let Some((xet, sha)) = hit else { continue };
            if self.holds_file(&xet, &mut present_xorbs).await? {
                known[*i] = Some((xet.hex(), hex::encode(sha)));
            }
        }

        let to_read: Vec<usize> = walked
            .iter()
            .enumerate()
            .filter(|(i, w)| matches!(w.kind, WalkedKind::File { .. }) && known[*i].is_none())
            .map(|(i, _)| i)
            .collect();
        let mut report = IngestReport::default();
        // Upstream lz4-compresses every chunk and keeps the result only if it is smaller, so for
        // data that is already compressed (Parquet, archives, media, most weights) all of that work
        // is wasted. Files whose samples do not compress are ingested with compression off; the
        // xorbs are valid either way. Ingest compression is the only compression on the wire.
        let (compressible, raw): (Vec<usize>, Vec<usize>) = to_read
            .iter()
            .copied()
            .partition(|i| compresses(&walked[*i].abs_path));
        report.raw_files = raw.len();
        let upload = |config: Arc<TranslatorConfig>, files: Vec<usize>| {
            let paths: Vec<&Path> = files
                .iter()
                .map(|i| walked[*i].abs_path.as_path())
                .collect();
            async move {
                if paths.is_empty() {
                    return Ok::<_, Error>((files, Vec::new(), DeduplicationMetrics::default()));
                }
                let session = FileUploadSession::new(config).await?;
                let infos = session
                    .upload_files(paths.into_iter().map(|p| (p, Sha256Policy::Compute)))
                    .await?;
                let dedup = session.finalize().await?;
                Ok((files, infos, dedup))
            }
        };
        let (a, b) = tokio::try_join!(
            upload(self.config.clone(), compressible),
            upload(self.config_raw.clone(), raw)
        )?;
        let mut dedup = a.2;
        dedup.merge_in(&b.2);
        if !to_read.is_empty() {
            // Make newly stored chunks visible to chunk-level planning.
            self.sync_index().await?;
        }
        let mut by_index: std::collections::HashMap<usize, _> = Default::default();
        for (files, infos) in [(a.0, a.1), (b.0, b.1)] {
            if infos.len() != files.len() {
                return Err(Error::Xet(
                    "upload returned fewer file infos than files".into(),
                ));
            }
            by_index.extend(files.into_iter().zip(infos));
        }
        report.dedup = dedup;
        let mut entries = Vec::with_capacity(walked.len());
        let mut hashed = Vec::new();

        for (i, w) in walked.into_iter().enumerate() {
            let kind = match w.kind {
                WalkedKind::Dir => {
                    report.dirs += 1;
                    EntryKind::Dir
                }
                WalkedKind::File {
                    size, executable, ..
                } if known[i].is_some() => {
                    report.files += 1;
                    report.cached_files += 1;
                    report.cached_bytes += size;
                    let (xet_hash, sha256) = known[i].take().unwrap();
                    EntryKind::File {
                        size,
                        xet_hash,
                        sha256,
                        executable,
                    }
                }
                WalkedKind::File {
                    size,
                    executable,
                    modified,
                    stamp,
                } => {
                    report.files += 1;
                    // Detect edits made while the file was being read: a changed size or mtime means
                    // the stored content may not be any single version of the file.
                    let now = fs::symlink_metadata(&w.abs_path)?;
                    if now.len() != size || now.modified().ok() != modified {
                        return Err(Error::Verification {
                            path: w.rel_path,
                            reason: "file changed during ingest; retry once it is stable".into(),
                        });
                    }
                    let info = by_index.remove(&i).ok_or_else(|| {
                        Error::Xet("upload returned no file info for a file".into())
                    })?;
                    if info.file_size != Some(size) {
                        return Err(Error::Verification {
                            path: w.rel_path,
                            reason: format!(
                                "file changed during ingest (size {size} vs {:?})",
                                info.file_size
                            ),
                        });
                    }
                    let sha256 = if size == 0 {
                        EMPTY_SHA256.to_owned()
                    } else {
                        info.sha256.ok_or_else(|| {
                            Error::Xet(format!("no SHA-256 computed for {:?}", w.rel_path))
                        })?
                    };
                    let sha256 = sha256.to_ascii_lowercase();
                    // Cache only a version that was stable across the read.
                    if size > 0
                        && let Some(stamp) =
                            stamp.filter(|s| ingest_cache::Stamp::of(&now) == Some(*s))
                        && let (Ok(xet), Ok(sha)) = (
                            MerkleHash::from_hex(&info.hash),
                            hex::decode(&sha256).map(<[u8; 32]>::try_from),
                        )
                        && let Ok(sha) = sha
                    {
                        hashed.push((i, stamp, xet, sha));
                    }
                    EntryKind::File {
                        size,
                        xet_hash: info.hash,
                        sha256,
                        executable,
                    }
                }
            };
            entries.push(Entry {
                path: w.rel_path,
                kind,
            });
        }

        let records: Vec<Hashed<'_>> = hashed
            .iter()
            .map(|(i, stamp, xet, sha)| Hashed {
                key: keys[*i].as_slice(),
                stamp: *stamp,
                xet: *xet,
                sha256: *sha,
            })
            .collect();
        let mut prefix = ingest_cache::key(&root);
        prefix.push(b'/');
        let keep: std::collections::HashSet<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        self.ingest_cache.record(
            &records,
            started,
            (!single_file).then_some((prefix.as_slice(), &keep)),
        )?;

        let manifest = Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            entries,
        };
        manifest.validate()?;
        Ok((manifest, report))
    }

    /// Whether this CAS can reconstruct file `hash`: its reconstruction and every xorb it uses
    /// are present. `present` memoizes xorbs already found.
    async fn holds_file(
        &self,
        hash: &MerkleHash,
        present: &mut std::collections::HashSet<MerkleHash>,
    ) -> Result<bool> {
        let Some((info, _)) = self
            .client
            .get_file_reconstruction_info(hash)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?
        else {
            return Ok(false);
        };
        for segment in &info.segments {
            let x = segment.xorb_hash;
            if present.contains(&x) {
                continue;
            }
            if !self
                .client
                .xorb_exists(&x)
                .await
                .map_err(|e| Error::Xet(e.to_string()))?
            {
                return Ok(false);
            }
            present.insert(x);
        }
        Ok(true)
    }

    /// Indexes the chunks of any stored xorb not yet in the chunk index (e.g. created by ingest,
    /// or by a version before the index existed).
    pub async fn sync_index(&self) -> Result<usize> {
        let mut added = 0;
        for xorb in self
            .client
            .list_xorbs()
            .await
            .map_err(|e| Error::Xet(e.to_string()))?
        {
            if self.index.is_indexed(&xorb)? {
                continue;
            }
            let footer = self.footer(&xorb).await?;
            self.index.insert_xorb(&xorb, &footer.info.chunk_hashes)?;
            added += 1;
        }
        Ok(added)
    }

    /// Reports which reconstruction metadata and xorbs `manifest` needs that this CAS lacks.
    /// Purely local: a lookup here is never a network transfer.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn missing_content(&self, manifest: &Manifest) -> Result<MissingContent> {
        let mut missing = MissingContent::default();
        for (entry, size, xet_hash, ..) in manifest.files() {
            if size == 0 {
                continue;
            }
            let hash = parse_hash(&entry.path, xet_hash)?;
            let info = self
                .client
                .get_file_reconstruction_info(&hash)
                .await
                .map_err(|e| Error::Xet(e.to_string()))?;
            let Some((info, _shard)) = info else {
                missing.reconstructions.insert(hash);
                continue;
            };
            for segment in &info.segments {
                let h = segment.xorb_hash;
                if missing.xorbs.contains(&h) {
                    missing.files_missing_xorbs.insert(hash);
                    continue;
                }
                if missing.present_xorbs.contains(&h) {
                    continue;
                }
                if self
                    .client
                    .xorb_exists(&h)
                    .await
                    .map_err(|e| Error::Xet(e.to_string()))?
                {
                    missing.present_xorbs.insert(h);
                } else {
                    missing.xorbs.insert(h);
                    missing.files_missing_xorbs.insert(hash);
                }
            }
        }
        Ok(missing)
    }

    /// Reconstructs every manifest entry under `dest` from this CAS.
    ///
    /// All files are reconstructed into a staging directory and verified (size + SHA-256) before
    /// any is moved into place, so a failure never exposes a partial or unverified file. Each file
    /// is installed with an atomic rename. Existing symlinks or non-directories in the way are
    /// rejected rather than followed.
    ///
    /// A file already present with identical size and SHA-256 is left untouched. Staging
    /// directories left by an interrupted earlier run in `dest` are removed; callers must not run
    /// two reconstructions into the same `dest` concurrently.
    pub async fn reconstruct(
        &self,
        manifest: &Manifest,
        dest: impl AsRef<Path>,
    ) -> Result<ReconstructReport> {
        self.reconstruct_with(manifest, dest, ReconstructOptions::default())
            .await
    }

    /// [`Self::reconstruct`] with options.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn reconstruct_with(
        &self,
        manifest: &Manifest,
        dest: impl AsRef<Path>,
        opts: ReconstructOptions,
    ) -> Result<ReconstructReport> {
        manifest.validate()?;
        let missing = self.missing_content(manifest).await?;
        if !missing.is_empty() {
            return Err(Error::MissingContent {
                files: missing.reconstructions.len(),
                xorbs: missing.xorbs.len(),
            });
        }

        let dest = dest.as_ref();
        fs::create_dir_all(dest)?;
        let dest = fs::canonicalize(dest)?;
        remove_stale_staging(&dest)?;

        let staging = dest.join(staging_name());
        fs::create_dir(&staging)?;
        let result = self.reconstruct_via(manifest, &dest, &staging, opts).await;
        let _ = fs::remove_dir_all(&staging);
        let t = std::time::Instant::now();
        if result.is_ok() {
            let files: BTreeSet<MerkleHash> = manifest
                .files()
                .filter(|(_, size, ..)| *size > 0)
                .filter_map(|(_, _, h, ..)| MerkleHash::from_hex(h).ok())
                .collect();
            if let Ok(xorbs) = self.referenced_xorbs(&files).await {
                self.touch_xorbs(&xorbs);
            }
        }
        tracing::debug!(ms = t.elapsed().as_millis() as u64, "xorbs touched");
        result
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn reconstruct_via(
        &self,
        manifest: &Manifest,
        dest: &Path,
        staging: &Path,
        opts: ReconstructOptions,
    ) -> Result<ReconstructReport> {
        let (work, report) = self.decide(manifest, dest, staging)?;
        let parallel = std::thread::available_parallelism().map_or(4, |n| n.get() * 2);
        let staged: Vec<Result<Staged>> = futures::stream::iter(work)
            .map(|w| self.stage(w))
            .buffer_unordered(parallel)
            .collect()
            .await;
        let staged = staged.into_iter().collect::<Result<Vec<_>>>()?;
        let (report, installed) = install(manifest, dest, staging, staged, opts, report)?;
        self.record_installed(manifest, &installed).await;
        Ok(report)
    }

    /// Records the stamps of files just installed, so the next transfer to this destination
    /// trusts them without re-reading (see `ingest_cache`). Best effort: a failure here only
    /// means they get verified by reading next time.
    async fn record_installed(&self, manifest: &Manifest, installed: &[(PathBuf, String)]) {
        if installed.is_empty() {
            return;
        }
        tokio::time::sleep(ingest_cache::INSTALL_SETTLE).await;
        let hashes: std::collections::HashMap<&str, (&str, &str)> = manifest
            .files()
            .map(|(e, _, xet, sha, _)| (e.path.as_str(), (xet, sha)))
            .collect();
        let mut keys = Vec::new();
        for (target, path) in installed {
            let Some((xet, sha)) = hashes.get(path.as_str()) else {
                continue;
            };
            if let (Ok(m), Ok(xet), Ok(Ok(sha))) = (
                fs::symlink_metadata(target),
                MerkleHash::from_hex(xet),
                hex::decode(sha).map(<[u8; 32]>::try_from),
            ) && let Some(stamp) = ingest_cache::Stamp::of(&m)
                && m.len() > 0
            {
                keys.push((ingest_cache::key(target), stamp, xet, sha));
            }
        }
        let records: Vec<Hashed<'_>> = keys
            .iter()
            .map(|(key, stamp, xet, sha)| Hashed {
                key: key.as_slice(),
                stamp: *stamp,
                xet: *xet,
                sha256: *sha,
            })
            .collect();
        if let Err(e) = self.ingest_cache.record_installed(&records) {
            tracing::debug!("could not record installed files: {e}");
        }
    }

    /// Decides what to write: identical files already in place are left alone. A file whose
    /// stamp matches the one recorded when it was last verified with these hashes is not read
    /// again (see `ingest_cache`); any other existing file is read and hashed.
    #[tracing::instrument(level = "debug", skip_all)]
    fn decide(
        &self,
        manifest: &Manifest,
        dest: &Path,
        staging: &Path,
    ) -> Result<(Vec<Work>, ReconstructReport)> {
        let mut report = ReconstructReport::default();
        let t = std::time::Instant::now();
        let started = std::time::SystemTime::now();
        let mut work = Vec::new();
        let mut buf = vec![0u8; VERIFY_BUF];
        let files: Vec<_> = manifest.files().collect();
        let mut existing = Vec::with_capacity(files.len());
        for (entry, ..) in &files {
            let rel = validate_rel_path(&entry.path)?;
            existing.push(existing_regular_file(dest, &rel).map(|(p, m)| {
                let key = ingest_cache::key(&p);
                (p, ingest_cache::Stamp::of(&m), key)
            }));
        }
        let probe: Vec<(usize, &[u8], ingest_cache::Stamp)> = existing
            .iter()
            .enumerate()
            .filter_map(|(i, e)| match e {
                Some((_, Some(stamp), key)) if files[i].1 > 0 => Some((i, key.as_slice(), *stamp)),
                _ => None,
            })
            .collect();
        let lookups: Vec<(&[u8], ingest_cache::Stamp)> =
            probe.iter().map(|(_, k, s)| (*k, *s)).collect();
        let mut trusted = vec![false; files.len()];
        for ((i, ..), hit) in probe.iter().zip(self.ingest_cache.lookup(&lookups)?) {
            let (_, _, xet_hash, sha256, _) = files[*i];
            trusted[*i] = hit.is_some_and(|(xet, sha)| {
                xet.hex().eq_ignore_ascii_case(xet_hash)
                    && hex::encode(sha).eq_ignore_ascii_case(sha256)
            });
        }
        let mut verified = Vec::new();
        for (i, (entry, size, xet_hash, sha256, executable)) in files.iter().copied().enumerate() {
            if let Some((path, _, key)) = &existing[i]
                && (trusted[i] || verify_file(path, size, sha256, &mut buf).is_ok())
            {
                set_executable(path, executable)?;
                report.files_unchanged += 1;
                report.files_trusted += usize::from(trusted[i]);
                if !trusted[i]
                    && size > 0
                    // Re-stat: fixing the executable bit moves ctime.
                    && let Some(now) = fs::symlink_metadata(path)
                        .ok()
                        .and_then(|m| ingest_cache::Stamp::of(&m))
                    && let (Ok(xet), Ok(Ok(sha))) = (
                        MerkleHash::from_hex(xet_hash),
                        hex::decode(sha256).map(<[u8; 32]>::try_from),
                    )
                {
                    verified.push((key.clone(), now, xet, sha));
                }
                continue;
            }
            work.push(Work {
                tmp: staging.join(i.to_string()),
                path: entry.path.clone(),
                size,
                xet_hash: xet_hash.to_owned(),
                sha256: sha256.to_owned(),
                executable,
            });
        }

        let records: Vec<Hashed<'_>> = verified
            .iter()
            .map(|(key, stamp, xet, sha)| Hashed {
                key: key.as_slice(),
                stamp: *stamp,
                xet: *xet,
                sha256: *sha,
            })
            .collect();
        self.ingest_cache.record(&records, started, None)?;
        tracing::debug!(
            ms = t.elapsed().as_millis() as u64,
            unchanged = report.files_unchanged,
            trusted = report.files_trusted,
            "existing files checked"
        );
        Ok((work, report))
    }

    /// Reconstructs one file into staging and checks its size and SHA-256 against the manifest.
    async fn stage(&self, w: Work) -> Result<Staged> {
        let fail = |reason: String| Error::Verification {
            path: w.path.clone(),
            reason,
        };
        if w.size == 0 {
            crate::durable::sync_staged(&fs::File::create(&w.tmp)?)?;
            if !w.sha256.eq_ignore_ascii_case(EMPTY_SHA256) {
                return Err(fail("SHA-256 of empty file does not match".into()));
            }
        } else {
            let hash = parse_hash(&w.path, &w.xet_hash)?;
            let (n, sha) = self.write_file_from_cas(&hash, &w.tmp).await?;
            if n != w.size {
                return Err(fail(format!("size {n}, expected {}", w.size)));
            }
            if !sha.eq_ignore_ascii_case(&w.sha256) {
                return Err(fail(format!("SHA-256 {sha}, expected {}", w.sha256)));
            }
        }
        set_executable(&w.tmp, w.executable)?;
        Ok((w.tmp, w.path, w.size))
    }
}

/// A reconstruction into a destination that can start files early, as soon as their content is
/// in this CAS (see [`Reconstruction::content_ready`]), while the rest is still arriving. Nothing
/// at the destination changes until [`Reconstruction::finish`] has verified every file; then all
/// are installed at once, as with [`LocalCas::reconstruct_with`].
pub struct Reconstruction {
    cas: Arc<LocalCas>,
    manifest: Manifest,
    dest: PathBuf,
    staging: PathBuf,
    opts: ReconstructOptions,
    report: ReconstructReport,
    /// Files still to start, by lowercase Xet hash (empty files under "").
    pending: std::collections::HashMap<String, Vec<Work>>,
    running: tokio::task::JoinSet<Result<Staged>>,
    permits: Arc<tokio::sync::Semaphore>,
}

impl LocalCas {
    /// Validates `manifest`, prepares staging under `dest` and decides which files are already in
    /// place. Locks are the caller's concern, as with [`LocalCas::reconstruct_with`].
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn begin_reconstruct(
        self: &Arc<Self>,
        manifest: &Manifest,
        dest: impl AsRef<Path>,
        opts: ReconstructOptions,
    ) -> Result<Reconstruction> {
        manifest.validate()?;
        let dest = dest.as_ref();
        fs::create_dir_all(dest)?;
        let dest = fs::canonicalize(dest)?;
        remove_stale_staging(&dest)?;
        let staging = dest.join(staging_name());
        fs::create_dir(&staging)?;
        let (work, report) = match self.decide(manifest, &dest, &staging) {
            Ok(r) => r,
            Err(e) => {
                let _ = fs::remove_dir_all(&staging);
                return Err(e);
            }
        };
        let mut pending: std::collections::HashMap<String, Vec<Work>> = Default::default();
        for w in work {
            let key = if w.size == 0 {
                String::new()
            } else {
                w.xet_hash.to_ascii_lowercase()
            };
            pending.entry(key).or_default().push(w);
        }
        let parallel = std::thread::available_parallelism().map_or(4, |n| n.get() * 2);
        Ok(Reconstruction {
            cas: self.clone(),
            manifest: manifest.clone(),
            dest,
            staging,
            opts,
            report,
            pending,
            running: Default::default(),
            permits: Arc::new(tokio::sync::Semaphore::new(parallel)),
        })
    }
}

impl Reconstruction {
    /// `file`'s content is now fully in the CAS: start staging every path that needs it.
    pub fn content_ready(&mut self, file: &MerkleHash) {
        if let Some(items) = self.pending.remove(&file.hex()) {
            for w in items {
                self.start(w);
            }
        }
    }

    fn start(&mut self, w: Work) {
        let (cas, permits) = (self.cas.clone(), self.permits.clone());
        self.running.spawn(
            async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .expect("semaphore never closed");
                cas.stage(w).await
            }
            .instrument(tracing::debug_span!("stage_file")),
        );
    }

    /// Stages everything not started yet, waits for all files to verify, then installs them.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn finish(mut self) -> Result<ReconstructReport> {
        let result = self.finish_inner().await;
        let _ = fs::remove_dir_all(&self.staging);
        if result.is_ok() {
            let files: BTreeSet<MerkleHash> = self
                .manifest
                .files()
                .filter(|(_, size, ..)| *size > 0)
                .filter_map(|(_, _, h, ..)| MerkleHash::from_hex(h).ok())
                .collect();
            if let Ok(xorbs) = self.cas.referenced_xorbs(&files).await {
                self.cas.touch_xorbs(&xorbs);
            }
        }
        result
    }

    async fn finish_inner(&mut self) -> Result<ReconstructReport> {
        for (_, items) in std::mem::take(&mut self.pending) {
            for w in items {
                self.start(w);
            }
        }
        let mut staged = Vec::new();
        while let Some(done) = self.running.join_next().await {
            staged.push(done.map_err(|e| Error::Xet(format!("staging task failed: {e}")))??);
        }
        let (report, installed) = install(
            &self.manifest,
            &self.dest,
            &self.staging,
            staged,
            self.opts,
            std::mem::take(&mut self.report),
        )?;
        self.cas.record_installed(&self.manifest, &installed).await;
        Ok(report)
    }
}

/// A file the caller stages itself, from its chunks as they arrive ([`Reconstruction::stream_file`]).
/// Every chunk position must be [`put`](Self::put), in any order.
pub struct StreamedFile {
    tx: tokio::sync::mpsc::Sender<(u32, Arc<Vec<u8>>)>,
}

impl StreamedFile {
    /// Supplies the uncompressed chunk at position `index` of the file's chunk list.
    pub async fn put(&self, index: u32, data: Arc<Vec<u8>>) -> Result<()> {
        self.tx
            .send((index, data))
            .await
            .map_err(|_| Error::Xet("streamed file writer stopped".into()))
    }
}

impl Reconstruction {
    /// Takes `file` (whose chunk sizes, in order, are `sizes`) out of the plan and returns a
    /// writer that stages it from its chunks as they arrive, instead of rebuilding it from the
    /// CAS once complete. The writer puts chunks in order (holding early ones), hashes as it
    /// goes and, after the last, syncs the file and checks its size and SHA-256 against the
    /// manifest, like any staged file. `None` unless exactly one path needs `file`'s content.
    pub fn stream_file(&mut self, file: &MerkleHash, sizes: Vec<u64>) -> Option<StreamedFile> {
        let key = file.hex();
        if self.pending.get(&key).map(Vec::len) != Some(1)
            || sizes.iter().sum::<u64>() < STREAM_MIN_BYTES
        {
            return None;
        }
        let w = self.pending.remove(&key)?.pop()?;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(u32, Arc<Vec<u8>>)>(64);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let r = assemble(&w, &sizes, &mut rx);
            let _ = done_tx.send(r.map(|()| w));
        });
        self.running.spawn(
            async move {
                let w = done_rx
                    .await
                    .map_err(|_| Error::Xet("streamed file writer stopped".into()))??;
                set_executable(&w.tmp, w.executable)?;
                Ok((w.tmp, w.path, w.size))
            }
            .instrument(tracing::debug_span!("stream_file")),
        );
        Some(StreamedFile { tx })
    }
}

/// Smaller files are rebuilt from the CAS once complete: a writer thread per small file costs
/// more than it saves.
const STREAM_MIN_BYTES: u64 = 8 << 20;

/// Writes `w` from its chunks in order as they arrive on `rx`, then verifies it.
fn assemble(
    w: &Work,
    sizes: &[u64],
    rx: &mut tokio::sync::mpsc::Receiver<(u32, Arc<Vec<u8>>)>,
) -> Result<()> {
    use sha2::Digest;
    use std::io::Write;
    let fail = |reason: String| Error::Verification {
        path: w.path.clone(),
        reason,
    };
    let mut out = std::io::BufWriter::with_capacity(1 << 20, fs::File::create(&w.tmp)?);
    let mut sha = sha2::Sha256::new();
    let (mut next, mut written) = (0usize, 0u64);
    let mut early: std::collections::HashMap<usize, Arc<Vec<u8>>> = Default::default();
    while next < sizes.len() {
        let (i, data) = rx
            .blocking_recv()
            .ok_or_else(|| fail("transfer ended before the file was complete".into()))?;
        let i = i as usize;
        if sizes.get(i) != Some(&(data.len() as u64)) {
            return Err(fail(format!("chunk {i} has the wrong size")));
        }
        if i != next {
            early.insert(i, data);
            continue;
        }
        let mut cur = Some(data);
        while let Some(d) = cur {
            out.write_all(&d)?;
            sha.update(&d[..]);
            written += d.len() as u64;
            next += 1;
            cur = early.remove(&next);
        }
    }
    out.flush()?;
    let file = out.into_inner().map_err(|e| e.into_error())?;
    crate::durable::sync_staged(&file)?;
    if written != w.size {
        return Err(fail(format!("size {written}, expected {}", w.size)));
    }
    let sha = hex::encode(sha.finalize());
    if !sha.eq_ignore_ascii_case(&w.sha256) {
        return Err(fail(format!("SHA-256 {sha}, expected {}", w.sha256)));
    }
    Ok(())
}

/// A file to reconstruct into staging.
struct Work {
    tmp: PathBuf,
    path: String,
    size: u64,
    xet_hash: String,
    sha256: String,
    executable: bool,
}

/// A verified file in staging: (staging path, manifest path, size).
type Staged = (PathBuf, String, u64);

/// Installs verified staged files (and the manifest's directories) into `dest`, after removing
/// extraneous entries if asked, then makes it all durable.
fn install(
    manifest: &Manifest,
    dest: &Path,
    staging: &Path,
    staged: Vec<Staged>,
    opts: ReconstructOptions,
    mut report: ReconstructReport,
) -> Result<(ReconstructReport, Vec<(PathBuf, String)>)> {
    let mut touched_dirs = BTreeSet::new();
    let mut installed = Vec::new();
    // Staged files were not synced one by one where one filesystem sync can do it: do it now,
    // before anything is renamed into place (or sync each file if that is not possible).
    let batched = crate::durable::batched();
    if batched && crate::durable::sync_filesystem(staging).is_err() {
        for (tmp, ..) in &staged {
            crate::durable::sync_file(&fs::File::open(tmp)?)?;
        }
    }
    let staged: Vec<(PathBuf, String)> = staged
        .into_iter()
        .map(|(tmp, path, size)| {
            report.bytes += size;
            (tmp, path)
        })
        .collect();
    // Everything is verified; only now may anything at the destination be removed.
    if opts.delete_extraneous {
        let wanted = wanted_paths(manifest)?;
        let staging_name = staging.file_name().map(|n| n.to_owned());
        prune(
            dest,
            Path::new(""),
            &wanted,
            staging_name.as_deref(),
            &mut report,
        )?;
        touched_dirs.insert(dest.to_owned());
    }

    for entry in &manifest.entries {
        if matches!(entry.kind, EntryKind::Dir) {
            ensure_dir(dest, &validate_rel_path(&entry.path)?)?;
            report.dirs += 1;
        }
    }

    for (tmp, path) in staged {
        let rel = validate_rel_path(&path)?;
        if let Some(parent) = rel.parent() {
            ensure_dir(dest, parent)?;
        }
        let target = dest.join(&rel);
        match fs::symlink_metadata(&target) {
            Ok(m) if m.is_file() => {}
            Ok(_) => {
                return Err(Error::UnsafePath {
                    path,
                    reason: "destination exists and is not a regular file",
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        fs::rename(&tmp, &target)?;
        installed.push((target.clone(), path));
        report.files += 1;
        touched_dirs.insert(target.parent().unwrap_or(dest).to_owned());
    }

    // Make the renames (and any new directories) durable.
    for entry in &manifest.entries {
        if matches!(entry.kind, EntryKind::Dir) {
            let dir = dest.join(validate_rel_path(&entry.path)?);
            touched_dirs.insert(dir.parent().unwrap_or(dest).to_owned());
        }
    }
    touched_dirs.insert(dest.to_owned());
    if !batched || crate::durable::sync_filesystem(dest).is_err() {
        for dir in touched_dirs {
            sync_dir(&dir)?;
        }
    }
    // One drive-cache flush makes every file and directory synced above durable.
    crate::durable::barrier(dest)?;

    Ok((report, installed))
}

/// Whether lz4 is worth trying on `path`: compresses five 64 KiB samples from the interior of the
/// file (at 1/6 .. 5/6 of its length, away from headers and footers, which often compress even
/// when the data does not, e.g. Parquet metadata) and asks for at least 5% savings on the median
/// sample. Small or unreadable files default to compressing.
fn compresses(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    const SAMPLE: u64 = 64 * 1024;
    let Ok(mut f) = fs::File::open(path) else {
        return true;
    };
    let Ok(len) = f.metadata().map(|m| m.len()) else {
        return true;
    };
    if len < 12 * SAMPLE {
        return true;
    }
    let mut buf = vec![0u8; SAMPLE as usize];
    let mut ratios = Vec::with_capacity(5);
    for k in 1..=5 {
        if f.seek(SeekFrom::Start(len * k / 6)).is_err() || f.read_exact(&mut buf).is_err() {
            return true;
        }
        ratios.push(lz4_flex::block::compress(&buf).len() * 100 / buf.len());
    }
    ratios.sort_unstable();
    ratios[2] < 95
}

fn parse_hash(path: &str, hex: &str) -> Result<MerkleHash> {
    MerkleHash::from_hex(hex)
        .map_err(|e| Error::InvalidManifest(format!("{path:?}: bad xet_hash: {e}")))
}

/// Makes one stored object (and its directory entry) durable.
pub(crate) fn sync_file_and_parent(path: &Path) -> Result<()> {
    crate::durable::persist(path)?;
    Ok(())
}

/// fsyncs a directory so entries created or renamed in it are durable (no-op off Unix).
fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    crate::durable::sync_dir(dir)?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

fn staging_name() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{STAGING_PREFIX}{}-{nanos}", std::process::id())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Wanted {
    File,
    Dir,
}

/// Every manifest path with its kind, plus every ancestor directory.
fn wanted_paths(manifest: &Manifest) -> Result<std::collections::HashMap<PathBuf, Wanted>> {
    let mut wanted = std::collections::HashMap::new();
    for entry in &manifest.entries {
        let rel = validate_rel_path(&entry.path)?;
        for ancestor in rel.ancestors().skip(1) {
            if !ancestor.as_os_str().is_empty() {
                wanted.insert(ancestor.to_owned(), Wanted::Dir);
            }
        }
        let kind = match entry.kind {
            EntryKind::Dir => Wanted::Dir,
            EntryKind::File { .. } => Wanted::File,
        };
        wanted.insert(rel, kind);
    }
    Ok(wanted)
}

/// Removes entries under `dest/rel` not in `wanted` (or of the wrong type). Never follows symlinks.
fn prune(
    dest: &Path,
    rel: &Path,
    wanted: &std::collections::HashMap<PathBuf, Wanted>,
    staging: Option<&std::ffi::OsStr>,
    report: &mut ReconstructReport,
) -> Result<()> {
    for entry in fs::read_dir(dest.join(rel))? {
        let entry = entry?;
        let name = entry.file_name();
        if rel.as_os_str().is_empty() && Some(name.as_os_str()) == staging {
            continue;
        }
        let child = rel.join(&name);
        let meta = fs::symlink_metadata(entry.path())?;
        match wanted.get(&child) {
            Some(Wanted::Dir) if meta.is_dir() => prune(dest, &child, wanted, staging, report)?,
            Some(Wanted::File) if meta.is_file() => {}
            _ if meta.is_dir() => remove_tree(&entry.path(), report)?,
            _ => {
                fs::remove_file(entry.path())?;
                report.files_deleted += 1;
            }
        }
    }
    Ok(())
}

/// Deletes a real directory tree bottom-up, counting what it removes. Symlinks inside are
/// removed as links.
fn remove_tree(dir: &Path, report: &mut ReconstructReport) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if fs::symlink_metadata(entry.path())?.is_dir() {
            remove_tree(&entry.path(), report)?;
        } else {
            fs::remove_file(entry.path())?;
            report.files_deleted += 1;
        }
    }
    fs::remove_dir(dir)?;
    report.dirs_deleted += 1;
    Ok(())
}

/// Removes `<dest>/.xet-p2p-staging-*` directories left by an interrupted reconstruction.
/// Manifests can never name such paths, so nothing a user transferred is touched.
fn remove_stale_staging(dest: &Path) -> Result<()> {
    for entry in fs::read_dir(dest)? {
        let entry = entry?;
        let is_staging = entry
            .file_name()
            .to_str()
            .is_some_and(|n| n.starts_with(STAGING_PREFIX));
        if is_staging && entry.file_type()?.is_dir() {
            fs::remove_dir_all(entry.path())?;
        }
    }
    Ok(())
}

/// `dest/rel` if it is a regular file reached without traversing any symlink; otherwise `None`.
fn existing_regular_file(dest: &Path, rel: &Path) -> Option<(PathBuf, fs::Metadata)> {
    let mut cur = dest.to_path_buf();
    let n = rel.components().count();
    let mut last = None;
    for (i, component) in rel.components().enumerate() {
        cur.push(component);
        let m = fs::symlink_metadata(&cur).ok()?;
        let ok = if i + 1 == n { m.is_file() } else { m.is_dir() };
        if !ok {
            return None;
        }
        last = Some(m);
    }
    Some((cur, last?))
}

/// Creates `rel` (a manifest-style relative path) under `root` without following symlinks, and
/// returns the resulting directory. Use this to confine a peer-chosen destination to a root.
pub fn create_confined_dir(root: &Path, rel: &str) -> Result<PathBuf> {
    let rel = validate_rel_path(rel)?;
    let root = fs::canonicalize(root)?;
    ensure_dir(&root, &rel)?;
    let dir = fs::canonicalize(root.join(&rel))?;
    if !dir.starts_with(&root) {
        return Err(Error::UnsafePath {
            path: rel.to_string_lossy().into_owned(),
            reason: "resolves outside the root",
        });
    }
    Ok(dir)
}

/// Resolves an existing `rel` under `root` without following symlinks: every component must
/// exist, every ancestor must be a real directory, and the final entry a real file or directory.
pub fn resolve_confined(root: &Path, rel: &str) -> Result<PathBuf> {
    let rel = validate_rel_path(rel)?;
    let mut cur = fs::canonicalize(root)?;
    let n = rel.components().count();
    for (i, component) in rel.components().enumerate() {
        cur.push(component);
        let m = fs::symlink_metadata(&cur).map_err(|_| Error::UnsafePath {
            path: rel.to_string_lossy().into_owned(),
            reason: "does not exist",
        })?;
        let ok = if i + 1 == n {
            m.is_dir() || m.is_file()
        } else {
            m.is_dir()
        };
        if !ok {
            return Err(Error::UnsafePath {
                path: rel.to_string_lossy().into_owned(),
                reason: "a symlink or special file is in the path",
            });
        }
    }
    Ok(cur)
}

/// Creates `rel` under `dest` one component at a time, refusing to traverse symlinks or files.
fn ensure_dir(dest: &Path, rel: &Path) -> Result<()> {
    let mut cur = dest.to_path_buf();
    for component in rel.components() {
        cur.push(component);
        match fs::symlink_metadata(&cur) {
            Ok(m) if m.is_dir() => {}
            Ok(_) => {
                return Err(Error::UnsafePath {
                    path: rel.to_string_lossy().into_owned(),
                    reason: "an existing symlink or non-directory is in the way",
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => fs::create_dir(&cur)?,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Read size for [`verify_file`]. The buffer is reused across files: a fresh large allocation per
/// file costs an mmap, page faults and munmap with some allocators (musl), which dominated checking
/// thousands of unchanged files in a gVisor sandbox.
const VERIFY_BUF: usize = 1 << 20;

fn verify_file(
    path: &Path,
    size: u64,
    sha256: &str,
    buf: &mut [u8],
) -> std::result::Result<(), String> {
    let mut file = fs::File::open(path).map_err(|e| e.to_string())?;
    let actual_size = file.metadata().map_err(|e| e.to_string())?.len();
    if actual_size != size {
        return Err(format!("size {actual_size}, expected {size}"));
    }

    let mut hasher = Sha256::new();
    loop {
        let n = file.read(buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let actual = hex::encode(hasher.finalize());
    if !actual.eq_ignore_ascii_case(sha256) {
        return Err(format!("SHA-256 {actual}, expected {sha256}"));
    }
    Ok(())
}

/// Grants execute wherever read is granted, or clears all execute bits.
fn set_executable(path: &Path, executable: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        let mode = perms.mode();
        let new = if executable {
            mode | ((mode & 0o444) >> 2)
        } else {
            mode & !0o111
        };
        if new != mode {
            perms.set_mode(new);
            fs::set_permissions(path, perms)?;
        }
    }
    #[cfg(not(unix))]
    let _ = (path, executable);
    Ok(())
}
