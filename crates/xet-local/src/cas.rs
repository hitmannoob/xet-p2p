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
use crate::index::ChunkIndex;
use crate::manifest::{
    Entry, EntryKind, MANIFEST_FORMAT_VERSION, Manifest, STAGING_PREFIX, validate_rel_path,
};
use crate::walk::{WalkedKind, walk};

/// A persistent, on-disk Xet content store. No network access is ever made.
///
/// Layout under `root` is owned by upstream (`xet/`, containing the `LocalClient` xorb and
/// shard directories and its redb index); treat it as opaque.
pub struct LocalCas {
    root: PathBuf,
    config: Arc<TranslatorConfig>,
    /// The same store upstream sessions open for `config` (redb handles are shared per path).
    pub(crate) client: Arc<LocalClient>,
    pub(crate) index: ChunkIndex,
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
    pub dedup: DeduplicationMetrics,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconstructReport {
    /// Files written (reconstructed, verified, installed).
    pub files: usize,
    /// Files already present at the destination with identical content; left untouched.
    pub files_unchanged: usize,
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
        // Mirrors `xet_data::processing::create_remote_client` for `local://` endpoints.
        let client = LocalClient::new(ctx.clone(), root.join("xet").join("xorbs"))
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        let index = ChunkIndex::open(&root.join("chunk_index.redb"))?;
        let cas = Self {
            root,
            config,
            client,
            index,
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
    pub async fn ingest(&self, src: impl AsRef<Path>) -> Result<(Manifest, IngestReport)> {
        let walked = walk(src.as_ref())?;
        let file_paths: Vec<&Path> = walked
            .iter()
            .filter(|w| matches!(w.kind, WalkedKind::File { .. }))
            .map(|w| w.abs_path.as_path())
            .collect();

        let (infos, dedup) = if file_paths.is_empty() {
            (Vec::new(), DeduplicationMetrics::default())
        } else {
            let session = FileUploadSession::new(self.config.clone()).await?;
            let infos = session
                .upload_files(file_paths.iter().map(|p| (*p, Sha256Policy::Compute)))
                .await?;
            let dedup = session.finalize().await?;
            // Make newly stored chunks visible to chunk-level planning.
            self.sync_index().await?;
            (infos, dedup)
        };

        let mut infos = infos.into_iter();
        let mut report = IngestReport {
            dedup,
            ..Default::default()
        };
        let mut entries = Vec::with_capacity(walked.len());

        for w in walked {
            let kind = match w.kind {
                WalkedKind::Dir => {
                    report.dirs += 1;
                    EntryKind::Dir
                }
                WalkedKind::File {
                    size,
                    executable,
                    modified,
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
                    let info = infos.next().ok_or_else(|| {
                        Error::Xet("upload returned fewer file infos than files".into())
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
                    EntryKind::File {
                        size,
                        xet_hash: info.hash,
                        sha256: sha256.to_ascii_lowercase(),
                        executable,
                    }
                }
            };
            entries.push(Entry {
                path: w.rel_path,
                kind,
            });
        }

        let manifest = Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            entries,
        };
        manifest.validate()?;
        Ok((manifest, report))
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
        result
    }

    async fn reconstruct_via(
        &self,
        manifest: &Manifest,
        dest: &Path,
        staging: &Path,
        opts: ReconstructOptions,
    ) -> Result<ReconstructReport> {
        let mut report = ReconstructReport::default();
        let mut staged = Vec::new();
        let mut touched_dirs = BTreeSet::new();

        // Decide what to write; identical files already in place are left alone.
        let mut work = Vec::new();
        for (i, (entry, size, xet_hash, sha256, executable)) in manifest.files().enumerate() {
            let rel = validate_rel_path(&entry.path)?;
            if let Some(existing) = existing_regular_file(dest, &rel)
                && verify_file(&existing, size, sha256).is_ok()
            {
                set_executable(&existing, executable)?;
                report.files_unchanged += 1;
                continue;
            }
            work.push((
                staging.join(i.to_string()),
                entry.path.clone(),
                size,
                xet_hash.to_owned(),
                sha256.to_owned(),
                executable,
            ));
        }

        // Reconstruct and verify concurrently. Each file is decoded from its native segments with
        // cached footers; every chunk is checked against its stored hash, and size + SHA-256
        // against the manifest, before anything is installed.
        let parallel = std::thread::available_parallelism().map_or(4, |n| n.get() * 2);
        let results: Vec<Result<_>> = futures::stream::iter(work)
            .map(
                |(tmp, path, size, xet_hash, sha256, executable)| async move {
                    let fail = |reason: String| Error::Verification {
                        path: path.clone(),
                        reason,
                    };
                    if size == 0 {
                        crate::durable::sync_file(&fs::File::create(&tmp)?)?;
                    } else {
                        let hash = parse_hash(&path, &xet_hash)?;
                        let (n, sha) = self.write_file_from_cas(&hash, &tmp).await?;
                        if n != size {
                            return Err(fail(format!("size {n}, expected {size}")));
                        }
                        if !sha.eq_ignore_ascii_case(&sha256) {
                            return Err(fail(format!("SHA-256 {sha}, expected {sha256}")));
                        }
                    }
                    if size == 0 && !sha256.eq_ignore_ascii_case(EMPTY_SHA256) {
                        return Err(fail("SHA-256 of empty file does not match".into()));
                    }
                    set_executable(&tmp, executable)?;
                    Ok((tmp, path, size))
                },
            )
            .buffer_unordered(parallel)
            .collect()
            .await;
        for r in results {
            let (tmp, path, size) = r?;
            staged.push((tmp, path));
            report.bytes += size;
        }

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
        for dir in touched_dirs {
            sync_dir(&dir)?;
        }
        // One drive-cache flush makes every file and directory synced above durable.
        crate::durable::barrier(dest)?;

        Ok(report)
    }
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
fn existing_regular_file(dest: &Path, rel: &Path) -> Option<PathBuf> {
    let mut cur = dest.to_path_buf();
    let n = rel.components().count();
    for (i, component) in rel.components().enumerate() {
        cur.push(component);
        let m = fs::symlink_metadata(&cur).ok()?;
        let ok = if i + 1 == n { m.is_file() } else { m.is_dir() };
        if !ok {
            return None;
        }
    }
    Some(cur)
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

fn verify_file(path: &Path, size: u64, sha256: &str) -> std::result::Result<(), String> {
    let mut file = fs::File::open(path).map_err(|e| e.to_string())?;
    let actual_size = file.metadata().map_err(|e| e.to_string())?.len();
    if actual_size != size {
        return Err(format!("size {actual_size}, expected {size}"));
    }

    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf).map_err(|e| e.to_string())?;
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
