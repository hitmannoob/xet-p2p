//! Embedded local CAS backed by upstream `LocalClient`, plus tree ingest and reconstruction.

use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use xet_client::cas_client::{Client, DirectAccessClient, LocalClient};
use xet_core_structures::merklehash::MerkleHash;
use xet_data::deduplication::DeduplicationMetrics;
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo};
use xet_runtime::core::XetContext;

use crate::error::{Error, Result};
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
    ctx: XetContext,
    config: Arc<TranslatorConfig>,
    /// The same store upstream sessions open for `config` (redb handles are shared per path).
    client: Arc<LocalClient>,
}

/// SHA-256 of zero bytes. Upstream 1.8.0 reports all zeros for empty files.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Content a manifest references that this CAS cannot yet reconstruct.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MissingContent {
    /// Files whose reconstruction metadata (shard) is absent.
    pub files_without_reconstruction: Vec<String>,
    /// Xorbs referenced by known reconstructions but not present locally.
    pub xorbs: BTreeSet<MerkleHash>,
}

impl MissingContent {
    pub fn is_empty(&self) -> bool {
        self.files_without_reconstruction.is_empty() && self.xorbs.is_empty()
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
    pub files: usize,
    pub dirs: usize,
    pub bytes: u64,
}

impl LocalCas {
    /// Opens (creating if needed) a local CAS rooted at `root`.
    ///
    /// When called inside a multi-threaded tokio runtime, upstream reuses that runtime.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        let root = fs::canonicalize(root)?;
        let ctx = XetContext::default()?;
        let config = Arc::new(TranslatorConfig::local_config(&ctx, &root)?);
        // Mirrors `xet_data::processing::create_remote_client` for `local://` endpoints.
        let client = LocalClient::new(ctx.clone(), root.join("xet").join("xorbs"))
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        Ok(Self {
            root,
            ctx,
            config,
            client,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
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
                WalkedKind::File { size, executable } => {
                    report.files += 1;
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
                missing
                    .files_without_reconstruction
                    .push(entry.path.clone());
                continue;
            };
            for segment in &info.segments {
                if !missing.xorbs.contains(&segment.xorb_hash)
                    && !self
                        .client
                        .xorb_exists(&segment.xorb_hash)
                        .await
                        .map_err(|e| Error::Xet(e.to_string()))?
                {
                    missing.xorbs.insert(segment.xorb_hash);
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
    pub async fn reconstruct(
        &self,
        manifest: &Manifest,
        dest: impl AsRef<Path>,
    ) -> Result<ReconstructReport> {
        manifest.validate()?;
        let missing = self.missing_content(manifest).await?;
        if !missing.is_empty() {
            return Err(Error::MissingContent {
                files: missing.files_without_reconstruction.len(),
                xorbs: missing.xorbs.len(),
            });
        }

        let dest = dest.as_ref();
        fs::create_dir_all(dest)?;
        let dest = fs::canonicalize(dest)?;

        let staging = dest.join(staging_name());
        fs::create_dir(&staging)?;
        let result = self.reconstruct_via(manifest, &dest, &staging).await;
        let _ = fs::remove_dir_all(&staging);
        result
    }

    async fn reconstruct_via(
        &self,
        manifest: &Manifest,
        dest: &Path,
        staging: &Path,
    ) -> Result<ReconstructReport> {
        let mut report = ReconstructReport::default();
        let mut staged = Vec::new();

        let session = FileDownloadSession::from_client(&self.ctx, self.client.clone(), None);
        for (i, (entry, size, xet_hash, sha256, executable)) in manifest.files().enumerate() {
            let tmp = staging.join(i.to_string());
            session
                .download_file(&XetFileInfo::new(xet_hash.to_owned(), size), &tmp)
                .await?;
            verify_file(&tmp, size, sha256).map_err(|reason| Error::Verification {
                path: entry.path.clone(),
                reason,
            })?;
            set_executable(&tmp, executable)?;
            staged.push((tmp, entry));
            report.bytes += size;
        }
        session.finalize().await?;

        for entry in &manifest.entries {
            if matches!(entry.kind, EntryKind::Dir) {
                ensure_dir(dest, &validate_rel_path(&entry.path)?)?;
                report.dirs += 1;
            }
        }

        for (tmp, entry) in staged {
            let rel = validate_rel_path(&entry.path)?;
            if let Some(parent) = rel.parent() {
                ensure_dir(dest, parent)?;
            }
            let target = dest.join(&rel);
            match fs::symlink_metadata(&target) {
                Ok(m) if m.is_file() => {}
                Ok(_) => {
                    return Err(Error::UnsafePath {
                        path: entry.path.clone(),
                        reason: "destination exists and is not a regular file",
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            fs::rename(&tmp, &target)?;
            report.files += 1;
        }

        Ok(report)
    }
}

fn parse_hash(path: &str, hex: &str) -> Result<MerkleHash> {
    MerkleHash::from_hex(hex)
        .map_err(|e| Error::InvalidManifest(format!("{path:?}: bad xet_hash: {e}")))
}

fn staging_name() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{STAGING_PREFIX}{}-{nanos}", std::process::id())
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

fn set_executable(path: &Path, executable: bool) -> Result<()> {
    #[cfg(unix)]
    if executable {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        let mode = perms.mode();
        // Grant execute wherever read is granted.
        perms.set_mode(mode | ((mode & 0o444) >> 2));
        fs::set_permissions(path, perms)?;
    }
    #[cfg(not(unix))]
    let _ = (path, executable);
    Ok(())
}
