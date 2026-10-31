//! Source tree walking. Only regular files and directories are accepted; symlinks are rejected.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::manifest::validate_rel_path;

pub(crate) enum WalkedKind {
    Dir,
    File { size: u64, executable: bool },
}

pub(crate) struct WalkedEntry {
    pub rel_path: String,
    pub abs_path: PathBuf,
    pub kind: WalkedKind,
}

/// Walks `src` and returns entries in sorted, deterministic order.
///
/// A directory yields its descendants (not itself). A single file yields one entry named after it.
pub(crate) fn walk(src: &Path) -> Result<Vec<WalkedEntry>> {
    let meta = fs::symlink_metadata(src)?;
    let mut out = Vec::new();

    if meta.is_file() {
        let name = src
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| non_utf8(src))?
            .to_owned();
        validate_rel_path(&name)?;
        out.push(WalkedEntry {
            rel_path: name,
            abs_path: src.to_owned(),
            kind: file_kind(&meta),
        });
    } else if meta.is_dir() {
        walk_dir(src, "", &mut out)?;
    } else {
        return Err(Error::UnsupportedFileType(src.to_owned()));
    }
    Ok(out)
}

fn walk_dir(dir: &Path, prefix: &str, out: &mut Vec<WalkedEntry>) -> Result<()> {
    let mut children = fs::read_dir(dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    children.sort();

    for abs_path in children {
        let name = abs_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| non_utf8(&abs_path))?;
        let rel_path = if prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{prefix}/{name}")
        };
        validate_rel_path(&rel_path)?;

        let meta = fs::symlink_metadata(&abs_path)?;
        if meta.is_dir() {
            out.push(WalkedEntry {
                rel_path: rel_path.clone(),
                abs_path: abs_path.clone(),
                kind: WalkedKind::Dir,
            });
            walk_dir(&abs_path, &rel_path, out)?;
        } else if meta.is_file() {
            out.push(WalkedEntry {
                rel_path,
                abs_path,
                kind: file_kind(&meta),
            });
        } else {
            // Symlinks, sockets, FIFOs, devices: deferred per the brief.
            return Err(Error::UnsupportedFileType(abs_path));
        }
    }
    Ok(())
}

fn file_kind(meta: &fs::Metadata) -> WalkedKind {
    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    };
    #[cfg(not(unix))]
    let executable = false;

    WalkedKind::File {
        size: meta.len(),
        executable,
    }
}

fn non_utf8(path: &Path) -> Error {
    Error::UnsafePath {
        path: path.to_string_lossy().into_owned(),
        reason: "file name is not valid UTF-8",
    }
}
