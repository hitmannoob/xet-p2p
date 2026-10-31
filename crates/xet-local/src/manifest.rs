//! File-tree manifest: path metadata kept separate from Xet's content-addressed objects.

use std::collections::HashSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Upper bound on a single relative path, in bytes.
pub const MAX_PATH_BYTES: usize = 4096;

/// Reserved name prefix for receiver staging directories; never valid in a manifest.
pub(crate) const STAGING_PREFIX: &str = ".xet-p2p-staging-";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub entries: Vec<Entry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Relative path with `/` separators. Validated by [`validate_rel_path`].
    pub path: String,
    #[serde(flatten)]
    pub kind: EntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EntryKind {
    Dir,
    File {
        size: u64,
        /// Xet file hash (hex), the key for reconstruction.
        xet_hash: String,
        /// SHA-256 of the file contents (hex), checked after reconstruction.
        sha256: String,
        #[serde(default)]
        executable: bool,
    },
}

impl Manifest {
    /// Checks every entry path and rejects duplicates or a file used as a directory.
    /// Receivers must call this before acting on a manifest from a peer.
    pub fn validate(&self) -> Result<()> {
        if self.format_version != MANIFEST_FORMAT_VERSION {
            return Err(Error::InvalidManifest(format!(
                "unsupported format version {} (expected {MANIFEST_FORMAT_VERSION})",
                self.format_version
            )));
        }

        let mut seen = HashSet::new();
        let mut files = HashSet::new();
        for entry in &self.entries {
            validate_rel_path(&entry.path)?;
            if !seen.insert(entry.path.as_str()) {
                return Err(Error::InvalidManifest(format!(
                    "duplicate path {:?}",
                    entry.path
                )));
            }
            if let EntryKind::File {
                xet_hash, sha256, ..
            } = &entry.kind
            {
                check_hex(&entry.path, "xet_hash", xet_hash)?;
                check_hex(&entry.path, "sha256", sha256)?;
                files.insert(entry.path.as_str());
            }
        }

        for entry in &self.entries {
            let mut ancestor = entry.path.as_str();
            while let Some((parent, _)) = ancestor.rsplit_once('/') {
                if files.contains(parent) {
                    return Err(Error::InvalidManifest(format!(
                        "{:?} is a file but is used as a directory by {:?}",
                        parent, entry.path
                    )));
                }
                ancestor = parent;
            }
        }
        Ok(())
    }

    pub fn files(&self) -> impl Iterator<Item = (&Entry, u64, &str, &str, bool)> {
        self.entries.iter().filter_map(|e| match &e.kind {
            EntryKind::File {
                size,
                xet_hash,
                sha256,
                executable,
            } => Some((e, *size, xet_hash.as_str(), sha256.as_str(), *executable)),
            EntryKind::Dir => None,
        })
    }

    pub fn total_file_bytes(&self) -> u64 {
        self.files().map(|(_, size, ..)| size).sum()
    }
}

fn check_hex(path: &str, field: &str, value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::InvalidManifest(format!(
            "{path:?}: {field} is not a 32-byte hex hash"
        )));
    }
    Ok(())
}

/// Validates a manifest-relative path and converts it to a native relative `PathBuf`.
///
/// Rejects empty paths, absolute paths, `.`/`..` components, empty components, backslashes,
/// NUL bytes, overlong paths, and the reserved staging prefix.
pub fn validate_rel_path(path: &str) -> Result<PathBuf> {
    let unsafe_path = |reason| Error::UnsafePath {
        path: path.to_owned(),
        reason,
    };

    if path.is_empty() {
        return Err(unsafe_path("empty path"));
    }
    if path.len() > MAX_PATH_BYTES {
        return Err(unsafe_path("path too long"));
    }
    if path.starts_with('/') {
        return Err(unsafe_path("absolute path"));
    }
    if path.contains('\\') {
        return Err(unsafe_path("backslash in path"));
    }
    if path.contains('\0') {
        return Err(unsafe_path("NUL byte in path"));
    }

    let mut out = PathBuf::new();
    for (i, component) in path.split('/').enumerate() {
        match component {
            "" => return Err(unsafe_path("empty path component")),
            "." | ".." => return Err(unsafe_path("relative traversal component")),
            c if i == 0 && c.starts_with(STAGING_PREFIX) => {
                return Err(unsafe_path("reserved name"));
            }
            c => out.push(c),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_normal_paths() {
        for p in ["a", "a/b/c.txt", "ünïcödé/文件.bin", ".hidden", "a..b"] {
            assert!(validate_rel_path(p).is_ok(), "{p}");
        }
    }

    #[test]
    fn rejects_unsafe_paths() {
        for p in [
            "",
            "/etc/passwd",
            "../x",
            "a/../../x",
            "a/./b",
            "a//b",
            "a/",
            "a\\b",
            "a\0b",
            ".xet-p2p-staging-1/x",
        ] {
            assert!(validate_rel_path(p).is_err(), "{p:?}");
        }
        assert!(validate_rel_path(&"a".repeat(MAX_PATH_BYTES + 1)).is_err());
    }

    fn file(path: &str) -> Entry {
        Entry {
            path: path.into(),
            kind: EntryKind::File {
                size: 0,
                xet_hash: "0".repeat(64),
                sha256: "0".repeat(64),
                executable: false,
            },
        }
    }

    #[test]
    fn rejects_bad_manifests() {
        let m = |entries| Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            entries,
        };
        assert!(m(vec![file("a"), file("b/c")]).validate().is_ok());
        assert!(m(vec![file("a"), file("a")]).validate().is_err());
        assert!(m(vec![file("a"), file("a/b")]).validate().is_err());
        let mut bad_hash = file("a");
        if let EntryKind::File { xet_hash, .. } = &mut bad_hash.kind {
            *xet_hash = "zz".into();
        }
        assert!(m(vec![bad_hash]).validate().is_err());
        assert!(
            Manifest {
                format_version: 99,
                entries: vec![]
            }
            .validate()
            .is_err()
        );
    }
}
