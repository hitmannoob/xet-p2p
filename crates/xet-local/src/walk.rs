//! Source tree walking. Only regular files and directories are accepted; symlinks are rejected.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::manifest::validate_rel_path;

pub(crate) enum WalkedKind {
    Dir,
    File {
        size: u64,
        executable: bool,
        modified: Option<std::time::SystemTime>,
        stamp: Option<crate::ingest_cache::Stamp>,
    },
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
        out = walk_parallel(src)?;
    } else {
        return Err(Error::UnsupportedFileType(src.to_owned()));
    }
    Ok(out)
}

/// Walks the tree under `root` with several threads (directory listings and `lstat`s are slow
/// one at a time on some filesystems, e.g. gVisor sandboxes), then sorts the entries into the
/// order a sequential depth-first walk over sorted names gives: by path components.
fn walk_parallel(root: &Path) -> Result<Vec<WalkedEntry>> {
    use std::collections::VecDeque;
    use std::sync::{Condvar, Mutex};

    struct Shared {
        queue: VecDeque<(PathBuf, String)>,
        active: usize,
        err: Option<Error>,
        out: Vec<WalkedEntry>,
    }
    let shared = Mutex::new(Shared {
        queue: VecDeque::from([(root.to_owned(), String::new())]),
        active: 0,
        err: None,
        out: Vec::new(),
    });
    let wake = Condvar::new();
    let threads = std::thread::available_parallelism()
        .map_or(4, |n| n.get())
        .clamp(1, 8);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                loop {
                    let (dir, prefix) = {
                        let mut g = shared.lock().unwrap();
                        loop {
                            if g.err.is_some() {
                                return;
                            }
                            if let Some(job) = g.queue.pop_front() {
                                g.active += 1;
                                break job;
                            }
                            if g.active == 0 {
                                wake.notify_all();
                                return;
                            }
                            g = wake.wait(g).unwrap();
                        }
                    };
                    let listed = list_dir(&dir, &prefix);
                    let mut g = shared.lock().unwrap();
                    g.active -= 1;
                    match listed {
                        Ok((entries, subdirs)) => {
                            g.out.extend(entries);
                            g.queue.extend(subdirs);
                        }
                        Err(e) => {
                            g.err.get_or_insert(e);
                        }
                    }
                    wake.notify_all();
                }
            });
        }
    });
    let shared = shared.into_inner().unwrap();
    if let Some(e) = shared.err {
        return Err(e);
    }
    let mut out = shared.out;
    out.sort_by(|a, b| a.rel_path.split('/').cmp(b.rel_path.split('/')));
    Ok(out)
}

/// One directory's entries (directories and regular files), and its subdirectories to walk.
#[allow(clippy::type_complexity)]
fn list_dir(dir: &Path, prefix: &str) -> Result<(Vec<WalkedEntry>, Vec<(PathBuf, String)>)> {
    let (mut entries, mut subdirs) = (Vec::new(), Vec::new());
    for child in fs::read_dir(dir)? {
        let abs_path = child?.path();
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
            entries.push(WalkedEntry {
                rel_path: rel_path.clone(),
                abs_path: abs_path.clone(),
                kind: WalkedKind::Dir,
            });
            subdirs.push((abs_path, rel_path));
        } else if meta.is_file() {
            entries.push(WalkedEntry {
                rel_path,
                abs_path,
                kind: file_kind(&meta),
            });
        } else {
            // Symlinks, sockets, FIFOs, devices: deferred per the brief.
            return Err(Error::UnsupportedFileType(abs_path));
        }
    }
    Ok((entries, subdirs))
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
        modified: meta.modified().ok(),
        stamp: crate::ingest_cache::Stamp::of(meta),
    }
}

fn non_utf8(path: &Path) -> Error {
    Error::UnsafePath {
        path: path.to_string_lossy().into_owned(),
        reason: "file name is not valid UTF-8",
    }
}

#[cfg(test)]
mod order_tests {
    use super::*;

    #[test]
    fn parallel_walk_keeps_depth_first_sorted_order() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path();
        for d in ["a/b/c", "a-x", "a.b", "b", "a/z"] {
            fs::create_dir_all(root.join(d)).unwrap();
        }
        for f in [
            "a/b/c/f1", "a/b/f2", "a/f3", "a-x/f4", "a.b/f5", "b/f6", "f7", "a/z/f8",
        ] {
            fs::write(root.join(f), b"x").unwrap();
        }
        let got: Vec<String> = walk(root)
            .unwrap()
            .into_iter()
            .map(|e| e.rel_path)
            .collect();
        assert_eq!(
            got,
            [
                "a", "a/b", "a/b/c", "a/b/c/f1", "a/b/f2", "a/f3", "a/z", "a/z/f8", "a-x",
                "a-x/f4", "a.b", "a.b/f5", "b", "b/f6", "f7"
            ]
        );
    }
}
