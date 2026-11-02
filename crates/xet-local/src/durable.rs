//! Durability primitives.
//!
//! On Linux `fsync` makes data durable. On macOS it only hands data to the drive, which may cache
//! it; durability needs `F_FULLFSYNC`, which flushes the whole drive cache and is expensive
//! (milliseconds). Apple's guidance for many files is: `fsync` each, then one `F_FULLFSYNC` as a
//! barrier. [`sync_file`]/[`sync_dir`] are the cheap per-object step; [`barrier`] must follow
//! before reporting anything durable.

use std::fs::File;
use std::io;
use std::path::Path;

/// Orders `file`'s data to storage (durable on Linux; needs [`barrier`] on macOS).
pub fn sync_file(file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: plain fsync(2) on a valid, owned descriptor.
        if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(target_vendor = "apple"))]
    file.sync_all()
}

/// [`sync_file`] for a directory, making entries created or renamed in it durable (Unix only).
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    return sync_file(&File::open(dir)?);
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Makes everything previously synced on the volume holding `path` durable. One `F_FULLFSYNC`
/// on macOS; a no-op where `fsync` is already durable.
pub fn barrier(path: &Path) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        File::open(path)?.sync_all() // std uses F_FULLFSYNC on Apple platforms
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        let _ = path;
        Ok(())
    }
}

/// Whether staged files can skip their own fsync because [`sync_filesystem`] will make them all
/// durable in one call before they are installed (Linux `syncfs`). Thousands of small files
/// otherwise cost one fsync each, which is slow on some filesystems (e.g. gVisor sandboxes).
pub fn batched() -> bool {
    cfg!(target_os = "linux")
}

/// A staged file's sync: now, unless [`batched`].
pub fn sync_staged(file: &File) -> io::Result<()> {
    if batched() { Ok(()) } else { sync_file(file) }
}

/// Makes everything written on the filesystem holding `path` durable, data and metadata
/// (`syncfs(2)` on Linux). Unsupported elsewhere.
pub fn sync_filesystem(path: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let f = File::open(path)?;
        // SAFETY: syncfs(2) on a valid, owned descriptor.
        if unsafe { libc::syncfs(f.as_raw_fd()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        Err(io::Error::new(io::ErrorKind::Unsupported, "syncfs"))
    }
}

/// `sync_file` + `sync_dir(parent)` + `barrier`: one object, fully durable.
pub fn persist(path: &Path) -> io::Result<()> {
    sync_file(&File::open(path)?)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    barrier(path)
}
