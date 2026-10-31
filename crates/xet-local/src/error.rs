use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An error surfaced by the upstream Xet crates.
    #[error("Xet error: {0}")]
    Xet(String),

    #[error("unsafe path {path:?}: {reason}")]
    UnsafePath { path: String, reason: &'static str },

    #[error("unsupported file type at {0}: only regular files and directories are supported")]
    UnsupportedFileType(PathBuf),

    #[error("invalid manifest: {0}")]
    InvalidManifest(String),

    #[error("local CAS is missing content: {files} file reconstruction(s), {xorbs} xorb(s)")]
    MissingContent { files: usize, xorbs: usize },

    #[error("verification failed for {path:?}: {reason}")]
    Verification { path: String, reason: String },
}

impl From<xet_data::DataError> for Error {
    fn from(e: xet_data::DataError) -> Self {
        Error::Xet(e.to_string())
    }
}

impl From<xet_runtime::error::RuntimeError> for Error {
    fn from(e: xet_runtime::error::RuntimeError) -> Self {
        Error::Xet(e.to_string())
    }
}
