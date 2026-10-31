pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("certificate error: {0}")]
    Cert(String),

    #[error("TLS error: {0}")]
    Tls(#[from] rustls::Error),

    #[error("configuration error: {0}")]
    Config(String),

    /// Establishing or keeping the connection failed (handshake, peer rejected, lost).
    #[error("connection error: {0}")]
    Connection(String),

    #[error("stream error: {0}")]
    Stream(String),

    #[error("frame of {len} bytes exceeds limit of {max}")]
    FrameTooLarge { len: u64, max: u64 },

    #[error("encode error: {0}")]
    Encode(String),

    #[error("decode error: {0}")]
    Decode(String),
}
