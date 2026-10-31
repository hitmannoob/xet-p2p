pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Local(#[from] xet_local::Error),

    #[error(transparent)]
    Transport(#[from] transport::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("transfer stalled: no data from the sender for {}s", .0.as_secs_f64())]
    Stalled(std::time::Duration),

    /// The peer reported a failure.
    #[error("peer reported error: {0}")]
    Remote(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Transport(e.into())
    }
}
