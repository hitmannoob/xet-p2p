pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Local(#[from] xet_local::Error),

    #[error(transparent)]
    Transport(#[from] transport_quic::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("transfer stalled: no data from the sender for {}s", .0.as_secs_f64())]
    Stalled(std::time::Duration),

    /// The peer reported a failure.
    #[error("peer reported error: {0}")]
    Remote(String),
}

impl From<transport_quic::quinn::ConnectionError> for Error {
    fn from(e: transport_quic::quinn::ConnectionError) -> Self {
        Error::Transport(e.into())
    }
}

impl From<transport_quic::quinn::WriteError> for Error {
    fn from(e: transport_quic::quinn::WriteError) -> Self {
        Error::Transport(e.into())
    }
}
