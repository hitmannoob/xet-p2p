pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid target: {0}")]
    BadUri(String),

    #[error("unknown provider {0:?} (supported: {1})")]
    UnknownProvider(String, String),

    #[error("provider {0} is not supported yet")]
    Unsupported(String),

    #[error("{0}")]
    Provider(String),

    /// No transport allowed by the policy can reach the agent.
    #[error("cannot reach the agent: {0}")]
    Unreachable(String),

    #[error(transparent)]
    Transport(#[from] transport::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
