//! Transfer coordination for `xet-p2p`: the control protocol, the sender (`push`), and the
//! receiver. Xet content handling lives in `xet-local`; authentication and framing in
//! `transport-quic`.

mod budget;
mod error;
mod fetch;
mod locks;
pub mod protocol;
mod pull;
mod rate;
mod receiver;
mod sender;
pub mod session;

pub use error::{Error, Result};
pub use protocol::{ReceiverReport, TransferMode};
pub use pull::{PullOptions, PullReport, pull};
pub use receiver::{Receiver, ReceiverOptions};
pub use sender::{DEFAULT_MAX_MEMORY_BYTES, PushOptions, PushReport, push};
pub use session::SessionRecord;

/// Opens (creating if needed) a receiver session directory, discarding state idle longer than
/// [`session::STATE_MAX_AGE`]. Used by `gc`.
pub fn purge_stale_session_state(dir: &std::path::Path) -> Result<()> {
    session::SessionStore::open(dir).map(|_| ())
}
