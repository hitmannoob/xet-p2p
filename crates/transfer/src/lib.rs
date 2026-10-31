//! Transfer coordination for `xet-p2p`: the control protocol, the sender (`push`), and the
//! receiver. Xet content handling lives in `xet-local`; authentication and framing in
//! `transport-quic`.

mod budget;
mod error;
mod locks;
pub mod protocol;
mod receiver;
mod sender;
mod session;

pub use error::{Error, Result};
pub use protocol::{ReceiverReport, TransferMode};
pub use receiver::{Receiver, ReceiverOptions};
pub use sender::{DEFAULT_MAX_MEMORY_BYTES, PushOptions, PushReport, push};
pub use session::SessionRecord;
