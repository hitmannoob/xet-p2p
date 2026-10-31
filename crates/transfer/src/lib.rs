//! Transfer coordination for `xet-p2p`: the control protocol, the sender (`push`), and the
//! receiver. Xet content handling lives in `xet-local`; authentication and framing in
//! `transport-quic`.

mod error;
mod locks;
pub mod protocol;
mod receiver;
mod sender;
mod session;

pub use error::{Error, Result};
pub use protocol::ReceiverReport;
pub use receiver::{Receiver, ReceiverOptions};
pub use sender::{PushOptions, PushReport, push};
pub use session::SessionRecord;
