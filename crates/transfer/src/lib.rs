//! Transfer coordination for `xet-p2p`: the control protocol, the sender (`push`), and the
//! receiver. Xet content handling lives in `xet-local`; authentication and framing in
//! `transport-quic`.

mod error;
pub mod protocol;
mod receiver;
mod sender;

pub use error::{Error, Result};
pub use protocol::ReceiverReport;
pub use receiver::Receiver;
pub use sender::{PushOptions, PushReport, push};
