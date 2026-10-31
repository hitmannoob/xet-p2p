//! Transport-agnostic pieces of `xet-p2p`: the connection interface ([`Connection`]), peer
//! identities and certificate pinning shared by every transport, and bounded framing.
//! Implementations live in `transport-quic` and `transport-tcp`.

mod conn;
mod error;
mod frame;
pub mod identity;
pub mod verify;

pub use conn::{
    Conn, Connection, Incoming, Listener, RecvStream, SendStream, TransportKind, WireStats,
};
pub use error::{Error, Result};
pub use frame::{read_frame, read_payload, write_frame};
pub use identity::{GeneratedIdentity, Identity, SERVER_NAME, fingerprint, generate, load_cert};
pub use rustls;
