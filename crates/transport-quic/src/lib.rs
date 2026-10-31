//! QUIC transport for `xet-p2p`: Quinn endpoints with pinned mutual TLS, and bounded framing.
//!
//! This crate moves bytes and authenticates peers. It knows nothing about Xet objects.

mod endpoint;
mod error;
mod frame;
pub mod identity;
mod verify;

pub use endpoint::{
    ALPN, EndpointOptions, bind_addr_for, client_endpoint, connect, peer_certificate,
    server_endpoint,
};
pub use error::{Error, Result};
pub use frame::{read_frame, read_payload, write_frame};
pub use identity::{GeneratedIdentity, Identity, fingerprint, generate, load_cert};
pub use quinn;
