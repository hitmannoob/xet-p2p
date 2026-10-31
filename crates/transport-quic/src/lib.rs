//! QUIC transport for `xet-p2p`: Quinn endpoints with pinned mutual TLS, implementing the
//! `transport` connection interface.
//!
//! This crate moves bytes and authenticates peers. It knows nothing about Xet objects.

mod endpoint;

pub use endpoint::{
    ALPN, EndpointOptions, QuicClient, QuicListener, bind_addr_for, client_endpoint, connect,
    server_endpoint,
};
pub use quinn;
pub use transport::identity;
pub use transport::{
    Conn, Error, GeneratedIdentity, Identity, Result, fingerprint, generate, load_cert, read_frame,
    read_payload, write_frame,
};
