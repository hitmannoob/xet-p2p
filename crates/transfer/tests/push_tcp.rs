//! End-to-end push tests over the TLS-over-TCP fallback transport (localhost).

const TRANSPORT: transport::TransportKind = transport::TransportKind::TlsTcp;
include!("suite/push_suite.rs");
