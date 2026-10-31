//! End-to-end push tests over QUIC (UDP, localhost).

const TRANSPORT: transport::TransportKind = transport::TransportKind::Quic;
include!("suite/push_suite.rs");
