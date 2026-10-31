//! Provider-agnostic targets for `xet-p2p`: URI parsing, the sandbox adapter interface,
//! per-session credentials, transport policy, and `doctor`.
//!
//! Adapters attach to existing sandboxes and start the agent; they never touch Xet content. The
//! Xet engine (`xet-local`, `transfer`) is the same for every provider and transport.

mod error;
pub mod local;
pub mod provider;
mod session;
pub mod uri;

pub use error::{Error, Result};
pub use provider::{
    AgentEndpoint, AgentEndpoints, AgentSpec, Capabilities, Sandbox, SandboxProvider,
};
pub use session::{
    Connected, DirectCredentials, DoctorReport, Probe, Registry, TransportPolicy, connect_target,
    doctor,
};
pub use uri::{DirectTransport, Target};
