//! The provider adapter interface.
//!
//! An adapter only connects to an existing sandbox and runs things in it: it never chunks, caches,
//! or encodes content. It reports what the sandbox can do, starts the `xet-p2p` agent there with
//! credentials this session generated, says how the agent can be reached, and cleans up.

use std::path::PathBuf;

use crate::error::Result;

/// What a sandbox can do, as determined by the adapter (by checks where possible).
#[derive(Debug, Clone, Default)]
pub struct Capabilities {
    /// e.g. "linux", "macos".
    pub os: String,
    /// e.g. "x86_64", "aarch64".
    pub arch: String,
    /// Absolute path every transfer path must lie under.
    pub workspace_root: String,
    /// Whether the agent can accept QUIC (UDP) from this machine.
    pub inbound_udp: bool,
    /// Whether the agent can accept TCP from this machine (possibly through a relay).
    pub inbound_tcp: bool,
    /// Whether the sandbox filesystem (and so the agent's CAS) survives the sandbox.
    pub persistent_storage: bool,
    /// Free-form notes for `doctor`.
    pub notes: Vec<String>,
}

/// Everything the agent needs for one session.
#[derive(Debug, Clone)]
pub struct AgentSpec {
    /// PEM certificate and key the agent presents (generated for this session only).
    pub server_cert_pem: String,
    pub server_key_pem: String,
    /// PEM certificate of the only client allowed to connect (this machine, this session).
    pub client_cert_pem: String,
    /// Sandbox path of the agent's CAS.
    pub cas_dir: String,
}

/// A reachable endpoint of a running agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentEndpoint {
    pub host_port: String,
    /// The provider relays the bytes (a tunnel or proxy is in the data path).
    pub relayed: bool,
}

#[derive(Debug, Clone, Default)]
pub struct AgentEndpoints {
    pub quic: Option<AgentEndpoint>,
    pub tcp: Option<AgentEndpoint>,
}

#[async_trait::async_trait]
pub trait SandboxProvider: Send + Sync {
    fn name(&self) -> &'static str;
    /// Attaches to an existing sandbox. Never creates one.
    async fn attach(
        &self,
        id: &str,
        options: &std::collections::BTreeMap<String, String>,
    ) -> Result<Box<dyn Sandbox>>;
}

#[async_trait::async_trait]
pub trait Sandbox: Send + Sync {
    fn id(&self) -> &str;
    async fn capabilities(&self) -> Result<Capabilities>;
    /// Starts a per-session agent with `spec`. Returns how it can be reached.
    async fn start_agent(&self, spec: &AgentSpec) -> Result<AgentEndpoints>;
    /// Stops the agent and removes session credentials. Never removes user files or the CAS.
    async fn close(&self) -> Result<()>;
}

/// Where the local `xet-p2p` binary is, for providers that run the agent on this machine.
pub fn current_binary() -> Result<PathBuf> {
    Ok(std::env::current_exe()?)
}
