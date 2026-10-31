//! `modal://SANDBOX_ID/abs/path[?port=N&root=/abs&state=/abs]`: an existing Modal sandbox.
//!
//! What Modal offers (verified against modal.com/docs/guide/sandbox-networking and SDK 1.5.5):
//! sandbox ports are reachable only through tunnels declared when the sandbox is created
//! (`unencrypted_ports=[N]` gives a raw TCP tunnel; nothing exposes UDP). So the agent listens on
//! TCP port `N` and is reached through Modal's tunnel with the TLS-over-TCP transport: not QUIC,
//! and relayed by Modal. Modal's raw TCP tunnels accept connections from any address; the
//! session's pinned mutual TLS is what authenticates both ends.
//!
//! There is no Rust SDK: the adapter drives a small helper (`modal_helper.py`) with the Python
//! that has the Modal SDK installed (`$XET_P2P_MODAL_PYTHON`, else the interpreter of the
//! `modal` command on `PATH`), using the user's existing Modal credentials. The helper attaches
//! with `Sandbox.from_id`; it never creates or terminates sandboxes.
//!
//! The agent must be a Linux build matching the sandbox CPU: `$XET_P2P_AGENT_LINUX_BIN` (see the
//! README for cross-compiling with `cargo zigbuild`).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::provider::{
    AgentEndpoint, AgentEndpoints, AgentSpec, Capabilities, Sandbox, SandboxProvider,
};

const HELPER: &str = include_str!("modal_helper.py");
const DEFAULT_PORT: u16 = 4434;
const DEFAULT_ROOT: &str = "/workspace";
const DEFAULT_STATE: &str = "/tmp/xet-p2p";

pub struct ModalProvider {
    python: PathBuf,
    agent: Option<PathBuf>,
}

impl ModalProvider {
    /// Finds the Modal SDK's Python and the Linux agent binary from the environment.
    pub fn from_env() -> Option<Self> {
        let python = std::env::var_os("XET_P2P_MODAL_PYTHON")
            .map(PathBuf::from)
            .or_else(modal_python)?;
        Some(Self {
            python,
            agent: std::env::var_os("XET_P2P_AGENT_LINUX_BIN").map(PathBuf::from),
        })
    }

    pub fn new(python: PathBuf, agent: Option<PathBuf>) -> Self {
        Self { python, agent }
    }
}

/// The interpreter on the `modal` command's shebang line.
fn modal_python() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    let modal = std::env::split_paths(&path)
        .map(|d| d.join("modal"))
        .find(|p| p.is_file())?;
    let first = std::fs::read(&modal).ok()?;
    let line = first.split(|b| *b == b'\n').next()?;
    let line = std::str::from_utf8(line).ok()?.strip_prefix("#!")?.trim();
    let interp = line.split_whitespace().next()?;
    Some(PathBuf::from(interp))
}

#[async_trait::async_trait]
impl SandboxProvider for ModalProvider {
    fn name(&self) -> &'static str {
        "modal"
    }

    async fn attach(
        &self,
        id: &str,
        options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Sandbox>> {
        for k in options.keys() {
            if !matches!(k.as_str(), "port" | "root" | "state") {
                return Err(Error::Provider(format!("modal: unknown option {k:?}")));
            }
        }
        let port = match options.get("port") {
            Some(p) => p
                .parse()
                .map_err(|_| Error::Provider(format!("modal: bad port {p:?}")))?,
            None => DEFAULT_PORT,
        };
        let abs = |k: &str, default: &str| -> Result<String> {
            let v = options.get(k).map(String::as_str).unwrap_or(default);
            if !v.starts_with('/') || v.split('/').any(|s| s == ".." || s == ".") {
                return Err(Error::Provider(format!(
                    "modal: {k} must be an absolute path"
                )));
            }
            Ok(v.trim_end_matches('/').to_owned())
        };
        let sandbox = ModalSandbox {
            id: id.to_owned(),
            python: self.python.clone(),
            agent: self.agent.clone(),
            port,
            root: abs("root", DEFAULT_ROOT)?,
            state: abs("state", DEFAULT_STATE)?,
            running: Mutex::new(None),
        };
        // Attaching means: the sandbox exists and answers. (`info` uses Sandbox.from_id.)
        sandbox.info().await?;
        Ok(Box::new(sandbox))
    }
}

struct ModalSandbox {
    id: String,
    python: PathBuf,
    agent: Option<PathBuf>,
    port: u16,
    root: String,
    state: String,
    running: Mutex<Option<(u64, String)>>,
}

impl ModalSandbox {
    async fn helper(&self, command: &str, request: Value, timeout: Duration) -> Result<Value> {
        let mut child = Command::new(&self.python)
            .arg("-c")
            .arg(HELPER)
            .arg(command)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                Error::Provider(format!(
                    "cannot run the Modal SDK's Python ({}): {e}",
                    self.python.display()
                ))
            })?;
        let mut stdin = child.stdin.take().expect("piped");
        stdin.write_all(request.to_string().as_bytes()).await?;
        drop(stdin);
        let out = tokio::time::timeout(timeout, child.wait_with_output())
            .await
            .map_err(|_| {
                Error::Provider(format!(
                    "modal {command}: no answer in {}s",
                    timeout.as_secs()
                ))
            })??;
        let v: Value = serde_json::from_slice(&out.stdout).map_err(|_| {
            Error::Provider(format!(
                "modal {command}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        })?;
        if let Some(e) = v.get("error").and_then(Value::as_str) {
            return Err(Error::Provider(format!("modal {command}: {e}")));
        }
        Ok(v)
    }

    async fn info(&self) -> Result<Value> {
        self.helper(
            "info",
            json!({ "id": self.id, "port": self.port }),
            Duration::from_secs(90),
        )
        .await
    }
}

#[async_trait::async_trait]
impl Sandbox for ModalSandbox {
    fn id(&self) -> &str {
        &self.id
    }

    async fn capabilities(&self) -> Result<Capabilities> {
        let info = self.info().await?;
        let tunnel = info["tunnel"]
            .as_array()
            .map(|t| format!("{}:{}", t[0].as_str().unwrap_or("?"), t[1]));
        let mut notes = vec![
            "Modal exposes sandbox ports only through tunnels; none carry UDP, so QUIC is impossible".into(),
            "traffic is relayed by Modal's tunnel; pinned mutual TLS authenticates both ends".into(),
        ];
        match &tunnel {
            Some(t) => notes.push(format!("raw TCP tunnel for port {}: {t}", self.port)),
            None => notes.push(format!(
                "no raw TCP tunnel for port {}: the sandbox must be created with unencrypted_ports=[{}]",
                self.port, self.port
            )),
        }
        Ok(Capabilities {
            os: info["os"].as_str().unwrap_or("unknown").into(),
            arch: info["arch"].as_str().unwrap_or("unknown").into(),
            workspace_root: self.root.clone(),
            inbound_udp: false,
            inbound_tcp: tunnel.is_some(),
            persistent_storage: false,
            notes,
        })
    }

    async fn start_agent(&self, spec: &AgentSpec) -> Result<AgentEndpoints> {
        let agent = self.agent.as_ref().ok_or_else(|| {
            Error::Provider(
                "set XET_P2P_AGENT_LINUX_BIN to a Linux build of xet-p2p matching the sandbox CPU"
                    .into(),
            )
        })?;
        let session = format!(
            "{}/session-{}",
            self.state,
            hex::encode(rand::random::<[u8; 8]>())
        );
        let v = self
            .helper(
                "start",
                json!({
                    "id": self.id,
                    "port": self.port,
                    "root": self.root,
                    "cas_dir": spec.cas_dir,
                    "state_dir": self.state,
                    "session_dir": session,
                    "agent": agent,
                    "server_cert_pem": spec.server_cert_pem,
                    "server_key_pem": spec.server_key_pem,
                    "client_cert_pem": spec.client_cert_pem,
                }),
                Duration::from_secs(300),
            )
            .await?;
        let pid = v["pid"].as_u64().unwrap_or(0);
        *self.running.lock().await = Some((pid, session));
        let tcp = v["tcp"].as_array().map(|t| AgentEndpoint {
            host_port: format!("{}:{}", t[0].as_str().unwrap_or_default(), t[1]),
            relayed: true,
        });
        Ok(AgentEndpoints { quic: None, tcp })
    }

    async fn close(&self) -> Result<()> {
        if let Some((pid, session)) = self.running.lock().await.take() {
            self.helper(
                "stop",
                json!({ "id": self.id, "pid": pid, "session_dir": session }),
                Duration::from_secs(90),
            )
            .await?;
        }
        Ok(())
    }
}
