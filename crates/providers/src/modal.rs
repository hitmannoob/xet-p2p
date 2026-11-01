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
//! `modal` command on `PATH`), using the user's existing Modal credentials. One helper process
//! serves the whole command (JSON lines over stdin/stdout), and remote work is batched into as
//! few Modal API calls as possible, since each is a round trip to Modal's servers. The helper
//! attaches with `Sandbox.from_id`; it never creates or terminates sandboxes.
//!
//! The agent must be a Linux build matching the sandbox CPU: `$XET_P2P_AGENT_LINUX_BIN` (see the
//! README for cross-compiling with `cargo zigbuild`).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
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

/// Idle timeout for an agent started for one command (stopped explicitly when the command ends).
const SAFETY_IDLE_EXIT: Duration = Duration::from_secs(120);

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
        let helper = Helper::spawn(&self.python)?;
        let sandbox = ModalSandbox {
            id: id.to_owned(),
            agent: self.agent.clone(),
            port,
            root: abs("root", DEFAULT_ROOT)?,
            state: abs("state", DEFAULT_STATE)?,
            helper: Mutex::new(helper),
            tunnel: Mutex::new(None),
            uname: Mutex::new(None),
            running: Mutex::new(None),
        };
        // Attaching means: the sandbox exists and answers (Sandbox.from_id + tunnels).
        let v = sandbox
            .call(
                json!({"cmd": "attach", "id": id, "port": port}),
                Duration::from_secs(90),
            )
            .await?;
        *sandbox.tunnel.lock().await = v["tunnel"]
            .as_array()
            .map(|t| format!("{}:{}", t[0].as_str().unwrap_or_default(), t[1]));
        Ok(Box::new(sandbox))
    }
}

/// One helper process for the whole command.
struct Helper {
    _child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Helper {
    fn spawn(python: &PathBuf) -> Result<Self> {
        let mut child = Command::new(python)
            .arg("-u")
            .arg("-c")
            .arg(HELPER)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                Error::Provider(format!(
                    "cannot run the Modal SDK's Python ({}): {e}",
                    python.display()
                ))
            })?;
        Ok(Self {
            stdin: child.stdin.take().expect("piped"),
            stdout: BufReader::new(child.stdout.take().expect("piped")),
            _child: child,
        })
    }
}

struct ModalSandbox {
    id: String,
    agent: Option<PathBuf>,
    port: u16,
    root: String,
    state: String,
    helper: Mutex<Helper>,
    tunnel: Mutex<Option<String>>,
    uname: Mutex<Option<String>>,
    running: Mutex<Option<(u64, String)>>,
}

impl ModalSandbox {
    async fn call(&self, request: Value, timeout: Duration) -> Result<Value> {
        let cmd = request["cmd"].as_str().unwrap_or("?").to_owned();
        let mut h = self.helper.lock().await;
        let exchange = async {
            h.stdin.write_all(format!("{request}\n").as_bytes()).await?;
            h.stdin.flush().await?;
            let mut line = String::new();
            if h.stdout.read_line(&mut line).await? == 0 {
                return Err(Error::Provider(format!(
                    "modal {cmd}: the helper exited (is the Modal SDK installed?)"
                )));
            }
            serde_json::from_str::<Value>(&line)
                .map_err(|_| Error::Provider(format!("modal {cmd}: bad helper reply {line:?}")))
        };
        let v = tokio::time::timeout(timeout, exchange)
            .await
            .map_err(|_| {
                Error::Provider(format!("modal {cmd}: no answer in {}s", timeout.as_secs()))
            })??;
        if let Some(e) = v.get("error").and_then(Value::as_str) {
            return Err(Error::Provider(format!("modal {cmd}: {e}")));
        }
        Ok(v)
    }
}

#[async_trait::async_trait]
impl Sandbox for ModalSandbox {
    fn id(&self) -> &str {
        &self.id
    }

    async fn capabilities(&self) -> Result<Capabilities> {
        let tunnel = self.tunnel.lock().await.clone();
        let (os, arch) = match self.uname.lock().await.as_deref() {
            Some(u) => {
                let mut it = u.split_whitespace();
                (
                    it.next().unwrap_or("unknown").to_lowercase(),
                    it.next().unwrap_or("unknown").to_owned(),
                )
            }
            None => ("checked when the agent starts".into(), String::new()),
        };
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
            os,
            arch,
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
        let Some(tunnel) = self.tunnel.lock().await.clone() else {
            return Err(Error::Provider(format!(
                "sandbox has no unencrypted (raw TCP) tunnel for port {}; create it with \
                 unencrypted_ports=[{}] (Modal cannot add tunnels to a running sandbox)",
                self.port, self.port
            )));
        };
        let session = format!(
            "{}/session-{}",
            self.state,
            hex::encode(rand::random::<[u8; 8]>())
        );
        let v = self
            .call(
                json!({
                    "cmd": "bootstrap",
                    "id": self.id,
                    "port": self.port,
                    "root": self.root,
                    "cas_dir": spec.cas_dir,
                    "state_dir": self.state,
                    "session_dir": session,
                    "agent": agent,
                    // Every agent exits once idle, so one whose stop is lost (e.g. the network
                    // dropped mid-command) cannot hold the port for good; only kept agents may
                    // be replaced by a later command.
                    "idle_exit_secs": spec.idle_exit.unwrap_or(SAFETY_IDLE_EXIT).as_secs().max(1),
                    "kept": spec.idle_exit.is_some(),
                    "server_cert_pem": spec.server_cert_pem,
                    "server_key_pem": spec.server_key_pem,
                    "client_cert_pem": spec.client_cert_pem,
                }),
                Duration::from_secs(300),
            )
            .await?;
        *self.uname.lock().await = v["uname"].as_str().map(str::to_owned);
        *self.running.lock().await = Some((v["pid"].as_u64().unwrap_or(0), session));
        Ok(AgentEndpoints {
            quic: None,
            tcp: Some(AgentEndpoint {
                host_port: tunnel,
                relayed: true,
            }),
        })
    }

    async fn close(&self) -> Result<()> {
        if let Some((pid, session)) = self.running.lock().await.take() {
            self.call(
                json!({ "cmd": "stop", "id": self.id, "pid": pid, "session_dir": session }),
                Duration::from_secs(90),
            )
            .await?;
        }
        Ok(())
    }

    async fn detach(&self) -> Result<()> {
        // The agent was started with an idle timeout: it exits and cleans up by itself.
        self.running.lock().await.take();
        Ok(())
    }
}
