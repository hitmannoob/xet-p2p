//! `local://NAME/...`: a fake provider whose "sandboxes" are directories on this machine.
//!
//! It exercises the whole provider path — attach to an existing sandbox, report capabilities,
//! write per-session credentials into the sandbox, start a real `xet-p2p serve` agent there,
//! connect, clean up — without any cloud account. Options simulate restricted networking:
//! `?udp=off` (no inbound UDP: QUIC impossible) and `?tcp=off`.
//!
//! Sandboxes live under `$XET_P2P_LOCAL_SANDBOXES` (default `~/.cache/xet-p2p/local-sandboxes`);
//! a sandbox's directory is its `/`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::provider::{
    AgentEndpoint, AgentEndpoints, AgentSpec, Capabilities, Sandbox, SandboxProvider,
};

pub struct LocalProvider {
    base: PathBuf,
    agent_binary: PathBuf,
}

impl LocalProvider {
    pub fn new(base: PathBuf, agent_binary: PathBuf) -> Self {
        Self { base, agent_binary }
    }

    /// The default sandbox base directory.
    pub fn default_base() -> PathBuf {
        if let Some(dir) = std::env::var_os("XET_P2P_LOCAL_SANDBOXES") {
            return dir.into();
        }
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default();
        home.join(".cache/xet-p2p/local-sandboxes")
    }
}

#[async_trait::async_trait]
impl SandboxProvider for LocalProvider {
    fn name(&self) -> &'static str {
        "local"
    }

    async fn attach(
        &self,
        id: &str,
        options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Sandbox>> {
        for k in options.keys() {
            if !matches!(k.as_str(), "udp" | "tcp") {
                return Err(Error::Provider(format!("local: unknown option {k:?}")));
            }
        }
        let dir = self.base.join(id);
        let meta = std::fs::symlink_metadata(&dir).map_err(|_| {
            Error::Provider(format!(
                "local sandbox {id:?} does not exist (create {} first; attach never creates)",
                dir.display()
            ))
        })?;
        if !meta.is_dir() {
            return Err(Error::Provider(format!(
                "local sandbox {id:?} is not a directory"
            )));
        }
        let off = |k: &str| options.get(k).is_some_and(|v| v == "off");
        Ok(Box::new(LocalSandbox {
            id: id.to_owned(),
            dir: std::fs::canonicalize(&dir)?,
            binary: self.agent_binary.clone(),
            udp: !off("udp"),
            tcp: !off("tcp"),
            running: Mutex::new(None),
        }))
    }
}

struct LocalSandbox {
    id: String,
    dir: PathBuf,
    binary: PathBuf,
    udp: bool,
    tcp: bool,
    running: Mutex<Option<(Child, PathBuf)>>,
}

#[async_trait::async_trait]
impl Sandbox for LocalSandbox {
    fn id(&self) -> &str {
        &self.id
    }

    async fn capabilities(&self) -> Result<Capabilities> {
        Ok(Capabilities {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            workspace_root: "/".into(),
            inbound_udp: self.udp,
            inbound_tcp: self.tcp,
            persistent_storage: true,
            notes: vec![format!("sandbox filesystem is {}", self.dir.display())],
        })
    }

    async fn start_agent(&self, spec: &AgentSpec) -> Result<AgentEndpoints> {
        let session = self.dir.join(".xet-p2p").join(format!(
            "session-{}",
            hex::encode(rand::random::<[u8; 8]>())
        ));
        std::fs::create_dir_all(&session)?;
        write_private(&session.join("server.key"), &spec.server_key_pem)?;
        std::fs::write(session.join("server.crt"), &spec.server_cert_pem)?;
        std::fs::write(session.join("client.crt"), &spec.client_cert_pem)?;
        let ready = session.join("ready.json");
        let log = std::fs::File::create(session.join("agent.log"))?;
        let cas = sandbox_path(&self.dir, &spec.cas_dir)?;

        let mut cmd = Command::new(&self.binary);
        cmd.arg("serve")
            .arg("--root")
            .arg(&self.dir)
            .arg("--cas-dir")
            .arg(&cas)
            .arg("--cert")
            .arg(session.join("server.crt"))
            .arg("--key")
            .arg(session.join("server.key"))
            .arg("--allow-client")
            .arg(session.join("client.crt"))
            .arg("--ready-file")
            .arg(&ready);
        if self.udp {
            cmd.args(["--listen", "127.0.0.1:0"]);
        } else {
            cmd.arg("--no-quic");
        }
        if self.tcp {
            cmd.args(["--listen-tcp", "127.0.0.1:0"]);
        }
        if let Some(idle) = spec.idle_exit {
            // A kept agent outlives this command, exits once idle, and deletes its credentials.
            cmd.arg("--idle-exit-secs")
                .arg(idle.as_secs().max(1).to_string())
                .arg("--cleanup-on-exit")
                .arg(&session);
        }
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .kill_on_drop(spec.idle_exit.is_none());
        let mut child = cmd.spawn().map_err(|e| {
            Error::Provider(format!("cannot start agent {}: {e}", self.binary.display()))
        })?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let addrs: serde_json::Value = loop {
            if let Ok(bytes) = std::fs::read(&ready)
                && let Ok(v) = serde_json::from_slice(&bytes)
            {
                break v;
            }
            if let Some(status) = child.try_wait()? {
                let tail = std::fs::read_to_string(session.join("agent.log")).unwrap_or_default();
                return Err(Error::Provider(format!(
                    "agent exited ({status}): {}",
                    tail.trim()
                )));
            }
            if tokio::time::Instant::now() > deadline {
                let _ = child.kill().await;
                return Err(Error::Provider("agent did not become ready in 20 s".into()));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        *self.running.lock().await = Some((child, session));

        let endpoint = |k: &str| {
            addrs[k].as_str().map(|a| AgentEndpoint {
                host_port: a.to_owned(),
                relayed: false,
            })
        };
        Ok(AgentEndpoints {
            quic: endpoint("quic"),
            tcp: endpoint("tcp"),
        })
    }

    async fn detach(&self) -> Result<()> {
        // Leave a kept agent running; dropping the handle does not kill it.
        self.running.lock().await.take();
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        if let Some((mut child, session)) = self.running.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
            std::fs::remove_dir_all(&session)?;
        }
        Ok(())
    }
}

/// Maps an absolute sandbox path onto the sandbox directory.
fn sandbox_path(dir: &Path, abs: &str) -> Result<PathBuf> {
    let rel = abs.trim_start_matches('/');
    if rel.split('/').any(|s| s == ".." || s == ".") {
        return Err(Error::Provider(format!("invalid sandbox path {abs:?}")));
    }
    Ok(dir.join(rel))
}

fn write_private(path: &Path, data: &str) -> Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)?.write_all(data.as_bytes())?;
    Ok(())
}
