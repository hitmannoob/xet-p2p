//! Turning a target into an authenticated connection, under a transport policy.

use std::time::Duration;

use transport::rustls::pki_types::CertificateDer;
use transport::{Conn, Identity, TransportKind, generate};
use transport_quic::{EndpointOptions, QuicClient, bind_addr_for, client_endpoint};

use crate::error::{Error, Result};
use crate::provider::{
    AgentEndpoint, AgentEndpoints, AgentSpec, Capabilities, Sandbox, SandboxProvider,
};
use crate::uri::{DirectTransport, Target};

/// Which transports may be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportPolicy {
    /// Prefer QUIC; fall back to TLS over TCP, and say so.
    Auto,
    /// QUIC only: fail rather than fall back.
    Quic,
    /// TLS over TCP only.
    Tcp,
}

impl std::str::FromStr for TransportPolicy {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "auto" => Ok(Self::Auto),
            "quic" => Ok(Self::Quic),
            "tcp" => Ok(Self::Tcp),
            _ => Err(Error::BadUri(format!(
                "unknown transport {s:?} (auto, quic, tcp)"
            ))),
        }
    }
}

/// Long-lived credentials for direct targets (`quic://`, `tcp://`), from `keygen`.
pub struct DirectCredentials {
    pub identity: Identity,
    pub pinned_server: CertificateDer<'static>,
}

pub struct Registry {
    providers: Vec<Box<dyn SandboxProvider>>,
}

impl Registry {
    pub fn new(providers: Vec<Box<dyn SandboxProvider>>) -> Self {
        Self { providers }
    }

    fn get(&self, name: &str) -> Result<&dyn SandboxProvider> {
        if let Some(p) = self.providers.iter().find(|p| p.name() == name) {
            return Ok(p.as_ref());
        }
        if matches!(name, "e2b" | "daytona") {
            return Err(Error::Unsupported(name.into()));
        }
        let known: Vec<_> = self.providers.iter().map(|p| p.name()).collect();
        Err(Error::UnknownProvider(
            name.into(),
            format!("quic, tcp, {}", known.join(", ")),
        ))
    }
}

/// An open connection to a target's agent, plus what it took to get there.
pub struct Connected {
    pub conn: Conn,
    /// The transfer path, relative to the agent's root.
    pub path: String,
    pub transport: TransportKind,
    /// A provider relay is in the data path.
    pub relayed: bool,
    /// What was tried and why, for the user.
    pub notes: Vec<String>,
    /// Reused a kept agent from an earlier command (no provider calls were made).
    pub reused_agent: bool,
    /// Time spent attaching and starting the agent (zero when reused or direct).
    pub bootstrap: Duration,
    sandbox: Option<Box<dyn Sandbox>>,
    quic: Option<QuicClient>,
    keep: bool,
}

impl Connected {
    /// Closes the connection. For sandboxes, stops the agent and removes its credentials, unless
    /// it was started to be kept (it then exits by itself once idle).
    pub async fn finish(self) -> Result<()> {
        self.conn.close(0, b"done");
        if let Some(q) = &self.quic {
            let _ = tokio::time::timeout(Duration::from_secs(5), q.wait_idle()).await;
        }
        if let Some(sb) = self.sandbox {
            if self.keep {
                sb.detach().await?;
            } else {
                sb.close().await?;
            }
        }
        Ok(())
    }
}

/// Keep the agent running between commands (`--keep-agent`).
#[derive(Debug, Clone)]
pub struct KeepAgent {
    /// The agent exits after this much idle time.
    pub idle: Duration,
    /// Where this machine caches kept sessions (client key, pinned agent cert, endpoint).
    pub cache_dir: std::path::PathBuf,
}

/// A kept agent session, cached locally (mode 0600: it holds this client's private key).
#[derive(serde::Serialize, serde::Deserialize)]
struct KeptSession {
    client_cert_pem: String,
    client_key_pem: String,
    server_cert_pem: String,
    host_port: String,
    relayed: bool,
    quic: bool,
    workspace_root: String,
    expires_unix: u64,
    /// [`agent_build`] when the agent was started: a rebuilt xet-p2p starts a new agent instead
    /// of reusing one running older code.
    #[serde(default)]
    agent_build: String,
}

/// Identifies the binaries an agent is started from (this executable, and the Linux agent build
/// named by `$XET_P2P_AGENT_LINUX_BIN`) by path, size and modification time.
fn agent_build() -> String {
    let id = |p: Option<std::path::PathBuf>| {
        p.and_then(|p| std::fs::metadata(&p).ok().map(|m| (p, m)))
            .map(|(p, m)| {
                let t = m
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map_or(0, |d| d.as_nanos());
                format!("{}:{}:{t}", p.display(), m.len())
            })
            .unwrap_or_else(|| "none".into())
    };
    format!(
        "exe={};linux={}",
        id(std::env::current_exe().ok()),
        id(std::env::var_os("XET_P2P_AGENT_LINUX_BIN").map(Into::into))
    )
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

fn kept_path(
    keep: &KeepAgent,
    provider: &str,
    id: &str,
    options: &std::collections::BTreeMap<String, String>,
) -> std::path::PathBuf {
    let opts: Vec<String> = options.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let key = format!("{provider}://{id}?{}", opts.join("&"));
    let digest = transport::fingerprint(key.as_bytes());
    keep.cache_dir
        .join(format!("{provider}-{}.json", &digest[..16]))
}

fn save_kept(path: &std::path::Path, session: &KeptSession) -> Result<()> {
    use std::io::Write;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(&tmp)?;
    f.write_all(&serde_json::to_vec(session).map_err(|e| Error::Provider(e.to_string()))?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Reconnects to a kept agent without any provider call. Returns `None` (and forgets the cached
/// session) if it has expired or no longer answers.
async fn reuse_kept(
    path: &std::path::Path,
    target_path: &str,
    policy: TransportPolicy,
) -> Option<Connected> {
    let session: KeptSession = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    let forget = || {
        let _ = std::fs::remove_file(path);
    };
    let kind = if session.quic {
        TransportKind::Quic
    } else {
        TransportKind::TlsTcp
    };
    let allowed = match policy {
        TransportPolicy::Auto => true,
        TransportPolicy::Quic => kind == TransportKind::Quic,
        TransportPolicy::Tcp => kind == TransportKind::TlsTcp,
    };
    if session.expires_unix <= now_unix() || !allowed || session.agent_build != agent_build() {
        forget();
        return None;
    }
    let caps = Capabilities {
        workspace_root: session.workspace_root.clone(),
        ..Default::default()
    };
    let rel = relative_to_workspace(&caps, target_path).ok()?;
    let identity = Identity::from_pem(&session.client_cert_pem, &session.client_key_pem).ok()?;
    let pinned = {
        use transport::rustls::pki_types::pem::PemObject;
        CertificateDer::from_pem_slice(session.server_cert_pem.as_bytes()).ok()?
    };
    let ep = AgentEndpoint {
        host_port: session.host_port.clone(),
        relayed: session.relayed,
    };
    match dial(kind, &ep, identity, pinned).await {
        Ok((conn, quic)) => {
            let mut notes = vec!["reused the agent kept from an earlier command".to_string()];
            if kind != TransportKind::Quic {
                notes.push(format!("using {} — this is not QUIC", conn.kind()));
            }
            if session.relayed {
                notes.push("a provider relay is in the data path".into());
            }
            Some(Connected {
                transport: conn.kind(),
                conn,
                path: rel,
                relayed: session.relayed,
                notes,
                reused_agent: true,
                bootstrap: Duration::ZERO,
                sandbox: None,
                quic,
                keep: true,
            })
        }
        Err(_) => {
            forget();
            None
        }
    }
}

/// Per-address connection attempt limit (handshake included).
const ATTEMPT: Duration = Duration::from_secs(8);

pub async fn connect_target(
    registry: &Registry,
    target: &Target,
    policy: TransportPolicy,
    direct: Option<DirectCredentials>,
    keep: Option<KeepAgent>,
) -> Result<Connected> {
    match target {
        Target::Direct {
            transport,
            host_port,
            path,
        } => {
            let creds = direct.ok_or_else(|| {
                Error::Provider(
                    "direct targets need --cert, --key and --peer-cert (from `xet-p2p keygen`)"
                        .into(),
                )
            })?;
            let wanted = match (transport, policy) {
                (DirectTransport::Quic, TransportPolicy::Tcp)
                | (DirectTransport::Tcp, TransportPolicy::Quic) => {
                    return Err(Error::BadUri(
                        "the URI scheme and --transport disagree".into(),
                    ));
                }
                (DirectTransport::Quic, _) => TransportKind::Quic,
                (DirectTransport::Tcp, _) => TransportKind::TlsTcp,
            };
            let ep = AgentEndpoint {
                host_port: host_port.clone(),
                relayed: false,
            };
            let (conn, quic) = dial(wanted, &ep, creds.identity, creds.pinned_server).await?;
            Ok(Connected {
                transport: conn.kind(),
                conn,
                path: path.clone(),
                relayed: false,
                notes: Vec::new(),
                reused_agent: false,
                bootstrap: Duration::ZERO,
                sandbox: None,
                quic,
                keep: false,
            })
        }
        Target::Sandbox {
            provider,
            id,
            path,
            options,
        } => {
            let kept = keep.as_ref().map(|k| kept_path(k, provider, id, options));
            if let Some(p) = &kept
                && let Some(c) = reuse_kept(p, path, policy).await
            {
                return Ok(c);
            }
            let started = std::time::Instant::now();
            let provider = registry.get(provider)?;
            let sandbox = provider.attach(id, options).await?;
            let idle = keep.as_ref().map(|k| k.idle);
            match connect_sandbox(sandbox.as_ref(), path, policy, idle).await {
                Ok((mut c, session)) => {
                    c.bootstrap = started.elapsed();
                    if let (Some(p), Some(session)) = (&kept, session) {
                        save_kept(p, &session)?;
                        c.keep = true;
                        c.notes.push(format!(
                            "agent kept for reuse; it exits after {} min idle",
                            idle.unwrap_or_default().as_secs() / 60
                        ));
                    }
                    c.sandbox = Some(sandbox);
                    Ok(c)
                }
                Err(e) => {
                    let _ = sandbox.close().await;
                    Err(e)
                }
            }
        }
    }
}

async fn connect_sandbox(
    sandbox: &dyn Sandbox,
    path: &str,
    policy: TransportPolicy,
    idle: Option<Duration>,
) -> Result<(Connected, Option<KeptSession>)> {
    let caps = sandbox.capabilities().await?;
    let rel = relative_to_workspace(&caps, path)?;
    let mut notes = Vec::new();
    match policy {
        TransportPolicy::Quic if !caps.inbound_udp => {
            return Err(Error::Unreachable(format!(
                "--transport quic requested, but sandbox {} cannot accept UDP from here; \
                 use --transport auto (or tcp) to allow the TLS-over-TCP fallback",
                sandbox.id()
            )));
        }
        TransportPolicy::Tcp if !caps.inbound_tcp => {
            return Err(Error::Unreachable(format!(
                "sandbox {} cannot accept TCP from here",
                sandbox.id()
            )));
        }
        _ => {}
    }

    // Credentials for this session only: the agent trusts exactly this client, and we pin
    // exactly this agent. Both keys are discarded when the session ends.
    let server = generate("xet-p2p-agent").map_err(Error::from)?;
    let client = generate("xet-p2p-client").map_err(Error::from)?;
    let spec = AgentSpec {
        server_cert_pem: server.cert_pem.clone(),
        server_key_pem: server.key_pem.clone(),
        client_cert_pem: client.cert_pem.clone(),
        cas_dir: format!("{}/.xet-p2p/cas", caps.workspace_root.trim_end_matches('/')),
        idle_exit: idle,
    };
    let endpoints = sandbox.start_agent(&spec).await?;
    let pinned = Identity::from_pem(&server.cert_pem, &server.key_pem)?.cert;

    let order: Vec<TransportKind> = match policy {
        TransportPolicy::Auto => vec![TransportKind::Quic, TransportKind::TlsTcp],
        TransportPolicy::Quic => vec![TransportKind::Quic],
        TransportPolicy::Tcp => vec![TransportKind::TlsTcp],
    };
    for kind in order {
        let ep = match (kind, &endpoints) {
            (TransportKind::Quic, AgentEndpoints { quic: Some(e), .. }) if caps.inbound_udp => e,
            (TransportKind::TlsTcp, AgentEndpoints { tcp: Some(e), .. }) if caps.inbound_tcp => e,
            (k, _) => {
                notes.push(format!("{k}: not offered by the sandbox"));
                continue;
            }
        };
        let identity = Identity::from_pem(&client.cert_pem, &client.key_pem)?;
        match dial(kind, ep, identity, pinned.clone()).await {
            Ok((conn, quic)) => {
                if kind != TransportKind::Quic && policy == TransportPolicy::Auto {
                    notes.push(format!("using {} — this is not QUIC", conn.kind()));
                }
                if ep.relayed {
                    notes.push("a provider relay is in the data path".into());
                }
                let kept = idle.map(|idle| KeptSession {
                    client_cert_pem: client.cert_pem.clone(),
                    client_key_pem: client.key_pem.clone(),
                    server_cert_pem: server.cert_pem.clone(),
                    host_port: ep.host_port.clone(),
                    relayed: ep.relayed,
                    quic: kind == TransportKind::Quic,
                    workspace_root: caps.workspace_root.clone(),
                    // Conservative: assume the agent may exit `idle` after this session starts.
                    expires_unix: now_unix() + idle.as_secs(),
                    agent_build: agent_build(),
                });
                return Ok((
                    Connected {
                        transport: conn.kind(),
                        conn,
                        path: rel,
                        relayed: ep.relayed,
                        notes,
                        reused_agent: false,
                        bootstrap: Duration::ZERO,
                        sandbox: None,
                        quic,
                        keep: false,
                    },
                    kept,
                ));
            }
            Err(e) => notes.push(format!("{kind} to {}: {e}", ep.host_port)),
        }
    }
    Err(Error::Unreachable(notes.join("; ")))
}

/// The sandbox path relative to the workspace root, which it must lie strictly under.
fn relative_to_workspace(caps: &Capabilities, path: &str) -> Result<String> {
    let root = caps.workspace_root.trim_end_matches('/');
    let rel = if root.is_empty() {
        path.trim_start_matches('/')
    } else {
        path.strip_prefix(root)
            .and_then(|r| r.strip_prefix('/'))
            .ok_or_else(|| {
                Error::BadUri(format!(
                    "{path:?} is outside the sandbox workspace {:?}",
                    caps.workspace_root
                ))
            })?
    };
    if rel.is_empty() {
        return Err(Error::BadUri(format!(
            "{path:?}: name a directory inside the workspace, not the workspace itself"
        )));
    }
    Ok(rel.to_owned())
}

async fn dial(
    kind: TransportKind,
    ep: &AgentEndpoint,
    identity: Identity,
    pinned: CertificateDer<'static>,
) -> Result<(Conn, Option<QuicClient>)> {
    // Try every address, IPv4 first: networks with DNS64 but broken NAT64 return unusable
    // synthesized IPv6 addresses first, and a single attempt would hang on them.
    let t = std::time::Instant::now();
    let mut addrs: Vec<_> = tokio::net::lookup_host(&ep.host_port).await?.collect();
    tracing::debug!(host = %ep.host_port, n = addrs.len(), ms = t.elapsed().as_millis() as u64, "resolved");
    addrs.sort_by_key(|a| !a.is_ipv4());
    if addrs.is_empty() {
        return Err(Error::Unreachable(format!(
            "{} resolved to no address",
            ep.host_port
        )));
    }
    let mut failures = Vec::new();
    for addr in addrs {
        let attempt = async {
            match kind {
                TransportKind::Quic => {
                    let client = client_endpoint(
                        identity.clone_for_attempt()?,
                        vec![pinned.clone()],
                        &EndpointOptions::default(),
                        bind_addr_for(addr),
                    )?;
                    let conn = client.connect(addr).await?;
                    Ok::<_, Error>((conn, Some(client)))
                }
                TransportKind::TlsTcp => Ok((
                    transport_tcp::tcp_connect(
                        addr,
                        identity.clone_for_attempt()?,
                        vec![pinned.clone()],
                    )
                    .await?,
                    None,
                )),
            }
        };
        match tokio::time::timeout(ATTEMPT, attempt).await {
            Ok(Ok(c)) => return Ok(c),
            Ok(Err(e)) => failures.push(format!("{addr}: {e}")),
            Err(_) => failures.push(format!("{addr}: no answer in {}s", ATTEMPT.as_secs())),
        }
    }
    Err(Error::Unreachable(format!(
        "{kind} to {}: {}",
        ep.host_port,
        failures.join("; ")
    )))
}

/// What `doctor` found.
#[derive(Debug, Clone)]
pub struct DoctorReport {
    pub provider: String,
    pub sandbox: String,
    pub capabilities: Option<Capabilities>,
    pub agent: std::result::Result<(), String>,
    pub quic: Probe,
    pub tcp: Probe,
    /// What `--transport auto` would use.
    pub auto_choice: Option<TransportKind>,
    pub relayed: bool,
}

#[derive(Debug, Clone)]
pub enum Probe {
    Ok(String),
    Failed(String),
    NotOffered(String),
}

/// Attaches, inspects, starts an agent, tries every transport, and cleans up. Touches no data.
pub async fn doctor(registry: &Registry, target: &Target) -> Result<DoctorReport> {
    let Target::Sandbox {
        provider,
        id,
        options,
        ..
    } = target
    else {
        return Err(Error::BadUri(
            "doctor inspects provider targets (provider://SANDBOX_ID); for a direct host just push to it".into(),
        ));
    };
    let p = registry.get(provider)?;
    let mut report = DoctorReport {
        provider: provider.clone(),
        sandbox: id.clone(),
        capabilities: None,
        agent: Err("not started".into()),
        quic: Probe::NotOffered("not tried".into()),
        tcp: Probe::NotOffered("not tried".into()),
        auto_choice: None,
        relayed: false,
    };
    let sandbox = p.attach(id, options).await?;
    let caps = match sandbox.capabilities().await {
        Ok(c) => c,
        Err(e) => {
            report.agent = Err(format!("capabilities: {e}"));
            return Ok(report);
        }
    };
    report.capabilities = Some(caps.clone());

    let server = generate("xet-p2p-agent").map_err(Error::from)?;
    let client = generate("xet-p2p-client").map_err(Error::from)?;
    let spec = AgentSpec {
        server_cert_pem: server.cert_pem.clone(),
        server_key_pem: server.key_pem.clone(),
        client_cert_pem: client.cert_pem.clone(),
        cas_dir: format!("{}/.xet-p2p/cas", caps.workspace_root.trim_end_matches('/')),
        idle_exit: None,
    };
    let endpoints = match sandbox.start_agent(&spec).await {
        Ok(e) => {
            report.agent = Ok(());
            // Some providers learn the platform only when the agent starts.
            if let Ok(c) = sandbox.capabilities().await {
                report.capabilities = Some(c);
            }
            e
        }
        Err(e) => {
            report.agent = Err(e.to_string());
            let _ = sandbox.close().await;
            return Ok(report);
        }
    };
    let pinned = Identity::from_pem(&server.cert_pem, &server.key_pem)?.cert;
    for (kind, ep, allowed) in [
        (TransportKind::Quic, &endpoints.quic, caps.inbound_udp),
        (TransportKind::TlsTcp, &endpoints.tcp, caps.inbound_tcp),
    ] {
        let probe = match ep {
            Some(ep) if allowed => {
                let identity = Identity::from_pem(&client.cert_pem, &client.key_pem)?;
                match dial(kind, ep, identity, pinned.clone()).await {
                    Ok((conn, _q)) => {
                        conn.close(0, b"doctor");
                        if report.auto_choice.is_none() {
                            report.auto_choice = Some(kind);
                            report.relayed = ep.relayed;
                        }
                        Probe::Ok(format!(
                            "{} {}",
                            ep.host_port,
                            if ep.relayed {
                                "(relayed by provider)"
                            } else {
                                "(direct)"
                            }
                        ))
                    }
                    Err(e) => Probe::Failed(e.to_string()),
                }
            }
            _ => Probe::NotOffered("the sandbox cannot accept it".into()),
        };
        match kind {
            TransportKind::Quic => report.quic = probe,
            TransportKind::TlsTcp => report.tcp = probe,
        }
    }
    sandbox.close().await?;
    Ok(report)
}
