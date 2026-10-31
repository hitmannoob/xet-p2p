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
    sandbox: Option<Box<dyn Sandbox>>,
    quic: Option<QuicClient>,
}

impl Connected {
    /// Closes the connection and, for sandboxes, stops the agent and removes session credentials.
    pub async fn finish(self) -> Result<()> {
        self.conn.close(0, b"done");
        if let Some(q) = &self.quic {
            let _ = tokio::time::timeout(Duration::from_secs(5), q.wait_idle()).await;
        }
        if let Some(sb) = self.sandbox {
            sb.close().await?;
        }
        Ok(())
    }
}

const QUIC_ATTEMPT: Duration = Duration::from_secs(5);

pub async fn connect_target(
    registry: &Registry,
    target: &Target,
    policy: TransportPolicy,
    direct: Option<DirectCredentials>,
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
                sandbox: None,
                quic,
            })
        }
        Target::Sandbox {
            provider,
            id,
            path,
            options,
        } => {
            let provider = registry.get(provider)?;
            let sandbox = provider.attach(id, options).await?;
            match connect_sandbox(sandbox.as_ref(), path, policy).await {
                Ok(mut c) => {
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
) -> Result<Connected> {
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
                return Ok(Connected {
                    transport: conn.kind(),
                    conn,
                    path: rel,
                    relayed: ep.relayed,
                    notes,
                    sandbox: None,
                    quic,
                });
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
    let addr = tokio::net::lookup_host(&ep.host_port)
        .await?
        .next()
        .ok_or_else(|| Error::Unreachable(format!("{} resolved to no address", ep.host_port)))?;
    match kind {
        TransportKind::Quic => {
            let client = client_endpoint(
                identity,
                vec![pinned],
                &EndpointOptions::default(),
                bind_addr_for(addr),
            )?;
            let conn = tokio::time::timeout(QUIC_ATTEMPT, client.connect(addr))
                .await
                .map_err(|_| {
                    Error::Unreachable(format!(
                        "no QUIC answer from {addr} in {}s",
                        QUIC_ATTEMPT.as_secs()
                    ))
                })??;
            Ok((conn, Some(client)))
        }
        TransportKind::TlsTcp => Ok((
            transport_tcp::tcp_connect(addr, identity, vec![pinned]).await?,
            None,
        )),
    }
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
    };
    let endpoints = match sandbox.start_agent(&spec).await {
        Ok(e) => {
            report.agent = Ok(());
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
