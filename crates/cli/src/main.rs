use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use transfer::{
    PullOptions, PushOptions, PushReport, Receiver, ReceiverOptions, TransferMode, prepare, pull,
    push_prepared,
};
use transport::Listener as _;
use transport_quic::{
    EndpointOptions, Identity, fingerprint, generate, load_cert, server_endpoint,
};
use xet_local::{LocalCas, Manifest};

mod profile;

#[cfg(target_env = "musl")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(
    name = "xet-p2p",
    version,
    about = "Direct peer-to-peer file transfer over native Xet storage"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a self-signed peer identity (certificate + private key).
    Keygen {
        /// Output directory.
        #[arg(long)]
        out: PathBuf,
        /// File stem: writes <name>.crt and <name>.key.
        #[arg(long)]
        name: String,
    },
    /// Receive pushes: listen for authorized senders and install into --root.
    Serve {
        #[arg(long, default_value = "0.0.0.0:4433")]
        listen: SocketAddr,
        /// Also accept the TLS-over-TCP transport on this address (for paths without UDP).
        #[arg(long)]
        listen_tcp: Option<SocketAddr>,
        /// Do not listen for QUIC (only with --listen-tcp).
        #[arg(long, requires = "listen_tcp")]
        no_quic: bool,
        /// Once listening, write {"quic": ADDR|null, "tcp": ADDR|null} here (agent bootstrap).
        #[arg(long, hide = true)]
        ready_file: Option<PathBuf>,
        /// Exit after this many seconds with no connection (agent mode).
        #[arg(long, hide = true)]
        idle_exit_secs: Option<u64>,
        /// Delete this directory (the agent's session credentials) when exiting idle.
        #[arg(long, hide = true, requires = "idle_exit_secs")]
        cleanup_on_exit: Option<PathBuf>,
        /// Directory every destination is confined to.
        #[arg(long)]
        root: PathBuf,
        /// Receiver's embedded CAS directory.
        #[arg(long)]
        cas_dir: PathBuf,
        /// This receiver's certificate.
        #[arg(long)]
        cert: PathBuf,
        #[arg(long)]
        key: PathBuf,
        /// A sender allowed to push: CERT or CERT=SCOPE (repeatable). With SCOPE, that sender may
        /// only write within <root>/SCOPE; without it, anywhere under the root.
        #[arg(long = "allow-client", required = true, value_name = "CERT[=SCOPE]")]
        allow_client: Vec<String>,
        /// Objects received and verified concurrently.
        #[arg(long, default_value_t = 4)]
        parallel: u32,
        /// Cap on xorb bytes held in memory across all transfers, in MiB (one xorb always fits).
        #[arg(long, default_value_t = 256)]
        max_memory_mib: u64,
        /// Abort a transfer when the sender sends no data for this many seconds.
        #[arg(long, default_value_t = 120)]
        stall_timeout_secs: u64,
        /// Resumable session state (default: <cas-dir>/sessions).
        #[arg(long)]
        session_dir: Option<PathBuf>,
        /// Test hook: pause after committing each xorb.
        #[arg(long, hide = true)]
        test_commit_delay_ms: Option<u64>,
    },
    /// Push a file or directory to a target.
    ///
    /// TARGET is `quic://HOST:PORT/DEST` or `tcp://HOST:PORT/DEST` for a running `serve` (with
    /// --cert/--key/--peer-cert), or `PROVIDER://SANDBOX_ID/abs/path` for an existing sandbox
    /// (credentials are generated per session). `--to HOST:PORT --dest DEST` is shorthand for
    /// `quic://HOST:PORT/DEST`.
    Push {
        src: PathBuf,
        target: Option<String>,
        #[arg(long, conflicts_with = "target", requires = "dest")]
        to: Option<String>,
        #[arg(long, requires = "to")]
        dest: Option<String>,
        #[command(flatten)]
        conn: ConnArgs,
        /// Xorbs in flight at once.
        #[arg(long, default_value_t = 4)]
        parallel: u32,
        /// Cap on xorb bytes held in memory, in MiB (one xorb always fits).
        #[arg(long, default_value_t = 256)]
        max_memory_mib: u64,
        /// Mirror: delete files and directories under the destination that are not in SRC. Runs
        /// only after every new file has been verified; never touches anything outside it.
        #[arg(long)]
        delete: bool,
        /// Cap on payload throughput, in MiB/s (all streams together).
        #[arg(long)]
        bwlimit_mib: Option<f64>,
        /// Transfer mode: "chunk" (only missing chunks, default) or "xorb" (whole xorbs).
        #[arg(long, default_value = "chunk", value_parser = ["chunk", "xorb"])]
        mode: String,
        /// Print the transfer report as JSON on stdout.
        #[arg(long)]
        json: bool,
    },
    /// Pull a file or directory from a target into a local directory.
    ///
    /// SOURCE takes the same forms as push's TARGET.
    Pull {
        source: String,
        dest: PathBuf,
        #[command(flatten)]
        conn: ConnArgs,
        /// Mirror: delete local files under DEST that the source does not contain.
        #[arg(long)]
        delete: bool,
        #[arg(long, default_value = "chunk", value_parser = ["chunk", "xorb"])]
        mode: String,
        #[arg(long)]
        json: bool,
    },
    /// Inspect a provider target: capabilities, agent startup, and which transports really work.
    /// Starts and stops a temporary agent; transfers no data.
    Doctor { target: String },
    /// Reclaim CAS space: delete unreferenced xorbs past a grace period and, with a size cap,
    /// evict least recently used content (re-fetched from a peer when next needed). Requires that
    /// no serve/push is using the CAS.
    Gc {
        #[arg(long)]
        cas_dir: PathBuf,
        /// Evict least recently used xorbs until the CAS holds at most this many MiB.
        #[arg(long)]
        max_size_mib: Option<u64>,
        /// Keep unreferenced xorbs younger than this many days (resumable transfers use them).
        #[arg(long, default_value_t = 7)]
        orphan_grace_days: u64,
        /// Receiver session state to purge of stale entries (default: <cas-dir>/sessions).
        #[arg(long)]
        session_dir: Option<PathBuf>,
        /// Report only; delete nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Offline operations against a local CAS (no network).
    #[command(subcommand)]
    Local(LocalCommand),
}

/// How to reach a target.
#[derive(clap::Args)]
struct ConnArgs {
    /// Local CAS directory (default: ~/.cache/xet-p2p/cas).
    #[arg(long)]
    cas_dir: Option<PathBuf>,
    /// "auto" (QUIC, else TLS over TCP; default), "quic" (fail rather than fall back), or "tcp".
    #[arg(long, default_value = "auto", value_parser = ["auto", "quic", "tcp"])]
    transport: String,
    /// Direct targets: this machine's certificate (in the server's --allow-client list).
    #[arg(long, requires_all = ["key", "peer_cert"])]
    cert: Option<PathBuf>,
    #[arg(long)]
    key: Option<PathBuf>,
    /// Direct targets: the server's certificate, pinned.
    #[arg(long)]
    peer_cert: Option<PathBuf>,
    /// Sandbox targets: keep the agent running for reuse by later commands, until it has been
    /// idle this many minutes (it then exits and deletes its credentials). Later commands
    /// reconnect directly, with no provider API calls.
    #[arg(long, value_name = "MINUTES", conflicts_with = "keep_agent_secs")]
    keep_agent: Option<u64>,
    /// Test hook: --keep-agent in seconds.
    #[arg(long, hide = true)]
    keep_agent_secs: Option<u64>,
}

#[derive(Subcommand)]
enum LocalCommand {
    /// Chunk and store a file or directory in the local CAS; write its manifest.
    Ingest {
        src: PathBuf,
        #[arg(long)]
        cas_dir: PathBuf,
        /// Manifest output path (stdout if omitted).
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Reconstruct a manifest's files from the local CAS into a destination directory.
    Restore {
        manifest: PathBuf,
        dest: PathBuf,
        #[arg(long)]
        cas_dir: PathBuf,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;
    let (trace, profiling) = profile::layer();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "warn,transfer=info".into()),
                ),
        )
        .with(trace)
        .init();
    let result = run(Cli::parse()).await;
    profiling.finish();
    result
}

/// Resolves on SIGTERM or Ctrl-C, so a server can exit cleanly (and write its profile).
async fn terminated() {
    #[cfg(unix)]
    {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => return std::future::pending().await,
            };
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Keygen { out, name } => keygen(&out, &name),
        Command::Serve {
            listen,
            listen_tcp,
            no_quic,
            ready_file,
            idle_exit_secs,
            cleanup_on_exit,
            root,
            cas_dir,
            cert,
            key,
            allow_client,
            parallel,
            max_memory_mib,
            stall_timeout_secs,
            session_dir,
            test_commit_delay_ms,
        } => {
            let mut allowed = Vec::new();
            let mut scopes: std::collections::HashMap<String, Vec<PathBuf>> = Default::default();
            for spec in &allow_client {
                let (path, scope) = parse_allow_client(spec)?;
                let cert =
                    load_cert(&path).with_context(|| format!("loading {}", path.display()))?;
                let fp = fingerprint(&cert);
                eprintln!(
                    "authorized sender {fp} ({}) -> {}",
                    path.display(),
                    if scope.as_os_str().is_empty() {
                        "<root>".to_string()
                    } else {
                        format!("<root>/{}", scope.display())
                    }
                );
                scopes.entry(fp).or_default().push(scope);
                allowed.push(cert);
            }
            let opts = EndpointOptions {
                max_data_streams: parallel,
                ..Default::default()
            };
            let quic = if no_quic {
                None
            } else {
                Some(server_endpoint(
                    listen,
                    Identity::load(&cert, &key)?,
                    allowed.clone(),
                    &opts,
                )?)
            };
            let tcp = match listen_tcp {
                Some(addr) => Some(
                    transport_tcp::tcp_listen(addr, Identity::load(&cert, &key)?, allowed).await?,
                ),
                None => None,
            };
            let cas = Arc::new(LocalCas::open(&cas_dir).await?);
            let mut ropts = ReceiverOptions::new(&cas);
            ropts.parallel_objects = parallel as usize;
            ropts.max_memory_bytes = max_memory_mib.saturating_mul(1024 * 1024);
            if let Some(dir) = session_dir {
                ropts.session_dir = dir;
            }
            ropts.commit_delay = test_commit_delay_ms.map(std::time::Duration::from_millis);
            ropts.client_scopes = scopes;
            ropts.stall_timeout = std::time::Duration::from_secs(stall_timeout_secs.max(1));
            let receiver = Arc::new(Receiver::new(&root, cas, ropts)?);

            let quic_addr = quic.as_ref().map(|q| q.local_addr()).transpose()?;
            let tcp_addr = tcp.as_ref().map(|t| t.local_addr()).transpose()?;
            if let Some(a) = quic_addr {
                eprintln!("listening on {a} (QUIC), root {}", root.display());
            }
            if let Some(a) = tcp_addr {
                eprintln!("listening on {a} (TLS over TCP), root {}", root.display());
            }
            if let Some(path) = ready_file {
                let json = serde_json::json!({
                    "quic": quic_addr.map(|a| a.to_string()),
                    "tcp": tcp_addr.map(|a| a.to_string()),
                });
                let tmp = path.with_extension("tmp");
                std::fs::write(&tmp, serde_json::to_vec(&json)?)?;
                std::fs::rename(&tmp, &path)?;
            }
            let serve_quic = async {
                if let Some(q) = quic {
                    receiver.clone().serve(Arc::new(q)).await;
                }
            };
            let serve_tcp = async {
                if let Some(t) = tcp {
                    receiver.clone().serve(Arc::new(t)).await;
                }
            };
            let idle_exit = async {
                let Some(secs) = idle_exit_secs else {
                    return std::future::pending::<()>().await;
                };
                let limit = std::time::Duration::from_secs(secs.max(1));
                let activity = receiver.activity();
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1).min(limit)).await;
                    if activity.idle_for().is_some_and(|idle| idle >= limit) {
                        break;
                    }
                }
            };
            tokio::select! {
                _ = async { tokio::join!(serve_quic, serve_tcp) } => {}
                _ = idle_exit => {
                    eprintln!("idle for {}s; exiting", idle_exit_secs.unwrap_or_default());
                    if let Some(dir) = &cleanup_on_exit {
                        let _ = std::fs::remove_dir_all(dir);
                    }
                }
                _ = terminated() => {
                    if let Some(dir) = &cleanup_on_exit {
                        let _ = std::fs::remove_dir_all(dir);
                    }
                }
            }
            Ok(())
        }
        Command::Push {
            src,
            target,
            to,
            dest,
            conn,
            parallel,
            max_memory_mib,
            delete,
            bwlimit_mib,
            mode,
            json,
        } => {
            let target = match (target, to, dest) {
                (Some(t), _, _) => t,
                (None, Some(to), Some(dest)) => format!("quic://{to}/{dest}"),
                _ => bail!("give a TARGET (or --to HOST:PORT --dest DEST)"),
            };
            std::fs::symlink_metadata(&src).with_context(|| format!("{}", src.display()))?;
            if delete {
                eprintln!(
                    "--delete: anything under {} not in {} will be removed",
                    target,
                    src.display()
                );
            }
            // Connect (possibly starting an agent) while the source is ingested locally.
            let t = std::time::Instant::now();
            let (remote, prepared) = tokio::join!(
                async {
                    let r = open_target(&target, &conn).await;
                    (r, t.elapsed())
                },
                async {
                    let t = std::time::Instant::now();
                    let cas = Arc::new(LocalCas::open(cas_dir(&conn)?).await?);
                    let cas_open = t.elapsed();
                    let prepared = prepare(&cas, &src).await?;
                    anyhow::Ok((cas, cas_open, prepared))
                }
            );
            let (remote, connect) = (remote.0?, remote.1);
            let (cas, cas_open, prepared) = match prepared {
                Ok(v) => v,
                Err(e) => {
                    let _ = remote.finish().await;
                    return Err(e);
                }
            };
            let opts = PushOptions {
                dest: remote.path.clone(),
                parallel_streams: parallel as usize,
                max_memory_bytes: max_memory_mib.saturating_mul(1024 * 1024),
                delete,
                mode: parse_mode(&mode),
                bandwidth_limit: bwlimit_mib
                    .filter(|m| *m > 0.0)
                    .map(|m| m * 1024.0 * 1024.0),
            };
            let result = explain(
                &remote.conn,
                push_prepared(cas, prepared, &remote.conn, &opts).await,
            )
            .await;
            let (bootstrap, reused) = (remote.bootstrap, remote.reused_agent);
            let t = std::time::Instant::now();
            remote.finish().await?;
            let close = t.elapsed();
            let report = result?;
            print_push_report(&report);
            if json {
                let mut j = report_json(&report);
                j["bootstrap_secs"] = bootstrap.as_secs_f64().into();
                j["reused_agent"] = reused.into();
                // Connecting (includes bootstrap) overlaps opening the local CAS and ingesting
                // (part of elapsed_secs); ending the session follows the transfer.
                j["connect_secs"] = connect.as_secs_f64().into();
                j["cas_open_secs"] = cas_open.as_secs_f64().into();
                j["close_secs"] = close.as_secs_f64().into();
                println!("{}", serde_json::to_string_pretty(&j)?);
            }
            Ok(())
        }
        Command::Pull {
            source,
            dest,
            conn,
            delete,
            mode,
            json,
        } => {
            let remote = open_target(&source, &conn).await?;
            let cas = Arc::new(LocalCas::open(cas_dir(&conn)?).await?);
            let mut opts = PullOptions::new(remote.path.clone());
            opts.delete = delete;
            opts.mode = parse_mode(&mode);
            let result = explain(&remote.conn, pull(cas, &remote.conn, &dest, &opts).await).await;
            remote.finish().await?;
            let r = result?;
            print_pull_report(&r);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "files": r.files,
                        "logical_bytes": r.logical_bytes,
                        "transport": r.transport.to_string(),
                        "wire_bytes_sent": r.wire_bytes_sent,
                        "wire_bytes_received": r.wire_bytes_received,
                        "elapsed_secs": r.elapsed.as_secs_f64(),
                        "local": r.local,
                    }))?
                );
            }
            Ok(())
        }
        Command::Doctor { target } => {
            let target = providers::Target::parse(&target)?;
            let r = providers::doctor(&registry()?, &target).await?;
            print_doctor(&r);
            Ok(())
        }
        Command::Gc {
            cas_dir,
            max_size_mib,
            orphan_grace_days,
            session_dir,
            dry_run,
        } => {
            let cas = LocalCas::open_exclusive(&cas_dir).await?;
            let opts = xet_local::gc::GcOptions {
                orphan_grace: std::time::Duration::from_secs(orphan_grace_days * 24 * 3600),
                max_bytes: max_size_mib.map(|m| m.saturating_mul(1024 * 1024)),
                dry_run,
            };
            let r = cas.gc(&opts).await?;
            if !dry_run {
                let sessions = session_dir.unwrap_or_else(|| cas.root().join("sessions"));
                if sessions.exists() {
                    transfer::purge_stale_session_state(&sessions)?;
                }
            }
            let mib = |b: u64| b as f64 / 1048576.0;
            eprintln!(
                "{}CAS before: {} files, {} xorbs, {:.1} MiB",
                if dry_run { "[dry run] " } else { "" },
                r.files_before,
                r.xorbs_before,
                mib(r.bytes_before)
            );
            eprintln!(
                "  orphans deleted: {} xorbs, {:.1} MiB ({} kept within {}-day grace)",
                r.orphan_xorbs_deleted,
                mib(r.orphan_bytes_deleted),
                r.orphans_kept_in_grace,
                orphan_grace_days
            );
            eprintln!(
                "  evicted (LRU): {} xorbs, {:.1} MiB; {} files will be re-fetched when needed",
                r.xorbs_evicted,
                mib(r.bytes_evicted),
                r.files_invalidated
            );
            eprintln!(
                "  shards deleted: {}; CAS after: {:.1} MiB",
                r.shards_deleted,
                mib(r.bytes_after)
            );
            Ok(())
        }
        Command::Local(LocalCommand::Ingest {
            src,
            cas_dir,
            output,
        }) => {
            let cas = LocalCas::open(&cas_dir).await?;
            let (manifest, report) = cas.ingest(&src).await?;
            let json = serde_json::to_string_pretty(&manifest)?;
            match output {
                Some(path) => std::fs::write(&path, json)
                    .with_context(|| format!("writing {}", path.display()))?,
                None => println!("{json}"),
            }
            let d = &report.dedup;
            eprintln!(
                "ingested {} files, {} dirs: {} logical bytes, {} new, {} deduped; {} chunks ({} new); {} xorb bytes stored",
                report.files,
                report.dirs,
                d.total_bytes,
                d.new_bytes,
                d.deduped_bytes,
                d.total_chunks,
                d.new_chunks,
                d.xorb_bytes_uploaded,
            );
            Ok(())
        }
        Command::Local(LocalCommand::Restore {
            manifest,
            dest,
            cas_dir,
        }) => {
            let manifest: Manifest = serde_json::from_slice(
                &std::fs::read(&manifest)
                    .with_context(|| format!("reading {}", manifest.display()))?,
            )
            .context("parsing manifest")?;
            let cas = LocalCas::open(&cas_dir).await?;
            let report = cas.reconstruct(&manifest, &dest).await?;
            eprintln!(
                "restored {} files, {} dirs, {} bytes (verified)",
                report.files, report.dirs, report.bytes
            );
            Ok(())
        }
    }
}

/// `CERT` or `CERT=SCOPE`. A spec naming an existing file is always a bare path.
fn parse_allow_client(spec: &str) -> Result<(PathBuf, PathBuf)> {
    if Path::new(spec).is_file() {
        return Ok((spec.into(), PathBuf::new()));
    }
    let Some((cert, scope)) = spec.rsplit_once('=') else {
        return Ok((spec.into(), PathBuf::new()));
    };
    let scope = xet_local::manifest::validate_rel_path(scope)
        .with_context(|| format!("invalid scope in --allow-client {spec}"))?;
    Ok((cert.into(), scope))
}

fn registry() -> Result<providers::Registry> {
    let mut list: Vec<Box<dyn providers::SandboxProvider>> =
        vec![Box::new(providers::local::LocalProvider::new(
            providers::local::LocalProvider::default_base(),
            std::env::current_exe()?,
        ))];
    if let Some(modal) = providers::modal::ModalProvider::from_env() {
        list.push(Box::new(modal));
    }
    Ok(providers::Registry::new(list))
}

fn cas_dir(conn: &ConnArgs) -> Result<PathBuf> {
    match &conn.cas_dir {
        Some(d) => Ok(d.clone()),
        None => {
            let home = std::env::var_os("HOME").context("HOME is not set; pass --cas-dir")?;
            Ok(PathBuf::from(home).join(".cache/xet-p2p/cas"))
        }
    }
}

fn parse_mode(mode: &str) -> TransferMode {
    if mode == "xorb" {
        TransferMode::Xorb
    } else {
        TransferMode::Chunk
    }
}

/// Parses `target`, connects under the --transport policy, and reports what was chosen.
/// A transfer can fail at its first stream because the peer refused this client after the
/// connection was set up (TLS 1.3); add the peer's reason to the error.
async fn explain<T, E>(conn: &transport::Conn, r: std::result::Result<T, E>) -> Result<T>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match r {
        Ok(v) => Ok(v),
        Err(e) => match conn.rejection().await {
            Some(why) => Err(anyhow::Error::new(e).context(why)),
            None => Err(e.into()),
        },
    }
}

async fn open_target(target: &str, args: &ConnArgs) -> Result<providers::Connected> {
    let parsed = providers::Target::parse(target)?;
    let policy: providers::TransportPolicy = args.transport.parse()?;
    let direct = match (&args.cert, &args.key, &args.peer_cert) {
        (Some(c), Some(k), Some(p)) => Some(providers::DirectCredentials {
            identity: Identity::load(c, k)?,
            pinned_server: load_cert(p)?,
        }),
        _ => None,
    };
    let keep_secs = args.keep_agent.map(|m| m * 60).or(args.keep_agent_secs);
    let keep = match keep_secs {
        Some(secs) if secs > 0 => Some(providers::KeepAgent {
            idle: std::time::Duration::from_secs(secs),
            cache_dir: cas_dir(args)?
                .parent()
                .map(|p| p.join("agents"))
                .unwrap_or_else(|| PathBuf::from("agents")),
        }),
        _ => None,
    };
    let remote = providers::connect_target(&registry()?, &parsed, policy, direct, keep).await?;
    if !remote.bootstrap.is_zero() {
        eprintln!(
            "attached and started the agent in {:.1}s",
            remote.bootstrap.as_secs_f64()
        );
    }
    eprintln!(
        "connected to {} via {}{} (peer certificate pinned)",
        target,
        remote.transport,
        if remote.relayed {
            ", relayed by the provider"
        } else {
            ""
        }
    );
    for note in &remote.notes {
        eprintln!("  note: {note}");
    }
    Ok(remote)
}

fn print_doctor(r: &providers::DoctorReport) {
    eprintln!("provider:   {}", r.provider);
    eprintln!("sandbox:    {}", r.sandbox);
    match &r.capabilities {
        Some(c) => {
            eprintln!("platform:   {} / {}", c.os, c.arch);
            eprintln!("workspace:  {}", c.workspace_root);
            eprintln!(
                "storage:    {}",
                if c.persistent_storage {
                    "persistent (CAS survives the sandbox)"
                } else {
                    "ephemeral (CAS is lost with the sandbox)"
                }
            );
            for n in &c.notes {
                eprintln!("note:       {n}");
            }
        }
        None => eprintln!("platform:   unknown"),
    }
    match &r.agent {
        Ok(()) => eprintln!("agent:      started and stopped"),
        Err(e) => eprintln!("agent:      FAILED: {e}"),
    }
    for (name, p) in [("QUIC (UDP)", &r.quic), ("TLS over TCP", &r.tcp)] {
        match p {
            providers::Probe::Ok(d) => eprintln!("{name:<13} ok      {d}"),
            providers::Probe::Failed(e) => eprintln!("{name:<13} FAILED  {e}"),
            providers::Probe::NotOffered(why) => eprintln!("{name:<13} n/a     {why}"),
        }
    }
    match r.auto_choice {
        Some(k) => eprintln!(
            "auto picks: {k}{}",
            if r.relayed {
                " (relayed by the provider)"
            } else {
                ""
            }
        ),
        None => eprintln!("auto picks: nothing works; see the failures above"),
    }
}

fn print_pull_report(r: &transfer::PullReport) {
    let l = &r.local;
    let secs = r.elapsed.as_secs_f64();
    eprintln!(
        "pulled {} files ({} logical bytes) in {secs:.2}s via {}",
        r.files, r.logical_bytes, r.transport
    );
    eprintln!(
        "  received {} payload bytes ({} chunk, {} chunk-list, {} xorb); wire: {} in / {} out",
        l.payload_bytes_received,
        l.chunk_bytes_received,
        l.chunk_list_bytes_received,
        l.payload_bytes_received - l.chunk_bytes_received - l.chunk_list_bytes_received,
        r.wire_bytes_received,
        r.wire_bytes_sent
    );
    eprintln!(
        "  local: {} files installed, {} unchanged, {} deleted; {} chunks already held",
        l.files_installed, l.files_unchanged, l.files_deleted, l.chunks_present
    );
}

fn keygen(out: &Path, name: &str) -> Result<()> {
    let cert_path = out.join(format!("{name}.crt"));
    let key_path = out.join(format!("{name}.key"));
    for p in [&cert_path, &key_path] {
        if p.exists() {
            bail!("{} already exists; refusing to overwrite", p.display());
        }
    }
    std::fs::create_dir_all(out)?;
    let id = generate(name)?;
    std::fs::write(&cert_path, &id.cert_pem)?;
    write_private(&key_path, id.key_pem.as_bytes())?;
    eprintln!("wrote {} and {}", cert_path.display(), key_path.display());
    eprintln!("fingerprint (SHA-256): {}", id.fingerprint);
    eprintln!("share only the .crt with the other peer; keep the .key private");
    Ok(())
}

fn write_private(path: &Path, data: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)?.write_all(data)?;
    Ok(())
}

fn print_push_report(r: &PushReport) {
    let d = &r.ingest.dedup;
    let secs = r.elapsed.as_secs_f64();
    eprintln!(
        "pushed {} files ({} logical bytes) in {secs:.2}s",
        r.ingest.files, r.logical_bytes
    );
    eprintln!(
        "  local ingest: {} files unchanged since last read ({} bytes, not re-read); read {}: {} chunks ({} new), {} new bytes, {} deduped",
        r.ingest.cached_files,
        r.ingest.cached_bytes,
        r.ingest.files - r.ingest.cached_files,
        d.total_chunks,
        d.new_chunks,
        d.new_bytes,
        d.deduped_bytes
    );
    let rr = &r.receiver;
    match rr.mode {
        TransferMode::Xorb => eprintln!(
            "  whole-xorb mode: {} xorbs referenced, {} sent, {} reused on receiver",
            r.xorbs_referenced,
            r.xorbs_sent,
            r.xorbs_reused()
        ),
        TransferMode::Chunk => eprintln!(
            "  chunk mode: {} chunks in new files, {} already on receiver ({} bytes), {} sent in {} runs; {} xorbs packed",
            rr.chunks_total,
            rr.chunks_present,
            rr.chunk_bytes_present,
            rr.chunks_received,
            r.chunk_runs_sent,
            rr.xorbs_packed
        ),
    }
    eprintln!(
        "  payload sent: {} bytes ({} xorb, {} shard, {} chunk, {} chunk-list); wire sent: {} bytes via {}",
        r.payload_bytes_sent(),
        r.xorb_bytes_sent,
        r.shard_bytes_sent,
        r.chunk_bytes_sent,
        r.chunk_list_bytes_sent,
        r.udp_bytes_sent,
        r.transport
    );
    if secs > 0.0 {
        eprintln!(
            "  effective throughput: {:.1} MiB/s logical, {:.1} MiB/s payload",
            r.logical_bytes as f64 / secs / 1048576.0,
            r.payload_bytes_sent() as f64 / secs / 1048576.0
        );
    }
    eprintln!(
        "  receiver: {} files installed, {} unchanged; {} bytes reconstructed and verified",
        rr.files_installed, rr.files_unchanged, rr.bytes_reconstructed
    );
    if rr.files_deleted + rr.dirs_deleted > 0 {
        eprintln!(
            "  deleted on receiver: {} files, {} directories (--delete)",
            rr.files_deleted, rr.dirs_deleted
        );
    }
    if rr.resumed {
        eprintln!(
            "  resumed interrupted session (attempt {}): {} xorbs / {} bytes already committed",
            rr.attempts, rr.prior_xorbs_committed, rr.prior_xorb_bytes_committed,
        );
    }
    if rr.reconstructions_reused > 0 {
        eprintln!(
            "  reused {} file reconstruction entries saved by an interrupted attempt",
            rr.reconstructions_reused
        );
    }
}

fn report_json(r: &PushReport) -> serde_json::Value {
    let d = &r.ingest.dedup;
    serde_json::json!({
        "files": r.ingest.files,
        "dirs": r.ingest.dirs,
        "logical_bytes": r.logical_bytes,
        "ingest": {
            "cached_files": r.ingest.cached_files,
            "cached_bytes": r.ingest.cached_bytes,
            "total_chunks": d.total_chunks,
            "new_chunks": d.new_chunks,
            "new_bytes": d.new_bytes,
            "deduped_bytes": d.deduped_bytes,
        },
        "xorbs_referenced": r.xorbs_referenced,
        "xorbs_sent": r.xorbs_sent,
        "xorbs_reused": r.xorbs_reused(),
        "xorb_bytes_sent": r.xorb_bytes_sent,
        "shard_bytes_sent": r.shard_bytes_sent,
        "chunk_bytes_sent": r.chunk_bytes_sent,
        "chunk_runs_sent": r.chunk_runs_sent,
        "chunk_list_bytes_sent": r.chunk_list_bytes_sent,
        "payload_bytes_sent": r.payload_bytes_sent(),
        "udp_bytes_sent": r.udp_bytes_sent,
        "udp_bytes_received": r.udp_bytes_received,
        "transport": r.transport.to_string(),
        "elapsed_secs": r.elapsed.as_secs_f64(),
        "receiver": r.receiver,
    })
}
