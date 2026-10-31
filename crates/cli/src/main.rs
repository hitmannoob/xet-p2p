use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use transfer::{PushOptions, PushReport, Receiver, ReceiverOptions, TransferMode, push};
use transport_quic::{
    EndpointOptions, Identity, bind_addr_for, client_endpoint, connect, fingerprint, generate,
    load_cert, server_endpoint,
};
use xet_local::{LocalCas, Manifest};

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
    /// Push a file or directory to a receiver.
    Push {
        src: PathBuf,
        /// Receiver address, host:port.
        #[arg(long)]
        to: String,
        /// Destination directory, relative to the receiver's --root.
        #[arg(long)]
        dest: String,
        /// Sender's embedded CAS directory.
        #[arg(long)]
        cas_dir: PathBuf,
        /// This sender's certificate (must be in the receiver's --allow-client list).
        #[arg(long)]
        cert: PathBuf,
        #[arg(long)]
        key: PathBuf,
        /// The receiver's certificate, pinned: the connection fails if it presents any other.
        #[arg(long)]
        peer_cert: PathBuf,
        /// Xorbs in flight at once.
        #[arg(long, default_value_t = 4)]
        parallel: u32,
        /// Cap on xorb bytes held in memory, in MiB (one xorb always fits).
        #[arg(long, default_value_t = 256)]
        max_memory_mib: u64,
        /// Mirror: delete files and directories under --dest that are not in SRC. Runs only after
        /// every new file has been verified; never touches anything outside --dest.
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
    /// Offline operations against a local CAS (no network).
    #[command(subcommand)]
    Local(LocalCommand),
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn,transfer=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    match Cli::parse().command {
        Command::Keygen { out, name } => keygen(&out, &name),
        Command::Serve {
            listen,
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
            let endpoint = server_endpoint(listen, Identity::load(&cert, &key)?, allowed, &opts)?;
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
            eprintln!(
                "listening on {} (QUIC), root {}",
                endpoint.local_addr()?,
                root.display()
            );
            receiver.serve(endpoint).await;
            Ok(())
        }
        Command::Push {
            src,
            to,
            dest,
            cas_dir,
            cert,
            key,
            peer_cert,
            parallel,
            max_memory_mib,
            delete,
            bwlimit_mib,
            mode,
            json,
        } => {
            let addr = tokio::net::lookup_host(&to)
                .await
                .with_context(|| format!("resolving {to}"))?
                .next()
                .with_context(|| format!("{to} resolved to no addresses"))?;
            let endpoint = client_endpoint(
                Identity::load(&cert, &key)?,
                vec![load_cert(&peer_cert)?],
                &EndpointOptions::default(),
                bind_addr_for(addr),
            )?;
            let conn = connect(&endpoint, addr)
                .await
                .with_context(|| format!("connecting to {addr}"))?;
            eprintln!("connected to {addr} (QUIC, direct; receiver certificate pinned)");

            let cas = Arc::new(LocalCas::open(&cas_dir).await?);
            let opts = PushOptions {
                dest,
                parallel_streams: parallel as usize,
                max_memory_bytes: max_memory_mib.saturating_mul(1024 * 1024),
                delete,
                mode: if mode == "xorb" {
                    TransferMode::Xorb
                } else {
                    TransferMode::Chunk
                },
                bandwidth_limit: bwlimit_mib
                    .filter(|m| *m > 0.0)
                    .map(|m| m * 1024.0 * 1024.0),
            };
            if delete {
                eprintln!(
                    "--delete: anything under the receiver's {} not in {} will be removed",
                    opts.dest,
                    src.display()
                );
            }
            let result = push(cas, &src, &conn, &opts).await;
            conn.close(0u32.into(), b"done");
            endpoint.wait_idle().await;
            let report = result?;
            print_push_report(&report);
            if json {
                println!("{}", serde_json::to_string_pretty(&report_json(&report))?);
            }
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
        "  local ingest: {} chunks ({} new), {} new bytes, {} deduped",
        d.total_chunks, d.new_chunks, d.new_bytes, d.deduped_bytes
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
        "  payload sent: {} bytes ({} xorb, {} shard, {} chunk, {} chunk-list); UDP sent: {} bytes",
        r.payload_bytes_sent(),
        r.xorb_bytes_sent,
        r.shard_bytes_sent,
        r.chunk_bytes_sent,
        r.chunk_list_bytes_sent,
        r.udp_bytes_sent
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
        "elapsed_secs": r.elapsed.as_secs_f64(),
        "receiver": r.receiver,
    })
}
