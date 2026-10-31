use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use transfer::{PushOptions, PushReport, Receiver, ReceiverOptions, push};
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
        /// Certificate of a sender allowed to push (repeatable).
        #[arg(long = "allow-client", required = true)]
        allow_client: Vec<PathBuf>,
        /// Objects received and verified concurrently.
        #[arg(long, default_value_t = 4)]
        parallel: u32,
        /// Cap on xorb bytes held in memory across all transfers, in MiB (one xorb always fits).
        #[arg(long, default_value_t = 256)]
        max_memory_mib: u64,
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
            session_dir,
            test_commit_delay_ms,
        } => {
            let allowed = allow_client
                .iter()
                .map(|p| load_cert(p).with_context(|| format!("loading {}", p.display())))
                .collect::<Result<Vec<_>>>()?;
            for (path, c) in allow_client.iter().zip(&allowed) {
                eprintln!("authorized sender {} ({})", fingerprint(c), path.display());
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
            };
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
    eprintln!(
        "  xorbs: {} referenced, {} sent, {} reused on receiver",
        r.xorbs_referenced,
        r.xorbs_sent,
        r.xorbs_reused()
    );
    eprintln!(
        "  payload sent: {} bytes ({} xorb, {} shard); UDP sent: {} bytes",
        r.payload_bytes_sent(),
        r.xorb_bytes_sent,
        r.shard_bytes_sent,
        r.udp_bytes_sent
    );
    if secs > 0.0 {
        eprintln!(
            "  effective throughput: {:.1} MiB/s logical, {:.1} MiB/s payload",
            r.logical_bytes as f64 / secs / 1048576.0,
            r.payload_bytes_sent() as f64 / secs / 1048576.0
        );
    }
    let rr = &r.receiver;
    eprintln!(
        "  receiver: {} files installed, {} unchanged; {} bytes reconstructed and verified",
        rr.files_installed, rr.files_unchanged, rr.bytes_reconstructed
    );
    if rr.resumed {
        eprintln!(
            "  resumed interrupted session (attempt {}): {} xorbs / {} bytes already committed{}",
            rr.attempts,
            rr.prior_xorbs_committed,
            rr.prior_xorb_bytes_committed,
            if rr.shard_from_session {
                "; shard reused"
            } else {
                ""
            }
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
        "payload_bytes_sent": r.payload_bytes_sent(),
        "udp_bytes_sent": r.udp_bytes_sent,
        "elapsed_secs": r.elapsed.as_secs_f64(),
        "receiver": r.receiver,
    })
}
