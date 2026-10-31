use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
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
    match Cli::parse().command {
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
        }
    }
    Ok(())
}
