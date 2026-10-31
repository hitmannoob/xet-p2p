//! M3 benchmark: wire bytes for chunk-level vs whole-xorb transfer, against a naive upload.
//!
//! Every scenario runs once per mode, each from fresh state (new temp dirs, CAS, endpoints), over
//! real QUIC on 127.0.0.1. Run with: `cargo run --release -p transfer --example bench_m3`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;
use transfer::{PushOptions, PushReport, Receiver, ReceiverOptions, TransferMode, push};
use transport::Listener as _;
use transport_quic::{
    EndpointOptions, Identity, client_endpoint, connect, generate, server_endpoint,
};
use xet_local::LocalCas;

const MIB: usize = 1024 * 1024;

fn random(seed: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    StdRng::seed_from_u64(seed).fill(&mut v[..]);
    v
}

fn write(root: &Path, rel: &str, data: &[u8]) {
    let p = root.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, data).unwrap();
}

/// One receiver and a way to push into it from any sender CAS.
struct Rig {
    tmp: TempDir,
    addr: std::net::SocketAddr,
    server_id: transport_quic::GeneratedIdentity,
    client_id: transport_quic::GeneratedIdentity,
    _server: Arc<transport_quic::QuicListener>,
}

impl Rig {
    async fn new() -> Self {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        let server_id = generate("receiver").unwrap();
        let client_id = generate("sender").unwrap();
        let id = |g: &transport_quic::GeneratedIdentity| {
            Identity::from_pem(&g.cert_pem, &g.key_pem).unwrap()
        };
        let server = Arc::new(
            server_endpoint(
                "127.0.0.1:0".parse().unwrap(),
                id(&server_id),
                vec![id(&client_id).cert],
                &EndpointOptions::default(),
            )
            .unwrap(),
        );
        let addr = server.local_addr().unwrap();
        let cas = Arc::new(
            LocalCas::open(tmp.path().join("receiver-cas"))
                .await
                .unwrap(),
        );
        let opts = ReceiverOptions::new(&cas);
        tokio::spawn(
            Arc::new(Receiver::new(&root, cas, opts).unwrap())
                .serve(server.clone() as Arc<dyn transport::Listener>),
        );
        Self {
            tmp,
            addr,
            server_id,
            client_id,
            _server: server,
        }
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.tmp.path().join(rel)
    }

    async fn sender(&self, name: &str) -> Arc<LocalCas> {
        Arc::new(LocalCas::open(self.path(name)).await.unwrap())
    }

    async fn push(
        &self,
        cas: &Arc<LocalCas>,
        src: &Path,
        dest: &str,
        mode: TransferMode,
    ) -> PushReport {
        let id = |g: &transport_quic::GeneratedIdentity| {
            Identity::from_pem(&g.cert_pem, &g.key_pem).unwrap()
        };
        let ep = client_endpoint(
            id(&self.client_id),
            vec![id(&self.server_id).cert],
            &EndpointOptions::default(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let conn = connect(&ep, self.addr).await.unwrap();
        let opts = PushOptions {
            dest: dest.into(),
            parallel_streams: 4,
            max_memory_bytes: transfer::DEFAULT_MAX_MEMORY_BYTES,
            delete: false,
            mode,
            bandwidth_limit: None,
        };
        let r = push(cas.clone(), src, &conn, &opts).await.unwrap();
        conn.close(0, b"done");
        // Byte-for-byte check of what landed.
        for (entry, ..) in xet_local::LocalCas::open(self.path("verify-cas"))
            .await
            .unwrap()
            .ingest(src)
            .await
            .unwrap()
            .0
            .files()
        {
            assert_eq!(
                fs::read(self.path("root").join(dest).join(&entry.path)).unwrap(),
                fs::read(if src.is_dir() {
                    src.join(&entry.path)
                } else {
                    src.to_owned()
                })
                .unwrap(),
                "{}",
                entry.path
            );
        }
        r
    }
}

struct Row {
    name: &'static str,
    logical: u64,
    xorb: PushReport,
    chunk: PushReport,
}

/// Runs `scenario` once per mode on fresh rigs; the closure prepares warm state and returns the
/// measured push.
async fn run<F, Fut>(name: &'static str, scenario: F) -> Row
where
    F: Fn(Rig, TransferMode) -> Fut,
    Fut: std::future::Future<Output = (u64, PushReport)>,
{
    let (logical, xorb) = scenario(Rig::new().await, TransferMode::Xorb).await;
    let (_, chunk) = scenario(Rig::new().await, TransferMode::Chunk).await;
    eprintln!("done: {name}");
    Row {
        name,
        logical,
        xorb,
        chunk,
    }
}

fn insert(base: &[u8], at: usize, extra: &[u8]) -> Vec<u8> {
    let mut v = base.to_vec();
    v.splice(at..at, extra.iter().copied());
    v
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let started = Instant::now();
    let mut rows = Vec::new();

    rows.push(
        run(
            "cold: 64 MiB new file, empty receiver",
            |rig, mode| async move {
                let src = rig.path("src");
                write(&src, "f.bin", &random(1, 64 * MIB));
                let s = rig.sender("s").await;
                (64 * MIB as u64, rig.push(&s, &src, "p", mode).await)
            },
        )
        .await,
    );

    for (name, at) in [
        (
            "edit: 4 KiB inserted at start of 64 MiB file (same sender)",
            0,
        ),
        (
            "edit: 4 KiB inserted mid 64 MiB file (same sender)",
            32 * MIB,
        ),
    ] {
        rows.push(
            run(name, move |rig, mode| async move {
                let src = rig.path("src");
                let base = random(2, 64 * MIB);
                write(&src, "f.bin", &base);
                let s = rig.sender("s").await;
                rig.push(&s, &src, "p", mode).await;
                write(&src, "f.bin", &insert(&base, at, &random(3, 4096)));
                (64 * MIB as u64 + 4096, rig.push(&s, &src, "p", mode).await)
            })
            .await,
        );
    }

    rows.push(
        run(
            "checkpoint: 4 of 64 x 2 MiB tensors rewritten (same sender)",
            |rig, mode| async move {
                let src = rig.path("src");
                let mut ckpt: Vec<u8> = (0..64).flat_map(|t| random(100 + t, 2 * MIB)).collect();
                write(&src, "model.ckpt", &ckpt);
                let s = rig.sender("s").await;
                rig.push(&s, &src, "p", mode).await;
                for t in [5usize, 17, 40, 63] {
                    ckpt[t * 2 * MIB..(t + 1) * 2 * MIB]
                        .copy_from_slice(&random(900 + t as u64, 2 * MIB));
                }
                write(&src, "model.ckpt", &ckpt);
                (128 * MIB as u64, rig.push(&s, &src, "p", mode).await)
            },
        )
        .await,
    );

    rows.push(
        run(
            "edit from a different sender CAS (receiver warm via sender A)",
            |rig, mode| async move {
                let src = rig.path("src");
                let base = random(4, 64 * MIB);
                write(&src, "f.bin", &base);
                let a = rig.sender("a").await;
                rig.push(&a, &src, "p", mode).await;
                write(&src, "f.bin", &insert(&base, 32 * MIB, &random(5, 4096)));
                let b = rig.sender("b").await;
                (64 * MIB as u64 + 4096, rig.push(&b, &src, "p", mode).await)
            },
        )
        .await,
    );

    rows.push(
        run(
            "one 1 MiB file whose xorb is shared with 63 others, empty receiver",
            |rig, mode| async move {
                let all = rig.path("all");
                for i in 0..64 {
                    write(&all, &format!("f{i:02}.bin"), &random(200 + i, MIB));
                }
                let s = rig.sender("s").await;
                s.ingest(&all).await.unwrap();
                (
                    MIB as u64,
                    rig.push(&s, &all.join("f07.bin"), "p", mode).await,
                )
            },
        )
        .await,
    );

    rows.push(
        run("unchanged re-push (warm)", |rig, mode| async move {
            let src = rig.path("src");
            write(&src, "f.bin", &random(6, 64 * MIB));
            let s = rig.sender("s").await;
            rig.push(&s, &src, "p", mode).await;
            (64 * MIB as u64, rig.push(&s, &src, "p", mode).await)
        })
        .await,
    );

    let mib = |b: u64| b as f64 / MIB as f64;
    println!(
        "| Scenario | Naive upload | Whole-xorb payload | Chunk payload | Chunk / naive | Whole-xorb UDP | Chunk UDP | Chunk time |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|");
    for r in &rows {
        println!(
            "| {} | {:.2} MiB | {:.2} MiB | {:.2} MiB | {:.2}% | {:.2} MiB | {:.2} MiB | {:.2} s |",
            r.name,
            mib(r.logical),
            mib(r.xorb.payload_bytes_sent()),
            mib(r.chunk.payload_bytes_sent()),
            100.0 * r.chunk.payload_bytes_sent() as f64 / r.logical as f64,
            mib(r.xorb.udp_bytes_sent),
            mib(r.chunk.udp_bytes_sent),
            r.chunk.elapsed.as_secs_f64(),
        );
    }
    println!();
    println!(
        "| Scenario | Chunks in new files | Already on receiver | Sent | Runs | Chunk-list bytes |"
    );
    println!("|---|---:|---:|---:|---:|---:|");
    for r in &rows {
        let c = &r.chunk.receiver;
        println!(
            "| {} | {} | {} | {} | {} | {} |",
            r.name,
            c.chunks_total,
            c.chunks_present,
            c.chunks_received,
            r.chunk.chunk_runs_sent,
            r.chunk.chunk_list_bytes_sent
        );
    }
    eprintln!("total {:.1}s", started.elapsed().as_secs_f64());
}
