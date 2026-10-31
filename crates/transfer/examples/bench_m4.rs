//! M4 benchmark: xet-p2p (chunk mode, QUIC) vs rsync's delta protocol vs a plain copy.
//!
//! Each tool runs every scenario from fresh directories. rsync runs its real remote-shell protocol
//! (delta transfer on) through a local pipe, so its `--stats` byte counts are what it would put on
//! an SSH connection, minus SSH's own framing and encryption. Every result is checked with
//! `diff -r`. Run with: `cargo run --release -p transfer --example bench_m4`.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;
use transfer::{PushOptions, Receiver, ReceiverOptions, TransferMode, push};
use transport::Listener as _;
use transport_quic::{
    EndpointOptions, GeneratedIdentity, Identity, client_endpoint, connect, generate,
    server_endpoint,
};
use xet_local::LocalCas;

const MIB: usize = 1024 * 1024;

// ---------- data ----------

fn random(seed: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    StdRng::seed_from_u64(seed).fill(&mut v[..]);
    v
}

/// Log-like text from a small vocabulary: compresses roughly 3-4x with lz4/zlib.
fn text(seed: u64, len: usize) -> Vec<u8> {
    const WORDS: &[&str] = &[
        "INFO",
        "WARN",
        "request",
        "completed",
        "user",
        "id",
        "latency_ms",
        "status=200",
        "GET",
        "/api/v1/items",
        "cache",
        "miss",
        "hit",
        "shard",
        "worker",
        "retry",
        "ok",
        "connection",
        "closed",
        "bytes",
        "elapsed",
        "upstream",
        "token",
        "refresh",
    ];
    let mut rng = StdRng::seed_from_u64(seed);
    let mut out = Vec::with_capacity(len + 64);
    let mut line = 0u64;
    while out.len() < len {
        line += 1;
        out.extend_from_slice(
            format!(
                "2026-10-31T12:{:02}:{:02} ",
                line % 60,
                rng.random_range(0..60)
            )
            .as_bytes(),
        );
        for _ in 0..rng.random_range(4..12) {
            out.extend_from_slice(WORDS[rng.random_range(0..WORDS.len())].as_bytes());
            out.push(b' ');
        }
        out.extend_from_slice(format!("{}\n", rng.random_range(0..100_000)).as_bytes());
    }
    out.truncate(len);
    out
}

fn write(root: &Path, rel: &str, data: &[u8]) {
    let p = root.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, data).unwrap();
}

/// Marks a file as edited later than any previous sync (rsync's quick check has 1 s resolution).
fn bump(path: &Path) {
    let f = fs::File::options().write(true).open(path).unwrap();
    f.set_modified(SystemTime::now() + Duration::from_secs(10))
        .unwrap();
}

fn insert(path: &Path, at: usize, extra: &[u8]) {
    let mut v = fs::read(path).unwrap();
    v.splice(at..at, extra.iter().copied());
    fs::write(path, v).unwrap();
    bump(path);
}

fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    for e in fs::read_dir(dir).unwrap().flatten() {
        let m = e.metadata().unwrap();
        total += if m.is_dir() {
            dir_bytes(&e.path())
        } else {
            m.len()
        };
    }
    total
}

fn assert_same(a: &Path, b: &Path) {
    let out = Command::new("diff")
        .arg("-rq")
        .arg(a)
        .arg(b)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "trees differ:\n{}",
        String::from_utf8_lossy(&out.stdout)
    );
}

// ---------- scenarios ----------

struct Scenario {
    name: &'static str,
    setup: fn(&Path),
    /// `None`: cold (empty destination). `Some`: sync once, then apply this edit and measure.
    edit: Option<fn(&Path)>,
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "cold, 64 MiB random file",
            setup: |s| write(s, "f.bin", &random(1, 64 * MIB)),
            edit: None,
        },
        Scenario {
            name: "warm, 4 KiB inserted mid 64 MiB random file",
            setup: |s| write(s, "f.bin", &random(2, 64 * MIB)),
            edit: Some(|s| insert(&s.join("f.bin"), 32 * MIB, &random(3, 4096))),
        },
        Scenario {
            name: "warm, checkpoint: 4 of 64 x 2 MiB tensors rewritten",
            setup: |s| {
                let ckpt: Vec<u8> = (0..64).flat_map(|t| random(100 + t, 2 * MIB)).collect();
                write(s, "model.ckpt", &ckpt);
            },
            edit: Some(|s| {
                let p = s.join("model.ckpt");
                let mut v = fs::read(&p).unwrap();
                for t in [5usize, 17, 40, 63] {
                    v[t * 2 * MIB..(t + 1) * 2 * MIB]
                        .copy_from_slice(&random(900 + t as u64, 2 * MIB));
                }
                fs::write(&p, v).unwrap();
                bump(&p);
            }),
        },
        Scenario {
            name: "cold, 64 MiB compressible log text",
            setup: |s| write(s, "app.log", &text(4, 64 * MIB)),
            edit: None,
        },
        Scenario {
            name: "warm, 1 MiB appended to 64 MiB log",
            setup: |s| write(s, "app.log", &text(5, 64 * MIB)),
            edit: Some(|s| {
                let p = s.join("app.log");
                let mut v = fs::read(&p).unwrap();
                v.extend(text(6, MIB));
                fs::write(&p, v).unwrap();
                bump(&p);
            }),
        },
        Scenario {
            name: "cold, 2000 small files (1-16 KiB text)",
            setup: tree,
            edit: None,
        },
        Scenario {
            name: "warm, 20 of 2000 small files edited",
            setup: tree,
            edit: Some(|s| {
                for i in (0..2000).step_by(100) {
                    let p = s.join(format!("d{:02}/f{i:04}.txt", i % 40));
                    let mut v = fs::read(&p).unwrap();
                    v.extend_from_slice(b"edited line\n");
                    fs::write(&p, v).unwrap();
                    bump(&p);
                }
            }),
        },
        Scenario {
            name: "warm, 2000 small files unchanged",
            setup: tree,
            edit: Some(|_| {}),
        },
    ]
}

fn tree(s: &Path) {
    let mut rng = StdRng::seed_from_u64(7);
    for i in 0..2000u64 {
        let len = rng.random_range(1024..16 * 1024);
        write(
            s,
            &format!("d{:02}/f{i:04}.txt", i % 40),
            &text(1000 + i, len),
        );
    }
}

// ---------- tools ----------

#[derive(Clone, Copy, Default)]
struct Measure {
    sent: u64,
    total: u64,
    secs: f64,
}

/// rsync over its remote-shell protocol, via a fake `rsh` that runs the server side locally.
fn rsync(scn: &Scenario, compress: bool) -> Measure {
    let tmp = TempDir::new().unwrap();
    let (src, dst) = (tmp.path().join("src"), tmp.path().join("dst"));
    fs::create_dir_all(&src).unwrap();
    fs::create_dir_all(&dst).unwrap();
    let rsh = tmp.path().join("rsh");
    fs::write(&rsh, "#!/bin/sh\nshift\nexec \"$@\"\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&rsh, fs::Permissions::from_mode(0o755)).unwrap();
    let run = || {
        let mut cmd = Command::new("rsync");
        cmd.arg(if compress { "-az" } else { "-a" })
            .arg("--stats")
            .arg("-e")
            .arg(&rsh)
            .arg(format!("{}/", src.display()))
            .arg(format!("fakehost:{}/", dst.display()));
        let start = Instant::now();
        let out = cmd.output().unwrap();
        let secs = start.elapsed().as_secs_f64();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        let field = |name: &str| -> u64 {
            stdout
                .lines()
                .find_map(|l| l.strip_prefix(name))
                .and_then(|v| {
                    v.trim()
                        .trim_end_matches(" B")
                        .replace(',', "")
                        .parse()
                        .ok()
                })
                .unwrap_or_else(|| panic!("no {name} in:\n{stdout}"))
        };
        let (sent, recv) = (field("Total sent:"), field("Total received:"));
        Measure {
            sent,
            total: sent + recv,
            secs,
        }
    };
    (scn.setup)(&src);
    if let Some(edit) = scn.edit {
        run();
        edit(&src);
    }
    let m = run();
    assert_same(&src, &dst);
    m
}

fn copy(scn: &Scenario) -> Measure {
    let tmp = TempDir::new().unwrap();
    let src = tmp.path().join("src");
    fs::create_dir_all(&src).unwrap();
    (scn.setup)(&src);
    if let Some(edit) = scn.edit {
        edit(&src);
    }
    let dst = tmp.path().join("dst");
    let start = Instant::now();
    assert!(
        Command::new("cp")
            .arg("-R")
            .arg(&src)
            .arg(&dst)
            .status()
            .unwrap()
            .success()
    );
    let secs = start.elapsed().as_secs_f64();
    assert_same(&src, &dst);
    let bytes = dir_bytes(&src);
    Measure {
        sent: bytes,
        total: bytes,
        secs,
    }
}

async fn xet(scn: &Scenario) -> Measure {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().join("root");
    let src = tmp.path().join("src");
    fs::create_dir_all(&root).unwrap();
    fs::create_dir_all(&src).unwrap();
    let server_id = generate("receiver").unwrap();
    let client_id = generate("sender").unwrap();
    let id = |g: &GeneratedIdentity| Identity::from_pem(&g.cert_pem, &g.key_pem).unwrap();
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
    let rcas = Arc::new(LocalCas::open(tmp.path().join("rcas")).await.unwrap());
    let ropts = ReceiverOptions::new(&rcas);
    tokio::spawn(
        Arc::new(Receiver::new(&root, rcas, ropts).unwrap())
            .serve(server.clone() as Arc<dyn transport::Listener>),
    );
    let scas = Arc::new(LocalCas::open(tmp.path().join("scas")).await.unwrap());

    let run = || async {
        let ep = client_endpoint(
            id(&client_id),
            vec![id(&server_id).cert],
            &EndpointOptions::default(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let start = Instant::now();
        let conn = connect(&ep, addr).await.unwrap();
        let opts = PushOptions {
            dest: "dst".into(),
            parallel_streams: 4,
            max_memory_bytes: transfer::DEFAULT_MAX_MEMORY_BYTES,
            delete: false,
            mode: TransferMode::Chunk,
            bandwidth_limit: None,
        };
        let r = push(scas.clone(), &src, &conn, &opts).await.unwrap();
        conn.close(0, b"done");
        Measure {
            sent: r.udp_bytes_sent,
            total: r.udp_bytes_sent + r.udp_bytes_received,
            secs: start.elapsed().as_secs_f64(),
        }
    };
    (scn.setup)(&src);
    if let Some(edit) = scn.edit {
        run().await;
        edit(&src);
    }
    let m = run().await;
    assert_same(&src, &root.join("dst"));
    drop(server);
    m
}

fn fmt_bytes(b: u64) -> String {
    if b >= 10 * 1024 * 1024 {
        format!("{:.1} MiB", b as f64 / MIB as f64)
    } else if b >= 10 * 1024 {
        format!("{:.0} KiB", b as f64 / 1024.0)
    } else {
        format!("{b} B")
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let started = Instant::now();
    let mut rows = Vec::new();
    for scn in scenarios() {
        let naive = copy(&scn);
        let r = rsync(&scn, false);
        let rz = rsync(&scn, true);
        let x = xet(&scn).await;
        eprintln!("done: {}", scn.name);
        rows.push((scn.name, naive, r, rz, x));
    }

    println!(
        "**Bytes sent by the source** (rsync: `Total sent`; xet-p2p: UDP datagrams sent, all QUIC/TLS overhead included)\n"
    );
    println!("| Scenario | Plain copy | rsync -a | rsync -az | xet-p2p |");
    println!("|---|---:|---:|---:|---:|");
    for (name, n, r, rz, x) in &rows {
        println!(
            "| {name} | {} | {} | {} | {} |",
            fmt_bytes(n.sent),
            fmt_bytes(r.sent),
            fmt_bytes(rz.sent),
            fmt_bytes(x.sent)
        );
    }
    println!(
        "\n**Bytes in both directions** (rsync adds `Total received`, i.e. block checksums; xet-p2p adds UDP received)\n"
    );
    println!("| Scenario | rsync -a | rsync -az | xet-p2p |");
    println!("|---|---:|---:|---:|");
    for (name, _, r, rz, x) in &rows {
        println!(
            "| {name} | {} | {} | {} |",
            fmt_bytes(r.total),
            fmt_bytes(rz.total),
            fmt_bytes(x.total)
        );
    }
    println!("\n**Wall-clock seconds** (local, no network limits: measures CPU and disk work)\n");
    println!("| Scenario | Plain copy | rsync -a | rsync -az | xet-p2p |");
    println!("|---|---:|---:|---:|---:|");
    for (name, n, r, rz, x) in &rows {
        println!(
            "| {name} | {:.2} | {:.2} | {:.2} | {:.2} |",
            n.secs, r.secs, rz.secs, x.secs
        );
    }
    eprintln!("total {:.0}s", started.elapsed().as_secs_f64());
}
