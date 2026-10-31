//! M2 exit gate with real processes: SIGKILL the receiver mid-transfer, restart it, push again.
//! Committed xorbs must survive the crash and must not be re-sent.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_xet-p2p");

fn run(args: &[&str]) -> std::process::Output {
    let out = Command::new(BIN).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Paths {
    root: PathBuf,
    cas: PathBuf,
    certs: PathBuf,
    src: PathBuf,
    sender_cas: PathBuf,
}

fn serve(p: &Paths, port: u16, delay_ms: Option<u64>) -> Child {
    let listen = format!("127.0.0.1:{port}");
    let mut args = vec![
        "serve".to_string(),
        "--listen".into(),
        listen,
        "--root".into(),
        p.root.display().to_string(),
        "--cas-dir".into(),
        p.cas.display().to_string(),
        "--cert".into(),
        p.certs.join("receiver.crt").display().to_string(),
        "--key".into(),
        p.certs.join("receiver.key").display().to_string(),
        "--allow-client".into(),
        p.certs.join("sender.crt").display().to_string(),
        "--parallel".into(),
        "1".into(),
    ];
    if let Some(d) = delay_ms {
        args.extend(["--test-commit-delay-ms".into(), d.to_string()]);
    }
    let mut child = Command::new(BIN)
        .args(&args)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Wait for the listening line.
    let (tx, rx) = mpsc::channel();
    let stderr = child.stderr.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = tx.send(line);
        }
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let line = rx
            .recv_timeout(deadline - Instant::now())
            .expect("serve never started listening");
        if line.contains("listening on") {
            break;
        }
    }
    // Keep draining so the child never blocks on a full pipe.
    std::thread::spawn(move || for _ in rx {});
    child
}

fn push_cmd(p: &Paths, port: u16) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.args(["push"])
        .arg(&p.src)
        .args([
            "--to",
            &format!("127.0.0.1:{port}"),
            "--dest",
            "project",
            "--parallel",
            "1",
            "--json",
        ])
        .arg("--cas-dir")
        .arg(&p.sender_cas)
        .arg("--cert")
        .arg(p.certs.join("sender.crt"))
        .arg("--key")
        .arg(p.certs.join("sender.key"))
        .arg("--peer-cert")
        .arg(p.certs.join("receiver.crt"));
    cmd
}

fn committed_in_sessions(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
        .filter_map(|e| std::fs::read(e.path()).ok())
        .filter_map(|b| serde_json::from_slice::<Value>(&b).ok())
        .filter_map(|v| v["xorbs_committed"].as_u64())
        .max()
        .unwrap_or(0)
}

#[test]
fn receiver_killed_mid_transfer_resumes_after_restart() {
    let tmp = tempfile::TempDir::new().unwrap();
    let p = Paths {
        root: tmp.path().join("root"),
        cas: tmp.path().join("receiver-cas"),
        certs: tmp.path().join("certs"),
        src: tmp.path().join("src"),
        sender_cas: tmp.path().join("sender-cas"),
    };
    std::fs::create_dir_all(&p.root).unwrap();
    std::fs::create_dir_all(&p.src).unwrap();
    let certs = p.certs.display().to_string();
    run(&["keygen", "--out", &certs, "--name", "receiver"]);
    run(&["keygen", "--out", &certs, "--name", "sender"]);

    let mut data = vec![0u8; 150 * 1024 * 1024];
    StdRng::seed_from_u64(42).fill(&mut data[..]);
    std::fs::write(p.src.join("big.bin"), &data).unwrap();
    std::fs::write(p.src.join("note.txt"), b"crash test\n").unwrap();

    // Attempt 1: slow receiver, killed after its first durable xorb commit.
    let port = free_udp_port();
    let mut receiver = serve(&p, port, Some(1500));
    let mut pusher = push_cmd(&p, port)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let sessions = p.cas.join("sessions");
    let deadline = Instant::now() + Duration::from_secs(120);
    while committed_in_sessions(&sessions) < 1 {
        assert!(Instant::now() < deadline, "no xorb committed in time");
        assert!(
            pusher.try_wait().unwrap().is_none(),
            "push finished before it could be interrupted"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    receiver.kill().unwrap(); // SIGKILL: no cleanup runs
    receiver.wait().unwrap();
    let _ = pusher.kill();
    pusher.wait().unwrap();
    let committed = committed_in_sessions(&sessions);
    assert!(
        !p.root.join("project/big.bin").exists(),
        "partial transfer must install nothing"
    );

    // Attempt 2: restarted receiver, normal speed.
    let port = free_udp_port();
    let mut receiver = serve(&p, port, None);
    let out = push_cmd(&p, port).output().unwrap();
    receiver.kill().unwrap();
    receiver.wait().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: Value = serde_json::from_slice(&out.stdout).unwrap();
    let rr = &report["receiver"];

    assert_eq!(rr["resumed"], true, "{report:#}");
    assert_eq!(rr["prior_xorbs_committed"].as_u64().unwrap(), committed);
    // Chunk mode (the default): chunk lists come from saved state, and every chunk packed into
    // a committed xorb before the crash is found locally instead of being re-sent.
    assert_eq!(rr["mode"], "chunk");
    assert!(
        rr["chunk_lists_reused"].as_u64().unwrap() >= 1,
        "{report:#}"
    );
    assert_eq!(report["chunk_list_bytes_sent"], 0);
    let total = rr["chunks_total"].as_u64().unwrap();
    let present = rr["chunks_present"].as_u64().unwrap();
    let received = rr["chunks_received"].as_u64().unwrap();
    assert!(
        committed >= 1 && present > 0 && received == total - present,
        "re-sent committed chunks: {report:#}"
    );
    assert_eq!(std::fs::read(p.root.join("project/big.bin")).unwrap(), data);
    assert_eq!(
        std::fs::read(p.root.join("project/note.txt")).unwrap(),
        b"crash test\n"
    );
}
