//! M5, offline: the provider flow end to end with the fake `local://` provider and real processes.

use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_xet-p2p");

struct Env {
    tmp: tempfile::TempDir,
}

impl Env {
    fn new() -> Self {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("sandboxes/box1")).unwrap();
        std::fs::create_dir_all(tmp.path().join("src/sub")).unwrap();
        std::fs::write(tmp.path().join("src/a.bin"), random(1, 2 * 1024 * 1024)).unwrap();
        std::fs::write(tmp.path().join("src/sub/b.txt"), b"provider flow\n").unwrap();
        Self { tmp }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(BIN)
            .args(args)
            .env("XET_P2P_LOCAL_SANDBOXES", self.tmp.path().join("sandboxes"))
            .env("HOME", self.tmp.path()) // default --cas-dir lands in the temp dir
            .output()
            .unwrap()
    }

    fn ok(&self, args: &[&str]) -> (Value, String) {
        let out = self.run(args);
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(out.status.success(), "{args:?} failed:\n{stderr}");
        let json = serde_json::from_slice(&out.stdout).unwrap_or(Value::Null);
        (json, stderr)
    }

    fn fails(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(!out.status.success(), "{args:?} unexpectedly succeeded");
        String::from_utf8_lossy(&out.stderr).into_owned()
    }

    fn p(&self, rel: &str) -> String {
        self.tmp.path().join(rel).display().to_string()
    }

    /// No per-session credentials may survive a command.
    fn assert_no_session_state(&self) {
        let dir = self.tmp.path().join("sandboxes/box1/.xet-p2p");
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                assert!(
                    !name.starts_with("session-"),
                    "leftover agent session {name}"
                );
            }
        }
    }
}

fn random(seed: u64, len: usize) -> Vec<u8> {
    use rand::{Rng, SeedableRng};
    let mut v = vec![0u8; len];
    rand::rngs::StdRng::seed_from_u64(seed).fill(&mut v[..]);
    v
}

fn assert_same(a: &Path, b: &Path) {
    let out = Command::new("diff")
        .arg("-r")
        .arg(a)
        .arg(b)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
}

#[test]
fn push_and_pull_through_a_local_sandbox() {
    let e = Env::new();
    let src = e.p("src");

    // Default policy: QUIC when the sandbox can take UDP.
    let (r, err) = e.ok(&["push", &src, "local://box1/work/proj", "--json"]);
    assert_eq!(r["transport"], "QUIC (UDP)", "{err}");
    assert_same(
        &e.tmp.path().join("src"),
        &e.tmp.path().join("sandboxes/box1/work/proj"),
    );
    e.assert_no_session_state();

    // Repeat push: the sandbox's CAS persisted, so nothing moves.
    let (r, _) = e.ok(&["push", &src, "local://box1/work/proj", "--json"]);
    assert_eq!(r["payload_bytes_sent"], 0);

    // Pull it back into a fresh directory.
    let back = e.p("back");
    let (r, _) = e.ok(&["pull", "local://box1/work/proj", &back, "--json"]);
    assert_eq!(r["local"]["files_installed"], 2);
    assert_same(&e.tmp.path().join("src"), &e.tmp.path().join("back"));
    e.assert_no_session_state();
}

#[test]
fn auto_falls_back_to_tcp_and_says_so_quic_only_fails_clearly() {
    let e = Env::new();
    let src = e.p("src");

    let (r, err) = e.ok(&["push", &src, "local://box1/p?udp=off", "--json"]);
    assert_eq!(r["transport"], "TLS over TCP (not QUIC)");
    assert!(err.contains("not QUIC"), "{err}");
    assert_same(
        &e.tmp.path().join("src"),
        &e.tmp.path().join("sandboxes/box1/p"),
    );

    let err = e.fails(&[
        "push",
        &src,
        "local://box1/q?udp=off",
        "--transport",
        "quic",
    ]);
    assert!(err.contains("--transport quic requested"), "{err}");
    assert!(!e.tmp.path().join("sandboxes/box1/q").exists());

    let (r, _) = e.ok(&[
        "push",
        &src,
        "local://box1/r",
        "--transport",
        "tcp",
        "--json",
    ]);
    assert_eq!(r["transport"], "TLS over TCP (not QUIC)");
    e.assert_no_session_state();
}

#[test]
fn doctor_reports_what_actually_works() {
    let e = Env::new();
    let (_, out) = e.ok(&["doctor", "local://box1"]);
    assert!(out.contains("agent:      started and stopped"), "{out}");
    assert!(out.contains("QUIC (UDP)    ok"), "{out}");
    assert!(out.contains("TLS over TCP  ok"), "{out}");
    assert!(out.contains("auto picks: QUIC (UDP)"), "{out}");

    let (_, out) = e.ok(&["doctor", "local://box1?udp=off"]);
    assert!(out.contains("QUIC (UDP)    n/a"), "{out}");
    assert!(out.contains("auto picks: TLS over TCP"), "{out}");
    e.assert_no_session_state();
}

#[test]
fn bad_targets_fail_without_side_effects() {
    let e = Env::new();
    let src = e.p("src");
    let err = e.fails(&["push", &src, "local://nope/x"]);
    assert!(err.contains("attach never creates"), "{err}");
    assert!(!e.tmp.path().join("sandboxes/nope").exists());

    let err = e.fails(&["push", &src, "e2b://sbx123/x"]);
    assert!(err.contains("not supported yet"), "{err}");
    let err = e.fails(&["push", &src, "local://box1/"]);
    assert!(err.contains("name a directory"), "{err}");
    let err = e.fails(&["push", &src, "local://box1/../escape"]);
    assert!(err.contains("relative path segments"), "{err}");
    let err = e.fails(&["push", &src, "quic://127.0.0.1:9/x"]);
    assert!(
        err.contains("--cert"),
        "direct targets need credentials: {err}"
    );
    e.assert_no_session_state();
}
