//! The Modal adapter end to end against a fake Modal SDK (tests/fake_modal): exercises the real
//! helper script, agent upload, credentials, detached agent, raw TCP tunnel, and cleanup, without
//! a Modal account. Real-Modal tests are opt-in (see README).

use std::process::{Command, Output};

use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_xet-p2p");

struct Env {
    tmp: tempfile::TempDir,
    port: u16,
}

impl Env {
    fn new() -> Self {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("sbroot")).unwrap();
        std::fs::create_dir_all(tmp.path().join("src/d")).unwrap();
        let mut data = vec![0u8; 1024 * 1024];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i * 31 % 251) as u8;
        }
        std::fs::write(tmp.path().join("src/a.bin"), data).unwrap();
        std::fs::write(tmp.path().join("src/d/b.txt"), b"via a fake modal tunnel\n").unwrap();
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        Self { tmp, port }
    }

    fn uri(&self, id: &str, path: &str) -> String {
        let root = self.tmp.path().join("sbroot").display().to_string();
        let target = if path.is_empty() {
            root
        } else {
            format!("{root}/{path}")
        };
        format!(
            "modal://{id}{target}?port={}&root={}&state={}",
            self.port,
            self.tmp.path().join("sbroot").display(),
            self.tmp.path().join("state").display()
        )
    }

    fn run(&self, args: &[&str], sandboxes: &str) -> Output {
        Command::new(BIN)
            .args(args)
            .env("HOME", self.tmp.path())
            .env("XET_P2P_MODAL_PYTHON", "python3")
            .env(
                "PYTHONPATH",
                concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fake_modal"),
            )
            .env("XET_P2P_AGENT_LINUX_BIN", BIN)
            .env("FAKE_MODAL_SANDBOXES", sandboxes)
            .output()
            .unwrap()
    }

    fn sandboxes(&self) -> String {
        format!(r#"{{"sb-test": [{}], "sb-noport": []}}"#, self.port)
    }

    fn no_session_state(&self) {
        if let Ok(entries) = std::fs::read_dir(self.tmp.path().join("state")) {
            for e in entries.flatten() {
                let n = e.file_name().to_string_lossy().into_owned();
                assert!(!n.starts_with("session-"), "leftover session {n}");
            }
        }
    }
}

#[test]
fn push_pull_and_doctor_through_fake_modal() {
    let e = Env::new();
    let src = e.tmp.path().join("src").display().to_string();
    let sb = e.sandboxes();

    let out = e.run(&["push", &src, &e.uri("sb-test", "proj"), "--json"], &sb);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{err}");
    let r: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(r["transport"], "TLS over TCP (not QUIC)");
    assert!(err.contains("relayed by the provider"), "{err}");
    let diff = Command::new("diff")
        .arg("-r")
        .arg(e.tmp.path().join("src"))
        .arg(e.tmp.path().join("sbroot/proj"))
        .status()
        .unwrap();
    assert!(diff.success());
    e.no_session_state();

    // The agent binary is uploaded once and reused.
    let bins = std::fs::read_dir(e.tmp.path().join("state/bin"))
        .unwrap()
        .count();
    let back = e.tmp.path().join("back").display().to_string();
    let out = e.run(&["pull", &e.uri("sb-test", "proj"), &back, "--json"], &sb);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read_dir(e.tmp.path().join("state/bin"))
            .unwrap()
            .count(),
        bins
    );
    let diff = Command::new("diff")
        .arg("-r")
        .arg(e.tmp.path().join("src"))
        .arg(&back)
        .status()
        .unwrap();
    assert!(diff.success());

    let out = e.run(&["doctor", &e.uri("sb-test", "")], &sb);
    let report = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{report}");
    assert!(report.contains("QUIC (UDP)    n/a"), "{report}");
    assert!(report.contains("TLS over TCP  ok"), "{report}");
    assert!(report.contains("relayed by provider"), "{report}");
    assert!(report.contains("ephemeral"), "{report}");
    e.no_session_state();
}

#[test]
fn quic_only_fails_and_misconfigured_sandboxes_are_explained() {
    let e = Env::new();
    let src = e.tmp.path().join("src").display().to_string();
    let sb = e.sandboxes();

    let out = e.run(
        &["push", &src, &e.uri("sb-test", "p"), "--transport", "quic"],
        &sb,
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(err.contains("--transport quic requested"), "{err}");

    let out = e.run(&["push", &src, &e.uri("sb-noport", "p")], &sb);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(err.contains("unencrypted_ports"), "{err}");

    let out = e.run(&["push", &src, &e.uri("sb-missing", "p")], &sb);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(err.contains("not found"), "{err}");
    assert!(!e.tmp.path().join("sbroot/p").exists());
    e.no_session_state();
}
