//! M1 end to end: authenticated QUIC push over localhost UDP into a receiver's embedded CAS.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;
use transfer::protocol::{MAX_CONTROL_FRAME, Message, ObjectHeader, ObjectKind, PROTOCOL_VERSION};
use transfer::{Error, PushOptions, Receiver, ReceiverOptions, push};
use transport_quic::quinn::{Connection, Endpoint};
use transport_quic::{
    EndpointOptions, GeneratedIdentity, Identity, client_endpoint, connect, generate, read_frame,
    server_endpoint, write_frame,
};
use xet_local::{LocalCas, MerkleHash};

const MIB: usize = 1024 * 1024;

fn random_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut data = vec![0u8; len];
    StdRng::seed_from_u64(seed).fill(&mut data[..]);
    data
}

fn write(root: &Path, rel: &str, data: &[u8]) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, data).unwrap();
}

fn assert_trees_equal(a: &Path, b: &Path) {
    let mut n = 0;
    for entry in fs::read_dir(a).unwrap() {
        let path = entry.unwrap().path();
        let other = b.join(path.file_name().unwrap());
        if path.is_dir() {
            assert_trees_equal(&path, &other);
        } else {
            assert_eq!(
                fs::read(&path).unwrap(),
                fs::read(&other).unwrap(),
                "{path:?}"
            );
        }
        n += 1;
    }
    assert_eq!(
        n,
        fs::read_dir(b).unwrap().count(),
        "entry count differs in {b:?}"
    );
}

fn identity(g: &GeneratedIdentity) -> Identity {
    Identity::from_pem(&g.cert_pem, &g.key_pem).unwrap()
}

struct Harness {
    tmp: TempDir,
    addr: SocketAddr,
    server_id: GeneratedIdentity,
    client_id: GeneratedIdentity,
    sender_cas: Arc<LocalCas>,
    _server: Endpoint,
}

impl Harness {
    async fn new() -> Self {
        Self::with_receiver(|_| {}).await
    }

    async fn with_receiver(configure: impl FnOnce(&mut ReceiverOptions)) -> Self {
        Self::with_receiver_for(|o, _| configure(o)).await
    }

    /// Like [`Self::with_receiver`], also passing the sender certificate's fingerprint.
    async fn with_receiver_for(configure: impl FnOnce(&mut ReceiverOptions, &str)) -> Self {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("root");
        fs::create_dir_all(root.join(".xet-p2p")).unwrap();
        fs::create_dir(tmp.path().join("src")).unwrap();

        let server_id = generate("receiver").unwrap();
        let client_id = generate("sender").unwrap();
        let server = server_endpoint(
            "127.0.0.1:0".parse().unwrap(),
            identity(&server_id),
            vec![identity(&client_id).cert],
            &EndpointOptions::default(),
        )
        .unwrap();
        let addr = server.local_addr().unwrap();

        // The receiver's CAS lives inside its root, as in the brief's example layout.
        let receiver_cas = Arc::new(LocalCas::open(root.join(".xet-p2p/cas")).await.unwrap());
        let mut opts = ReceiverOptions::new(&receiver_cas);
        configure(
            &mut opts,
            &transport_quic::fingerprint(&identity(&client_id).cert),
        );
        let receiver = Arc::new(Receiver::new(&root, receiver_cas, opts).unwrap());
        tokio::spawn(receiver.serve(server.clone()));

        let sender_cas = Arc::new(LocalCas::open(tmp.path().join("sender-cas")).await.unwrap());
        Self {
            tmp,
            addr,
            server_id,
            client_id,
            sender_cas,
            _server: server,
        }
    }

    fn src(&self) -> PathBuf {
        self.tmp.path().join("src")
    }

    fn root(&self) -> PathBuf {
        self.tmp.path().join("root")
    }

    async fn connect_as(&self, client: &GeneratedIdentity) -> transport_quic::Result<Connection> {
        let ep = client_endpoint(
            identity(client),
            vec![identity(&self.server_id).cert],
            &EndpointOptions::default(),
            "127.0.0.1:0".parse().unwrap(),
        )?;
        connect(&ep, self.addr).await
    }

    async fn push(&self, dest: &str) -> transfer::Result<transfer::PushReport> {
        self.push_with(dest, |_| {}).await
    }

    async fn push_with(
        &self,
        dest: &str,
        configure: impl FnOnce(&mut PushOptions),
    ) -> transfer::Result<transfer::PushReport> {
        let conn = self.connect_as(&self.client_id).await?;
        let mut opts = PushOptions {
            dest: dest.into(),
            parallel_streams: 4,
            max_memory_bytes: transfer::DEFAULT_MAX_MEMORY_BYTES,
            delete: false,
        };
        configure(&mut opts);
        let result = push(self.sender_cas.clone(), &self.src(), &conn, &opts).await;
        conn.close(0u32.into(), b"done");
        result
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn push_tree_to_empty_receiver() {
    let h = Harness::new().await;
    write(&h.src(), "a.bin", &random_bytes(1, 3 * MIB));
    write(&h.src(), "nested/dir/b.txt", b"hello over quic\n");
    write(&h.src(), "ünïcödé.bin", &random_bytes(2, 70_000));
    write(&h.src(), "empty", b"");
    fs::create_dir_all(h.src().join("empty-dir")).unwrap();

    let r = h.push("project").await.unwrap();
    assert_trees_equal(&h.src(), &h.root().join("project"));
    assert_eq!(r.receiver.files_installed, 4);
    assert_eq!(r.xorbs_sent, r.xorbs_referenced);
    assert!(r.xorb_bytes_sent > 3 * MIB as u64);
    assert!(r.shard_bytes_sent > 0);
    assert_eq!(r.receiver.payload_bytes_received, r.payload_bytes_sent());
    assert!(r.udp_bytes_sent >= r.payload_bytes_sent());
}

#[tokio::test(flavor = "multi_thread")]
async fn push_multi_xorb_file() {
    let h = Harness::new().await;
    write(&h.src(), "big.bin", &random_bytes(3, 90 * MIB));
    let r = h.push("big").await.unwrap();
    assert!(r.xorbs_sent >= 2, "{r:?}");
    assert_trees_equal(&h.src(), &h.root().join("big"));
}

#[tokio::test(flavor = "multi_thread")]
async fn second_push_of_unchanged_tree_sends_zero_payload() {
    let h = Harness::new().await;
    write(&h.src(), "a.bin", &random_bytes(4, 2 * MIB));
    write(&h.src(), "b/c.bin", &random_bytes(5, MIB));
    h.push("p").await.unwrap();

    let r = h.push("p").await.unwrap();
    assert_eq!(r.payload_bytes_sent(), 0, "{r:?}");
    assert_eq!(r.xorbs_sent, 0);
    assert_eq!(r.xorbs_reused(), r.xorbs_referenced);
    assert_eq!(
        r.receiver.files_installed, 0,
        "unchanged files must not be rewritten"
    );
    assert_eq!(r.receiver.files_unchanged, 2);
    assert_eq!(r.receiver.xorbs_already_present, r.xorbs_referenced);
    assert_trees_equal(&h.src(), &h.root().join("p"));
}

#[tokio::test(flavor = "multi_thread")]
async fn push_to_second_destination_reuses_receiver_cas() {
    let h = Harness::new().await;
    write(&h.src(), "a.bin", &random_bytes(6, 2 * MIB));
    h.push("one").await.unwrap();
    let r = h.push("two").await.unwrap();
    assert_eq!(r.payload_bytes_sent(), 0);
    assert_trees_equal(&h.src(), &h.root().join("two"));
}

#[tokio::test(flavor = "multi_thread")]
async fn edited_file_resends_only_new_xorbs() {
    let h = Harness::new().await;
    let base = random_bytes(7, 4 * MIB);
    write(&h.src(), "a.bin", &base);
    let first = h.push("p").await.unwrap();

    let mut grown = base.clone();
    grown.extend(random_bytes(8, MIB));
    write(&h.src(), "a.bin", &grown);
    let second = h.push("p").await.unwrap();
    assert!(second.xorbs_reused() >= 1, "{second:?}");
    assert!(second.xorb_bytes_sent < first.xorb_bytes_sent + MIB as u64);
    assert_eq!(fs::read(h.root().join("p/a.bin")).unwrap(), grown);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_escapes_rejected() {
    let h = Harness::new().await;
    write(&h.src(), "a.txt", b"x");
    for dest in ["../escape", "/abs", "a/../../b", ""] {
        let err = h.push(dest).await.unwrap_err();
        assert!(matches!(err, Error::Remote(_)), "{dest:?}: {err:?}");
    }
    assert!(!h.tmp.path().join("escape").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_overlapping_cas_rejected() {
    let h = Harness::new().await;
    write(&h.src(), "a.txt", b"x");
    for dest in [".xet-p2p", ".xet-p2p/cas", ".xet-p2p/cas/xet"] {
        let err = h.push(dest).await.unwrap_err();
        assert!(
            matches!(err, Error::Remote(ref m) if m.contains("CAS")),
            "{dest:?}: {err:?}"
        );
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn destination_symlink_escape_rejected() {
    let h = Harness::new().await;
    write(&h.src(), "a.txt", b"x");
    let outside = h.tmp.path().join("outside");
    fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, h.root().join("link")).unwrap();
    assert!(h.push("link/p").await.is_err());
    assert!(fs::read_dir(&outside).unwrap().next().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn unauthorized_client_cannot_push() {
    let h = Harness::new().await;
    write(&h.src(), "a.txt", b"secret");
    let stranger = generate("stranger").unwrap();
    let outcome = async {
        let conn = h.connect_as(&stranger).await?;
        let opts = PushOptions {
            dest: "p".into(),
            parallel_streams: 1,
            max_memory_bytes: transfer::DEFAULT_MAX_MEMORY_BYTES,
            delete: false,
        };
        push(h.sender_cas.clone(), &h.src(), &conn, &opts).await
    }
    .await;
    assert!(outcome.is_err());
    assert!(!h.root().join("p").exists());
}

// ---- A hand-driven, misbehaving sender ----

/// Runs the protocol up to the receiver's xorb request, then lets `respond` send objects.
async fn misbehaving_push<F, Fut>(h: &Harness, respond: F) -> Message
where
    F: FnOnce(Connection, Vec<MerkleHash>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let (manifest, _) = h.sender_cas.ingest(h.src()).await.unwrap();
    let conn = h.connect_as(&h.client_id).await.unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    let mut next = async || {
        read_frame::<Message>(&mut recv, MAX_CONTROL_FRAME)
            .await
            .unwrap()
            .unwrap()
    };

    write_frame(
        &mut send,
        &Message::Hello {
            protocol_version: PROTOCOL_VERSION,
            session_id: "test".into(),
            dest: "p".into(),
            delete: false,
        },
    )
    .await
    .unwrap();
    assert!(matches!(next().await, Message::HelloOk { .. }));
    write_frame(&mut send, &Message::Manifest { manifest })
        .await
        .unwrap();

    // Honest shard.
    let Message::Need {
        reconstructions, ..
    } = next().await
    else {
        panic!()
    };
    let files = reconstructions
        .iter()
        .map(|s| MerkleHash::from_hex(s).unwrap())
        .collect();
    let shard = h.sender_cas.export_shard(&files).await.unwrap();
    send_raw(&conn, ObjectKind::Shard, None, &shard).await;
    assert!(matches!(next().await, Message::Ack { .. }));

    let Message::Need { xorbs, .. } = next().await else {
        panic!()
    };
    let xorbs = xorbs
        .iter()
        .map(|s| MerkleHash::from_hex(s).unwrap())
        .collect();
    respond(conn.clone(), xorbs).await;

    let last = loop {
        match next().await {
            Message::Ack { .. } => continue,
            m => break m,
        }
    };
    conn.close(0u32.into(), b"done");
    last
}

async fn send_raw(conn: &Connection, kind: ObjectKind, hash: Option<MerkleHash>, bytes: &[u8]) {
    let mut s = conn.open_uni().await.unwrap();
    write_frame(
        &mut s,
        &ObjectHeader {
            kind,
            hash: hash.map(|h| h.hex()),
            len: bytes.len() as u64,
        },
    )
    .await
    .unwrap();
    s.write_all(bytes).await.unwrap();
    s.finish().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupted_xorb_rejected_and_nothing_installed() {
    let h = Harness::new().await;
    write(&h.src(), "a.bin", &random_bytes(9, 2 * MIB));
    let cas = h.sender_cas.clone();
    let last = misbehaving_push(&h, |conn, xorbs| async move {
        for x in xorbs {
            let mut bytes = cas.read_xorb(&x).await.unwrap();
            let mid = bytes.len() / 2;
            bytes[mid] ^= 0x55;
            send_raw(&conn, ObjectKind::Xorb, Some(x), &bytes).await;
        }
    })
    .await;
    assert!(
        matches!(last, Message::Error { ref message } if message.contains("verification")),
        "{last:?}"
    );
    assert!(!h.root().join("p/a.bin").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn unrequested_xorb_rejected() {
    let h = Harness::new().await;
    write(&h.src(), "a.bin", &random_bytes(10, MIB));
    let last = misbehaving_push(&h, |conn, xorbs| async move {
        send_raw(
            &conn,
            ObjectKind::Xorb,
            Some(MerkleHash::default()),
            b"junk",
        )
        .await;
        drop(xorbs);
    })
    .await;
    assert!(
        matches!(last, Message::Error { ref message } if message.contains("unrequested")),
        "{last:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_xorb_header_rejected() {
    let h = Harness::new().await;
    write(&h.src(), "a.bin", &random_bytes(11, MIB));
    let last = misbehaving_push(&h, |conn, xorbs| async move {
        let mut s = conn.open_uni().await.unwrap();
        let header = ObjectHeader {
            kind: ObjectKind::Xorb,
            hash: Some(xorbs[0].hex()),
            len: 1 << 40,
        };
        write_frame(&mut s, &header).await.unwrap();
        let _ = s.finish();
    })
    .await;
    assert!(
        matches!(last, Message::Error { ref message } if message.contains("exceeds")),
        "{last:?}"
    );
}

// ---- M2: resume and concurrency ----

/// Runs an honest push that drops the connection after `commit` xorbs have been ACKed.
/// Returns how many xorbs the receiver asked for.
async fn interrupted_push(h: &Harness, dest: &str, commit: usize) -> usize {
    let (manifest, _) = h.sender_cas.ingest(h.src()).await.unwrap();
    let conn = h.connect_as(&h.client_id).await.unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    let mut next = async || {
        read_frame::<Message>(&mut recv, MAX_CONTROL_FRAME)
            .await
            .unwrap()
            .unwrap()
    };

    write_frame(
        &mut send,
        &Message::Hello {
            protocol_version: PROTOCOL_VERSION,
            session_id: "interrupted".into(),
            dest: dest.into(),
            delete: false,
        },
    )
    .await
    .unwrap();
    assert!(matches!(next().await, Message::HelloOk { .. }));
    write_frame(&mut send, &Message::Manifest { manifest })
        .await
        .unwrap();

    let Message::Need {
        reconstructions, ..
    } = next().await
    else {
        panic!()
    };
    let files = reconstructions
        .iter()
        .map(|s| MerkleHash::from_hex(s).unwrap())
        .collect();
    send_raw(
        &conn,
        ObjectKind::Shard,
        None,
        &h.sender_cas.export_shard(&files).await.unwrap(),
    )
    .await;
    assert!(matches!(
        next().await,
        Message::Ack {
            kind: ObjectKind::Shard,
            ..
        }
    ));

    let Message::Need { xorbs, .. } = next().await else {
        panic!()
    };
    for x in xorbs.iter().take(commit) {
        let x = MerkleHash::from_hex(x).unwrap();
        send_raw(
            &conn,
            ObjectKind::Xorb,
            Some(x),
            &h.sender_cas.read_xorb(&x).await.unwrap(),
        )
        .await;
        assert!(matches!(
            next().await,
            Message::Ack {
                kind: ObjectKind::Xorb,
                ..
            }
        ));
    }
    // Simulate the sender dying mid-transfer.
    conn.close(1u32.into(), b"killed");
    xorbs.len()
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_push_resumes_without_resending_committed_content() {
    let h = Harness::new().await;
    write(&h.src(), "big.bin", &random_bytes(30, 150 * MIB));
    write(&h.src(), "small.txt", b"also here");

    let needed = interrupted_push(&h, "p", 2).await;
    assert!(
        needed >= 3,
        "need several xorbs to interrupt between; got {needed}"
    );
    assert!(
        !h.root().join("p/big.bin").exists(),
        "nothing installed by the interrupted attempt"
    );

    let r = h.push("p").await.unwrap();
    let rr = &r.receiver;
    assert!(rr.resumed, "{rr:?}");
    assert_eq!(rr.attempts, 2);
    assert_eq!(rr.prior_xorbs_committed, 2);
    assert_eq!(
        rr.reconstructions_reused, 2,
        "entries must come from saved state"
    );
    assert_eq!(r.shard_bytes_sent, 0);
    assert_eq!(
        r.xorbs_sent,
        needed - 2,
        "committed xorbs must not be re-sent"
    );
    assert_eq!(rr.xorbs_already_present, 2);
    assert_trees_equal(&h.src(), &h.root().join("p"));
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_before_any_xorb_still_reuses_shard() {
    let h = Harness::new().await;
    write(&h.src(), "a.bin", &random_bytes(31, 3 * MIB));
    let needed = interrupted_push(&h, "p", 0).await;
    let r = h.push("p").await.unwrap();
    assert!(r.receiver.resumed && r.receiver.reconstructions_reused == 1);
    assert_eq!(r.shard_bytes_sent, 0);
    assert_eq!(r.xorbs_sent, needed);
    assert_trees_equal(&h.src(), &h.root().join("p"));
}

#[tokio::test(flavor = "multi_thread")]
async fn completed_session_is_removed() {
    let h = Harness::new().await;
    write(&h.src(), "a.bin", &random_bytes(32, MIB));
    let r1 = h.push("p").await.unwrap();
    assert!(!r1.receiver.resumed);
    let sessions = h.root().join(".xet-p2p/cas/sessions");
    let leftovers: Vec<_> = fs::read_dir(&sessions)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .filter(|n| n != "pending")
        .collect();
    assert!(leftovers.is_empty(), "session left behind: {leftovers:?}");
    assert_eq!(
        fs::read_dir(sessions.join("pending")).unwrap().count(),
        0,
        "pending entries left behind"
    );
    let r2 = h.push("p").await.unwrap();
    assert!(!r2.receiver.resumed);
}

#[tokio::test(flavor = "multi_thread")]
async fn changed_source_after_interruption_starts_fresh_but_reuses_xorbs() {
    let h = Harness::new().await;
    write(&h.src(), "big.bin", &random_bytes(33, 150 * MIB));
    interrupted_push(&h, "p", 2).await;
    write(&h.src(), "new.txt", b"added after the interruption");
    let r = h.push("p").await.unwrap();
    // Different manifest, so a different session; the committed xorbs are still found in the CAS,
    // and the unchanged file's entry is reused, so only new.txt's metadata is requested.
    assert!(!r.receiver.resumed);
    assert!(r.receiver.xorbs_already_present >= 2, "{:?}", r.receiver);
    assert_eq!(r.receiver.reconstructions_reused, 1, "{:?}", r.receiver);
    assert_eq!(r.receiver.shards_received, 1);
    assert_trees_equal(&h.src(), &h.root().join("p"));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_pushes_to_disjoint_destinations() {
    let h = Harness::new().await;
    write(&h.src(), "shared.bin", &random_bytes(34, 8 * MIB));
    write(&h.src(), "d/x.txt", b"x");
    // Prime the sender's CAS so both pushes only differ on the receiver side.
    h.sender_cas.ingest(h.src()).await.unwrap();
    let (a, b, c) = tokio::join!(h.push("a"), h.push("b"), h.push("c"));
    for (dest, r) in [("a", a), ("b", b), ("c", c)] {
        r.unwrap_or_else(|e| panic!("{dest}: {e}"));
        assert_trees_equal(&h.src(), &h.root().join(dest));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_pushes_to_overlapping_destinations_serialize() {
    let h = Harness::new().await;
    write(&h.src(), "a.bin", &random_bytes(35, 4 * MIB));
    h.sender_cas.ingest(h.src()).await.unwrap();
    let (outer, inner) = tokio::join!(h.push("p"), h.push("p/inner"));
    outer.unwrap();
    inner.unwrap();
    assert_eq!(
        fs::read(h.root().join("p/a.bin")).unwrap(),
        fs::read(h.src().join("a.bin")).unwrap()
    );
    assert_trees_equal(&h.src(), &h.root().join("p/inner"));
}

#[tokio::test(flavor = "multi_thread")]
async fn tiny_memory_budgets_still_complete_one_xorb_at_a_time() {
    // Budgets below one xorb clamp to "one xorb in memory at a time" on both sides.
    let h = Harness::with_receiver(|o| o.max_memory_bytes = 1).await;
    write(&h.src(), "big.bin", &random_bytes(36, 90 * MIB));
    let r = h
        .push_with("p", |o| {
            o.max_memory_bytes = 1;
            o.parallel_streams = 8;
        })
        .await
        .unwrap();
    assert!(r.xorbs_sent >= 2);
    assert_trees_equal(&h.src(), &h.root().join("p"));
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_overlapping_session_dir_rejected() {
    let tmp_sessions = std::sync::Mutex::new(None);
    let h = Harness::with_receiver(|o| {
        let dir = o
            .session_dir
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("state");
        *tmp_sessions.lock().unwrap() = Some(dir.clone());
        o.session_dir = dir; // <root>/state
    })
    .await;
    write(&h.src(), "a.txt", b"x");
    let err = h.push("state").await.unwrap_err();
    assert!(
        matches!(err, Error::Remote(ref m) if m.contains("session")),
        "{err:?}"
    );
    assert!(h.push("elsewhere").await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn sender_confined_to_its_scope() {
    let h = Harness::with_receiver_for(|o, client_fp| {
        o.client_scopes
            .insert(client_fp.to_owned(), vec![PathBuf::from("projects/alice")]);
    })
    .await;
    write(&h.src(), "a.txt", b"x");
    assert!(h.push("projects/alice").await.is_ok());
    assert!(h.push("projects/alice/deep/er").await.is_ok());
    for dest in ["projects/bob", "projects", "projects/alicex", "other"] {
        let err = h.push(dest).await.unwrap_err();
        assert!(
            matches!(err, Error::Remote(ref m) if m.contains("allowed")),
            "{dest}: {err:?}"
        );
    }
    assert!(!h.root().join("projects/bob").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn sender_without_scope_entry_rejected_when_scopes_configured() {
    let h = Harness::with_receiver_for(|o, _| {
        o.client_scopes
            .insert("someone-else".into(), vec![PathBuf::new()]);
    })
    .await;
    write(&h.src(), "a.txt", b"x");
    let err = h.push("p").await.unwrap_err();
    assert!(
        matches!(err, Error::Remote(ref m) if m.contains("no destination scope")),
        "{err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn push_with_delete_mirrors_source() {
    let h = Harness::new().await;
    write(&h.src(), "keep.txt", b"keep");
    write(&h.src(), "remove-later.txt", b"soon gone");
    write(&h.src(), "dir/inner.txt", b"inner");
    h.push("p").await.unwrap();

    fs::remove_file(h.src().join("remove-later.txt")).unwrap();
    fs::remove_dir_all(h.src().join("dir")).unwrap();
    // Without --delete the receiver keeps them.
    let r = h.push("p").await.unwrap();
    assert_eq!(r.receiver.files_deleted, 0);
    assert!(h.root().join("p/remove-later.txt").exists());

    let r = h.push_with("p", |o| o.delete = true).await.unwrap();
    assert_eq!(
        (r.receiver.files_deleted, r.receiver.dirs_deleted),
        (2, 1),
        "{:?}",
        r.receiver
    );
    assert_trees_equal(&h.src(), &h.root().join("p"));
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_never_reaches_outside_destination() {
    let h = Harness::new().await;
    write(&h.root(), "sibling/precious.txt", b"not part of the push");
    write(&h.src(), "a.txt", b"a");
    h.push_with("p", |o| o.delete = true).await.unwrap();
    assert!(h.root().join("sibling/precious.txt").exists());
    assert!(h.root().join(".xet-p2p/cas").exists());
}
