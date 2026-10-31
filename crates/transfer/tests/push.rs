//! M1 end to end: authenticated QUIC push over localhost UDP into a receiver's embedded CAS.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;
use transfer::protocol::{MAX_CONTROL_FRAME, Message, ObjectHeader, ObjectKind, PROTOCOL_VERSION};
use transfer::{Error, PushOptions, Receiver, push};
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
        let receiver = Arc::new(Receiver::new(&root, receiver_cas, 4).unwrap());
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
        let conn = self.connect_as(&self.client_id).await?;
        let opts = PushOptions {
            dest: dest.into(),
            parallel_streams: 4,
        };
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
    assert_eq!(r.receiver.files_installed, 2);
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
