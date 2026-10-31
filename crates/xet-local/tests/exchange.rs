//! Moving native Xet objects between two independent local CAS instances, in process.
//! This is the content half of M1, without the network.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;
use xet_core_structures::metadata_shard::shard_in_memory::MDBInMemoryShard;
use xet_local::{Error, LocalCas, Manifest, MerkleHash, PendingShard};

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
    let mut entries = Vec::new();
    for entry in fs::read_dir(a).unwrap() {
        let path = entry.unwrap().path();
        let other = b.join(path.file_name().unwrap());
        if path.is_dir() {
            assert!(other.is_dir(), "{other:?} missing");
            assert_trees_equal(&path, &other);
        } else {
            assert_eq!(
                fs::read(&path).unwrap(),
                fs::read(&other).unwrap(),
                "{path:?}"
            );
        }
        entries.push(path.file_name().unwrap().to_owned());
    }
    assert_eq!(
        entries.len(),
        fs::read_dir(b).unwrap().count(),
        "extra entries in {b:?}"
    );
}

struct Peers {
    tmp: TempDir,
    sender: LocalCas,
    receiver: LocalCas,
}

async fn peers() -> Peers {
    let tmp = TempDir::new().unwrap();
    fs::create_dir(tmp.path().join("src")).unwrap();
    let sender = LocalCas::open(tmp.path().join("sender-cas")).await.unwrap();
    let receiver = LocalCas::open(tmp.path().join("receiver-cas"))
        .await
        .unwrap();
    Peers {
        tmp,
        sender,
        receiver,
    }
}

impl Peers {
    fn src(&self) -> std::path::PathBuf {
        self.tmp.path().join("src")
    }

    /// The receiver-driven exchange: returns (xorbs transferred, shard files transferred).
    async fn transfer(&self, manifest: &Manifest) -> (usize, usize) {
        let missing = self.receiver.missing_content(manifest).await.unwrap();
        let mut xorbs = missing.xorbs.clone();
        let mut pending = None;
        if !missing.reconstructions.is_empty() {
            let shard = self
                .sender
                .export_shard(&missing.reconstructions)
                .await
                .unwrap();
            let p = PendingShard::parse(&shard, &missing.reconstructions).unwrap();
            xorbs.extend(
                self.receiver
                    .absent_xorbs(&p.referenced_xorbs())
                    .await
                    .unwrap(),
            );
            pending = Some(p);
        }
        for hash in &xorbs {
            let bytes = self.sender.read_xorb(hash).await.unwrap();
            self.receiver.commit_xorb(*hash, bytes).await.unwrap();
        }
        let files = pending.as_ref().map_or(0, PendingShard::num_files);
        if let Some(p) = pending {
            self.receiver.commit_shard(p).await.unwrap();
        }
        assert!(
            self.receiver
                .missing_content(manifest)
                .await
                .unwrap()
                .is_empty()
        );
        (xorbs.len(), files)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_transfers_between_independent_cas() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(1, 3 * MIB));
    write(&p.src(), "dir/b.bin", &random_bytes(2, MIB + 5));
    write(&p.src(), "dir/one", b"1");
    write(&p.src(), "empty", b"");
    let (manifest, _) = p.sender.ingest(p.src()).await.unwrap();

    let (xorbs, files) = p.transfer(&manifest).await;
    assert!(xorbs >= 1);
    assert_eq!(files, 3, "empty file needs no reconstruction");

    let dest = p.tmp.path().join("dest");
    p.receiver.reconstruct(&manifest, &dest).await.unwrap();
    assert_trees_equal(&p.src(), &dest);
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_xorb_file_transfers() {
    let p = peers().await;
    write(&p.src(), "big.bin", &random_bytes(3, 70 * MIB));
    let (manifest, _) = p.sender.ingest(p.src()).await.unwrap();
    let (xorbs, _) = p.transfer(&manifest).await;
    assert!(xorbs >= 2, "expected several xorbs, got {xorbs}");
    let dest = p.tmp.path().join("dest");
    p.receiver.reconstruct(&manifest, &dest).await.unwrap();
    assert_trees_equal(&p.src(), &dest);
}

#[tokio::test(flavor = "multi_thread")]
async fn second_transfer_of_unchanged_tree_moves_nothing() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(4, 2 * MIB));
    let (manifest, _) = p.sender.ingest(p.src()).await.unwrap();
    p.transfer(&manifest).await;
    assert_eq!(p.transfer(&manifest).await, (0, 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn appended_file_reuses_existing_xorbs() {
    let p = peers().await;
    let base = random_bytes(5, 4 * MIB);
    write(&p.src(), "a.bin", &base);
    let (m1, _) = p.sender.ingest(p.src()).await.unwrap();
    let (first, _) = p.transfer(&m1).await;

    let mut grown = base.clone();
    grown.extend(random_bytes(6, MIB));
    write(&p.src(), "a.bin", &grown);
    let (m2, _) = p.sender.ingest(p.src()).await.unwrap();
    let total = p.sender.referenced_xorbs(&hashes(&m2)).await.unwrap().len();
    let (second, files) = p.transfer(&m2).await;
    assert_eq!(files, 1);
    assert!(
        second < total,
        "should reuse the {first} xorb(s) already sent; sent {second} of {total}"
    );

    let dest = p.tmp.path().join("dest");
    p.receiver.reconstruct(&m2, &dest).await.unwrap();
    assert_eq!(fs::read(dest.join("a.bin")).unwrap(), grown);
}

fn hashes(m: &Manifest) -> BTreeSet<MerkleHash> {
    m.files()
        .filter(|(_, size, ..)| *size > 0)
        .map(|(_, _, h, ..)| MerkleHash::from_hex(h).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupted_xorb_rejected() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(7, MIB));
    let (manifest, _) = p.sender.ingest(p.src()).await.unwrap();
    let xorb = *p
        .sender
        .referenced_xorbs(&hashes(&manifest))
        .await
        .unwrap()
        .first()
        .unwrap();
    let mut bytes = p.sender.read_xorb(&xorb).await.unwrap();
    bytes[100] ^= 0xff;
    assert!(matches!(
        p.receiver.commit_xorb(xorb, bytes).await,
        Err(Error::Verification { .. })
    ));
    assert!(!p.receiver.absent_xorbs([&xorb]).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn xorb_under_wrong_hash_rejected() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(8, MIB));
    let (manifest, _) = p.sender.ingest(p.src()).await.unwrap();
    let xorb = *p
        .sender
        .referenced_xorbs(&hashes(&manifest))
        .await
        .unwrap()
        .first()
        .unwrap();
    let bytes = p.sender.read_xorb(&xorb).await.unwrap();
    assert!(
        p.receiver
            .commit_xorb(MerkleHash::default(), bytes)
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn truncated_and_garbage_xorbs_rejected() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(9, MIB));
    let (manifest, _) = p.sender.ingest(p.src()).await.unwrap();
    let xorb = *p
        .sender
        .referenced_xorbs(&hashes(&manifest))
        .await
        .unwrap()
        .first()
        .unwrap();
    let bytes = p.sender.read_xorb(&xorb).await.unwrap();
    for bad in [
        bytes[..bytes.len() / 2].to_vec(),
        Vec::new(),
        vec![0u8; 3],
        random_bytes(10, 4096),
    ] {
        assert!(p.receiver.commit_xorb(xorb, bad).await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn shard_with_unrequested_files_rejected() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(11, MIB));
    write(&p.src(), "b.bin", &random_bytes(12, MIB));
    let (manifest, _) = p.sender.ingest(p.src()).await.unwrap();
    let all = hashes(&manifest);
    let shard = p.sender.export_shard(&all).await.unwrap();
    let only_one: BTreeSet<_> = all.iter().take(1).copied().collect();
    assert!(PendingShard::parse(&shard, &only_one).is_err());
    assert!(PendingShard::parse(b"not a shard", &all).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn shard_lying_about_file_hash_rejected() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(13, MIB));
    write(&p.src(), "b.bin", &random_bytes(14, MIB));
    let (manifest, _) = p.sender.ingest(p.src()).await.unwrap();
    let all: Vec<_> = hashes(&manifest).into_iter().collect();
    let (a, b) = (all[0], all[1]);

    // A forged shard: file `b`'s hash pointing at file `a`'s chunks.
    let real = p.sender.export_shard(&[a].into()).await.unwrap();
    let mut shard = MDBInMemoryShard::from_reader(&mut std::io::Cursor::new(&real)).unwrap();
    let mut info = shard.file_content.remove(&a).unwrap();
    info.metadata.file_hash = b;
    let mut forged = MDBInMemoryShard::default();
    forged.add_file_reconstruction_info(info).unwrap();
    let forged = forged.to_bytes().unwrap();

    let expected: BTreeSet<_> = [b].into();
    let pending = PendingShard::parse(&forged, &expected).unwrap();
    for x in p
        .receiver
        .absent_xorbs(&pending.referenced_xorbs())
        .await
        .unwrap()
    {
        p.receiver
            .commit_xorb(x, p.sender.read_xorb(&x).await.unwrap())
            .await
            .unwrap();
    }
    assert!(matches!(
        p.receiver.commit_shard(pending).await,
        Err(Error::Verification { .. })
    ));
    // Not registered: the receiver still lacks a reconstruction for `b`.
    let missing = p.receiver.missing_content(&manifest).await.unwrap();
    assert!(missing.reconstructions.contains(&b));
}

#[tokio::test(flavor = "multi_thread")]
async fn shard_before_its_xorbs_rejected() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(15, MIB));
    let (manifest, _) = p.sender.ingest(p.src()).await.unwrap();
    let all = hashes(&manifest);
    let shard = p.sender.export_shard(&all).await.unwrap();
    let pending = PendingShard::parse(&shard, &all).unwrap();
    assert!(matches!(
        p.receiver.commit_shard(pending).await,
        Err(Error::Verification { .. })
    ));
}
