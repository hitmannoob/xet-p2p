//! M3, in process: chunk-level exchange with receiver-side repacking into native xorbs.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;
use xet_local::chunks::{XorbPacker, decode_chunk_lists, encode_chunk_lists, parse_chunk_run};
use xet_local::{EntryKind, LocalCas, Manifest, MerkleHash};

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

fn files_of(m: &Manifest) -> Vec<(MerkleHash, String)> {
    m.files()
        .filter(|(_, size, ..)| *size > 0)
        .map(|(_, _, h, sha, _)| (MerkleHash::from_hex(h).unwrap(), sha.to_owned()))
        .collect()
}

#[derive(Debug, Default)]
struct Stats {
    chunk_list_bytes: usize,
    chunk_bytes: usize,
    chunks_received: usize,
    present_chunks: usize,
    xorbs_packed: usize,
}

/// The receiver-driven chunk-level exchange, in process.
async fn chunk_transfer(sender: &LocalCas, receiver: &LocalCas, manifest: &Manifest) -> Stats {
    let missing = receiver.missing_content(manifest).await.unwrap();
    let mut wanted: BTreeSet<MerkleHash> = missing.reconstructions.clone();
    wanted.extend(&missing.files_missing_xorbs);
    let mut stats = Stats::default();
    if wanted.is_empty() {
        return stats;
    }

    let encoded = encode_chunk_lists(&sender.chunk_lists(&wanted).await.unwrap());
    stats.chunk_list_bytes = encoded.len();
    let lists = decode_chunk_lists(&encoded, &wanted).unwrap();

    let plan = receiver.plan_chunks(&lists).unwrap();
    stats.present_chunks = plan.present_chunks;
    let mut outstanding = plan.missing.clone();
    let mut packer = XorbPacker::default();
    for (file, ranges) in &plan.requests {
        for run in sender.read_chunk_ranges(file, ranges).await.unwrap() {
            stats.chunk_bytes += run.bytes.len();
            for c in parse_chunk_run(&run.bytes).unwrap() {
                assert!(outstanding.remove(&c.hash).is_some(), "unrequested chunk");
                packer.push(&run.bytes[c.span.clone()], c.size);
                stats.chunks_received += 1;
                if packer.is_full() {
                    receiver.commit_packed(packer.take()).await.unwrap();
                    stats.xorbs_packed += 1;
                }
            }
        }
    }
    assert!(
        outstanding.is_empty(),
        "{} chunks never arrived",
        outstanding.len()
    );
    if !packer.is_empty() {
        receiver.commit_packed(packer.take()).await.unwrap();
        stats.xorbs_packed += 1;
    }

    let sha: std::collections::HashMap<_, _> = files_of(manifest).into_iter().collect();
    let files: Vec<_> = lists
        .into_iter()
        .map(|(f, l)| (f, l, sha[&f].clone()))
        .collect();
    receiver.register_files(&files).await.unwrap();
    assert!(receiver.missing_content(manifest).await.unwrap().is_empty());
    stats
}

struct Peers {
    tmp: TempDir,
    sender: LocalCas,
    receiver: LocalCas,
}

async fn peers() -> Peers {
    let tmp = TempDir::new().unwrap();
    fs::create_dir(tmp.path().join("src")).unwrap();
    let sender = LocalCas::open(tmp.path().join("s")).await.unwrap();
    let receiver = LocalCas::open(tmp.path().join("r")).await.unwrap();
    Peers {
        tmp,
        sender,
        receiver,
    }
}

impl Peers {
    fn src(&self) -> PathBuf {
        self.tmp.path().join("src")
    }
    fn dest(&self) -> PathBuf {
        self.tmp.path().join("dest")
    }
    async fn restore_and_check(&self, manifest: &Manifest, src: &Path) {
        let dest = self.dest();
        let _ = fs::remove_dir_all(&dest);
        self.receiver.reconstruct(manifest, &dest).await.unwrap();
        for (entry, ..) in manifest.files() {
            assert_eq!(
                fs::read(dest.join(&entry.path)).unwrap(),
                fs::read(src.join(&entry.path)).unwrap(),
                "{}",
                entry.path
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cold_transfer_round_trips() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(1, 5 * MIB));
    write(&p.src(), "b/c.txt", b"small file");
    write(&p.src(), "empty", b"");
    let (m, _) = p.sender.ingest(p.src()).await.unwrap();
    let s = chunk_transfer(&p.sender, &p.receiver, &m).await;
    assert!(s.chunks_received > 0 && s.present_chunks == 0);
    p.restore_and_check(&m, &p.src()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_file_packs_into_several_native_xorbs() {
    let p = peers().await;
    write(&p.src(), "big.bin", &random_bytes(2, 40 * MIB));
    let (m, _) = p.sender.ingest(p.src()).await.unwrap();
    let s = chunk_transfer(&p.sender, &p.receiver, &m).await;
    assert!(s.xorbs_packed >= 2, "{s:?}");
    p.restore_and_check(&m, &p.src()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_file_from_a_shared_xorb_sends_only_its_chunks() {
    let p = peers().await;
    // 40 files ingested together share one sender xorb.
    for i in 0..40 {
        write(
            &p.src(),
            &format!("f{i:02}.bin"),
            &random_bytes(100 + i, 512 * 1024),
        );
    }
    p.sender.ingest(p.src()).await.unwrap();
    let (m, _) = p.sender.ingest(p.src().join("f07.bin")).await.unwrap();
    let file: BTreeSet<_> = files_of(&m).into_iter().map(|(h, _)| h).collect();
    let xorb_bytes: usize = {
        let mut total = 0;
        for x in p.sender.referenced_xorbs(&file).await.unwrap() {
            total += p.sender.read_xorb(&x).await.unwrap().len();
        }
        total
    };
    let s = chunk_transfer(&p.sender, &p.receiver, &m).await;
    assert!(
        xorb_bytes > 10 * MIB,
        "setup: shared xorb should be large ({xorb_bytes})"
    );
    assert!(
        s.chunk_bytes < 600 * 1024,
        "{s:?} vs whole-xorb {xorb_bytes}"
    );
    p.restore_and_check(&m, &p.src()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn content_held_in_different_xorbs_is_not_resent() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(3, 8 * MIB));
    // The receiver got the same bytes some other way: its own xorbs, unrelated to the sender's.
    p.receiver.ingest(p.src()).await.unwrap();
    let (m, _) = p.sender.ingest(p.src()).await.unwrap();
    let s = chunk_transfer(&p.sender, &p.receiver, &m).await;
    // Same file hash, so the receiver already has its reconstruction: nothing to do at all.
    assert_eq!(s.chunk_bytes + s.chunk_list_bytes, 0, "{s:?}");

    // Now an edited version, from a fresh sender CAS that shares no xorbs with anyone.
    let mut v2 = random_bytes(3, 8 * MIB);
    v2.splice(0..0, random_bytes(4, 3000));
    write(&p.src(), "a.bin", &v2);
    let fresh = LocalCas::open(p.tmp.path().join("fresh-sender"))
        .await
        .unwrap();
    let (m2, _) = fresh.ingest(p.src()).await.unwrap();
    let s = chunk_transfer(&fresh, &p.receiver, &m2).await;
    assert!(s.present_chunks > 100, "{s:?}");
    assert!(
        s.chunk_bytes < 300 * 1024,
        "only the edited region should move: {s:?}"
    );
    p.restore_and_check(&m2, &p.src()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_chunks_are_requested_once() {
    let p = peers().await;
    let block = random_bytes(5, MIB);
    write(&p.src(), "rep.bin", &block.repeat(6));
    write(&p.src(), "rep2.bin", &block.repeat(3));
    let (m, _) = p.sender.ingest(p.src()).await.unwrap();
    let s = chunk_transfer(&p.sender, &p.receiver, &m).await;
    assert!(s.chunk_bytes < 2 * MIB, "{s:?}");
    p.restore_and_check(&m, &p.src()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn forged_chunk_list_rejected() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(6, MIB));
    write(&p.src(), "b.bin", &random_bytes(7, MIB));
    let (m, _) = p.sender.ingest(p.src()).await.unwrap();
    let files: Vec<_> = files_of(&m).into_iter().map(|(h, _)| h).collect();
    let mut lists = p
        .sender
        .chunk_lists(&files.iter().copied().collect())
        .await
        .unwrap();
    // Claim file 0 has file 1's chunks.
    lists[0].1 = lists[1].1.clone();
    let expected: BTreeSet<_> = files.into_iter().collect();
    assert!(decode_chunk_lists(&encode_chunk_lists(&lists), &expected).is_err());
    assert!(decode_chunk_lists(b"short", &expected).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn tampered_chunk_bytes_rejected() {
    let p = peers().await;
    write(&p.src(), "a.bin", &random_bytes(8, MIB));
    let (m, _) = p.sender.ingest(p.src()).await.unwrap();
    let files: BTreeSet<_> = files_of(&m).into_iter().map(|(h, _)| h).collect();
    let lists = p.sender.chunk_lists(&files).await.unwrap();
    let plan = p.receiver.plan_chunks(&lists).unwrap();
    let (file, ranges) = &plan.requests[0];
    let mut run = p
        .sender
        .read_chunk_ranges(file, ranges)
        .await
        .unwrap()
        .remove(0);
    let mid = run.bytes.len() / 2;
    run.bytes[mid] ^= 0x5a;
    // Either decompression fails or a chunk hashes to something never requested.
    match parse_chunk_run(&run.bytes) {
        Err(_) => {}
        Ok(chunks) => assert!(chunks.iter().any(|c| !plan.missing.contains_key(&c.hash))),
    }
    run.bytes.truncate(10);
    assert!(parse_chunk_run(&run.bytes).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn index_survives_reopen_and_covers_ingested_content() {
    let tmp = TempDir::new().unwrap();
    let src = tmp.path().join("src");
    write(&src, "a.bin", &random_bytes(9, 2 * MIB));
    let lists = {
        let cas = LocalCas::open(tmp.path().join("cas")).await.unwrap();
        let (m, _) = cas.ingest(&src).await.unwrap();
        let files = files_of(&m).into_iter().map(|(h, _)| h).collect();
        cas.chunk_lists(&files).await.unwrap()
    };
    let cas = LocalCas::open(tmp.path().join("cas")).await.unwrap();
    let plan = cas.plan_chunks(&lists).unwrap();
    assert!(plan.missing.is_empty(), "ingested chunks must be indexed");
    assert_eq!(plan.present_chunks, lists[0].1.len());
    let _ = EntryKind::Dir;
}
