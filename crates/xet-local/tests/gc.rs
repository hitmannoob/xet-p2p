//! M4: garbage collection and CAS locking.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::Duration;

use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;
use xet_local::chunks::{XorbPacker, parse_chunk_run};
use xet_local::gc::GcOptions;
use xet_local::{Error, LocalCas, Manifest, MerkleHash};

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

fn hashes(m: &Manifest) -> BTreeSet<MerkleHash> {
    m.files()
        .filter(|(_, size, ..)| *size > 0)
        .map(|(_, _, h, ..)| MerkleHash::from_hex(h).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn exclusive_and_shared_opens_exclude_each_other() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("cas");
    let shared = LocalCas::open(&dir).await.unwrap();
    let _also_shared = LocalCas::open(&dir).await.unwrap();
    assert!(matches!(
        LocalCas::open_exclusive(&dir).await,
        Err(Error::Busy(_))
    ));
    assert!(matches!(
        shared.gc(&GcOptions::default()).await,
        Err(Error::Busy(_))
    ));
    drop(shared);
    drop(_also_shared);

    let exclusive = LocalCas::open_exclusive(&dir).await.unwrap();
    assert!(matches!(LocalCas::open(&dir).await, Err(Error::Busy(_))));
    drop(exclusive);
    LocalCas::open(&dir).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consistent_cas_loses_nothing() {
    let tmp = TempDir::new().unwrap();
    let src = tmp.path().join("src");
    write(&src, "a.bin", &random_bytes(1, 3 * MIB));
    write(&src, "b.bin", &random_bytes(2, MIB));
    let dir = tmp.path().join("cas");
    let (m, _) = LocalCas::open(&dir)
        .await
        .unwrap()
        .ingest(&src)
        .await
        .unwrap();

    let cas = LocalCas::open_exclusive(&dir).await.unwrap();
    let r = cas
        .gc(&GcOptions {
            orphan_grace: Duration::ZERO,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        r.orphan_xorbs_deleted + r.xorbs_evicted + r.files_invalidated,
        0,
        "{r:?}"
    );
    assert_eq!(r.bytes_after, r.bytes_before);
    cas.reconstruct(&m, tmp.path().join("out")).await.unwrap();
}

/// A packed xorb whose files were never registered (an abandoned chunk-mode transfer).
async fn make_orphan(tmp: &Path, cas: &LocalCas) -> MerkleHash {
    let src = tmp.join("orphan-src");
    write(&src, "o.bin", &random_bytes(9, 2 * MIB));
    let other = LocalCas::open(tmp.join("other")).await.unwrap();
    let (m, _) = other.ingest(&src).await.unwrap();
    let file = *hashes(&m).first().unwrap();
    let lists = other.chunk_lists(&[file].into()).await.unwrap();
    let n = lists[0].1.len() as u32;
    let mut packer = XorbPacker::default();
    for run in other.read_chunk_ranges(&file, &[(0, n)]).await.unwrap() {
        for c in parse_chunk_run(&run.bytes).unwrap() {
            packer.push(&run.bytes[c.span.clone()], c.size);
        }
    }
    cas.commit_packed(packer.take()).await.unwrap().0
}

#[tokio::test(flavor = "multi_thread")]
async fn orphans_deleted_after_grace_kept_within_it() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("cas");
    let orphan = {
        let cas = LocalCas::open(&dir).await.unwrap();
        make_orphan(tmp.path(), &cas).await
    };

    let cas = LocalCas::open_exclusive(&dir).await.unwrap();
    let kept = cas.gc(&GcOptions::default()).await.unwrap();
    assert_eq!(
        (kept.orphan_xorbs_deleted, kept.orphans_kept_in_grace),
        (0, 1)
    );

    let dry = cas
        .gc(&GcOptions {
            orphan_grace: Duration::ZERO,
            dry_run: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(dry.orphan_xorbs_deleted, 1);
    assert!(
        cas.absent_xorbs([&orphan]).await.unwrap().is_empty(),
        "dry run deleted"
    );

    let r = cas
        .gc(&GcOptions {
            orphan_grace: Duration::ZERO,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(r.orphan_xorbs_deleted, 1);
    assert!(r.orphan_bytes_deleted > MIB as u64);
    assert!(
        !cas.absent_xorbs([&orphan]).await.unwrap().is_empty(),
        "orphan still present"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn size_cap_evicts_least_recently_used_and_invalidates_their_files() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("cas");
    let manifests = {
        let cas = LocalCas::open(&dir).await.unwrap();
        let mut ms = Vec::new();
        for (i, name) in ["old", "mid", "new"].iter().enumerate() {
            let src = tmp.path().join(name);
            write(&src, "f.bin", &random_bytes(20 + i as u64, 2 * MIB));
            ms.push(cas.ingest(&src).await.unwrap().0);
        }
        // Establish recency: old < mid < new.
        for m in &ms {
            let xorbs = cas.referenced_xorbs(&hashes(m)).await.unwrap();
            cas.touch_xorbs(&xorbs);
            std::thread::sleep(Duration::from_millis(30));
        }
        ms
    };

    let cas = LocalCas::open_exclusive(&dir).await.unwrap();
    let r = cas
        .gc(&GcOptions {
            max_bytes: Some(3 * MIB as u64),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!((r.xorbs_evicted, r.files_invalidated), (2, 2), "{r:?}");
    assert!(r.bytes_after <= 3 * MIB as u64);

    // Evicted files are now simply missing (a peer would re-send them); the newest still works.
    for m in &manifests[..2] {
        let missing = cas.missing_content(m).await.unwrap();
        assert_eq!(missing.reconstructions.len(), 1, "{missing:?}");
        assert!(cas.reconstruct(m, tmp.path().join("x")).await.is_err());
    }
    assert!(cas.missing_content(&manifests[2]).await.unwrap().is_empty());
    cas.reconstruct(&manifests[2], tmp.path().join("ok"))
        .await
        .unwrap();

    // The chunk index no longer claims the evicted chunks.
    let old = hashes(&manifests[0]);
    let other = LocalCas::open(tmp.path().join("other")).await.unwrap();
    let src = tmp.path().join("old");
    other.ingest(&src).await.unwrap();
    let lists = other.chunk_lists(&old).await.unwrap();
    let plan = cas.plan_chunks(&lists).unwrap();
    assert_eq!(plan.present_chunks, 0);
}
