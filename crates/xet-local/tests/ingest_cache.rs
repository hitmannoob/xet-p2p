//! The ingest cache: unchanged source files are not read again; any write is noticed.

use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime};

use tempfile::TempDir;
use xet_local::LocalCas;

fn write(root: &Path, rel: &str, data: &[u8]) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, data).unwrap();
}

/// Files are only cached once settled (unchanged for a couple of seconds before being hashed).
fn settle() {
    std::thread::sleep(Duration::from_millis(3100));
}

#[tokio::test(flavor = "multi_thread")]
async fn unchanged_files_reuse_hashes_and_edits_are_seen() {
    let tmp = TempDir::new().unwrap();
    let src = tmp.path().join("src");
    for i in 0..20 {
        write(
            &src,
            &format!("d/f{i}.txt"),
            format!("file {i}\n").repeat(200).as_bytes(),
        );
    }
    write(&src, "empty", b"");
    let cas = LocalCas::open(tmp.path().join("cas")).await.unwrap();

    // Just written: hashed, and not cached yet (too recent to trust its timestamps).
    let (first, r) = cas.ingest(&src).await.unwrap();
    assert_eq!(r.cached_files, 0);
    let (_, r) = cas.ingest(&src).await.unwrap();
    assert_eq!(r.cached_files, 0, "racy files must not be cached");

    settle();
    let (m, r) = cas.ingest(&src).await.unwrap();
    assert_eq!(r.cached_files, 0, "first settled ingest reads and records");
    assert_eq!(m, first);
    let (m, r) = cas.ingest(&src).await.unwrap();
    assert_eq!(r.cached_files, 20, "{r:?}");
    assert_eq!(r.dedup.total_chunks, 0, "nothing was read");
    assert_eq!(m, first, "cached hashes give the same manifest");

    // Same size, mtime restored: still seen, because the write moved ctime.
    let f = src.join("d/f3.txt");
    let mtime = fs::metadata(&f).unwrap().modified().unwrap();
    let mut edited = fs::read(&f).unwrap();
    edited[0] ^= 1;
    fs::write(&f, &edited).unwrap();
    fs::File::options()
        .write(true)
        .open(&f)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
    let (m, r) = cas.ingest(&src).await.unwrap();
    assert_eq!(r.cached_files, 19);
    assert_ne!(m, first, "the edit must change the manifest");
    let (_, _, sha, ..) = m.files().find(|(e, ..)| e.path == "d/f3.txt").unwrap();
    assert_ne!(
        sha,
        first
            .files()
            .find(|(e, ..)| e.path == "d/f3.txt")
            .unwrap()
            .2
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_hit_needs_the_content_in_this_cas() {
    let tmp = TempDir::new().unwrap();
    let src = tmp.path().join("src");
    write(&src, "a.txt", &b"abc\n".repeat(1000));
    let old = SystemTime::now() - Duration::from_secs(60);
    fs::File::options()
        .write(true)
        .open(src.join("a.txt"))
        .unwrap()
        .set_modified(old)
        .unwrap();
    settle();
    {
        let cas = LocalCas::open(tmp.path().join("cas")).await.unwrap();
        cas.ingest(&src).await.unwrap();
        let (_, r) = cas.ingest(&src).await.unwrap();
        assert_eq!(r.cached_files, 1);
    }
    // A fresh CAS has no record and no content: the file is read.
    let other = LocalCas::open(tmp.path().join("cas2")).await.unwrap();
    let (_, r) = other.ingest(&src).await.unwrap();
    assert_eq!(r.cached_files, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn receiver_trusts_settled_files_and_repairs_edited_ones() {
    let tmp = TempDir::new().unwrap();
    let src = tmp.path().join("src");
    let dest = tmp.path().join("dest");
    for i in 0..10 {
        write(
            &src,
            &format!("f{i}.txt"),
            format!("v1 {i}\n").repeat(300).as_bytes(),
        );
    }
    let cas = LocalCas::open(tmp.path().join("cas")).await.unwrap();
    let (m, _) = cas.ingest(&src).await.unwrap();
    let r = cas.reconstruct(&m, &dest).await.unwrap();
    assert_eq!(r.files, 10);
    settle();
    // First check after settling reads and records; the next trusts the stamps.
    let r = cas.reconstruct(&m, &dest).await.unwrap();
    assert_eq!((r.files_unchanged, r.files_trusted), (10, 0));
    let r = cas.reconstruct(&m, &dest).await.unwrap();
    assert_eq!((r.files_unchanged, r.files_trusted), (10, 10));

    // Edited in place at the destination (same size, mtime restored): found and repaired.
    let f = dest.join("f4.txt");
    let mtime = fs::metadata(&f).unwrap().modified().unwrap();
    let mut bad = fs::read(&f).unwrap();
    bad[1] ^= 1;
    fs::write(&f, &bad).unwrap();
    fs::File::options()
        .write(true)
        .open(&f)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
    let r = cas.reconstruct(&m, &dest).await.unwrap();
    assert_eq!((r.files, r.files_trusted), (1, 9));
    assert_eq!(fs::read(&f).unwrap(), fs::read(src.join("f4.txt")).unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn reconstruction_stages_early_and_installs_only_at_finish() {
    let tmp = TempDir::new().unwrap();
    let src = tmp.path().join("src");
    for i in 0..6 {
        write(
            &src,
            &format!("f{i}.txt"),
            format!("file {i}\n").repeat(500).as_bytes(),
        );
    }
    let cas = std::sync::Arc::new(LocalCas::open(tmp.path().join("cas")).await.unwrap());
    let (m, _) = cas.ingest(&src).await.unwrap();
    let dest = tmp.path().join("dest");
    let mut r = cas
        .begin_reconstruct(&m, &dest, xet_local::ReconstructOptions::default())
        .unwrap();
    // Start half the files early; the rest start at finish.
    for (_, _, h, ..) in m.files().take(3) {
        r.content_ready(&xet_local::MerkleHash::from_hex(h).unwrap());
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !dest.join("f0.txt").exists(),
        "nothing is installed before finish"
    );
    let report = r.finish().await.unwrap();
    assert_eq!(report.files, 6);
    for i in 0..6 {
        assert_eq!(
            fs::read(dest.join(format!("f{i}.txt"))).unwrap(),
            fs::read(src.join(format!("f{i}.txt"))).unwrap()
        );
    }
    let leftovers: Vec<_> = fs::read_dir(&dest)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with('.'))
        .collect();
    assert!(leftovers.is_empty(), "staging removed: {leftovers:?}");
}
