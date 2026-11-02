//! M0: offline round trip through native Xet storage in a local CAS.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;
use xet_local::{EntryKind, Error, LocalCas, Manifest};

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

/// Every file and directory under `root`, mapped to its contents (`None` for directories).
fn snapshot(root: &Path) -> BTreeMap<String, Option<Vec<u8>>> {
    fn go(root: &Path, dir: &Path, out: &mut BTreeMap<String, Option<Vec<u8>>>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_str()
                .unwrap()
                .replace('\\', "/");
            if path.is_dir() {
                out.insert(rel, None);
                go(root, &path, out);
            } else {
                out.insert(rel, Some(fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = BTreeMap::new();
    if root.exists() {
        go(root, root, &mut out);
    }
    out
}

struct Fixture {
    _tmp: TempDir,
    src: std::path::PathBuf,
    dest: std::path::PathBuf,
    cas: LocalCas,
}

async fn fixture() -> Fixture {
    let tmp = TempDir::new().unwrap();
    let src = tmp.path().join("src");
    let dest = tmp.path().join("dest");
    fs::create_dir(&src).unwrap();
    let cas = LocalCas::open(tmp.path().join("cas")).await.unwrap();
    Fixture {
        _tmp: tmp,
        src,
        dest,
        cas,
    }
}

async fn round_trip(f: &Fixture) -> Manifest {
    let (manifest, _) = f.cas.ingest(&f.src).await.unwrap();
    f.cas.reconstruct(&manifest, &f.dest).await.unwrap();
    assert_eq!(snapshot(&f.src), snapshot(&f.dest));
    manifest
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_file() {
    let f = fixture().await;
    write(&f.src, "empty", b"");
    let manifest = round_trip(&f).await;
    let EntryKind::File { sha256, .. } = &manifest.entries[0].kind else {
        panic!()
    };
    assert_eq!(
        sha256,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_content_is_empty_after_ingest() {
    let f = fixture().await;
    write(&f.src, "a.bin", &random_bytes(13, 2 * MIB));
    let (manifest, _) = f.cas.ingest(&f.src).await.unwrap();
    assert!(f.cas.missing_content(&manifest).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn one_byte_file() {
    let f = fixture().await;
    write(&f.src, "one", b"x");
    round_trip(&f).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn random_binary() {
    let f = fixture().await;
    write(&f.src, "random.bin", &random_bytes(1, 3 * MIB + 17));
    round_trip(&f).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_xorb_file_over_64_mib() {
    let f = fixture().await;
    // Incompressible and non-repeating, so it cannot fit in one 64 MiB xorb.
    write(&f.src, "big.bin", &random_bytes(2, 80 * MIB + 3));
    let (manifest, report) = f.cas.ingest(&f.src).await.unwrap();
    assert!(report.dedup.xorb_bytes_uploaded > 64 * MIB as u64);
    f.cas.reconstruct(&manifest, &f.dest).await.unwrap();
    assert_eq!(snapshot(&f.src), snapshot(&f.dest));
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_content_dedups_within_file() {
    let f = fixture().await;
    let block = random_bytes(3, MIB);
    write(&f.src, "repeated.bin", &block.repeat(8));
    let (manifest, report) = f.cas.ingest(&f.src).await.unwrap();
    assert_eq!(report.dedup.total_bytes, 8 * MIB as u64);
    assert!(
        report.dedup.new_bytes < 2 * MIB as u64,
        "repeated blocks should dedup: {:?}",
        report.dedup
    );
    f.cas.reconstruct(&manifest, &f.dest).await.unwrap();
    assert_eq!(snapshot(&f.src), snapshot(&f.dest));
}

#[tokio::test(flavor = "multi_thread")]
async fn two_files_sharing_chunks() {
    let f = fixture().await;
    let shared = random_bytes(4, 4 * MIB);
    let mut a = shared.clone();
    a.extend(random_bytes(5, MIB));
    let mut b = random_bytes(6, MIB);
    b.extend(&shared);
    write(&f.src, "a.bin", &a);
    // Files in one ingest are chunked in parallel and do not dedup against each other, so
    // store `a` first; `b` must then reuse the shared chunks.
    f.cas.ingest(f.src.join("a.bin")).await.unwrap();
    write(&f.src, "b.bin", &b);
    let (manifest, report) = f.cas.ingest(&f.src).await.unwrap();
    assert!(
        report.dedup.deduped_bytes > 8 * MIB as u64,
        "{:?}",
        report.dedup
    );
    assert!(
        report.dedup.new_bytes < 2 * MIB as u64,
        "{:?}",
        report.dedup
    );
    f.cas.reconstruct(&manifest, &f.dest).await.unwrap();
    assert_eq!(snapshot(&f.src), snapshot(&f.dest));
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_with_many_small_files_unicode_and_empty_dirs() {
    let f = fixture().await;
    for i in 0..200 {
        write(
            &f.src,
            &format!("small/{:03}/f{i}.txt", i % 17),
            format!("file {i}\n").as_bytes(),
        );
    }
    write(
        &f.src,
        "ünïcödé/文件 with spaces.bin",
        &random_bytes(7, 4096),
    );
    fs::create_dir_all(f.src.join("empty/nested")).unwrap();
    round_trip(&f).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn single_file_source() {
    let f = fixture().await;
    write(&f.src, "only.bin", &random_bytes(8, 1000));
    let (manifest, _) = f.cas.ingest(f.src.join("only.bin")).await.unwrap();
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].path, "only.bin");
    f.cas.reconstruct(&manifest, &f.dest).await.unwrap();
    assert_eq!(snapshot(&f.src), snapshot(&f.dest));
}

#[tokio::test(flavor = "multi_thread")]
async fn reingest_unchanged_stores_no_new_bytes() {
    let f = fixture().await;
    write(&f.src, "a.bin", &random_bytes(9, 2 * MIB));
    write(&f.src, "b/c.bin", &random_bytes(10, MIB));
    let (first, _) = f.cas.ingest(&f.src).await.unwrap();
    let (second, report) = f.cas.ingest(&f.src).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(report.dedup.new_bytes, 0, "{:?}", report.dedup);
    assert_eq!(report.dedup.xorb_bytes_uploaded, 0, "{:?}", report.dedup);
}

#[tokio::test(flavor = "multi_thread")]
async fn cas_persists_across_reopen() {
    let tmp = TempDir::new().unwrap();
    let src = tmp.path().join("src");
    write(&src, "data.bin", &random_bytes(11, MIB));
    let manifest = {
        let cas = LocalCas::open(tmp.path().join("cas")).await.unwrap();
        cas.ingest(&src).await.unwrap().0
    };
    let cas = LocalCas::open(tmp.path().join("cas")).await.unwrap();
    cas.reconstruct(&manifest, tmp.path().join("dest"))
        .await
        .unwrap();
    assert_eq!(snapshot(&src), snapshot(&tmp.path().join("dest")));
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn executable_bit_preserved() {
    use std::os::unix::fs::PermissionsExt;
    let f = fixture().await;
    write(&f.src, "run.sh", b"#!/bin/sh\necho hi\n");
    fs::set_permissions(f.src.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    round_trip(&f).await;
    let mode = fs::metadata(f.dest.join("run.sh"))
        .unwrap()
        .permissions()
        .mode();
    assert_ne!(mode & 0o111, 0);
}

// ---- Failure and safety ----

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn source_symlink_rejected() {
    let f = fixture().await;
    write(&f.src, "real", b"data");
    std::os::unix::fs::symlink("real", f.src.join("link")).unwrap();
    assert!(matches!(
        f.cas.ingest(&f.src).await,
        Err(Error::UnsupportedFileType(_))
    ));
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn symlink_in_destination_not_followed() {
    let f = fixture().await;
    write(&f.src, "sub/file.txt", b"payload");
    let (manifest, _) = f.cas.ingest(&f.src).await.unwrap();

    let outside = f._tmp.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::create_dir(&f.dest).unwrap();
    std::os::unix::fs::symlink(&outside, f.dest.join("sub")).unwrap();

    assert!(matches!(
        f.cas.reconstruct(&manifest, &f.dest).await,
        Err(Error::UnsafePath { .. })
    ));
    assert!(
        fs::read_dir(&outside).unwrap().next().is_none(),
        "wrote through symlink"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn traversal_in_manifest_rejected() {
    let f = fixture().await;
    write(&f.src, "a", b"data");
    let (mut manifest, _) = f.cas.ingest(&f.src).await.unwrap();
    manifest.entries[0].path = "../escape".into();
    assert!(matches!(
        f.cas.reconstruct(&manifest, &f.dest).await,
        Err(Error::UnsafePath { .. })
    ));
    assert!(!f._tmp.path().join("escape").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn sha256_mismatch_installs_nothing() {
    let f = fixture().await;
    write(&f.src, "a.txt", b"first file");
    write(&f.src, "b.txt", b"second file");
    let (mut manifest, _) = f.cas.ingest(&f.src).await.unwrap();
    // Corrupt the expected digest of the *second* file: the first must still not be installed.
    let EntryKind::File { sha256, .. } = &mut manifest.entries[1].kind else {
        panic!()
    };
    *sha256 = "0".repeat(64);

    assert!(matches!(
        f.cas.reconstruct(&manifest, &f.dest).await,
        Err(Error::Verification { .. })
    ));
    assert!(
        snapshot(&f.dest).is_empty(),
        "partial install: {:?}",
        snapshot(&f.dest).keys()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_content_fails_without_partial_files() {
    let f = fixture().await;
    write(&f.src, "a.bin", &random_bytes(12, MIB));
    let (manifest, _) = f.cas.ingest(&f.src).await.unwrap();

    // A fresh CAS has none of the referenced xorbs or shards.
    let empty = LocalCas::open(f._tmp.path().join("empty-cas"))
        .await
        .unwrap();
    let missing = empty.missing_content(&manifest).await.unwrap();
    assert_eq!(missing.reconstructions.len(), 1);
    assert!(matches!(
        empty.reconstruct(&manifest, &f.dest).await,
        Err(Error::MissingContent { files: 1, .. })
    ));
    assert!(snapshot(&f.dest).is_empty());
}

// ---- M2: incremental install ----

#[tokio::test(flavor = "multi_thread")]
async fn unchanged_files_are_not_rewritten() {
    let f = fixture().await;
    write(&f.src, "keep.bin", &random_bytes(20, MIB));
    write(&f.src, "edit.txt", b"version 1");
    let (m1, _) = f.cas.ingest(&f.src).await.unwrap();
    let r1 = f.cas.reconstruct(&m1, &f.dest).await.unwrap();
    assert_eq!((r1.files, r1.files_unchanged), (2, 0));

    // Mark the installed copy so a rewrite would be detectable (a new inode from rename).
    #[cfg(unix)]
    let inode = {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(f.dest.join("keep.bin")).unwrap().ino()
    };

    write(&f.src, "edit.txt", b"version 2");
    let (m2, _) = f.cas.ingest(&f.src).await.unwrap();
    let r2 = f.cas.reconstruct(&m2, &f.dest).await.unwrap();
    assert_eq!((r2.files, r2.files_unchanged), (1, 1));
    assert_eq!(r2.bytes, 9);
    assert_eq!(snapshot(&f.src), snapshot(&f.dest));
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(fs::metadata(f.dest.join("keep.bin")).unwrap().ino(), inode);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn locally_modified_destination_file_is_restored() {
    let f = fixture().await;
    write(&f.src, "a.txt", b"original");
    let (m, _) = f.cas.ingest(&f.src).await.unwrap();
    f.cas.reconstruct(&m, &f.dest).await.unwrap();
    fs::write(f.dest.join("a.txt"), b"tampered").unwrap();
    let r = f.cas.reconstruct(&m, &f.dest).await.unwrap();
    assert_eq!((r.files, r.files_unchanged), (1, 0));
    assert_eq!(fs::read(f.dest.join("a.txt")).unwrap(), b"original");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn executable_bit_change_alone_is_applied() {
    use std::os::unix::fs::PermissionsExt;
    let f = fixture().await;
    write(&f.src, "run.sh", b"#!/bin/sh\n");
    let (m1, _) = f.cas.ingest(&f.src).await.unwrap();
    f.cas.reconstruct(&m1, &f.dest).await.unwrap();
    fs::set_permissions(f.src.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    let (m2, _) = f.cas.ingest(&f.src).await.unwrap();
    let r = f.cas.reconstruct(&m2, &f.dest).await.unwrap();
    assert_eq!(r.files_unchanged, 1);
    let mode = fs::metadata(f.dest.join("run.sh"))
        .unwrap()
        .permissions()
        .mode();
    assert_ne!(mode & 0o111, 0);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn identical_file_behind_symlink_is_not_trusted() {
    let f = fixture().await;
    write(&f.src, "sub/a.txt", b"same");
    let (m, _) = f.cas.ingest(&f.src).await.unwrap();
    // An identical file reachable only through a symlinked directory must not count as present.
    let outside = f._tmp.path().join("outside");
    write(&outside, "a.txt", b"same");
    fs::create_dir(&f.dest).unwrap();
    std::os::unix::fs::symlink(&outside, f.dest.join("sub")).unwrap();
    assert!(matches!(
        f.cas.reconstruct(&m, &f.dest).await,
        Err(Error::UnsafePath { .. })
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_staging_directory_is_cleaned_up() {
    let f = fixture().await;
    write(&f.src, "a.txt", b"x");
    let (m, _) = f.cas.ingest(&f.src).await.unwrap();
    // As if a previous reconstruction was killed mid-way.
    write(&f.dest, ".xet-p2p-staging-123-456/0", b"partial");
    f.cas.reconstruct(&m, &f.dest).await.unwrap();
    assert_eq!(snapshot(&f.src), snapshot(&f.dest));
}

#[tokio::test(flavor = "multi_thread")]
async fn file_modified_during_ingest_is_rejected() {
    let f = fixture().await;
    let path = f.src.join("busy.bin");
    write(&f.src, "busy.bin", &random_bytes(40, 48 * MIB));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = std::thread::spawn({
        let (path, stop) = (path.clone(), stop.clone());
        move || {
            use std::io::{Seek, Write};
            let mut i = 0u8;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();
                file.seek(std::io::SeekFrom::Start(1024)).unwrap();
                file.write_all(&[i; 4096]).unwrap();
                i = i.wrapping_add(1);
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        }
    });
    let result = f.cas.ingest(&f.src).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
    assert!(
        matches!(result, Err(Error::Verification { ref reason, .. }) if reason.contains("changed during ingest")),
        "{result:?}"
    );
}

// ---- opt-in delete ----

use xet_local::ReconstructOptions;

const DELETE: ReconstructOptions = ReconstructOptions {
    delete_extraneous: true,
    link_identical: false,
};

#[tokio::test(flavor = "multi_thread")]
async fn without_delete_extra_files_are_kept() {
    let f = fixture().await;
    write(&f.src, "a.txt", b"a");
    let (m, _) = f.cas.ingest(&f.src).await.unwrap();
    write(&f.dest, "extra.txt", b"keep me");
    f.cas.reconstruct(&m, &f.dest).await.unwrap();
    assert!(f.dest.join("extra.txt").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_mirrors_the_manifest() {
    let f = fixture().await;
    write(&f.src, "keep/a.txt", b"a");
    write(&f.src, "b.txt", b"b");
    fs::create_dir_all(f.src.join("empty-kept")).unwrap();
    let (m, _) = f.cas.ingest(&f.src).await.unwrap();

    write(&f.dest, "keep/stale.txt", b"old");
    write(&f.dest, "gone/deep/x.txt", b"old");
    write(&f.dest, "b.txt", b"old version");
    // Type conflicts: a file where a directory belongs.
    write(&f.dest, "empty-kept", b"i am a file");

    let r = f.cas.reconstruct_with(&m, &f.dest, DELETE).await.unwrap();
    assert_eq!(snapshot(&f.src), snapshot(&f.dest));
    assert_eq!(r.files_deleted, 3, "{r:?}"); // keep/stale.txt, gone/deep/x.txt, empty-kept file
    assert_eq!(r.dirs_deleted, 2, "{r:?}"); // gone/deep, gone
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn delete_removes_symlinks_without_following() {
    let f = fixture().await;
    write(&f.src, "a.txt", b"a");
    let (m, _) = f.cas.ingest(&f.src).await.unwrap();
    let outside = f._tmp.path().join("outside");
    write(&outside, "precious.txt", b"must survive");
    fs::create_dir_all(&f.dest).unwrap();
    std::os::unix::fs::symlink(&outside, f.dest.join("link-dir")).unwrap();
    std::os::unix::fs::symlink(outside.join("precious.txt"), f.dest.join("link-file")).unwrap();

    f.cas.reconstruct_with(&m, &f.dest, DELETE).await.unwrap();
    assert!(fs::symlink_metadata(f.dest.join("link-dir")).is_err());
    assert!(fs::symlink_metadata(f.dest.join("link-file")).is_err());
    assert_eq!(
        fs::read(outside.join("precious.txt")).unwrap(),
        b"must survive"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_replaces_a_directory_where_a_file_belongs() {
    let f = fixture().await;
    write(&f.src, "thing", b"now a file");
    let (m, _) = f.cas.ingest(&f.src).await.unwrap();
    write(&f.dest, "thing/inner.txt", b"was a dir");
    let r = f.cas.reconstruct_with(&m, &f.dest, DELETE).await.unwrap();
    assert_eq!(snapshot(&f.src), snapshot(&f.dest));
    assert_eq!((r.files_deleted, r.dirs_deleted), (1, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_reconstruction_deletes_nothing() {
    let f = fixture().await;
    write(&f.src, "a.txt", b"a");
    let (mut m, _) = f.cas.ingest(&f.src).await.unwrap();
    let EntryKind::File { sha256, .. } = &mut m.entries[0].kind else {
        panic!()
    };
    *sha256 = "0".repeat(64);
    write(&f.dest, "extra.txt", b"keep me");
    assert!(f.cas.reconstruct_with(&m, &f.dest, DELETE).await.is_err());
    assert!(
        f.dest.join("extra.txt").exists(),
        "a failed run must not delete"
    );
}
