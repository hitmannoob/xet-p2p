# Progress

Living log for `xet-p2p`. Spec: [XET_P2P_CODEX_START.md](XET_P2P_CODEX_START.md). Upstream findings: [docs/UPSTREAM_AUDIT.md](docs/UPSTREAM_AUDIT.md).

## Status

| Milestone | State |
|---|---|
| M0 — Upstream audit + offline Xet round trip | **Done** (exit gate met) |
| M1 — Direct QUIC push, full-xorb transport | **Done** (exit gate met) |
| M2 — Incremental transfer + durable resume | **Done** (exit gate met) |
| M3 — Chunk-level transfer optimization | Not started |
| M4 — Performance and reliability | Not started |
| M5 — Provider-agnostic sandbox integration | Not started |

## Pins

- `xet-runtime`, `xet-core-structures`, `xet-client`, `xet-data` = `1.8.0` (upstream `ce8c6b2`)
- Rust `1.95.0` (`rust-toolchain.toml`)

## Log

### 2026-10-31

- Repo initialised, pushed to `hitmannoob/xet-p2p` (`main`).
- Audited `xet-core`. Upstream `LocalClient` (disk + redb) is a usable embedded CAS; upload/download
  sessions accept it via `TranslatorConfig::local_config`. No format forks needed.
- Probe crate against all four crates at `=1.8.0` builds on Rust 1.95.0 (fails on 1.91.1: `sysinfo` MSRV).
- Pinned toolchain to 1.95.0.
- **M0 implemented.** Workspace with `crates/xet-local` and `crates/cli` (`xet-p2p`).
  - `LocalCas::open / ingest / missing_content / reconstruct`. Ingest walks a file or directory
    (regular files + dirs only; symlinks and special files rejected), runs upstream
    `FileUploadSession` against `local://`, and emits a versioned JSON manifest (paths, sizes, Xet
    hash, SHA-256, executable bit).
  - Reconstruct validates the manifest, checks every referenced shard/xorb is present, rebuilds all
    files into a staging dir inside the destination, verifies size + SHA-256, then installs each file
    by atomic rename. Never follows symlinks in the destination; a failure installs nothing.
  - CLI: `xet-p2p local ingest <src> --cas-dir <dir> [-o manifest.json]` and
    `xet-p2p local restore <manifest> <dest> --cas-dir <dir>`.
  - Tests: 3 unit + 17 integration — empty, 1-byte, random binary, 80 MiB multi-xorb, repeated
    content, two files sharing chunks, 200 small files + Unicode + empty dirs, single-file source,
    unchanged re-ingest stores 0 new bytes, CAS persists across reopen, executable bit, source symlink
    rejected, destination symlink not followed, `..` in manifest rejected, SHA mismatch installs
    nothing, missing content fails cleanly.
  - **Exit gate:** full suite passes under `sandbox-exec` with all network denied (verified the
    sandbox blocks DNS/HTTP). `cargo clippy --all-targets -D warnings` clean.
- Found three upstream quirks (empty-file SHA-256, debug assert on missing content, no intra-batch
  cross-file dedup); worked around in `xet-local`, documented in the audit.

- **M1 implemented.**
  - Decision: sender-side shards are built with upstream `MDBInMemoryShard` from `MDBFileInfo`
    (option b), not read from `LocalClient`'s on-disk layout.
  - `xet-local` exchange layer: `export_shard`, `read_xorb`, `absent_xorbs`, `commit_xorb`
    (full re-hash via `XorbObject::validate_xorb_object`), `PendingShard::parse` (file set must equal
    the request; peer xorb/chunk index ignored), `commit_shard` (recomputes each file hash from
    verified xorb chunk hashes before registering). 10 in-process tests incl. a forged shard.
  - `transport-quic`: self-signed identities (rcgen), exact-match certificate pinning both ways with
    TLS 1.3 signature verification, explicit ring provider, length-prefixed CBOR frames and raw
    payload reads with size limits. 5 tests (good handshake, unpinned server, unauthorized client,
    oversized frame, payload length/trailing bytes).
  - `transfer`: protocol v1 (HELLO, HELLO_OK, MANIFEST, NEED, ACK, RESULT, ERROR; FINALIZE implicit),
    one control bidi stream + bounded uni streams for objects. Receiver: NEED round 1 = shard for
    unknown files, round 2 = absent xorbs; commit shard only after xorbs; reconstruct into
    `create_confined_dir(root, dest)`; dest may not overlap the CAS. Sender serves only objects the
    manifest references. 12 localhost tests: tree push, 90 MiB multi-xorb push, zero-payload repeat,
    second destination reuses CAS, edited file reuses xorbs, `..`/absolute/empty dests, CAS overlap,
    symlink escape, unauthorized client, corrupted/unrequested/oversized xorbs from a hand-driven
    sender.
  - CLI: `keygen` (key written `0600`, no overwrite), `serve`, `push` with metrics.
  - **Exit gate:** real binary, localhost UDP: pushed this repo's `crates/` (26 files), `diff -r`
    identical; repeat push sent **0 payload bytes** (14 KB UDP for handshake + manifest); wrong
    pinned cert refused at handshake. No external CAS or cloud requests.
  - README with build, quick start, and security model.

- **M2 implemented.**
  - Incremental install: reconstruct leaves a destination file untouched when it is already a
    regular file (reached without symlinks) with matching size + SHA-256, applying only an exec-bit
    change; reports `files_unchanged`. A locally modified file is restored. Stale
    `.xet-p2p-staging-*` dirs from a crashed run are removed.
  - Durable receiver sessions (`<cas-dir>/sessions`, overridable with `--session-dir`): key =
    `sha256(dest ‖ 0 ‖ manifest-json)`; record (attempts, xorbs/bytes committed) and the received
    shard are written with temp-file + fsync + rename + dir fsync, updated after every committed xorb,
    deleted on success, purged after 7 days idle. On resume the saved shard is reused
    (`PendingShard::parse_containing`; still fully verified at commit) and committed xorbs are found
    in the CAS, so neither is re-sent.
  - Per-destination locks replace the global lock: overlapping destinations (one a path prefix of
    the other) serialize, disjoint ones run concurrently. Waits up to 60 s, longer than the QUIC idle
    timeout, so a resume outwaits a dead predecessor connection.
  - Metrics: `ReceiverReport` adds `files_unchanged`, `xorbs_already_present`, `resumed`,
    `attempts`, `prior_xorbs_committed`, `prior_xorb_bytes_committed`, `shard_from_session`;
    `push` prints them and `push --json` emits the whole report.
  - **Bug found and fixed:** the receiver awaited the next object stream inside a `select!` arm,
    blocking ACKs; a sender that waits for each ACK deadlocked. Accepting is now its own branch.
    (M1's `push` never waits for ACKs, so M1 tests could not see it; the resume tests did.)
  - Tests: 5 more `xet-local` (unchanged files keep their inode, tampered file restored, exec-bit-only
    change, identical file behind a symlink not trusted, stale staging cleaned); 6 more transfer
    tests (resume after 2 of ≥3 xorbs: shard from session, 0 shard bytes, only remaining xorbs sent;
    resume with 0 xorbs committed; session removed on success; changed source after interruption
    starts a fresh session but reuses xorbs; 3 concurrent disjoint pushes sharing content; concurrent
    overlapping pushes serialize); 1 dest-lock unit test; 1 process test that SIGKILLs `serve`
    mid-transfer (hidden `--test-commit-delay-ms` makes timing deterministic), restarts it, and
    verifies the resumed push re-sends no committed xorb and no shard.

## Next: M3 — chunk-level transfer

1. Receiver resolves needed chunks per xorb range, not whole xorbs: skip chunks it already holds in
   *other* xorbs (cross-xorb dedup), using the global-dedup chunk index.
2. Choose between partial-range caching (with explicit completeness metadata) and repacking missing
   chunks into new native xorbs with rewritten reconstruction terms; measure both.
3. Benchmark wire bytes vs whole-xorb transfer on overlapping edits (insertions at start/middle,
   checkpoint-like binaries). Report exact conditions.

## Known gaps / risks

- `LocalClient::upload_xorb` does not verify when a footer is supplied; receiver must call
  `XorbObject::validate_xorb_object` first.
- Ingest does not detect a file modified mid-read beyond a size check; content changes of equal size are
  hashed as read.
- Directory install is per-file atomic after all files verify, not a single atomic swap of the tree.
- Push does not delete receiver files absent from the source (no mirroring; out of scope for v0.1).
- Xorb commits are not fsynced by upstream (`SafeFileCreator` renames without `sync_all`). Process
  crashes are safe (tested); a power loss could leave a truncated xorb that `xorb_exists` still
  reports, failing later reconstructions of files that use it. Fix options: fsync after commit
  (needs the xorb path — layout coupling) or verify-on-resume; decide in M4.
- Any authorized sender may write anywhere under `--root`; no per-sender destination policy yet.
- A resumed push must re-ingest the source (cheap: all chunks dedup locally) and must present an
  identical manifest to hit its session; an edited source starts a new session (xorbs still reused).
- Each xorb is held fully in memory while sent/verified (≤ 80 MiB × `--parallel`); streaming is M4.
- No `transport` trait yet: `transfer` uses Quinn types directly. Abstract when M5 adds a fallback.
- Upstream debug-asserts on an inconsistent `MDBFileInfo` (`file_structs.rs:372`). Shards parsed from
  the wire are internally consistent, but a task panic would only abort that one transfer.
- `LocalClient` lives in upstream's `simulation` module — no stability promise. Wrapped behind `xet-local`.
