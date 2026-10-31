# Progress

Living log for `xet-p2p`. Spec: [XET_P2P_CODEX_START.md](XET_P2P_CODEX_START.md). Upstream findings: [docs/UPSTREAM_AUDIT.md](docs/UPSTREAM_AUDIT.md).

## Status

| Milestone | State |
|---|---|
| M0 — Upstream audit + offline Xet round trip | **Done** (exit gate met) |
| M1 — Direct QUIC push, full-xorb transport | **Done** (exit gate met) |
| M2 — Incremental transfer + durable resume | Not started |
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

## Next: M2 — incremental transfer + durable resume

1. Persist session state on the receiver (session id → manifest digest, verified objects).
2. Resume: on reconnect with the same manifest, re-check the CAS and request only what is still
   missing (already true for committed xorbs; make it explicit, tested, and reported).
3. Kill-mid-transfer test proving committed xorbs are not re-sent.
4. Skip rewriting destination files whose content already matches (today every push reinstalls).
5. Concurrent-push safety beyond the current one-transfer-at-a-time lock.

## Known gaps / risks

- `LocalClient::upload_xorb` does not verify when a footer is supplied; receiver must call
  `XorbObject::validate_xorb_object` first.
- Ingest does not detect a file modified mid-read beyond a size check; content changes of equal size are
  hashed as read.
- Directory install is per-file atomic after all files verify, not a single atomic swap of the tree.
- Push does not delete receiver files absent from the source (no mirroring; out of scope for v0.1).
- Receiver handles one transfer at a time (global lock).
- Each xorb is held fully in memory while sent/verified (≤ 80 MiB × `--parallel`); streaming is M4.
- No `transport` trait yet: `transfer` uses Quinn types directly. Abstract when M5 adds a fallback.
- Upstream debug-asserts on an inconsistent `MDBFileInfo` (`file_structs.rs:372`). Shards parsed from
  the wire are internally consistent, but a task panic would only abort that one transfer.
- `LocalClient` lives in upstream's `simulation` module — no stability promise. Wrapped behind `xet-local`.
