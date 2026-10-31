# Progress

Living log for `xet-p2p`. Spec: [XET_P2P_CODEX_START.md](XET_P2P_CODEX_START.md). Upstream findings: [docs/UPSTREAM_AUDIT.md](docs/UPSTREAM_AUDIT.md).

## Status

| Milestone | State |
|---|---|
| M0 — Upstream audit + offline Xet round trip | **Done** (exit gate met) |
| M1 — Direct QUIC push, full-xorb transport | Not started |
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

## Next: M1

1. Quinn endpoints with mTLS (receiver cert pinned by sender; sender authorised by receiver).
2. Versioned, length-prefixed control frames (HELLO / MANIFEST / NEED / OBJECT_DATA / ACK / FINALIZE / RESULT).
3. Sender ships native shard(s) + whole missing xorbs; receiver validates each xorb
   (`XorbObject::validate_xorb_object`) before ingest, then reuses `LocalCas::reconstruct`.
4. `serve` / `push` CLI with `--root` + `--dest` confinement; localhost end-to-end test.

## Open decisions

- Sender-side shard bytes: read `LocalClient`'s `shards/` dir vs. build a minimal shard via `MDBInMemoryShard` (decide in M1).

## Known gaps / risks

- `LocalClient::upload_xorb` does not verify when a footer is supplied; receiver must call
  `XorbObject::validate_xorb_object` first.
- Ingest does not detect a file modified mid-read beyond a size check; content changes of equal size are
  hashed as read.
- Directory install is per-file atomic after all files verify, not a single atomic swap of the tree.
- `LocalClient` lives in upstream's `simulation` module — no stability promise. Wrapped behind `xet-local`.
