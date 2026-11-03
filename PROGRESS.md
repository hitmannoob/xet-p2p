# Progress

Living log for `xet-p2p`. Spec: [XET_P2P_CODEX_START.md](XET_P2P_CODEX_START.md). Upstream findings: [docs/UPSTREAM_AUDIT.md](docs/UPSTREAM_AUDIT.md).

## Status

| Milestone | State |
|---|---|
| M0 — Upstream audit + offline Xet round trip | **Done** (exit gate met) |
| M1 — Direct QUIC push, full-xorb transport | **Done** (exit gate met) |
| M2 — Incremental transfer + durable resume | **Done** (exit gate met) |
| M3 — Chunk-level transfer optimization | **Done** (exit gate met; [benchmark](docs/BENCHMARK_M3.md)) |
| M4 — Performance and reliability | **Done** ([benchmark vs rsync](docs/BENCHMARK_M4.md)) |
| M5 — Provider-agnostic sandbox integration | **Done for Modal** (real sandbox verified); E2B/Daytona not implemented |

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


### 2026-10-31 — M3

- Strategy chosen: **receiver-side repacking** (not partial-range caching). Rationale: partial xorbs
  are not valid content-addressed objects; repacking keeps every stored object a complete native
  xorb and every reconstruction a native, verifiable shard entry, with no completeness metadata.
- Chunk index (`xet-local`, redb `chunk_index.redb`): chunk hash → (xorb, index) for every xorb;
  maintained on commit, reconciled at open and after ingest. Upstream's own index covers only
  global-dedup-eligible chunks (~1/1024), so it cannot answer "do I hold this chunk?".
- Chunk exchange: chunk lists (36 B/chunk, verified by recomputing the file hash), sender serves
  requested ranges as native chunk bytes straight from its xorbs (`get_byte_offset` + raw read),
  receiver decompresses + hashes each chunk (bounded by `MAX_CHUNK_SIZE` before decompression),
  concatenates native bytes into new xorbs with upstream `reconstruct_xorb_with_footer` (no
  recompression), commits every 16 MiB, and writes fresh native reconstructions (segments +
  verification range hashes + SHA-256 ext) that pass `commit_shard`'s full verification.
- Protocol v2 (ALPN `xet-p2p/2`): `HELLO.mode` (chunk default, `--mode xorb` retained),
  `NEED_CHUNK_LISTS`, `NEED_CHUNKS`, objects `CHUNK_LISTS`, `CHUNK_RUN`. Files whose stored
  reconstruction lost a xorb are re-described and re-registered in chunk mode.
- Resume in chunk mode: chunk lists pooled per file (`pending/<hash>.chunks`), packed xorbs are
  durable and indexed, so a retry requests only chunks not yet packed.
- Tests: 8 in-process chunk tests (cold, multi-xorb repack, shared-xorb single file, content in
  different xorbs + edit from a fresh sender, repeated chunks once, forged chunk list, tampered
  chunk bytes, index persistence); 6 chunk-mode QUIC tests (cold tree, zero-payload repeat,
  90 MiB repack, shared-xorb cold receiver, cross-sender reuse vs xorb mode, interrupted push
  resumes); crash test now exercises chunk mode. Workspace: 88 tests.
- **Exit gate (benchmark, [docs/BENCHMARK_M3.md](docs/BENCHMARK_M3.md))**: warm-receiver edits of a
  64 MiB file move 0.05–0.17 MiB (0.08–0.27% of a naive upload). Chunk mode beats whole-xorb by
  64× when content sits in shared or foreign xorbs (1.00 vs 64.05 MiB; 0.17 vs 64.05 MiB). For
  same-sender edits whole-xorb is already near-optimal and chunk mode costs +0.03 MiB of chunk
  lists. Conditions: Apple M4, localhost UDP (no latency/loss), incompressible data, release.

### 2026-10-31 — M4

- **Stall timeout** (receiver, `--stall-timeout-secs`, default 120): a transfer whose sender delivers
  no QUIC STREAM frames while the receiver waits on it is aborted and its destination lock freed.
  Keep-alive pings do not count; local work never trips it. Sender-side has none by design (the
  receiver may legitimately work for minutes; the sender's user can interrupt).
- **Bandwidth limit** (`push --bwlimit-mib`): shared token bucket over all streams.
- **Garbage collection** (`xet-p2p gc`): orphan xorbs past a 7-day grace, optional LRU eviction to
  `--max-size-mib` (dependent file entries dropped first, so a crash mid-GC never leaves a dangling
  reconstruction), empty shards and chunk-index entries removed, `--dry-run`. Recency = xorb mtime,
  refreshed on commit, reconstruct, and each push. Store locking: shared `<cas>/.lock` for normal
  opens, exclusive for GC.
- **Performance**, found by profiling during the rsync benchmark:
  - Local reconstruction now decodes files directly from native segments with cached footers,
    concurrently, checking every chunk hash (upstream `LocalClient` re-read footers per file with
    unbuffered reads). Restore of 2000 small files 13.7 s → 1.4 s.
  - Durability on macOS: fsync per object + one `F_FULLFSYNC` barrier per batch (`durable` module),
    instead of a full drive-cache flush per file.
  - Chunk runs read by range (upstream `get_xorb_raw_bytes` reads the whole xorb), batched ~4 MiB
    per stream; resume state written with one barrier per batch. Push of 2000 small files
    9.7 s → 1.0 s.
  - Wire manifest: binary + lz4 (≈200 → 80 B/file before compression).
- **Benchmark vs rsync and plain copy** ([docs/BENCHMARK_M4.md](docs/BENCHMARK_M4.md)): large-file
  edits 3–5× faster than rsync at comparable bytes; rsync wins on compressible data (zlib vs lz4,
  ~2×), on tiny edits (literal bytes vs whole 64 KiB chunks, plus ~10 KiB handshake), and on
  small-file manifests (hashes vs mtimes). No network shaping available (needs root).
- **Considered and not done**:
  - *Hybrid chunk/xorb mode*: a chunk-mode receiver never holds the sender's xorbs, and deciding
    that a whole xorb is needed requires the chunk list anyway; the saving would be the chunk list
    itself (≈0.055% of file size). Not worth a second fetch path.
  - *Streaming xorb I/O*: memory is already bounded by the byte budget; true streaming means
    writing xorbs ourselves instead of through upstream `upload_xorb`. Deferred.
- Tests: 96 (stall + slow-but-steady sender, bandwidth limiter unit, 4 GC/locking, manifest
  encoding round trip). Note: the suite needs ~2–3 GiB of temp space; on a full disk use
  `-- --test-threads=4`.

### 2026-10-31 — M5 (local parts)

- **Transport abstraction** (`transport` crate): `Connection`/`Listener` traits with boxed tokio
  streams; identities, pinning, and framing moved here. `transfer` no longer depends on QUIC.
- **TLS-over-TCP fallback** (`transport-tcp`): same pinned mTLS, yamux for streams, always reported
  as "TLS over TCP (not QUIC)". Found and fixed: a graceful yamux close waits to flush and hangs when
  the peer stops reading, leaving destination locks held — close is now abrupt, like QUIC. The full
  push suite runs over both transports.
- **Pull**: HELLO carries an operation (protocol v3). Fetch and send halves extracted (`Fetcher`,
  `serve_requests`) so the client of a pull and the receiver of a push share verification,
  repacking, resume, atomic install, and `--delete`. Pull sources are confined, scoped, and locked
  like destinations.
- **Providers** (`providers` crate): strict target URIs; `SandboxProvider`/`Sandbox` traits
  (attach existing only, capabilities, start agent, close); per-session credentials (fresh key
  pair each side, 0600, removed on close); transport policy auto/quic/tcp with honest reporting;
  `doctor` with real handshakes on each transport. Fake `local://` provider with `?udp=off` to
  simulate restricted networks. `e2b://`, `daytona://` recognised, not implemented.
- **Modal adapter**: verified against Modal docs + SDK 1.5.5 (tunnels declared at creation, TCP
  only). Agent reached via `unencrypted_ports` raw TCP tunnel over TLS-over-TCP, reported relayed.
  Embedded Python helper drives the user's SDK; tested end to end against a fake Modal SDK.
- Agent Linux build: `cargo zigbuild --release --target x86_64-unknown-linux-musl -p xet-p2p`.
- Tests: 38 push/pull tests × 2 transports, 5 TCP transport tests, URI parsing, 4 local-provider
  CLI tests, 2 fake-Modal CLI tests.

### 2026-10-31 — M5: real Modal run

Run against a real Modal sandbox created for the test (`sb-SH3jRTgHpHwUYozzZZDbUS`: debian_slim,
1 CPU, 30-min timeout, `unencrypted_ports=[4434]`, Linux x86_64), then terminated. The user's
pre-existing sandbox was not touched.

| Check | Result |
|---|---|
| push 8 MB tree | arrived, reconstructed and SHA-256-verified in the sandbox; "TLS over TCP (not QUIC), relayed by the provider" |
| repeat push | 0 payload bytes; 2 files unchanged |
| push after an 8-byte edit | 131 KiB (1 of 120 chunks; 119 already present) |
| pull back | byte-identical (`diff -r`) |
| `--transport quic` | refused with an explanation |
| `doctor` | linux/x86_64, ephemeral storage, QUIC n/a, TCP tunnel ok, relayed |
| cleanup | no session credentials or agent left; only the cached agent binary |

Throughput is set by the path, not the stack: raw sockets through the tunnel measured 1.86 MB/s
up and 0.12 MB/s down from this machine; xet-p2p push ran at 0.8 MB/s (room to tune: one TCP
connection, TLS + yamux) and pull at 0.21 MB/s.

Found by the real run (none visible to the fake SDK):

1. **NAT64 on this network**: DNS returns synthesized `64:ff9b::` IPv6 addresses first and they do
   not route. The Modal SDK (gRPC) never fell back, so every API call timed out at 60 s. The helper
   now prefers IPv4 (IPv6 kept as fallback), and xet-p2p's dialer tries every resolved address,
   IPv4 first, each with its own timeout.
2. **Modal retired `Sandbox.open()`** ("legacy Sandbox filesystem API is no longer supported").
   The helper now uses `sb.filesystem.write_bytes` / `copy_from_local` (agent uploaded without
   buffering it in Python); the fake SDK now rejects `open()` too.
3. **A failed agent start left an empty session directory** (no key had been written yet). The
   helper now kills any started agent and deletes the session on every start failure; covered by a
   fake-SDK test.

Setup to reproduce: `cargo zigbuild --release --target x86_64-unknown-linux-musl -p xet-p2p`,
`XET_P2P_AGENT_LINUX_BIN=…`, a sandbox with `unencrypted_ports=[4434]`, then
`xet-p2p push SRC modal://SANDBOX_ID/workspace/project`.

### 2026-11-01 — M5: bootstrap cost, `--keep-agent`, repeated real-network benchmark

- **Bootstrap batched** (dcb1bff): one long-lived Modal helper per command and one remote shell
  script for the whole agent start (checks, credentials over stdin, launch, readiness wait inside
  the sandbox). ~33 s per command → ~3.5 s at ~145 ms RTT.
- **`--keep-agent MINUTES`** (dcb1bff): the agent stays up until idle that long, then exits and
  deletes its credentials; the client caches the session (0600) and later commands reuse it with
  no bootstrap. Fresh Modal stdin rule (no writes after exit) fixed in 21640a1.
- **Bug found by the benchmark, fixed** (e341118): if a command could not reach its kept agent, it
  started a new one on the same fixed sandbox port and failed with "Address in use" until the old
  agent idled out. The bootstrap now records kept agents per port and replaces a live one
  (stopping it and deleting its credentials). Regression test in `modal_fake.rs`.
- **Repeated benchmark** ([docs/BENCHMARK_M5_MODAL.md](docs/BENCHMARK_M5_MODAL.md)): 5 repetitions
  × 8 scenarios × 4 tools, new data per repetition, shuffled order, SHA-256 verified, 160/160 runs
  clean. With `--keep-agent`, xet-p2p is 0.3–0.6 s behind rsync per command (1.3 s on 500 cold
  small files); the transfer itself is faster than rsync's whole command in 7 of 8 scenarios, and
  ~1 s per command is spent outside it.

### 2026-11-01 — Round trips cut from every command

Measured per phase on a real Modal sandbox (new `push --json` fields `connect_secs`,
`cas_open_secs`, `close_secs`; debug logs for DNS/TCP/TLS): a full round trip through the tunnel
is ~400 ms, and a kept-agent command spent 1.05 s connecting (TCP 200 ms, TLS 400 ms, then a
blocking session probe 400 ms) and 0.97 s in the protocol.

- d7dae76: the TLS-over-TCP client sends the session probe without waiting (a refused client's
  error is still explained, via `Connection::rejection`); the push manifest follows the HELLO
  without waiting for HELLO_OK (a refused HELLO still wins; new test with a large manifest).
- 57a96c2: local ingest runs while connecting (`transfer::prepare` + `push_prepared`).
- Result: unchanged 100-file tree 2.1 → 1.13 s. Full rerun of the benchmark: `--keep-agent` now
  beats rsync on all large-file edits (checkpoint 2.5 vs 2.9 s, log append 1.8 vs 2.1 s, 4 KiB
  insert 1.8 vs 2.0 s) and trails by ~0.5 s on small-file trees.

### 2026-11-01/02 — Profiler, cold path, small files, sandbox-to-sandbox benchmarks

Constraint kept throughout: no protocol or storage-model changes (native Xet CAS on both ends).
Details and numbers: [docs/BENCHMARK_S2S.md](docs/BENCHMARK_S2S.md).

- **Profiler** (`crates/cli/src/profile.rs`): `XET_P2P_TRACE` (span timeline + per-stage summary,
  ours and upstream's), `XET_P2P_PROFILE` (CPU flame graph, `--features cpu-profile`, `profiling`
  build profile). Works for client and agent, inside sandboxes.
- **Small files**: verify buffer reused (`75cd244`); stamp cache on both sides, like git's index
  (`a50f6c6`); kept agents never reused across builds (`f8b40cd`); receiver trusts its own installs
  (`cf13a73`); empty files never read (`3e54e50`).
- **Cold path**: 4 TCP lanes per session, ALPN `xet-p2p-tcp/4`, `/3` peers get one (`ecc451f`);
  background xorb commits (`8e682ba`); mimalloc for the musl agent, which removed mmap/munmap churn
  (20–40% of CPU) and halved a cold 4.3 GB push (`4ff5952`); lz4 skipped for incompressible files
  (`57a9056`, `fbb9ae4`).
- **Receiver**: files staged as soon as their content is in (`bec6041`); entirely new large files
  written from arriving chunks (`db76c14`); one `syncfs` per install on Linux instead of an fsync per
  file (`5fc2afb`); small files batched per xorb (`bec56a5`); directories checked once, renames in
  parallel (`989cbf4`, `3e54e50`).
- **Benchmarks**: FineWeb-Edu between sandboxes (xet-p2p ahead on every step on one pair; the second
  destination 6× faster), coding-agent fleet and single-agent rollouts (even with rsync `--link-dest`
  on time, more bytes because of the full manifest per push). An earlier fleet table was unfair to
  rsync (`--link-dest` path ignored in daemon mode); corrected in `c3d1a29`.
- **Dev cycle**: dev builds keep line tables only (target/debug 3–6 GB → 1.5 GB, kept between
  builds); `bench/devpair.py` keeps a sandbox pair up for repeated runs.

### 2026-11-02/03 — Linking, incremental manifest, file-format benchmarks

The no-protocol-change constraint was lifted for one change, after measuring it (kept on request).
Numbers: [docs/BENCHMARK_S2S.md](docs/BENCHMARK_S2S.md), raw output in
[bench/results/s2s_formats.log](bench/results/s2s_formats.log).

- **Receiver linking** (`f2c34df`, `3f28725`): a file the receiver already holds at another path is
  made by clone (APFS `clonefile`, Linux `FICLONE`) or, with `serve --link-identical`, hard link.
  Sender walks in parallel and reuses cached files' xorbs.
- **Incremental manifest** (protocol extension, `PROTOCOL_VERSION` unchanged): `HELLO_OK` carries
  `manifest_delta` (serde default false, so older receivers never get a delta). The receiver keeps
  its last 16 installed manifests under `<cas>/manifests/<sha256>.bin`; the sender keeps, per
  receiver certificate, the last manifest that receiver stored (`<cas>/peers/<fingerprint>.manifest`)
  and sends `MANIFEST_DELTA {base, upserts, removed}`. A receiver without the base answers
  `NEED_FULL_MANIFEST` and gets the full one. Pull is unchanged. Rollouts: 611 → 88 KiB per
  rollout (rsync `--link-dest` 244 KiB); fleet incremental rounds 13.5 → 1.3 MiB (rsync 5.0 MiB).
- **File formats** (`devpair.py formats`, `bench/formats_sim.py`): model weights with a real
  fine-tune, SQLite, tar/tar.gz, CSV, raw floats, and Sintel as MP4, WAV, FLAC, MP3 and a clip
  dataset. xet-p2p wins on content the receiver holds (second destinations, moved clips: 0.02 vs
  48 MiB) and on the fine-tune swap (3.7 vs 20.9 s); rsync wins on small edits inside one large
  file, scattered SQLite edits and cold compressible data.

## Next

- Check whether the per-file lz4 skip turns off upstream's `bg4-lz4` (byte grouping) for bf16
  weights and 16-bit PCM; rsync -z sends 22–27% less on both. Sample bg4 too, then re-measure.
- Profile a small edit inside one 1.1 GB file (xet-p2p 2.0 s, rsync 1.0 s): likely whole-file
  re-chunking and re-hashing on the sender, not measured yet.

- E2B and Daytona adapters (same `Sandbox` trait; verify their exec/file/port APIs first).
- Re-run the Mac → Modal benchmark on the current build (the README table predates the small-file
  and receiver work).
- Keep the receiver's store on a Modal Volume (`?state=`/`--cas-dir`), so dedup survives sandbox
  restarts; untested.
- TLS 0-RTT session resumption for kept agents would remove one more round trip (~400 ms on
  Modal); weigh replay safety first.
- Fix the benchmark's raw-upload baseline probe (fails through Modal tunnels).
- Opt-in automated real-provider test (`XET_P2P_TEST_MODAL=1`), off by default since it bills.

## Known gaps / risks

- Directory install is per-file atomic after all files verify, not one atomic swap of the tree. A
  crash mid-install leaves a mix of old and new verified files; re-running the push completes it
  (unchanged files skipped). An atomic tree swap was considered and not taken.
- Xorbs are still buffered whole (bounded by the memory budget): upstream's `upload_xorb` takes a
  whole buffer. True streaming needs our own xorb file writes.
- Sender-side CAS writes during ingest are not fsynced (upstream sessions). A sender power loss can
  corrupt its own CAS; the receiver rejects any bad xorb, so the result is a failed push, never
  corrupt data on the receiver. Recovery: delete the sender's `--cas-dir`.
- Unchanged files are detected by stamp (size, mtime, ctime, inode, device), like git's index: any
  write moves ctime. Files changed within 2 s of being hashed are not trusted; files the receiver
  itself installed are trusted after 50 ms where timestamps have sub-second resolution.
- The incremental manifest needs a previous push to the same receiver: the first push from a new
  sender (or after the receiver dropped the base, or on pull) still sends the full manifest, ~80
  bytes per file.
- A small edit inside one large file re-chunks and re-hashes the whole file, and SQLite-style
  scattered edits touch most 64 KiB chunks; rsync is faster or sends less on both.
- The receiver keeps full copies plus its store (about 2× the disk of the data); rsync
  `--link-dest` shares files between copies via hard links.
- Chunk mode sends a chunk list (36 B/chunk) for every new file before knowing what is missing;
  for same-sender edits this is slightly more than whole-xorb (+0.03 MiB per 64 MiB file).
- Compression is upstream's lz4; on compressible data rsync -az (zlib) sends ~2× less. A
  transport-level compressor over chunk runs would close this without changing object bytes, but
  changes the wire protocol (out of scope).
- Smallest unit resent is one content-defined chunk (~64 KiB); a tiny edit costs 1–2 chunks.
- Local reconstruction no longer uses upstream `FileDownloadSession` (see audit): formats and
  decoding are upstream's, the driver loop is ours.
- The receiver's repacked xorbs differ from the sender's; content identity is per chunk and per
  file hash, so this is invisible to reconstruction, but receiver-side dedup ratios depend on
  arrival order of chunks within a pack.
- `LocalClient` lives in upstream's `simulation` module — no stability promise; pinned `=1.8.0` and
  wrapped behind `xet-local`, with durability paths coupled to its on-disk layout.
- Certificates: no rotation/revocation beyond replacing the pinned file and restarting.
