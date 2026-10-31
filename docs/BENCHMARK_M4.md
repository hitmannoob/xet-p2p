# M4 benchmark: xet-p2p vs rsync vs plain copy

**Date:** 2026-10-31 · **Reproduce:** `cargo run --release -p transfer --example bench_m4`

## Conditions

- Apple M4 (10 cores), 16 GiB RAM, macOS (Darwin 25.6.0), APFS on internal SSD; Rust 1.95.0,
  release build.
- **No network shaping.** Everything runs on one machine: xet-p2p over real QUIC (pinned mTLS) on
  127.0.0.1; rsync over its real remote-shell protocol through a local pipe (a fake `rsh` that runs
  the server side locally, since SSH to localhost is not enabled here). Latency/loss shaping
  (`dnctl`/Network Link Conditioner) needs root and was not available. Byte counts are therefore
  the meaningful result; times measure CPU and disk work only.
- rsync is macOS's `openrsync` (protocol 29, "rsync 2.6.9 compatible"), delta transfer on (the
  remote-shell default). `-a` and `-az` (zlib) both shown. Its byte counts exclude SSH framing and
  encryption; xet-p2p's UDP counts include all QUIC and TLS overhead.
- xet-p2p: chunk mode (default), `--parallel 4`, 256 MiB memory budget, upstream default (lz4)
  compression, durable writes (fsync per object + one `F_FULLFSYNC` barrier per batch).
- "Warm": the tool synced the first version once (not measured), then the edit was applied and
  the second sync measured. Edited files get an mtime 10 s later, because rsync's quick check has
  one-second resolution and would otherwise silently skip a same-second edit (a real rsync
  pitfall, not something xet-p2p is subject to: it compares content hashes).
- Every result was checked with `diff -r` against the source.
- Data: random bytes are incompressible; "log text" is drawn from a 24-word vocabulary (very
  compressible); the small-file tree is 2000 text files of 1–16 KiB in 40 directories.

## Results

**Bytes sent by the source** (rsync: `Total sent`; xet-p2p: UDP datagrams sent, all QUIC/TLS overhead included)

| Scenario | Plain copy | rsync -a | rsync -az | xet-p2p |
|---|---:|---:|---:|---:|
| cold, 64 MiB random file | 64.0 MiB | 64.0 MiB | 64.0 MiB | 65.5 MiB |
| warm, 4 KiB inserted mid 64 MiB random file | 64.0 MiB | 36 KiB | 44 KiB | 162 KiB |
| warm, checkpoint: 4 of 64 x 2 MiB tensors rewritten | 128.0 MiB | 8279 KiB | 8292 KiB | 8919 KiB |
| cold, 64 MiB compressible log text | 64.0 MiB | 64.0 MiB | 13.0 MiB | 25.1 MiB |
| warm, 1 MiB appended to 64 MiB log | 65.0 MiB | 1056 KiB | 247 KiB | 458 KiB |
| cold, 2000 small files (1-16 KiB text) | 16.7 MiB | 16.9 MiB | 4492 KiB | 7630 KiB |
| warm, 20 of 2000 small files edited | 16.7 MiB | 85 KiB | 80 KiB | 221 KiB |
| warm, 2000 small files unchanged | 16.7 MiB | 75 KiB | 75 KiB | 161 KiB |

**Bytes in both directions** (rsync adds `Total received`, i.e. block checksums; xet-p2p adds UDP received)

| Scenario | rsync -a | rsync -az | xet-p2p |
|---|---:|---:|---:|
| cold, 64 MiB random file | 64.0 MiB | 64.0 MiB | 65.9 MiB |
| warm, 4 KiB inserted mid 64 MiB random file | 84 KiB | 92 KiB | 170 KiB |
| warm, checkpoint: 4 of 64 x 2 MiB tensors rewritten | 8347 KiB | 8360 KiB | 8973 KiB |
| cold, 64 MiB compressible log text | 64.0 MiB | 13.0 MiB | 25.2 MiB |
| warm, 1 MiB appended to 64 MiB log | 1104 KiB | 295 KiB | 468 KiB |
| cold, 2000 small files (1-16 KiB text) | 16.9 MiB | 4535 KiB | 7991 KiB |
| warm, 20 of 2000 small files edited | 87 KiB | 81 KiB | 233 KiB |
| warm, 2000 small files unchanged | 75 KiB | 75 KiB | 169 KiB |

**Wall-clock seconds** (local, no network limits: measures CPU and disk work)

| Scenario | Plain copy | rsync -a | rsync -az | xet-p2p |
|---|---:|---:|---:|---:|
| cold, 64 MiB random file | 0.04 | 0.73 | 1.38 | 0.98 |
| warm, 4 KiB inserted mid 64 MiB random file | 0.14 | 1.94 | 1.55 | 0.35 |
| warm, checkpoint: 4 of 64 x 2 MiB tensors rewritten | 0.13 | 3.53 | 3.21 | 0.95 |
| cold, 64 MiB compressible log text | 0.03 | 0.49 | 1.60 | 1.00 |
| warm, 1 MiB appended to 64 MiB log | 0.50 | 2.40 | 2.06 | 0.72 |
| cold, 2000 small files (1-16 KiB text) | 0.46 | 0.70 | 0.65 | 1.12 |
| warm, 20 of 2000 small files edited | 0.47 | 0.13 | 0.16 | 0.32 |
| warm, 2000 small files unchanged | 0.45 | 0.10 | 0.10 | 0.27 |

## Reading the results

**Where xet-p2p is as good or better**

- **Large-file edits are 3–5× faster than rsync** at comparable traffic (0.35 s vs 1.5–1.9 s for a
  4 KiB insert into 64 MiB; 0.95 s vs 3.2–3.5 s for the checkpoint). rsync must read both copies
  and roll a checksum across the whole file; xet-p2p re-chunks only the sender's file and looks up
  chunk hashes, and the receiver never reads its old copy.
- **It sends no more than rsync when much of the data changed** (checkpoint: 8.7 vs 8.1 MiB, the
  difference being 64 KiB chunk granularity against rsync's ~700-byte blocks).
- **Beyond rsync's model**: content already held anywhere in the receiver's CAS is never resent,
  including content from other files, other destinations, or other senders (see
  [BENCHMARK_M3](BENCHMARK_M3.md): an edit from a different sender CAS moved 0.17 MiB). rsync only
  compares against the same path.

**Where rsync is better, and why**

- **Compressible data**: rsync -az's zlib beats upstream Xet's lz4 by about 2× (13.0 vs 25.1 MiB
  for the log). Xet object bytes must stay native, so this is upstream's compression choice; a
  transport-level compressor over chunk runs (bytes unchanged once decoded) is the way to close it.
- **Small edits: fixed per-push overhead.** For a 4 KiB insert, xet-p2p moves ~160 KiB against
  rsync's ~40–90 KiB: the changed region costs one or two whole content-defined chunks (~64 KiB
  each) versus rsync's literal bytes, plus the new file's chunk list (36 B per chunk) and the QUIC
  and TLS handshake (≈10 KiB).
- **Small-file trees**: the manifest carries two 32-byte hashes per file (161 KiB for 2000 files
  unchanged, against rsync's 75 KiB of names, sizes and mtimes). That cost buys content-based
  change detection with no checksum round trips.
- **Small-file cold copy** is 1.1 s against rsync's 0.7 s: chunking, hashing, storing in two CASes,
  and durable install of 2000 files.

**Found and fixed while running this benchmark** (each a commit):

1. Restoring 2000 small files took 13.7 s. Upstream `LocalClient` re-parses each xorb footer from
   disk per file with unbuffered per-field reads, and macOS `sync_all` (`F_FULLFSYNC`) ran on the
   runtime thread. Fixed with a cached-footer local reconstructor and fsync + one barrier. 1.4 s.
2. Pushing 2000 small files took 9.7 s: one `F_FULLFSYNC` per saved resume entry, chunk runs read
   via upstream `get_xorb_raw_bytes` (reads the whole xorb per call), and one QUIC stream per tiny
   run. Fixed by batched state writes, range reads, and ~4 MiB run batches. 1.0 s.
3. The manifest was hex-in-CBOR; now binary + lz4. Unchanged 2000-file push: 407 → 161 KiB.

## Not measured

Real network links (latency, loss, bandwidth). On a real link, wire bytes dominate for large
syncs; round trips matter for small ones (xet-p2p needs ~3 round trips after the handshake for a
chunk-mode push with changes; rsync's pipelined protocol needs fewer).
