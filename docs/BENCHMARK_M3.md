# M3 benchmark: chunk-level vs whole-xorb transfer

**Date:** 2026-10-31 · **Commit:** measured on the tree that introduced `crates/transfer/examples/bench_m3.rs`
**Reproduce:** `cargo run --release -p transfer --example bench_m3`

## Conditions

- Apple M4 (10 cores), 16 GiB RAM, macOS (Darwin 25.6.0); Rust 1.95.0, release build.
- Sender and receiver in one process, real QUIC (pinned mTLS) over 127.0.0.1 UDP: **no network
  latency, loss, or bandwidth limit**. Times therefore measure CPU work — sender ingest (chunking,
  hashing, compression), receiver verification, repacking, reconstruction, and fsyncs — not a
  realistic link. Wire-byte numbers are the meaningful result.
- `--parallel 4`, 256 MiB memory budget, upstream default compression.
- All data is pseudo-random (seeded `StdRng`): **incompressible**, so payload ≈ logical bytes when
  everything must move. Compressible data would make every payload column smaller.
- Each scenario runs **once per mode, each from fresh state** (new CAS directories, endpoints,
  identities). "Warm" setup pushes are not counted; only the final push is measured.
- Every measured push is verified byte-for-byte against the source.

**Columns.** *Naive upload*: the file bytes a whole-file copy would send (uncompressed).
*Payload*: object bytes sent (xorbs + shards in whole-xorb mode; chunk runs + chunk lists in chunk
mode). *UDP*: all bytes Quinn sent on the connection — payload plus TLS handshake, control
messages, framing, and QUIC overhead. *Chunk time*: wall clock of the chunk-mode push.

## Results

| Scenario | Naive upload | Whole-xorb payload | Chunk payload | Chunk / naive | Whole-xorb UDP | Chunk UDP | Chunk time |
|---|---:|---:|---:|---:|---:|---:|---:|
| cold: 64 MiB new file, empty receiver | 64.00 MiB | 64.05 MiB | 64.04 MiB | 100.07% | 65.51 MiB | 65.51 MiB | 1.30 s |
| edit: 4 KiB inserted at start of 64 MiB file (same sender) | 64.00 MiB | 0.02 MiB | 0.05 MiB | 0.08% | 0.03 MiB | 0.06 MiB | 0.42 s |
| edit: 4 KiB inserted mid 64 MiB file (same sender) | 64.00 MiB | 0.11 MiB | 0.15 MiB | 0.23% | 0.12 MiB | 0.16 MiB | 0.44 s |
| checkpoint: 4 of 64 x 2 MiB tensors rewritten (same sender) | 128.00 MiB | 8.44 MiB | 8.51 MiB | 6.65% | 8.64 MiB | 8.71 MiB | 2.20 s |
| edit from a different sender CAS (receiver warm via sender A) | 64.00 MiB | 64.05 MiB | 0.17 MiB | 0.27% | 65.51 MiB | 0.19 MiB | 0.56 s |
| one 1 MiB file whose xorb is shared with 63 others, empty receiver | 1.00 MiB | 64.05 MiB | 1.00 MiB | 100.07% | 65.51 MiB | 1.03 MiB | 0.08 s |
| unchanged re-push (warm) | 64.00 MiB | 0.00 MiB | 0.00 MiB | 0.00% | 0.01 MiB | 0.01 MiB | 0.20 s |

| Scenario | Chunks in new files | Already on receiver | Sent | Runs | Chunk-list bytes |
|---|---:|---:|---:|---:|---:|
| cold: 64 MiB new file, empty receiver | 1056 | 0 | 1056 | 1 | 38052 |
| edit: 4 KiB inserted at start of 64 MiB file (same sender) | 1020 | 1019 | 1 | 1 | 36756 |
| edit: 4 KiB inserted mid 64 MiB file (same sender) | 1020 | 1019 | 1 | 1 | 36756 |
| checkpoint: 4 of 64 x 2 MiB tensors rewritten (same sender) | 2101 | 1957 | 144 | 1 | 75672 |
| edit from a different sender CAS (receiver warm via sender A) | 1052 | 1050 | 2 | 1 | 37908 |
| one 1 MiB file whose xorb is shared with 63 others, empty receiver | 16 | 0 | 16 | 1 | 612 |
| unchanged re-push (warm) | 0 | 0 | 0 | 0 | 0 |

## Reading the results

- **Chunk mode wins decisively when the receiver holds the content in different xorbs than the
  sender would send.** Two common cases:
  - *A file packed in a shared xorb* (upstream packs many files into one ≤64 MiB xorb): pushing one
    1 MiB file sent **1.00 MiB vs 64.05 MiB**.
  - *A different sender CAS* (another machine, or a re-created cache) pushing an edit of content
    the receiver already has: **0.17 MiB vs 64.05 MiB** — 0.27% of a naive upload.
- **For edits pushed from the same sender CAS, whole-xorb mode is already near-optimal.** Upstream
  dedup on the sender puts only new chunks into new xorbs, so only those move (0.02–0.11 MiB).
  Chunk mode is slightly larger here (+0.03 MiB) because it sends each new file's chunk list
  (36 bytes per chunk, ≈0.055% of the file) before it knows what is missing.
- **Checkpoint-style rewrites** (4 of 64 tensors, 8 MiB of 128 MiB changed) cost about the
  changed bytes in both modes (8.44 / 8.51 MiB): chunks inside rewritten tensors are new
  everywhere.
- **Cold pushes cost the same in both modes** (+0.07% over naive for xorb/chunk framing; +2.3% UDP
  overhead for QUIC/TLS on incompressible data).
- **Unchanged re-push** moves no payload in either mode (≈10 KiB of handshake and manifest).

Full-xorb reuse and chunk-level wire dedup are different mechanisms, reported separately above.
No claim is made here about QUIC vs TCP speed; that comparison (vs `rsync` over SSH) is M4.

## Possible follow-ups

- Skip chunk lists when the receiver can fall back to sender xorb boundaries (would remove the
  +0.03 MiB in same-sender edits): e.g. a hybrid that requests whole xorbs when the plan says every
  chunk of that xorb is needed.
- Measure with realistic latency/loss (`tc netem` / Network Link Conditioner) and compressible data.
