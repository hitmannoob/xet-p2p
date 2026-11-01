# Real-network benchmark: xet-p2p vs rsync, Mac → Modal sandbox

**Date:** 2026-11-01 · **Reproduce:** `bench/modal_vs_rsync.py` (method in its docstring) · Two
full runs: **current** (commit 57a96c2, raw data
[modal_vs_rsync_after_rtt_cuts.json](../bench/results/modal_vs_rsync_after_rtt_cuts.json) /
[.md](../bench/results/modal_vs_rsync_after_rtt_cuts.md)) and **before the round-trip cuts**
(commit e341118, [modal_vs_rsync.json](../bench/results/modal_vs_rsync.json) /
[.md](../bench/results/modal_vs_rsync.md)).

## Method

- **Path:** this Mac (home network, India; IPv4) → one Modal sandbox (Linux x86_64,
  `debian_slim` + rsync, 1 CPU) through **Modal raw TCP tunnels**. Modal tunnels carry no UDP, so
  xet-p2p used its **TLS-over-TCP** transport, relayed by Modal — not QUIC.
- **Same path for every tool**, one tunnel per server so nothing is switched mid-run: an rsync
  daemon (started once; `rsync://`, delta transfer on, **unencrypted**), an xet-p2p agent started
  and stopped **per command**, and an xet-p2p agent kept between commands (`--keep-agent 15`).
- **5 repetitions × 8 scenarios × 4 tools = 160 measured runs.** Every repetition uses new data
  (seeded by scenario and repetition, identical for every tool within it), so no run benefits from
  content left by an earlier one. Tool order is shuffled per scenario, so network drift does not
  favour one tool. The two xet-p2p variants use separate CASes in the sandbox.
- "Warm" = the first version was synced (unmeasured), then edited (mtime +10 s so rsync's quick
  check notices). **Every measured tree was verified by SHA-256 inside the sandbox.** All 160 runs
  verified; none needed a retry.
- Baseline: TCP connect RTT through the tunnel (to Modal's edge) was **141–148 ms** in the first
  run and **142–178 ms** in the current one. A full application round trip to the sandbox is about
  twice that (~400 ms, measured from the TLS handshake). The raw-upload probe failed in every
  repetition, so there is no raw throughput baseline.
- Local rsync: macOS `openrsync` (protocol 29); sandbox: Debian GNU rsync. xet-p2p: chunk mode,
  defaults, a fresh local CAS per scenario.
- Cells are **median [min–max]** over the 5 repetitions. One value means all 5 were equal.

## Current results (commit 57a96c2)

Two round trips were cut from every kept-agent command (the TLS-over-TCP client no longer waits
for the session-confirmation probe; the push manifest follows the HELLO without waiting for its
reply), and local ingest now overlaps connecting. 160/160 runs verified, no retries.

### Wall-clock seconds (whole command)

| Scenario | rsync -a | rsync -az | xet-p2p | xet-p2p --keep-agent |
|---|---:|---:|---:|---:|
| cold, 8 MiB random file | 3.6 [3.3–6.4] | 3.7 [3.5–4.1] | 7.4 [6.6–12.8] | 3.8 [3.5–6.1] |
| warm, 4 KiB inserted mid 8 MiB file | 2.0 [2.0–2.1] | 2.0 [2.0–2.1] | 4.9 [4.8–5.5] | **1.8** [1.7–2.1] |
| warm, checkpoint: 2 of 32 tensors rewritten (16 MiB) | 2.9 [2.7–4.3] | 2.9 [2.8–3.5] | 5.7 [5.5–6.2] | **2.5** [2.4–2.6] |
| cold, 8 MiB compressible log | 3.6 [3.6–4.0] | 2.9 [2.7–2.9] | 6.2 [5.8–6.5] | 2.9 [2.7–3.1] |
| warm, 256 KiB appended to 8 MiB log | 2.4 [2.4–2.5] | 2.1 [2.1–2.3] | 5.0 [4.8–5.3] | **1.8** [1.6–1.9] |
| cold, 500 small files (1–16 KiB) | 3.1 [3.0–3.2] | 2.6 [2.5–2.8] | 6.3 [6.1–9.2] | 3.1 [3.0–3.4] |
| warm, 10 of 500 small files edited | 1.9 [1.8–2.0] | 1.9 [1.9–2.0] | 5.7 [5.5–5.8] | 2.4 [2.3–2.6] |
| warm, 500 small files unchanged | 1.6 [1.5–1.7] | 1.7 [1.6–1.7] | 4.8 [4.7–5.7] | 1.8 [1.7–1.8] |

### Where xet-p2p's time goes (seconds)

"Transfer" is preparing (local ingest) plus pushing, without any wait between them; with
connecting overlapped, wall time is no longer exactly transfer + bootstrap + a fixed overhead.

| Scenario | xet-p2p transfer | xet-p2p bootstrap | --keep-agent transfer | --keep-agent bootstrap |
|---|---:|---:|---:|---:|
| cold, 8 MiB random file | 2.8 [2.8–3.2] | 3.7 [3.2–9.5] | 2.9 [2.9–3.3] | 0.0 [0.0–3.2] |
| warm, 4 KiB inserted mid 8 MiB file | 1.2 [1.1–1.3] | 3.1 [3.1–3.5] | 1.3 [1.1–1.5] | 0.0 |
| warm, checkpoint: 2 of 32 tensors rewritten (16 MiB) | 1.9 [1.8–2.0] | 3.2 [3.0–3.6] | 2.0 [1.9–2.1] | 0.0 |
| cold, 8 MiB compressible log | 2.3 [2.2–2.4] | 3.3 [3.1–3.5] | 2.3 [2.2–2.6] | 0.0 |
| warm, 256 KiB appended to 8 MiB log | 1.2 [1.1–1.5] | 3.2 [2.9–3.2] | 1.2 [1.1–1.3] | 0.0 |
| cold, 500 small files (1–16 KiB) | 2.5 [2.3–5.4] | 3.2 [3.1–3.3] | 2.5 [2.4–2.8] | 0.0 |
| warm, 10 of 500 small files edited | 2.0 [1.8–2.0] | 3.2 [3.1–3.2] | 1.9 [1.7–2.1] | 0.0 |
| warm, 500 small files unchanged | 1.1 [1.1–1.3] | 3.2 [3.0–3.7] | 1.2 [1.1–1.2] | 0.0 |

Bytes are unchanged from the earlier run (same data, same protocol content).

### Reading the current results

- **With `--keep-agent`, xet-p2p is now faster than both rsync modes on every large-file edit**:
  4 KiB insert 1.8 vs 2.0 s, checkpoint 2.5 vs 2.9 s, log append 1.8 vs 2.1 s (ranges do not
  overlap). It ties on the cold compressible log (2.9 s) and is within 0.2 s on the cold random
  file and the unchanged tree.
- **rsync still wins on small-file trees**: 10 of 500 edited 1.9 vs 2.4 s, 500 cold 2.6 vs 3.1 s.
  Inside the push, an unchanged 500-file tree takes 1.2 s against 0.4 s for a 100-file tree, so
  part of the cost grows with the file count (manifest size over a fresh TCP connection and
  per-file work on the receiver are the candidates; not yet measured).
- **Per kept-agent command, the fixed cost dropped by ~0.4–0.5 s** (unchanged tree 2.1 → 1.8 s,
  checkpoint 3.3 → 2.5 s, log append 2.6 → 1.8 s). Measured directly on a 100-file unchanged tree:
  connect 1.05 → 0.64 s (TCP ~200 ms + TLS ~400 ms remain), push protocol 0.97 → 0.40 s.
- **A fresh agent per command still costs ~3.2 s** of bootstrap; plain `xet-p2p` stays 2.4–3×
  slower than rsync here.

## Before the round-trip cuts (commit e341118)

### Wall-clock seconds (whole command)

rsync's time is its whole command against the already-running daemon. xet-p2p's is its whole
command, including attaching to the sandbox and, for `xet-p2p`, starting and stopping the agent.

| Scenario | rsync -a | rsync -az | xet-p2p | xet-p2p --keep-agent |
|---|---:|---:|---:|---:|
| cold, 8 MiB random file | 3.8 [3.6–11.3] | 3.7 [3.4–3.8] | 7.7 [6.9–12.0] | 4.2 [4.0–6.4] |
| warm, 4 KiB inserted mid 8 MiB file | 2.2 [2.1–2.2] | 2.1 [2.1–2.2] | 5.8 [5.4–6.0] | 2.5 [2.3–2.8] |
| warm, checkpoint: 2 of 32 tensors rewritten (16 MiB) | 3.0 [2.8–3.3] | 3.0 [2.8–3.1] | 6.4 [6.1–7.0] | 3.3 [3.0–3.5] |
| cold, 8 MiB compressible log | 3.9 [3.5–4.0] | 3.1 [2.9–3.3] | 6.6 [6.4–7.1] | 3.6 [3.6–3.8] |
| warm, 256 KiB appended to 8 MiB log | 2.6 [2.5–2.6] | 2.3 [2.2–2.4] | 5.8 [5.4–6.2] | 2.6 [2.4–2.9] |
| cold, 500 small files (1–16 KiB) | 3.0 [2.8–3.3] | 2.7 [2.7–3.1] | 7.6 [7.1–13.9] | 4.0 [3.5–4.1] |
| warm, 10 of 500 small files edited | 2.1 [2.0–2.2] | 2.1 [2.0–2.2] | 6.0 [5.6–6.5] | 2.7 [2.6–3.2] |
| warm, 500 small files unchanged | 1.8 [1.7–1.9] | 1.8 [1.7–1.8] | 5.3 [5.2–5.7] | 2.1 [2.0–2.3] |

### Where xet-p2p's time goes (seconds)

"Transfer" is xet-p2p's own push (ingest → verified install). "Bootstrap" is attaching to the
sandbox and starting the agent through Modal's API. The kept agent's first command in the run
started it (the 3.3 s max); every later command reused it.

| Scenario | xet-p2p transfer | xet-p2p bootstrap | --keep-agent transfer | --keep-agent bootstrap |
|---|---:|---:|---:|---:|
| cold, 8 MiB random file | 2.9 [2.8–3.4] | 3.7 [3.4–7.7] | 3.1 [3.0–3.4] | 0.0 [0.0–3.3] |
| warm, 4 KiB inserted mid 8 MiB file | 1.6 [1.3–1.9] | 3.5 [3.4–3.7] | 1.6 [1.3–1.7] | 0.0 |
| warm, checkpoint: 2 of 32 tensors rewritten (16 MiB) | 2.3 [2.0–2.3] | 3.5 [3.4–4.1] | 2.3 [2.0–2.5] | 0.0 |
| cold, 8 MiB compressible log | 2.5 [2.5–2.8] | 3.4 [3.3–3.9] | 2.6 [2.5–2.8] | 0.0 |
| warm, 256 KiB appended to 8 MiB log | 1.6 [1.4–1.7] | 3.6 [3.3–3.9] | 1.6 [1.5–1.9] | 0.0 |
| cold, 500 small files (1–16 KiB) | 3.0 [2.9–9.4] | 3.7 [3.6–4.0] | 3.0 [2.6–3.1] | 0.0 |
| warm, 10 of 500 small files edited | 1.7 [1.6–2.1] | 3.6 [3.4–3.7] | 1.8 [1.6–2.2] | 0.0 |
| warm, 500 small files unchanged | 1.1 [1.0–1.3] | 3.6 [3.6–3.8] | 1.1 [1.0–1.2] | 0.0 |

### Bytes sent from the Mac

rsync's counts are its own (unencrypted daemon protocol); xet-p2p's include TLS and all framing.

| Scenario | rsync -a | rsync -az | xet-p2p (both variants) |
|---|---:|---:|---:|
| cold, 8 MiB random file | 8193 KiB | 8196 KiB | 8228 KiB |
| warm, 4 KiB inserted mid 8 MiB file | 18 KiB | 21 KiB | 106 KiB [42–172] |
| warm, checkpoint: 2 of 32 tensors rewritten (16 MiB) | 1039 KiB | 1043 KiB | 1281 KiB [1165–1387] |
| cold, 8 MiB compressible log | 8193 KiB | 1661 KiB | 3149 KiB [3143–3160] |
| warm, 256 KiB appended to 8 MiB log | 269 KiB | 67 KiB | 124 KiB [108–146] |
| cold, 500 small files (1–16 KiB) | 4294 KiB [4201–4400] | 1117 KiB [1098–1143] | 1861 KiB [1825–1904] |
| warm, 10 of 500 small files edited | 24 KiB [23–25] | 21 KiB [21–22] | 77 KiB [65–84] |
| warm, 500 small files unchanged | 19 KiB | 19 KiB | 39 KiB |

Bytes in both directions follow the same pattern (in the generated tables).

### Reading the results

- **With `--keep-agent`, xet-p2p is close to rsync but not faster: 0.3–0.6 s slower** per command
  in 7 of 8 scenarios (2.1 vs 1.8 s for an unchanged tree, 3.3 vs 3.0 s for the checkpoint), and
  1.3 s slower for 500 cold small files (4.0 vs 2.7 s). The ranges barely overlap, so the gap is
  real, not noise.
- **The transfer itself is faster than rsync's whole command in 7 of 8 scenarios** (e.g. 2.3 vs
  3.0 s checkpoint, 1.6 vs 2.3 s log append, 1.1 vs 1.8 s unchanged tree). The rest of a
  kept-agent command, **~1.0 s** (0.9–1.1 s in every scenario), is spent outside the transfer: process start, opening the local
  CAS, and the TLS connection through the tunnel (~145 ms RTT). That fixed cost, not data
  movement, is what puts xet-p2p behind rsync on this path. It has not been broken down further.
- **A fresh agent per command costs 3.5 s** (median, every scenario 3.3–4.1 s, plus 7.7 s once
  when the agent binary is uploaded). That makes plain `xet-p2p` about 2–3× slower than rsync
  end to end here. Use `--keep-agent` for repeated pushes to the same sandbox.
- **Bytes: same pattern as on localhost.** Large changed data costs about the same (the
  checkpoint: 1.2 vs 1.0 MiB). rsync -az sends less for compressible data (log 1.6 vs 3.1 MiB:
  zlib stream vs Xet's per-chunk lz4) and for tiny edits (literal bytes vs whole ~64 KiB chunks
  plus chunk lists and the TLS handshake). The 4 KiB insert varies from 42 to 172 KiB with where
  the insert lands relative to chunk boundaries.
- **Variance:** three runs stalled (rsync -a 11.3 s, xet-p2p 12.0 and 13.9 s), consistent with
  occasional network stalls. Medians are stable.
- Not exercised here, and where xet-p2p does something rsync cannot: content already on the
  receiver from other files, destinations or senders is never re-sent
  ([BENCHMARK_M3](BENCHMARK_M3.md)); resume after interruption; TLS with pinned peers.

## Earlier single-run result (superseded)

The first single-run benchmark on this path (commit 15ee1fc, 16 MiB / 1000-file scenarios, RTT
then ~350–380 ms) measured **~33 s of bootstrap per command**, because the Modal adapter made a
separate Modal API call for every step. Batching them into one remote script (dcb1bff) brought
that to ~7 s at that RTT and 3.5 s here, and `--keep-agent` removes it for later commands. While
running this benchmark, a kept agent that its client could not reach blocked the port for new
agents; fixed in e341118 (the new agent replaces it).
