# Real-network benchmark: xet-p2p vs rsync, Mac → Modal sandbox

**Date:** 2026-11-01 · **Reproduce:** `bench/modal_vs_rsync.py` (see its docstring)

## Conditions

- **Path:** this Mac (home network, India; DNS64 with broken NAT64, so IPv4 was used) → a Modal
  sandbox (Linux x86_64, `debian_slim` + rsync, 1 CPU) through **one Modal raw TCP tunnel**
  (`unencrypted_ports=[4434]`). Modal tunnels carry no UDP, so xet-p2p used its **TLS-over-TCP**
  transport, relayed by Modal — not QUIC.
- **Same path for both tools.** rsync ran as a daemon in the sandbox on the tunnelled port
  (`rsync://`, delta transfer on, `-a` and `-az`); then the daemon was stopped and xet-p2p's agent
  used the same port. rsync's daemon protocol is **unencrypted**; xet-p2p's numbers include TLS.
- Raw tunnel throughput measured earlier from this machine: ~1.9 MB/s up, ~0.12 MB/s down.
- Local rsync: macOS `openrsync` (protocol 29); sandbox: Debian GNU rsync. xet-p2p: chunk mode,
  defaults, a fresh local CAS per scenario.
- Each scenario has its own seeded data (no sharing between scenarios). "Warm" = the first version
  was synced (unmeasured), then edited (mtime moved +10 s so rsync's quick check notices).
- **Every final tree was verified by SHA-256 inside the sandbox** for every tool and scenario.
- **Single run per cell.** Across three runs the same rsync cold 16 MiB transfer took 12.2 s,
  18.3 s and 24.2 s, so times on this path vary by ~2×; bytes are exact and repeatable.

## Bytes sent from the Mac

| Scenario | rsync -a | rsync -az | xet-p2p |
|---|---:|---:|---:|
| cold, 16 MiB random file | 16.0 MiB | 16.0 MiB | 16.1 MiB |
| warm, 4 KiB inserted mid 16 MiB random file | 20 KiB | 24 KiB | 153 KiB |
| warm, checkpoint: 2 of 32 x 1 MiB tensors rewritten | 2081 KiB | 2087 KiB | 2175 KiB |
| cold, 16 MiB compressible log | 16.0 MiB | 3319 KiB | 6308 KiB |
| warm, 512 KiB appended to 16 MiB log | 528 KiB | 124 KiB | 211 KiB |
| cold, 1000 small files (1-16 KiB text) | 8324 KiB | 2177 KiB | 3615 KiB |
| warm, 10 of 1000 small files edited | 43 KiB | 40 KiB | 114 KiB |
| warm, 1000 small files unchanged | 38 KiB | 38 KiB | 76 KiB |

## Bytes in both directions

| Scenario | rsync -a | rsync -az | xet-p2p |
|---|---:|---:|---:|
| cold, 16 MiB random file | 16.0 MiB | 16.0 MiB | 16.1 MiB |
| warm, 4 KiB inserted mid 16 MiB random file | 44 KiB | 48 KiB | 156 KiB |
| warm, checkpoint: 2 of 32 x 1 MiB tensors rewritten | 2120 KiB | 2126 KiB | 2178 KiB |
| cold, 16 MiB compressible log | 16.0 MiB | 3319 KiB | 6311 KiB |
| warm, 512 KiB appended to 16 MiB log | 552 KiB | 148 KiB | 213 KiB |
| cold, 1000 small files (1-16 KiB text) | 8346 KiB | 2199 KiB | 3764 KiB |
| warm, 10 of 1000 small files edited | 44 KiB | 41 KiB | 118 KiB |
| warm, 1000 small files unchanged | 38 KiB | 38 KiB | 78 KiB |

## Seconds

"Transfer" is xet-p2p's own push time (ingest → verified install). "End to end" adds attaching to
the sandbox through Modal's API and starting and stopping the per-session agent (the first run also
uploads the 19 MB agent once). rsync's time is its whole command against an already-running daemon.

| Scenario | rsync -a | rsync -az | xet-p2p (transfer) | xet-p2p (end to end) |
|---|---:|---:|---:|---:|
| cold, 16 MiB random file | 24.2 | 23.1 | 11.8 | 73.8 |
| warm, 4 KiB inserted mid 16 MiB random file | 4.4 | 4.4 | 3.5 | 36.9 |
| warm, checkpoint: 2 of 32 x 1 MiB tensors rewritten | 6.7 | 8.4 | 5.8 | 39.8 |
| cold, 16 MiB compressible log | 13.0 | 7.4 | 6.9 | 41.1 |
| warm, 512 KiB appended to 16 MiB log | 5.6 | 5.0 | 3.8 | 38.1 |
| cold, 1000 small files (1-16 KiB text) | 8.4 | 6.5 | 7.3 | 41.5 |
| warm, 10 of 1000 small files edited | 4.2 | 4.6 | 3.9 | 37.4 |
| warm, 1000 small files unchanged | 3.6 | 3.5 | 2.6 | 38.2 |

## Reading the results

- **Transfer time: xet-p2p was faster in 7 of 8 scenarios** despite TLS, from 1.1× to 2× (cold
  random file 11.8 s vs 23–24 s; checkpoint 5.8 s vs 6.7–8.4 s; log append 3.8 s vs 5.0–5.6 s).
  Given the ~2× run-to-run variance, treat this as "at least as fast", not a precise ratio.
- **End-to-end time is dominated by provider bootstrap: ~33 s per command.** The Modal adapter
  makes many separate Modal API calls (uname, mkdir, sha256 check, three file writes, chmods,
  launch, ready polling, stop, cleanup), each a round trip from India to Modal's API. A
  long-running agent (like the rsync daemon here) or a single-call bootstrap would remove most of
  it. **This is the biggest practical gap and the next thing to fix.**
- **Bytes: same pattern as on localhost.** Large changed data costs about the same (checkpoint
  2175 vs 2081 KiB). rsync -az sends less for compressible data (log 3.3 vs 6.2 MiB: zlib stream
  vs Xet's per-chunk lz4) and for tiny edits (literal bytes vs whole ~64 KiB chunks plus chunk
  lists and the TLS handshake).
- Not exercised here, and where xet-p2p does something rsync cannot: content already present on
  the receiver from other files, destinations or senders is never re-sent
  ([BENCHMARK_M3](BENCHMARK_M3.md)); resume after interruption; TLS with pinned peers.
