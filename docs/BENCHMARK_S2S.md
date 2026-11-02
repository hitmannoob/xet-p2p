# Sandbox to sandbox: xet-p2p vs rsync on Modal

**Dates:** 2026-11-01 – 2026-11-02 · **Tools:** `bench/devpair.py` (persistent pair: `up`, `deploy`,
`run`, `agents`, `rollouts`), `bench/s2s_dataset.py`, `bench/sandbox_to_sandbox.py`

## Setup

- Two Modal sandboxes (debian_slim + rsync, 4 CPUs, 8 GiB, `region="us-east"`). A sends, B
  receives through B's raw TCP tunnels, relayed by Modal's edge. Tunnels carry no UDP, so xet-p2p
  uses TLS over TCP, with 4 TCP connections ("lanes") per session. Private sandbox networking
  (i6pn) needs region pinning, which this account's plan does not allow.
- rsync runs as a daemon on B (`rsync://`, unencrypted, delta transfer on). xet-p2p runs
  `xet-p2p serve` on B with pinned certificates from `xet-p2p keygen`.
- Every changed tree is verified by SHA-256 of every file on both sides.
- **Single runs on one pair each.** Sandbox placement varies a lot: one TCP stream measured
  107–390 MiB/s on different pairs, and results that depend on raw throughput move with it.

## Raw throughput (4 GiB of new data, sender already ingested)

| Pair | rsync -a (1 TCP stream) | xet-p2p, 1 lane | xet-p2p, 4 lanes (default) |
|---|---:|---:|---:|
| A | 206–213 MiB/s | 184–225 MiB/s | **323–390 MiB/s** |
| B (fast single stream) | 344–388 MiB/s | 193–211 MiB/s | 257–281 MiB/s |

Pair B was measured before the allocator fix (below), when xet-p2p was still CPU-bound; pair A
after it.

## Real dataset: FineWeb-Edu `sample/100BT` (3 Parquet shards, 6.5 GB)

Build `db76c14`, one pair:

| Step | rsync -a | xet-p2p |
|---|---:|---:|
| Cold (2 shards, 4.3 GB) | 19.5 s | **13.8 s** |
| Unchanged, first re-push | 0.2 s | **0.1 s** |
| Unchanged, steady state | 0.2 s | **0.1 s** |
| Grown (+1 shard, 2.15 GB) | 10.1 s | **8.7 s** |
| Same dataset to a second destination | 21.4 s (6.0 GiB sent) | **3.5 s (~0 sent)** |

On an earlier pair with a fast single stream (an older build, `fbb9ae4`), rsync won the cold
(11.3 vs 15.9 s) and grown (5.6 vs 11.3 s) steps. Treat cold and grown as "close, depends on the
path". The second destination is a structural win.

## Coding-agent fleet (8 agents, Django workspace of 6,811 files, 3 incremental rounds)

Each agent has its own copy of the same repo plus a trajectory log; the collector receives each
agent into its own destination. xet-p2p: one collector store, one sender store per agent.
Totals, build `c3d1a29`:

| Phase | xet-p2p | rsync -az | rsync -az --link-dest=/a1 |
|---|---:|---:|---:|
| First sync, agent 1 | 2.8 s, 20.7 MiB | **2.1 s, 9.5 MiB** | **2.1 s, 9.5 MiB** |
| First sync, agents 2–8 | 13.4 s, 3.7 MiB | 14.8 s, 66.2 MiB | **8.7 s, 1.4 MiB** |
| Incremental rounds (24 syncs) | 24.3 s, 13.5 MiB | **22.0 s, 5.0 MiB** | 24.6 s, 5.0 MiB |

An earlier version of this table showed xet-p2p ahead of `--link-dest`. It was wrong: in daemon
mode `--link-dest` must be absolute relative to the module, and the relative form used then was
silently ignored.

## Single-agent rollouts (10 rollouts from the same Django base)

Each rollout resets the files the previous one touched, edits 3 files, adds a test file and writes
a ~200 KB trajectory. The collector keeps each rollout as `rollout-k`.

| Per rollout | xet-p2p | rsync -az | rsync -az --link-dest=<previous> |
|---|---:|---:|---:|
| Rollout 1 (cold) | 2.7 s, 21.2 MiB | **2.1 s, 9.7 MiB** | **2.1 s, 9.7 MiB** |
| Rollouts 2–10, median | **1.3 s**, 611 KiB | 2.0 s, 9.7 MiB | **1.3 s, 244 KiB** |

## Reading the results

- **Against plain rsync**, xet-p2p wins clearly wherever the receiver already holds content: second
  destinations, rollouts, agents sharing a base (16–18× fewer bytes, up to 6× faster).
- **Against rsync `--link-dest`**, it ties on time for rollouts and incremental syncs and loses on
  agents joining a shared base. It sends more bytes everywhere on code trees.
- **Why more bytes on code trees: the full manifest on every push.** About 80 bytes per file (path,
  size, Xet hash and SHA-256), ~545 KB for 6,800 files, even when 5 files changed. The hashes do
  not compress. An incremental manifest would need a protocol change, which is out of scope.
- **Cold pushes of text and code:** lz4 (the Xet format's compression) against rsync's zlib, about
  2.2× the bytes.
- **What `--link-dest` cannot do and xet-p2p does:** dedup against anything the receiver ever
  stored (other files, renamed or moved files, other destinations, other senders) without naming
  a reference directory; end-to-end verification of every file; independent copies (`--link-dest`
  hard-links rollouts to each other, so editing one in place edits all). The cost is receiver disk:
  full copies plus the store.

## What changed to get here (all found with the built-in profiler)

| Change | Commit | Measured effect |
|---|---|---|
| Two round trips cut per command (probe, manifest pipelining) | `d7dae76` | Mac → Modal kept-agent push 2.1 → 1.13 s |
| Reuse the verify buffer across unchanged files | `75cd244` | 5,000 unchanged files checked in a sandbox: 7.0 → 0.17 s |
| Stamp cache on both sides (unchanged files not re-read) | `a50f6c6` | in-sandbox unchanged 5,000-file push 1.55 → 0.49 s |
| 4 TCP lanes per session | `ecc451f` | raw transfer 193–211 → 257–281 MiB/s (pair B) |
| Commit xorbs in the background | `8e682ba` | 2 GiB in-sandbox transfer 11.4 → 8.8 s |
| mimalloc for the Linux agent | `4ff5952` | cold 4.3 GB push 50 → 23 s (ingest 26.1 → 3.8 s) |
| Stage files as their content completes; faster rebuild | `bec6041` | 2 GB file rebuild 4.8 → 1.5 s |
| Skip lz4 for incompressible files | `fbb9ae4` | grown-push ingest 4.2 → 3.0 s |
| Trust files the receiver installed | `cf13a73` | first re-push 2.7 → 0.1 s |
| Write new files from arriving chunks | `db76c14` | rebuild tail 1.8 s → 55 ms per 2 GB |
| One `syncfs` per install on Linux; no thread per small file; batch small files per xorb; parallel install | `5fc2afb`, `bec56a5`, `989cbf4` | one agent sync 4.4 → 2.8 s; fleet agents 2–8 30.5 → 22.7 s |
| Skip empty files; cache directory probes | `3e54e50` | fleet agents 2–8 22.7 → 13.7 s; incremental rounds 42.1 → 23.5 s |
