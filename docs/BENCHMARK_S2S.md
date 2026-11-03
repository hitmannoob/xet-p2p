# Sandbox to sandbox: xet-p2p vs rsync on Modal

**Dates:** 2026-11-01 – 2026-11-02 · **Tools:** `bench/devpair.py` (persistent pair: `up`, `deploy`,
`run`, `agents`, `rollouts`, `formats`), `bench/s2s_dataset.py`, `bench/sandbox_to_sandbox.py`,
`bench/formats_sim.py`

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

| Phase | xet-p2p | xet-p2p + incremental manifest | rsync -az | rsync -az --link-dest=/a1 |
|---|---:|---:|---:|---:|
| First sync, agent 1 | 2.8 s, 20.7 MiB | 3.3 s, 20.7 MiB | **2.1 s, 9.5 MiB** | **2.1–2.2 s, 9.5 MiB** |
| First sync, agents 2–8 | 13.4 s, 3.7 MiB | 17.5 s, 3.7 MiB | 14.8 s, 66.2 MiB | **8.7–9.2 s, 1.4 MiB** |
| Incremental rounds (24 syncs) | 24.3 s, 13.5 MiB | 25.1 s, **1.3 MiB** | **22.0 s**, 5.0 MiB | 24.6–25.4 s, 5.0 MiB |

The incremental-manifest column and the second `--link-dest` figures come from a later run on the
same pair. First syncs cannot use the delta (there is nothing to diff against yet), so the
differences in those rows are run-to-run noise.

An earlier version of this table showed xet-p2p ahead of `--link-dest`. It was wrong: in daemon
mode `--link-dest` must be absolute relative to the module, and the relative form used then was
silently ignored.

## Single-agent rollouts (10 rollouts from the same Django base)

Each rollout resets the files the previous one touched, edits 3 files, adds a test file and writes
a ~200 KB trajectory. The collector keeps each rollout as `rollout-k`.

| Per rollout | xet-p2p | + clone/link identical files | + incremental manifest | rsync -az | rsync -az --link-dest=<previous> |
|---|---:|---:|---:|---:|---:|
| Rollout 1 (cold) | 2.7–3.5 s, 21.2 MiB | 3.4 s, 21.2 MiB | 3.3 s, 21.2 MiB | **2.1 s, 9.7 MiB** | **2.1 s, 9.7 MiB** |
| Rollouts 2–10, median | 1.3–1.6 s, 611 KiB | 1.3 s, 611 KiB | **1.3 s, 88 KiB** | 2.0 s, 9.7 MiB | 1.3–1.5 s, 244 KiB |

"Clone/link identical files" is `serve --link-identical`: the receiver makes a file it already holds
at another path by hard link (copy-on-write clone where the filesystem has one; gVisor has none).

## File formats (one file each: cold, a realistic edit to the same destination, then a second destination)

`devpair.py formats`; datasets from `bench/formats_sim.py`, raw output in
[`bench/results/s2s_formats.log`](../bench/results/s2s_formats.log). xet-p2p with
`--link-identical` and the incremental manifest; both tools mirror (`--delete`). Wall time / MiB
sent.

| Format | Step | xet-p2p | rsync -az | rsync -a |
|---|---|---:|---:|---:|
| Model weights (Qwen2.5-0.5B safetensors, 943 MiB) | cold | 3.2 s / 946 | 3.6 s / **734** | 3.3 s / 943 |
| | base → the real -Instruct fine-tune | **3.7 s** / 946 | 20.9 s / 734 | 19.6 s / 943 |
| | second destination | **0.8 s / 0** | 3.3 s / 734 | 3.1 s / 943 |
| SQLite (1.2M rows) | cold | 1.4 s / 171 | 2.3 s / **102** | **0.7 s** / 182 |
| | 1% rows updated, 1% inserted | 2.1 s / 164 | 3.1 s / **64** | **1.8 s** / 115 |
| | second destination | **0.2 s / 0** | 2.4 s / 102 | 0.8 s / 182 |
| tar.gz (Django source) | cold | 0.2 s / 9.94 | 0.3 s / 9.90 | 0.3 s / 9.90 |
| | one file edited, archive rebuilt | 0.2 s / 5.55 | 0.2 s / **4.71** | 0.2 s / 4.77 |
| | second destination | **0.1 s / 0** | 0.2 s / 9.90 | 0.3 s / 9.90 |
| tar (uncompressed) | cold | 0.6 s / 16.6 | **0.4 s / 9.13** | 0.4 s / 48.4 |
| | one file edited, archive rebuilt | 0.3 s / 0.12 | 0.3 s / **0.00** | 0.2 s / 0.04 |
| | second destination | **0.1 s / 0** | 0.4 s / 9.13 | 0.3 s / 48.4 |
| CSV (4M rows) | cold | 1.5 s / 180 | 2.7 s / **106** | **0.8 s** / 208 |
| | 2% rows appended | 0.5 s / 3.76 | 0.5 s / **2.13** | 0.4 s / 4.26 |
| | second destination | **0.2 s / 0** | 2.8 s / 108 | 0.8 s / 213 |
| Raw floats (500 MB) | cold | 1.8 s / 502 | 2.1 s / 500 | **1.5 s** / 500 |
| | 2% overwritten in place | 1.5 s / 10.5 | **0.8 s / 10.0** | 0.8 s / 10.1 |
| | second destination | **0.4 s / 0** | 1.6 s / 500 | 1.5 s / 500 |

## Video and audio (all from Blender's open movie Sintel, 1080p H.264, 14:48)

Same procedure. Edits are stream copies with ffmpeg, as an editor or tagger would make them. Where
two runs measured a step, both values are shown.

| Format | Step | xet-p2p | rsync -az | rsync -a |
|---|---|---:|---:|---:|
| MP4 (1.18 GB, index at the end) | cold | 3.7–3.9 s / 1,130 | 3.4–3.9 s / 1,125 | **3.2–3.5 s** / 1,126 |
| | retitle, index moved to the front (`faststart`) | 2.1 s / 2.60 | **1.0 s / 0.44** | 1.0 s / 0.72 |
| | first minute cut | 2.0 s / 2.96 | **1.0 s / 1.05** | 1.0 s / 1.35 |
| | second destination | **0.9–1.0 s / 0** | 3.1–4.8 s / 1,031–1,125 | 3.8–5.2 s / 1,031–1,126 |
| WAV (16-bit PCM, 170 MB) | cold | 0.8 s / 163 | 1.5 s / **119** | 0.8 s / 163 |
| | 5 s intro trimmed | 0.4 s / 0.23 | 0.4 s / **0.00** | 0.4 s / 0.05 |
| | second destination | **0.2 s / 0** | 1.5 s / 119 | 0.8 s / 162 |
| FLAC (56 MB) | cold | 0.5 s / 54.1 | 0.4 s / 53.3 | 0.5 s / 53.9 |
| | retagged | 0.2 s / 0.07 | 0.3 s / **0.00** | 0.2 s / 0.04 |
| | second destination | **0.1 s / 0** | 0.3 s / 53.3 | 0.6 s / 53.9 |
| MP3 (21 MB) | cold | 0.3 s / 20.4 | 0.3 s / 20.3 | 0.3 s / 20.3 |
| | cover art embedded | **0.1 s** / 0.19 | 0.2 s / **0.13** | 0.2 s / 0.15 |
| | second destination | **0.1 s / 0** | 0.3 s / 20.5 | 0.3 s / 20.5 |
| Audio clips (178 × 5 s FLAC) | cold | 0.5 s / 54.2 | **0.3 s** / 53.3 | 0.4 s / 55.3 |
| | moved into `train/` and `val/`, 10% dropped | **0.1 s / 0.02** | 0.5 s / 48.0 | 0.4 s / 49.8 |
| | second destination | **0.1 s / 0** | 0.3 s / 48.0 | 0.3 s / 49.8 |

With the full-length FLAC already in the receiver's store, the cold clips push sent 18.7 MiB instead
of 54: the clips were encoded from the same samples, and their FLAC frames dedup against the long
file's.

## Reading the results

- **Against plain rsync**, xet-p2p wins clearly wherever the receiver already holds content: second
  destinations, rollouts, agents sharing a base (16–18× fewer bytes, up to 6× faster).
- **Against rsync `--link-dest`**, it ties on time for rollouts and incremental syncs and loses on
  agents joining a shared base.
- **Bytes on code trees: the manifest.** Without the incremental manifest, every push sends the
  full manifest, about 80 bytes per file (~545 KB for 6,800 files) even when 5 files changed, so
  xet-p2p sent more than `--link-dest`. With it (a backward-compatible protocol extension: a
  receiver advertises `manifest_delta` in `HELLO_OK`, the sender then sends only the entries that
  changed since the last manifest that receiver stored), rollouts send 88 KiB against 244 KiB and
  incremental fleet rounds 1.3 MiB against 5.0 MiB.
- **Formats where xet-p2p wins:** any content the receiver already holds (second destinations,
  moved or renamed files: 2,400× fewer bytes reorganizing a clip dataset) and wholesale changes
  such as a fine-tune (3.7 s against ~20 s: rsync's block search finds nothing and still costs
  ~17 s).
- **Formats where rsync wins:**
  - small edits inside one large file: MP4 retitle/trim, in-place float overwrite, 1.0 against
    2.0 s. rsync sends fewer boundary bytes, and xet-p2p re-chunks and re-hashes the whole file
    (not profiled yet);
  - scattered small edits: 1% of SQLite rows touches nearly every 64 KiB chunk, 164 against 64 MiB;
  - cold compressible data: lz4 (the Xet format's compression) against rsync's zlib, 1.4–2.2× the
    bytes on text and code. On weights (946 against 734 MiB) and PCM audio (163 against 119 MiB)
    the per-file lz4 skip likely also turns off upstream's byte-grouping `bg4-lz4`; to be checked.
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
