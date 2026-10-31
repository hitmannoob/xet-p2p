# xet-p2p

Direct peer-to-peer file and directory transfer built on **native Xet storage** and **QUIC**.

```
Xet (chunks + xorbs + shards + local CAS)  ↔  QUIC (pinned mutual TLS)  ↔  Xet (local CAS + reconstruction)
```

- Content-defined chunking, hashing, xorbs, shards, and reconstruction come from Hugging Face's
  [`xet-core`](https://github.com/huggingface/xet-core) (pinned `=1.8.0`). No custom formats.
- Each peer keeps an **embedded, on-disk CAS**. There is no CAS server, no cloud storage, no Hugging
  Face account. The receiver runs a QUIC listener that is part of this tool, not a CAS API.
- Only content the receiver lacks is sent — at **chunk** granularity: chunks the receiver already
  holds in any xorb (from any sender) are never re-sent, and received chunks are repacked into
  native xorbs on the receiver ([benchmark](docs/BENCHMARK_M3.md)). `--mode xorb` sends whole
  sender xorbs instead.
- A repeat push of unchanged files sends zero payload bytes
  and leaves the destination files untouched.
- Interrupted pushes resume: re-run the same command and only what is still missing is sent, even if
  the receiver process was killed.
- Every received object is verified before it is stored; every file is verified (size + SHA-256)
  before it is installed, and installation is atomic per file. A failed transfer installs nothing.

Status: **M0–M5 complete for Modal** (sandbox providers, `pull`, TLS-over-TCP fallback,
`doctor`; verified against a real Modal sandbox). E2B and Daytona are not implemented yet. See [PROGRESS.md](PROGRESS.md) for details and what is next, and
[XET_P2P_CODEX_START.md](XET_P2P_CODEX_START.md) for the full design brief.

## Build

Requires Rust 1.95 (pinned in `rust-toolchain.toml`; rustup installs it automatically).

```bash
cargo build --release
# binary: target/release/xet-p2p
cargo test            # all tests run offline over localhost
```

## Quick start

Two machines that can reach each other over UDP (direct network, VPN, or port forward). QUIC does
not traverse NAT by itself.

**1. Each peer generates an identity** (once):

```bash
# on the receiver
xet-p2p keygen --out ./certs --name receiver
# on the sender
xet-p2p keygen --out ./certs --name sender
```

Each command writes `<name>.crt` (public, share it) and `<name>.key` (private, mode `0600`, never
share it), and prints the certificate's SHA-256 fingerprint.

**2. Exchange certificates.** Copy `receiver.crt` to the sender and `sender.crt` to the receiver
by any channel you trust. Compare fingerprints out of band if the channel is not trusted.

**3. Start the receiver:**

```bash
xet-p2p serve --listen 0.0.0.0:4433 \
  --root /workspace --cas-dir /workspace/.xet-p2p/cas \
  --cert ./certs/receiver.crt --key ./certs/receiver.key \
  --allow-client ./certs/sender.crt
```

**4. Push from the sender:**

```bash
xet-p2p push ./project --to receiver.example:4433 --dest project \
  --cas-dir ~/.cache/xet-p2p \
  --cert ./certs/sender.crt --key ./certs/sender.key \
  --peer-cert ./certs/receiver.crt
```

The files land in `/workspace/project`. Push again after editing; only new content is sent:

```text
pushed 26 files (124868 logical bytes) in 0.03s
  local ingest: 26 chunks (0 new), 0 new bytes, 124868 deduped
  xorbs: 1 referenced, 0 sent, 1 reused on receiver
  payload sent: 0 bytes (0 xorb, 0 shard); UDP sent: 14439 bytes
  receiver: installed 26 files, 124868 bytes reconstructed and verified
```

## Security model

- **Mutual authentication by pinning.** The sender accepts only the exact receiver certificate given
  with `--peer-cert`; the receiver accepts only senders listed with `--allow-client`. Both directions
  verify the TLS 1.3 handshake signature, so a copied certificate without its key is useless. TLS
  verification is never disabled.
- **Destination confinement.** `--dest` is a relative path resolved inside `--root`. Absolute paths,
  `..`, empty components, backslashes, NUL bytes, and symlinks along the path are rejected. A
  destination may not overlap the receiver's CAS directory. Manifest paths get the same checks.
- **Untrusted content.** The receiver re-hashes every chunk of every xorb before storing it. It
  registers a file's reconstruction entry only after recomputing the file's Xet hash from verified
  xorbs, then checks each reconstructed file's size and SHA-256 against the manifest.
- **Least disclosure.** The sender serves only objects referenced by the manifest it is pushing, so a
  receiver cannot pull other content from the sender's CAS.
- **Per-sender scopes.** `--allow-client sender.crt=projects/alice` confines that sender to
  `<root>/projects/alice`. A bare `--allow-client sender.crt` grants the whole root. Once any scope is
  configured, every sender needs an entry.
- **Bounded resources.** Control frames, object headers, xorbs (80 MiB), and shards (256 MiB) have
  hard size limits; concurrent streams are capped by `--parallel` and in-memory xorb bytes by
  `--max-memory-mib` (default 256; at least one xorb always fits).
- **Durability.** Received xorbs and shards are fsynced on commit, installed files before their
  rename and directories after; session state is written temp-file + fsync + rename.
- Keys and certificates are never logged. Errors returned to a peer contain paths and hashes only.

Not yet covered: certificate rotation/revocation (replace the pinned file and restart).

## Sandboxes and targets

`push` and `pull` take a target URI:

| Target | What it is | Credentials |
|---|---|---|
| `quic://HOST:PORT/DEST` | A `serve` you run yourself (QUIC) | `--cert --key --peer-cert` from `keygen` |
| `tcp://HOST:PORT/DEST` | The same, over TLS on TCP (`serve --listen-tcp`) | same |
| `modal://SANDBOX_ID/abs/path` | An existing Modal sandbox | generated per session |
| `local://NAME/abs/path` | A fake sandbox (a local directory) for testing | generated per session |

```bash
xet-p2p push ./project modal://sb-abc123/workspace/project
xet-p2p pull modal://sb-abc123/workspace/results ./results
xet-p2p doctor modal://sb-abc123        # what works, what doesn't, what auto would use
xet-p2p push ./project modal://sb-abc123/workspace/project --transport quic   # fail, don't fall back
```

For sandbox targets, `xet-p2p` attaches to the **existing** sandbox (it never creates or destroys
one), uploads and starts a per-session agent, and generates a fresh key pair for each side: the
agent accepts only this client, and the client pins only this agent. The session's keys, agent
process and files are removed when the command ends; your files and the sandbox's CAS are kept.

**Transports.** `--transport auto` (default) uses QUIC when the sandbox can accept UDP, otherwise
the TLS-over-TCP fallback, and prints which one it chose and whether a provider relay is in the
path. `--transport quic` fails instead of falling back. Both use the same pinned mutual TLS and
the same Xet objects; only the wire protocol differs.

**Modal.** Modal exposes sandbox ports only through tunnels declared when the sandbox is created,
and none carry UDP, so Modal transfers use TLS over TCP through a raw TCP tunnel (relayed by
Modal). Create the sandbox with the port exposed (default 4434):

```python
sb = modal.Sandbox.create("sleep", "infinity", app=app, image=modal.Image.debian_slim(),
                          unencrypted_ports=[4434], timeout=3600)
```

The agent must be a Linux build. Cross-compile one and point at it:

```bash
pip install ziglang && cargo install cargo-zigbuild
rustup target add x86_64-unknown-linux-musl
cargo zigbuild --release --target x86_64-unknown-linux-musl -p xet-p2p
export XET_P2P_AGENT_LINUX_BIN=target/x86_64-unknown-linux-musl/release/xet-p2p
```

The adapter uses your installed Modal SDK and credentials (`XET_P2P_MODAL_PYTHON` overrides the
Python it uses). URI options: `?port=N` (default 4434), `?root=/abs` (workspace, default
`/workspace`), `?state=/abs` (agent binary and session files, default `/tmp/xet-p2p`). Modal raw
TCP tunnels accept connections from any address; the pinned mutual TLS is what protects them.
Sandbox storage is ephemeral unless you mount a Volume, so its CAS does not outlive the sandbox.

`e2b://` and `daytona://` are recognised but not implemented yet.

## Commands

| Command | Purpose |
|---|---|
| `xet-p2p keygen --out DIR --name NAME` | Create a self-signed identity |
| `xet-p2p serve ... [--listen-tcp ADDR]` | Receive pushes / serve pulls under `--root` (QUIC, optionally also TLS over TCP) |
| `xet-p2p push SRC TARGET` | Push a file or directory (`--to HOST:PORT --dest REL` = `quic://HOST:PORT/REL`) |
| `xet-p2p pull SOURCE DEST` | Pull a file or directory into a local directory |
| `xet-p2p doctor TARGET` | Inspect a sandbox: capabilities, agent, which transports work |
| `xet-p2p gc --cas-dir DIR [--max-size-mib N] [--dry-run]` | Reclaim CAS space (orphans; LRU eviction to a cap) |
| `xet-p2p local ingest SRC --cas-dir DIR [-o manifest.json]` | Offline: store into a CAS, write manifest |
| `xet-p2p local restore MANIFEST DEST --cas-dir DIR` | Offline: reconstruct from a CAS |

Run any command with `--help` for all options. Set `RUST_LOG=debug` for verbose logs.

## Mirroring with `--delete`

By default a push never removes anything on the receiver. With `push --delete`, everything under
`--dest` that is not in the source is removed, rsync-style — including a file sitting where the
source has a directory, and vice versa. Deletion happens only after every new file has been
reconstructed and verified, so a failed push deletes nothing. Symlinks are removed, never followed,
and nothing outside `--dest` (nor the CAS or session state) is ever touched.

## Resume and concurrency

The receiver keeps a durable session per (destination, manifest) under `<cas-dir>/sessions`
(`--session-dir` to move it). If a push is interrupted — network drop, sender killed, receiver
killed — run the same push again. The receiver reuses the reconstruction metadata it already has and
requests only xorbs it has not yet verified and stored. `push` reports `resumed interrupted session`
with what was saved. If the source changed in between, a new session starts, but every xorb already
stored is still reused, and so is the reconstruction metadata of every file that did not change. Sessions are removed on success and discarded after 7 days idle.

Pushes into disjoint destinations run concurrently; pushes into overlapping destinations (one inside
the other) wait for each other.

## Operations

- **Bandwidth**: `push --bwlimit-mib 20` caps payload throughput across all streams.
- **Stalled senders**: `serve --stall-timeout-secs 120` (default) aborts a transfer whose sender
  stops sending data, freeing its destination for others.
- **Disk space**: the CAS is a cache. `xet-p2p gc --cas-dir DIR` deletes content no file references
  (after a 7-day grace that protects resumable transfers); add `--max-size-mib N` to evict least
  recently used content down to a cap (it is re-fetched from a peer when next needed). `gc` needs
  the CAS to itself and refuses to run while a `serve` or `push` is using it.

## Compared with rsync

Bytes sent by the source and wall-clock time, from [docs/BENCHMARK_M4.md](docs/BENCHMARK_M4.md)
(Apple M4, localhost, no network shaping, so times reflect CPU and disk work):

| Scenario | Plain copy | rsync -a | rsync -az | **xet-p2p** | xet time | rsync -az time |
|---|---:|---:|---:|---:|---:|---:|
| Cold: 64 MiB random file | 64.0 MiB | 64.0 MiB | 64.0 MiB | **65.5 MiB** | 0.98 s | 1.38 s |
| 4 KiB inserted mid 64 MiB file | 64.0 MiB | 36 KiB | 44 KiB | **162 KiB** | **0.35 s** | 1.55 s |
| Checkpoint: 4 of 64 tensors rewritten (128 MiB) | 128.0 MiB | 8.1 MiB | 8.1 MiB | **8.7 MiB** | **0.95 s** | 3.21 s |
| Cold: 64 MiB compressible log | 64.0 MiB | 64.0 MiB | 13.0 MiB | **25.1 MiB** | 1.00 s | 1.60 s |
| 1 MiB appended to 64 MiB log | 65.0 MiB | 1056 KiB | 247 KiB | **458 KiB** | **0.72 s** | 2.06 s |
| Cold: 2000 small files | 16.7 MiB | 16.9 MiB | 4.4 MiB | **7.5 MiB** | 1.12 s | 0.65 s |
| 20 of 2000 small files edited | 16.7 MiB | 85 KiB | 80 KiB | **221 KiB** | 0.32 s | 0.16 s |
| 2000 small files, unchanged | 16.7 MiB | 75 KiB | 75 KiB | **161 KiB** | 0.27 s | 0.10 s |
| Edit pushed from a **different sender** (receiver already holds the content) | 64 MiB | n/a | n/a | **0.17 MiB** | 0.56 s | n/a |

xet-p2p counts every UDP byte (QUIC + TLS included); rsync's counts exclude SSH framing and
encryption. The last row is from [docs/BENCHMARK_M3.md](docs/BENCHMARK_M3.md): rsync only compares
against the same path, so it has no equivalent.

Over a **real network** (this Mac → a Modal sandbox, same tunnel for both tools,
[docs/BENCHMARK_M5_MODAL.md](docs/BENCHMARK_M5_MODAL.md)), bytes follow the same pattern, xet-p2p's
transfer is faster than rsync's whole command in 7 of 8 scenarios (TLS included), and end to end
(5 repetitions, medians):

| Scenario (Mac → Modal, ~145 ms RTT) | rsync -az | xet-p2p `--keep-agent` | xet-p2p (agent per command) |
|---|---:|---:|---:|
| warm, 2 of 32 checkpoint tensors rewritten (16 MiB) | 3.0 s | 3.3 s | 6.4 s |
| warm, 256 KiB appended to 8 MiB log | 2.3 s | 2.6 s | 5.8 s |
| warm, 10 of 500 small files edited | 2.1 s | 2.7 s | 6.0 s |
| cold, 500 small files | 2.7 s | 4.0 s | 7.6 s |

With `--keep-agent`, xet-p2p is 0.3–0.6 s behind rsync per command (1.3 s on cold small files):
~1 s per command goes to process start, opening the CAS and the TLS connection, not to the data.
Starting a fresh agent through Modal's API adds ~3.5 s per command.

- **xet-p2p wins** on large-file edits (3–5× faster at similar bytes) and never resends content the
  receiver holds anywhere — in other files, other destinations, or from other senders.
- **rsync wins** on compressible data (zlib vs Xet's lz4, ~2×), on tiny edits (literal bytes vs
  whole ~64 KiB chunks plus a ~10 KiB handshake), and on big trees of tiny files (mtimes vs content
  hashes in the file list).

## Transfer modes

| Mode | What moves | Best for |
|---|---|---|
| `chunk` (default) | Only chunks the receiver holds nowhere, as native compressed chunk bytes; the receiver repacks them into its own native xorbs | Everything; decisively better when content sits in shared or foreign xorbs |
| `xorb` | Whole native xorbs the receiver lacks, exactly as the sender stored them | Same-sender incremental edits (marginally smaller: no chunk lists) |

On a warm receiver, a 4 KiB insert into a 64 MiB file moves ~0.05–0.17 MiB in chunk mode
(0.08–0.27% of the file). See [docs/BENCHMARK_M3.md](docs/BENCHMARK_M3.md) for all scenarios and
exact conditions.

## Metrics

`push` prints (or emits with `--json`) logical bytes, local chunk/dedup counts, xorbs referenced/sent/reused, payload bytes
(native xorb + shard bytes), UDP bytes actually sent (all QUIC overhead included), elapsed time, and
throughput, files installed vs unchanged, xorbs already present on the receiver, and resume
savings. Whole-xorb reuse (what M1 does) and chunk-level wire dedup (M3) are different; the
numbers here are whole-xorb.

## Layout

```text
crates/
  xet-local/        Offline Xet storage: wraps upstream LocalClient; ingest, manifest,
                    verified object exchange, verified atomic reconstruction
  transport/        Connection/stream traits, identities, certificate pinning, framing
  transport-quic/   QUIC transport (Quinn), pinned mTLS
  transport-tcp/    TLS-over-TCP fallback transport, yamux-multiplexed streams
  transfer/         Control protocol; push, pull, Receiver; fetch and send cores
  providers/        Target URIs, SandboxProvider adapters (local, modal), transport policy, doctor
  cli/              xet-p2p binary
docs/UPSTREAM_AUDIT.md   What is reused from xet-core, and every divergence
```

## License

Apache-2.0, matching `xet-core`.
