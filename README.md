# xet-p2p

Sync files and directories directly between two machines, like rsync, but matching on **content**
instead of **paths**.

rsync compares each file with the file of the same name on the other side. xet-p2p splits every
file into content-defined chunks (Hugging Face's [Xet](https://github.com/huggingface/xet-core)
storage format) and keeps a local store of them on both ends. Anything the receiver already holds,
under any name, in any folder, from any earlier transfer, is never sent again. Every file is
verified end to end before it is installed.

```bash
xet-p2p push ./dataset tcp://receiver:4434/dataset ...   # first time: sends everything
mv dataset/clips dataset/train                            # reorganize
xet-p2p push ./dataset tcp://receiver:4434/dataset ...   # sends a few KB, not the clips again
```

No server, no cloud storage, no Hugging Face account: just two peers and a pair of pinned
certificates. Modal sandboxes are supported out of the box.

## When it beats rsync, and when it doesn't

Measured between two Modal sandboxes (single runs; [all results](docs/BENCHMARK_S2S.md)):

| Scenario | xet-p2p | rsync |
|---|---:|---:|
| Reorganize 178 audio clips into `train/` and `val/` | **0.1 s, 0.02 MiB** | 0.5 s, 48 MiB |
| Same 1.1 GB movie to a second folder | **1.0 s, 0 MiB** | 4.8 s, 1,125 MiB |
| Model weights replaced by their fine-tune (943 MiB) | **3.7 s** | 20.9 s |
| FineWeb-Edu Parquet, first copy (4.3 GB) | **13.8 s** | 19.5 s |
| Coding-agent rollouts, per rollout (rsync with `--link-dest`) | 1.3 s, **88 KiB** | 1.3–1.5 s, 244 KiB |
| Trim the first minute off that 1.1 GB movie | 2.0 s, 3.0 MiB | **1.0 s, 1.1 MiB** |
| Update 1% of the rows of a 190 MB SQLite database | 2.1 s, 164 MiB | 3.1 s, **64 MiB** |
| 7 agents joining a shared code base (rsync with `--link-dest`) | 13–18 s, 3.7 MiB | **9 s, 1.4 MiB** |

rsync is `rsync -az` unless noted. Speed on first copies depends on the network path: on another
sandbox pair with a faster single TCP stream, rsync won the FineWeb-Edu first copy.

**Use xet-p2p** when data moves around: renamed or reorganized files, copies to several
destinations, datasets and checkpoints that grow or get replaced, many machines sharing a base.

**Use rsync** for small edits inside one large file at the same path (rsync sends only the changed
bytes; xet-p2p re-chunks the file and resends whole ~64 KiB chunks), for databases with scattered
edits, and for first copies of compressible text on slow links (rsync's zlib beats Xet's lz4 by
about 1.5–2×).

## How it works

```
 sender                                                  receiver
 ┌──────────────────────┐      pinned mutual TLS      ┌──────────────────────┐
 │ files → Xet chunks   │◄── QUIC, or TLS over TCP ──►│ Xet chunks → files   │
 │ local store (CAS)    │                             │ local store (CAS)    │
 └──────────────────────┘                             └──────────────────────┘
```

Xet is the storage layer behind the Hugging Face Hub: files are cut into ~64 KiB chunks at
content-defined boundaries (so an insertion only changes the chunks around it), packed into
**xorbs**, and described by **shards** (how to rebuild each file). Upstream, clients talk to a CAS
server. xet-p2p keeps that format unchanged (`xet-core` pinned at `=1.8.0`) and replaces the server
with a local store on each peer plus a direct peer protocol. A push:

1. **Ingest.** The sender chunks and hashes changed files into its store. Files whose size,
   timestamps and inode are unchanged are not read at all (like git's index).
2. **Handshake.** Each side checks the other's pinned certificate.
3. **Manifest.** The sender lists every file (path, size, Xet hash, SHA-256), or, after the first
   push to that receiver, only what changed since.
4. **Receiver decides** per file: unchanged, skip; content already held elsewhere, copy or clone it
   locally; otherwise ask for the file's chunk list, then only the chunks it lacks.
5. **Transfer.** Missing chunks stream over parallel streams; the receiver packs them into its own
   xorbs and records progress durably, so an interrupted push resumes where it stopped.
6. **Verify and install.** Each file is rebuilt from the store, checked against its Xet hash and
   SHA-256, and renamed into place atomically.

Details of what is reused from `xet-core`, and where this tool diverges:
[docs/UPSTREAM_AUDIT.md](docs/UPSTREAM_AUDIT.md).

## Build

Requires Rust 1.95 (pinned in `rust-toolchain.toml`; rustup installs it automatically).

```bash
cargo build --release
# binary: target/release/xet-p2p
cargo test            # all tests run offline over localhost
```

## Quick start

Two machines that can reach each other. QUIC needs UDP; if only TCP gets through (most firewalls,
tunnels, Modal), use the `tcp://` lines instead.

**1. Each peer creates an identity** (once):

```bash
xet-p2p keygen --out ./certs --name receiver   # on the receiver
xet-p2p keygen --out ./certs --name sender     # on the sender
```

Each writes `<name>.crt` (public, share it) and `<name>.key` (private, mode `0600`, never share it)
and prints the certificate's SHA-256 fingerprint.

**2. Exchange certificates.** Copy `receiver.crt` to the sender and `sender.crt` to the receiver
by any channel you trust. Compare fingerprints out of band if the channel is not trusted.

**3. Start the receiver:**

```bash
xet-p2p serve --listen 0.0.0.0:4433 --listen-tcp 0.0.0.0:4434 \
  --root /workspace --cas-dir /workspace/.xet-p2p/cas \
  --cert ./certs/receiver.crt --key ./certs/receiver.key \
  --allow-client ./certs/sender.crt
```

**4. Push from the sender:**

```bash
xet-p2p push ./project quic://receiver.example:4433/project \
  --cas-dir ~/.cache/xet-p2p \
  --cert ./certs/sender.crt --key ./certs/sender.key \
  --peer-cert ./certs/receiver.crt
# or, over TCP:
xet-p2p push ./project tcp://receiver.example:4434/project ...same options...
```

The files land in `/workspace/project`. Push again after editing one file; only its changed chunk
is sent, and the 25 other files are not even re-read:

```text
pushed 26 files (4450271 logical bytes) in 0.07s
  local ingest: 25 files unchanged since last read (4440180 bytes, not re-read); read 1 (0 incompressible, stored uncompressed): 1 chunks (1 new), 10091 new bytes, 0 deduped
  chunk mode: 1 chunks in new files, 0 already on receiver (0 bytes), 1 sent in 1 runs; 1 xorbs packed
  payload sent: 2787 bytes (0 xorb, 0 shard, 2715 chunk, 72 chunk-list); wire sent: 11752 bytes via QUIC (UDP)
  effective throughput: 62.4 MiB/s logical, 0.0 MiB/s payload
  receiver: 1 files installed, 25 unchanged; 10091 bytes reconstructed and verified
```

Rename `src/` to `lib/` and push with `--delete`: the receiver already holds every file's content,
so nothing but the file list is sent:

```text
pushed 26 files (4450271 logical bytes) in 0.05s
  ...
  payload sent: 0 bytes (0 xorb, 0 shard, 0 chunk, 0 chunk-list); wire sent: 6249 bytes via TLS over TCP (not QUIC)
  receiver: 25 files installed, 1 unchanged; 255967 bytes reconstructed and verified
  deleted on receiver: 25 files, 1 directories (--delete)
```

`xet-p2p pull` works the same way in the other direction.

## Sandboxes

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
```

For a sandbox target, `xet-p2p` attaches to the **existing** sandbox (it never creates or destroys
one), uploads and starts a small agent there, and generates a fresh key pair for each side: the
agent accepts only this client, and the client pins only this agent. When the command ends, the
session's keys, agent and files are removed; your files and the sandbox's store are kept.
`--keep-agent MINUTES` keeps the agent running so later commands skip the ~3 s startup.

**Transports.** `--transport auto` (default) uses QUIC when the sandbox accepts UDP, otherwise TLS
over TCP, and says which it chose. `--transport quic` fails instead of falling back.

**Modal.** Modal exposes sandbox ports only through tunnels declared when the sandbox is created,
and none carry UDP, so Modal transfers use TLS over TCP through a raw TCP tunnel. Create the sandbox
with the port exposed (default 4434):

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
Sandbox storage is ephemeral unless you mount a Volume, so its store does not outlive the sandbox.

E2B and Daytona (`e2b://`, `daytona://`) are recognised but not implemented yet.

## Commands

| Command | Purpose |
|---|---|
| `xet-p2p keygen --out DIR --name NAME` | Create a self-signed identity |
| `xet-p2p serve ...` | Receive pushes and serve pulls under `--root` (QUIC, and TLS over TCP with `--listen-tcp`) |
| `xet-p2p push SRC TARGET` | Push a file or directory |
| `xet-p2p pull SOURCE DEST` | Pull a file or directory into a local directory |
| `xet-p2p doctor TARGET` | Inspect a sandbox: capabilities, agent, which transports work |
| `xet-p2p gc --cas-dir DIR [--max-size-mib N] [--dry-run]` | Reclaim store space |
| `xet-p2p local ingest SRC --cas-dir DIR [-o manifest.json]` | Offline: store into a local store, write a manifest |
| `xet-p2p local restore MANIFEST DEST --cas-dir DIR` | Offline: rebuild files from a local store |

Run any command with `--help` for all options. `RUST_LOG=debug` gives verbose logs; `--json`
prints a push's numbers (bytes sent, files installed and unchanged, timings) as JSON.

## Behaviour and options

**Mirroring (`push --delete`).** By default a push never removes anything on the receiver. With
`--delete`, everything under the destination that is not in the source is removed, rsync-style.
Deletion happens only after every new file is rebuilt and verified, so a failed push deletes
nothing. Symlinks are removed, never followed; nothing outside the destination is touched.

**Resume.** If a push is interrupted (network drop, either side killed), run it again: only what is
still missing is sent. The receiver keeps a durable session per destination and manifest under
`<cas-dir>/sessions`; sessions are removed on success and after 7 days idle. If the source changed
in between, a new session starts but everything already received is reused.

**Concurrency.** Pushes into separate destinations run at the same time; pushes into overlapping
destinations wait for each other.

**Identical files on the receiver.** When the receiver already holds a file's content at another
path, it makes the new file locally: a copy-on-write clone where the filesystem supports one, or,
with `serve --link-identical`, a hard link (like rsync `--link-dest`; the two paths then share one
file, so editing one in place edits both).

**Transfer modes.** `--mode chunk` (default) sends only the chunks the receiver holds nowhere.
`--mode xorb` sends whole xorbs as the sender stored them, slightly smaller for repeated edits from
the same sender.

**Operations.**

- `push --bwlimit-mib 20` caps throughput.
- Over TLS on TCP a session opens 4 TCP connections (`XET_P2P_TCP_LANES=N`, 1–16) so parallel
  streams don't share one congestion window.
- `serve --stall-timeout-secs 120` (default) aborts a transfer whose sender stops sending.
- The store is a cache. `xet-p2p gc --cas-dir DIR` deletes content no file references (after a
  7-day grace that protects resumable transfers); `--max-size-mib N` also evicts the least recently
  used content down to a cap. `gc` refuses to run while a `serve` or `push` is using the store.

## Security model

- **Mutual authentication by pinning.** The sender accepts only the exact receiver certificate given
  with `--peer-cert`; the receiver accepts only senders listed with `--allow-client`. Both directions
  verify the TLS 1.3 handshake signature, so a copied certificate without its key is useless. TLS
  verification is never disabled.
- **Destination confinement.** The destination is a relative path inside `--root`. Absolute paths,
  `..`, empty components, backslashes, NUL bytes, and symlinks along the path are rejected, and a
  destination may not overlap the receiver's store. Paths in the manifest get the same checks.
- **Per-sender scopes.** `--allow-client sender.crt=projects/alice` confines that sender to
  `<root>/projects/alice`. A bare `--allow-client sender.crt` grants the whole root. Once any scope
  is configured, every sender needs one.
- **Untrusted content.** The receiver re-hashes every chunk it receives, recomputes each file's Xet
  hash from verified data, and checks each rebuilt file's size and SHA-256 before installing it.
- **Least disclosure.** The sender serves only content referenced by the manifest it is pushing, so
  a receiver cannot read anything else from the sender's store.
- **Bounded resources.** Messages, xorbs (80 MiB) and shards (256 MiB) have hard size limits;
  concurrent streams are capped by `--parallel`, in-memory data by `--max-memory-mib` (default 256).
- **Durability.** Received data is fsynced when committed, installed files are flushed to disk
  before they are renamed into place (one `syncfs` per install on Linux), and session state is
  written atomically.
- Keys and certificates are never logged. Errors sent to a peer contain paths and hashes only.

Not covered yet: certificate rotation or revocation (replace the pinned file and restart).

## Profiling

Both the client and the agent (also inside a sandbox) can profile themselves:

- `XET_P2P_TRACE=PATH` records every pipeline stage (this tool's and `xet-core`'s) and at exit
  writes `PATH.<pid>.json` (open in <https://ui.perfetto.dev>) and `PATH.<pid>.txt`, a per-stage
  summary of count, time and concurrency. `serve` writes it when it exits (idle, SIGTERM, Ctrl-C).
- `XET_P2P_PROFILE=PATH` samples CPU stacks into a flame graph (`.svg`) and folded stacks
  (`.folded`). Build with `RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile profiling
  --features cpu-profile`.

## Benchmarks

- [docs/BENCHMARK_S2S.md](docs/BENCHMARK_S2S.md): between two Modal sandboxes. Datasets, coding
  agents, model weights, databases, archives, video and audio. The most recent results.
- [docs/BENCHMARK_M5_MODAL.md](docs/BENCHMARK_M5_MODAL.md): a Mac to a Modal sandbox over the
  internet (an older build).
- [docs/BENCHMARK_M4.md](docs/BENCHMARK_M4.md) and [docs/BENCHMARK_M3.md](docs/BENCHMARK_M3.md):
  on one machine over localhost, which isolates CPU and disk cost.

`bench/devpair.py` keeps a pair of Modal sandboxes up for repeated runs (`up`, `deploy`, `run`,
`agents`, `rollouts`, `formats`, `down`).

## Project layout

```text
crates/
  xet-local/        Local Xet store: wraps xet-core's LocalClient; ingest, manifests,
                    verified exchange of chunks and xorbs, verified file rebuild
  transport/        Connection and stream traits, identities, certificate pinning, framing
  transport-quic/   QUIC transport (Quinn), pinned mutual TLS
  transport-tcp/    TLS-over-TCP transport, multiplexed streams over parallel connections
  transfer/         The peer protocol: push, pull, receiver, manifests
  providers/        Target URIs and sandbox adapters (local, Modal), transport choice, doctor
  cli/              The xet-p2p binary and profiler
bench/              Benchmark scripts and raw results
docs/               Benchmarks and the xet-core audit
```

## Status

Works between any two machines and with Modal sandboxes. E2B and Daytona adapters are not written
yet. [PROGRESS.md](PROGRESS.md) is the development log, with known gaps and what is next.

## License

Apache-2.0, matching `xet-core`.
