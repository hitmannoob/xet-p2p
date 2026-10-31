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

Status: **M0–M3 complete** (offline round trip; authenticated QUIC push; incremental install,
durable resume, concurrent pushes, scopes, `--delete`; chunk-level transfer with native repacking). See [PROGRESS.md](PROGRESS.md) for details and what is next, and
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

## Commands

| Command | Purpose |
|---|---|
| `xet-p2p keygen --out DIR --name NAME` | Create a self-signed identity |
| `xet-p2p serve ...` | Receive pushes into `--root` |
| `xet-p2p push SRC --to HOST:PORT --dest REL ...` | Push a file or directory |
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
  transport-quic/   Quinn endpoints, pinned mTLS, bounded framing (knows nothing of Xet)
  transfer/         Control protocol, push (sender) and Receiver
  cli/              xet-p2p binary
docs/UPSTREAM_AUDIT.md   What is reused from xet-core, and every divergence
```

## License

Apache-2.0, matching `xet-core`.
