# Progress

Living log for `xet-p2p`. Spec: [XET_P2P_CODEX_START.md](XET_P2P_CODEX_START.md). Upstream findings: [docs/UPSTREAM_AUDIT.md](docs/UPSTREAM_AUDIT.md).

## Status

| Milestone | State |
|---|---|
| M0 — Upstream audit + offline Xet round trip | In progress |
| M1 — Direct QUIC push, full-xorb transport | Not started |
| M2 — Incremental transfer + durable resume | Not started |
| M3 — Chunk-level transfer optimization | Not started |
| M4 — Performance and reliability | Not started |
| M5 — Provider-agnostic sandbox integration | Not started |

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

## Open decisions

- Sender-side shard bytes: read `LocalClient`'s `shards/` dir vs. build a minimal shard via `MDBInMemoryShard` (decide in M1).

## Known gaps / risks

- `LocalClient::upload_xorb` does not verify when a footer is supplied; receiver must call
  `XorbObject::validate_xorb_object` first.
- `LocalClient` lives in upstream's `simulation` module — no stability promise. Wrapped behind `xet-local`.
