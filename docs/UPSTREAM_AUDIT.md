# Upstream audit: xet-core

**Audited:** 31 October 2026
**Upstream:** https://github.com/huggingface/xet-core @ `ce8c6b254061450bca1daee4ca0df9071d93eb30` (2026-10-30)
**Published crates:** `xet-runtime`, `xet-core-structures`, `xet-client`, `xet-data` — all `=1.8.0` on crates.io
**License:** Apache-2.0
**Toolchain:** Rust 1.95.0 required (transitive `sysinfo@0.39.6`; matches upstream CI). Edition 2024.

A probe crate depending on all four crates at `=1.8.0` builds cleanly with `cargo +1.95.0 build`.

## Summary

Upstream already ships a disk-backed, offline CAS: `xet_client::cas_client::LocalClient`. It implements
the full `Client` trait that the upload and download pipelines use, and the pipelines select it
natively via a `local://` endpoint. No upstream code needs to be forked or reimplemented for M0.

## Reusable interfaces

| Need (brief §3) | Upstream interface | Notes |
|---|---|---|
| Embedded CAS | `xet_client::cas_client::LocalClient::new(ctx, path)` | Not behind a feature flag (only excluded on wasm). Layout: `xorbs/`, `shards/`, `global_dedup_lookup.redb`. |
| Config pointing at local CAS | `xet_data::processing::configurations::TranslatorConfig::local_config(ctx, base_dir)` | Endpoint becomes `local://<base_dir>`; client is `LocalClient` at `<base_dir>/xet/xorbs`. |
| CDC + hashing + xorb packing + shard creation | `FileUploadSession::new(config)` → `upload_files` / `spawn_upload_from_path` → `finalize_with_file_info()` | Returns `DeduplicationMetrics` and `Vec<MDBFileInfo>`. Dedup against local shards happens automatically. |
| `find_reconstruction(file_id)` | `Client::get_file_reconstruction_info(&hash)` → `(MDBFileInfo, Option<shard_hash>)`; `Client::get_reconstruction(&hash, range)` → V2 terms | `MDBFileInfo` segments name `(xorb_hash, chunk_start..chunk_end)`. |
| `has_xorb` | `DirectAccessClient::xorb_exists(&hash)` | |
| `read_xorb_range` / raw xorb bytes | `DirectAccessClient::get_xorb_raw_bytes(&hash, Option<FileRange>)`, `xorb_footer(&hash)`, `get_xorb_ranges(...)` | Raw bytes are the native serialized xorb, unchanged — what goes on the wire. |
| `commit_verified_xorb` | `Client::upload_xorb(prefix, SerializedXorbObject, ..)` | Atomic (temp file + rename). **Does not verify when `footer_start` is `Some`** — see below. |
| Xorb verification | `xet_core_structures::xorb_object::XorbObject::validate_xorb_object(reader, &hash)` | Re-hashes every chunk and the xorb; returns `Ok(None)` on mismatch/malformed. |
| Shard ingest | `Client::upload_shard(bytes, ..)` | Parses with `MDBMinimalShard::from_reader`, re-serializes, stores under its own hash, updates redb file→shard and chunk→shard tables. |
| `has_chunk` / `locate_chunk` | `Client::query_for_global_dedup_shard(prefix, &chunk_hash)` + `ShardFileManager` | Needed for M3; not required for M1. |
| Reconstruction | `FileDownloadSession::from_client(ctx, client, None)` → `download_file(&XetFileInfo, path)` / `download_to_writer` | Reads only from the local CAS. |

## Divergences / adapters required

1. **Receiver must verify before ingest.** `LocalClient::upload_xorb` trusts a provided footer. The
   receiver will call `XorbObject::validate_xorb_object` on every received xorb and reject mismatches
   before calling `upload_xorb`.
2. **Sender-side shard bytes.** `get_file_reconstruction_info` returns the owning shard hash, but no
   public API returns shard bytes by hash. Options: read `<cas>/shards/<shard_file_name(hash)>` (narrow,
   documented coupling to `LocalClient`'s on-disk layout), or build a minimal native shard from
   `MDBFileInfo` + xorb info with `MDBInMemoryShard`. Decide in M1.
3. **"Simulation" naming.** `LocalClient` lives in `cas_client::simulation` and is documented as a
   testing client. It is production-quality (redb, atomic writes) but upstream does not promise
   stability for it. Mitigation: pin `=1.8.0` and wrap it behind our own `xet-local` crate API.
4. **Global dedup query** is shaped like the remote API (returns whole shard bytes). Acceptable locally;
   revisit for M3.

None of these require a custom chunk, xorb, or shard format.
