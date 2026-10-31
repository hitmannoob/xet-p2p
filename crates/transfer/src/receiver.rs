//! Receiver side: decide what is missing locally, request it, verify it, reconstruct, install.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info, warn};
use transport_quic::quinn::{Connection, Endpoint, RecvStream, SendStream};
use transport_quic::{read_frame, read_payload, write_frame};
use xet_local::manifest::validate_rel_path;
use xet_local::{LocalCas, MerkleHash, PendingShard, create_confined_dir};

use crate::error::{Error, Result};
use crate::protocol::{
    MAX_CONTROL_FRAME, MAX_OBJECT_HEADER, MAX_SHARD_BYTES, MAX_XORB_BYTES, Message, ObjectHeader,
    ObjectKind, PROTOCOL_VERSION, ReceiverReport, parse_hash, to_hex,
};

const CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

pub struct Receiver {
    root: PathBuf,
    cas: Arc<LocalCas>,
    /// Objects verified concurrently (each may hold one xorb in memory).
    parallel_objects: usize,
    /// One transfer at a time: concurrent pushes into overlapping trees are not coordinated.
    busy: tokio::sync::Mutex<()>,
}

impl Receiver {
    /// `root` must exist; every destination is confined to it. The CAS must not overlap any
    /// destination (checked per transfer).
    pub fn new(root: &Path, cas: Arc<LocalCas>, parallel_objects: usize) -> Result<Self> {
        Ok(Self {
            root: std::fs::canonicalize(root).map_err(xet_local::Error::from)?,
            cas,
            parallel_objects: parallel_objects.max(1),
            busy: tokio::sync::Mutex::new(()),
        })
    }

    /// Accepts connections until the endpoint closes. Per-connection failures are logged.
    pub async fn serve(self: Arc<Self>, endpoint: Endpoint) {
        while let Some(incoming) = endpoint.accept().await {
            let this = self.clone();
            tokio::spawn(async move {
                let remote = incoming.remote_address();
                match incoming.await {
                    Ok(conn) => match this.handle(conn).await {
                        Ok(r) => info!(%remote, ?r, "transfer complete"),
                        Err(e) => warn!(%remote, "transfer failed: {e}"),
                    },
                    Err(e) => warn!(%remote, "handshake failed: {e}"),
                }
            });
        }
    }

    /// Runs one transfer on an established, authenticated connection.
    pub async fn handle(&self, conn: Connection) -> Result<ReceiverReport> {
        let (mut send, mut recv) = conn.accept_bi().await?;
        let _guard = self.busy.lock().await;
        let result = self.run(&conn, &mut send, &mut recv).await;
        match &result {
            Ok(report) => {
                write_frame(
                    &mut send,
                    &Message::Result {
                        report: report.clone(),
                    },
                )
                .await?;
            }
            Err(e) => {
                let _ = write_frame(
                    &mut send,
                    &Message::Error {
                        message: e.to_string(),
                    },
                )
                .await;
            }
        }
        let _ = send.finish();
        // Give the sender a moment to read the final frame and close first, then close regardless:
        // its keep-alives would otherwise hold the connection open indefinitely.
        let _ = tokio::time::timeout(CLOSE_GRACE, conn.closed()).await;
        conn.close(0u32.into(), b"done");
        result
    }

    async fn run(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> Result<ReceiverReport> {
        let dest = match next(recv).await? {
            Message::Hello {
                protocol_version,
                session_id,
                dest,
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    return Err(Error::Protocol(format!(
                        "unsupported protocol version {protocol_version} (receiver speaks {PROTOCOL_VERSION})"
                    )));
                }
                info!(session = %session_id, %dest, "HELLO");
                self.check_dest(&dest)?;
                dest
            }
            other => return Err(unexpected(other)),
        };
        write_frame(
            send,
            &Message::HelloOk {
                protocol_version: PROTOCOL_VERSION,
            },
        )
        .await?;

        let manifest = match next(recv).await? {
            Message::Manifest { manifest } => manifest,
            other => return Err(unexpected(other)),
        };
        manifest.validate()?;

        let mut report = ReceiverReport::default();
        let missing = self.cas.missing_content(&manifest).await?;
        let mut xorbs = missing.xorbs.clone();

        // Round 1: reconstruction metadata for files this CAS has never seen.
        let mut pending = None;
        if !missing.reconstructions.is_empty() {
            write_frame(
                send,
                &Message::Need {
                    reconstructions: to_hex(missing.reconstructions.iter().copied()),
                    xorbs: vec![],
                },
            )
            .await?;
            let shard = self.receive_shard(conn).await?;
            report.shards_received += 1;
            report.payload_bytes_received += shard.len() as u64;
            let p = PendingShard::parse(&shard, &missing.reconstructions)?;
            write_frame(
                send,
                &Message::Ack {
                    kind: ObjectKind::Shard,
                    hash: None,
                },
            )
            .await?;
            xorbs.extend(self.cas.absent_xorbs(&p.referenced_xorbs()).await?);
            pending = Some(p);
        }

        // Round 2: whole xorbs not present locally.
        if !xorbs.is_empty() {
            write_frame(
                send,
                &Message::Need {
                    reconstructions: vec![],
                    xorbs: to_hex(xorbs.iter().copied()),
                },
            )
            .await?;
            report.payload_bytes_received += self.receive_xorbs(conn, send, xorbs.clone()).await?;
            report.xorbs_received += xorbs.len();
        }

        // Only now, with every xorb verified locally, are the new file entries checked and kept.
        if let Some(p) = pending {
            self.cas.commit_shard(p).await?;
        }

        let dest_dir = create_confined_dir(&self.root, &dest)?;
        let r = self.cas.reconstruct(&manifest, &dest_dir).await?;
        report.files_installed = r.files;
        report.dirs_created = r.dirs;
        report.bytes_reconstructed = r.bytes;
        Ok(report)
    }

    /// Rejects destinations that escape the root or overlap the CAS directory.
    fn check_dest(&self, dest: &str) -> Result<()> {
        let rel = validate_rel_path(dest)?;
        let lexical = self.root.join(rel);
        let cas = self.cas.root();
        if lexical.starts_with(cas) || cas.starts_with(&lexical) {
            return Err(Error::Local(xet_local::Error::UnsafePath {
                path: dest.to_owned(),
                reason: "destination overlaps the receiver's CAS directory",
            }));
        }
        Ok(())
    }

    async fn receive_shard(&self, conn: &Connection) -> Result<Vec<u8>> {
        let mut stream = conn.accept_uni().await?;
        let header = read_header(&mut stream).await?;
        if header.kind != ObjectKind::Shard || header.hash.is_some() {
            return Err(Error::Protocol("expected a shard object".into()));
        }
        Ok(read_payload(&mut stream, header.len, MAX_SHARD_BYTES).await?)
    }

    /// Receives exactly the requested xorbs, verifying and committing each, and ACKs each one.
    async fn receive_xorbs(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        expected: BTreeSet<MerkleHash>,
    ) -> Result<u64> {
        let mut remaining = expected.len();
        let expected = Arc::new(Mutex::new(expected));
        let permits = Arc::new(Semaphore::new(self.parallel_objects));
        let mut tasks: JoinSet<Result<(MerkleHash, u64)>> = JoinSet::new();
        let mut total = 0;

        while remaining > 0 || !tasks.is_empty() {
            tokio::select! {
                permit = permits.clone().acquire_owned(), if remaining > 0 => {
                    let permit = permit.expect("semaphore never closed");
                    let mut stream = conn.accept_uni().await?;
                    remaining -= 1;
                    let (cas, expected) = (self.cas.clone(), expected.clone());
                    tasks.spawn(async move {
                        let _permit = permit;
                        let header = read_header(&mut stream).await?;
                        let hash = match (&header.kind, &header.hash) {
                            (ObjectKind::Xorb, Some(h)) => parse_hash(h)?,
                            _ => return Err(Error::Protocol("expected a xorb object".into())),
                        };
                        if !expected.lock().unwrap().remove(&hash) {
                            return Err(Error::Protocol(format!("unrequested or duplicate xorb {}", hash.hex())));
                        }
                        let bytes = read_payload(&mut stream, header.len, MAX_XORB_BYTES).await?;
                        let len = bytes.len() as u64;
                        cas.commit_xorb(hash, bytes).await?;
                        Ok((hash, len))
                    });
                }
                Some(done) = tasks.join_next(), if !tasks.is_empty() => {
                    let (hash, len) = done.map_err(|e| Error::Protocol(format!("receive task failed: {e}")))??;
                    total += len;
                    write_frame(send, &Message::Ack { kind: ObjectKind::Xorb, hash: Some(hash.hex()) }).await?;
                }
            }
        }
        Ok(total)
    }
}

async fn read_header(stream: &mut RecvStream) -> Result<ObjectHeader> {
    read_frame::<ObjectHeader>(stream, MAX_OBJECT_HEADER)
        .await?
        .ok_or_else(|| Error::Protocol("empty object stream".into()))
}

async fn next(recv: &mut RecvStream) -> Result<Message> {
    match read_frame::<Message>(recv, MAX_CONTROL_FRAME).await? {
        Some(Message::Error { message }) => Err(Error::Remote(message)),
        Some(m) => Ok(m),
        None => Err(Error::Protocol("sender closed the control stream".into())),
    }
}

fn unexpected(m: Message) -> Error {
    Error::Protocol(format!("unexpected {} from sender", m.name()))
}
