//! Manifests kept for incremental pushes ([`Message::ManifestDelta`]).
//!
//! The receiver keeps the manifests of its last transfers, by [`manifest_digest`]; the sender
//! keeps, per receiver (its pinned certificate), the last manifest that receiver stored, and
//! sends later pushes as changes to it.
//!
//! [`Message::ManifestDelta`]: crate::protocol::Message::ManifestDelta

use std::path::{Path, PathBuf};

use transport::rustls::pki_types::CertificateDer;
use xet_local::Manifest;

use crate::protocol::{decode_manifest, manifest_digest};

/// Manifests a receiver keeps (the newest [`KEEP`]).
const KEEP: usize = 16;

pub(crate) struct ReceiverManifests {
    dir: PathBuf,
}

impl ReceiverManifests {
    pub fn new(cas_root: &Path) -> Self {
        Self {
            dir: cas_root.join("manifests"),
        }
    }

    /// The encoded manifest named `digest`, if kept (and intact).
    pub fn load(&self, digest: &str) -> Option<Vec<u8>> {
        if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let bytes = std::fs::read(self.dir.join(format!("{digest}.bin"))).ok()?;
        (manifest_digest(&bytes) == digest).then_some(bytes)
    }

    /// Keeps `encoded` (best effort), dropping the oldest beyond [`KEEP`].
    pub fn save(&self, encoded: &[u8]) {
        let digest = manifest_digest(encoded);
        let _ = std::fs::create_dir_all(&self.dir);
        let tmp = self.dir.join(format!(".{digest}.tmp"));
        if std::fs::write(&tmp, encoded).is_err()
            || std::fs::rename(&tmp, self.dir.join(format!("{digest}.bin"))).is_err()
        {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        let mut kept: Vec<(std::time::SystemTime, PathBuf)> = entries
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "bin"))
            .filter_map(|e| Some((e.metadata().ok()?.modified().ok()?, e.path())))
            .collect();
        kept.sort();
        let excess = kept.len().saturating_sub(KEEP);
        for (_, p) in kept.into_iter().take(excess) {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// The sender's record of the last manifest one receiver stored.
pub(crate) struct SenderBase {
    path: PathBuf,
}

impl SenderBase {
    pub fn for_peer(cas_root: &Path, peer: Option<&CertificateDer<'_>>) -> Option<Self> {
        let peer = peer?;
        Some(Self {
            path: cas_root.join("peers").join(format!(
                "{}.manifest",
                transport::fingerprint(peer.as_ref())
            )),
        })
    }

    /// The base manifest and its digest, if recorded.
    pub fn load(&self) -> Option<(String, Manifest)> {
        let bytes = std::fs::read(&self.path).ok()?;
        let manifest = decode_manifest(&bytes).ok()?;
        Some((manifest_digest(&bytes), manifest))
    }

    /// Records `encoded` as what the receiver now holds (best effort).
    pub fn save(&self, encoded: &[u8]) {
        if let Some(dir) = self.path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let tmp = self.path.with_extension("tmp");
        if std::fs::write(&tmp, encoded).is_err() || std::fs::rename(&tmp, &self.path).is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
    }
}
