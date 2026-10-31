//! Peer identities: self-signed certificates that the other side pins by exact match.

use std::path::Path;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// The TLS server name every peer certificate carries. Peers are authenticated by pinning,
/// not by hostname, so the name on the wire is fixed regardless of address.
pub const SERVER_NAME: &str = "xet-p2p";

/// A certificate and its private key.
#[derive(Debug)]
pub struct Identity {
    pub cert: CertificateDer<'static>,
    pub key: PrivateKeyDer<'static>,
}

/// PEM-encoded output of [`generate`].
pub struct GeneratedIdentity {
    pub cert_pem: String,
    pub key_pem: String,
    pub fingerprint: String,
}

/// Generates a fresh self-signed identity (ECDSA P-256).
pub fn generate(label: &str) -> Result<GeneratedIdentity> {
    let mut names = vec![SERVER_NAME.to_owned()];
    if !label.is_empty() && label != SERVER_NAME {
        names.push(label.to_owned());
    }
    let certified =
        rcgen::generate_simple_self_signed(names).map_err(|e| Error::Cert(e.to_string()))?;
    Ok(GeneratedIdentity {
        fingerprint: fingerprint(certified.cert.der()),
        cert_pem: certified.cert.pem(),
        key_pem: certified.signing_key.serialize_pem(),
    })
}

impl Identity {
    pub fn load(cert_path: &Path, key_path: &Path) -> Result<Self> {
        Ok(Self {
            cert: load_cert(cert_path)?,
            key: PrivateKeyDer::from_pem_file(key_path)
                .map_err(|e| Error::Cert(format!("{}: {e}", key_path.display())))?,
        })
    }

    pub fn from_pem(cert_pem: &str, key_pem: &str) -> Result<Self> {
        Ok(Self {
            cert: CertificateDer::from_pem_slice(cert_pem.as_bytes())
                .map_err(|e| Error::Cert(e.to_string()))?,
            key: PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
                .map_err(|e| Error::Cert(e.to_string()))?,
        })
    }
}

/// Loads exactly one certificate from a PEM file.
pub fn load_cert(path: &Path) -> Result<CertificateDer<'static>> {
    let mut certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| Error::Cert(format!("{}: {e}", path.display())))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Cert(format!("{}: {e}", path.display())))?;
    match certs.len() {
        1 => Ok(certs.remove(0)),
        n => Err(Error::Cert(format!(
            "{}: expected 1 certificate, found {n}",
            path.display()
        ))),
    }
}

/// SHA-256 of a certificate's DER encoding, as lowercase hex.
pub fn fingerprint(cert_der: &[u8]) -> String {
    hex::encode(Sha256::digest(cert_der))
}
