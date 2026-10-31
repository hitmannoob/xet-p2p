//! Certificate pinning for both directions of mutual TLS.
//!
//! A peer is accepted only if its end-entity certificate is byte-for-byte one of the pinned
//! certificates. The TLS 1.3 handshake signature is still verified against that certificate's key,
//! so presenting a pinned certificate without its private key fails.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{
    CryptoProvider, WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature,
};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};

#[derive(Debug)]
struct Pinned {
    certs: Vec<CertificateDer<'static>>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl Pinned {
    fn new(certs: Vec<CertificateDer<'static>>, provider: &CryptoProvider) -> Self {
        Self {
            certs,
            algorithms: provider.signature_verification_algorithms,
        }
    }

    fn check(&self, end_entity: &CertificateDer<'_>) -> Result<(), rustls::Error> {
        if self.certs.iter().any(|c| c.as_ref() == end_entity.as_ref()) {
            Ok(())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer,
            ))
        }
    }
}

/// Client-side verifier: the server must present a pinned certificate.
#[derive(Debug)]
pub struct PinnedServerVerifier(Pinned);

impl PinnedServerVerifier {
    pub fn new(certs: Vec<CertificateDer<'static>>, provider: &CryptoProvider) -> Arc<Self> {
        Arc::new(Self(Pinned::new(certs, provider)))
    }
}

impl ServerCertVerifier for PinnedServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.0.check(end_entity)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.0.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.0.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.algorithms.supported_schemes()
    }
}

/// Server-side verifier: a client certificate is mandatory and must be one of the allowed set.
#[derive(Debug)]
pub struct PinnedClientVerifier(Pinned);

impl PinnedClientVerifier {
    pub fn new(certs: Vec<CertificateDer<'static>>, provider: &CryptoProvider) -> Arc<Self> {
        Arc::new(Self(Pinned::new(certs, provider)))
    }
}

impl ClientCertVerifier for PinnedClientVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.0.check(end_entity)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.0.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.0.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.algorithms.supported_schemes()
    }
}
