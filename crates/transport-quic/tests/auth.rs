//! Pinned mutual TLS and framing over localhost UDP.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use transport_quic::quinn::{Connection, Endpoint};
use transport_quic::{
    EndpointOptions, Error, GeneratedIdentity, Identity, client_endpoint, connect, generate,
    read_frame, read_payload, server_endpoint, write_frame,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Ping {
    n: u32,
    text: String,
}

fn identity(g: &GeneratedIdentity) -> Identity {
    Identity::from_pem(&g.cert_pem, &g.key_pem).unwrap()
}

fn cert(
    g: &GeneratedIdentity,
) -> transport_quic::quinn::rustls::pki_types::CertificateDer<'static> {
    identity(g).cert
}

fn localhost() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

struct Setup {
    server: Endpoint,
    server_id: GeneratedIdentity,
    client_id: GeneratedIdentity,
}

fn setup() -> Setup {
    let server_id = generate("receiver").unwrap();
    let client_id = generate("sender").unwrap();
    let server = server_endpoint(
        localhost(),
        identity(&server_id),
        vec![cert(&client_id)],
        &EndpointOptions::default(),
    )
    .unwrap();
    Setup {
        server,
        server_id,
        client_id,
    }
}

async fn dial(
    s: &Setup,
    client: &GeneratedIdentity,
    pinned: &GeneratedIdentity,
) -> transport_quic::Result<Connection> {
    let ep = client_endpoint(
        identity(client),
        vec![cert(pinned)],
        &EndpointOptions::default(),
        localhost(),
    )?;
    connect(&ep, s.server.local_addr().unwrap()).await
}

/// Echo one frame back on the first bidirectional stream.
fn spawn_echo(server: Endpoint) -> tokio::task::JoinHandle<transport_quic::Result<()>> {
    tokio::spawn(async move {
        let conn = server.accept().await.unwrap().await?;
        let (mut send, mut recv) = conn.accept_bi().await?;
        let msg: Ping = read_frame(&mut recv, 1024).await?.unwrap();
        write_frame(&mut send, &msg).await?;
        send.finish().unwrap();
        conn.closed().await;
        Ok(())
    })
}

#[tokio::test]
async fn mutual_tls_round_trip() {
    let s = setup();
    let server = spawn_echo(s.server.clone());
    let conn = dial(&s, &s.client_id, &s.server_id).await.unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    let ping = Ping {
        n: 7,
        text: "hëllo".into(),
    };
    write_frame(&mut send, &ping).await.unwrap();
    let back: Ping = read_frame(&mut recv, 1024).await.unwrap().unwrap();
    assert_eq!(back, ping);
    assert!(
        read_frame::<Ping>(&mut recv, 1024).await.unwrap().is_none(),
        "clean EOF"
    );
    conn.close(0u32.into(), b"done");
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn client_rejects_unpinned_server() {
    let s = setup();
    let _server = spawn_echo(s.server.clone());
    let impostor = generate("impostor").unwrap();
    // The client pins a different certificate than the one the server presents.
    let err = dial(&s, &s.client_id, &impostor).await.unwrap_err();
    assert!(matches!(err, Error::Connection(_)), "{err:?}");
}

#[tokio::test]
async fn server_rejects_unauthorized_client() {
    let s = setup();
    let server = spawn_echo(s.server.clone());
    let stranger = generate("stranger").unwrap();
    // With TLS 1.3 the client may finish its side before the server rejects its certificate,
    // so the failure can surface at connect or at the first stream operation.
    let outcome = async {
        let conn = dial(&s, &stranger, &s.server_id).await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        write_frame(
            &mut send,
            &Ping {
                n: 1,
                text: "x".into(),
            },
        )
        .await?;
        read_frame::<Ping>(&mut recv, 1024).await
    }
    .await;
    assert!(
        !matches!(outcome, Ok(Some(_))),
        "unauthorized client got a reply: {outcome:?}"
    );
    assert!(
        server.await.unwrap().is_err(),
        "server must not accept the connection"
    );
}

#[tokio::test]
async fn oversized_frame_rejected() {
    let s = setup();
    let server = tokio::spawn({
        let server = s.server.clone();
        async move {
            let conn = server.accept().await.unwrap().await.unwrap();
            let (_send, mut recv) = conn.accept_bi().await.unwrap();
            read_frame::<Ping>(&mut recv, 16).await
        }
    });
    let conn = dial(&s, &s.client_id, &s.server_id).await.unwrap();
    let (mut send, _recv) = conn.open_bi().await.unwrap();
    write_frame(
        &mut send,
        &Ping {
            n: 1,
            text: "x".repeat(100),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        server.await.unwrap(),
        Err(Error::FrameTooLarge { .. })
    ));
}

#[tokio::test]
async fn payload_length_enforced() {
    let s = setup();
    let server = tokio::spawn({
        let server = s.server.clone();
        async move {
            let conn = server.accept().await.unwrap().await.unwrap();
            let mut results = Vec::new();
            for (len, max) in [(4, 100), (4, 100), (200, 100)] {
                let mut recv = conn.accept_uni().await.unwrap();
                results.push(read_payload(&mut recv, len, max).await.map(|b| b.len()));
            }
            results
        }
    });
    let conn = dial(&s, &s.client_id, &s.server_id).await.unwrap();
    for payload in [&b"abcd"[..], &b"abcdef"[..], &[0u8; 200][..]] {
        let mut send = conn.open_uni().await.unwrap();
        send.write_all(payload).await.unwrap();
        send.finish().unwrap();
    }
    let results = server.await.unwrap();
    assert_eq!(results[0].as_ref().unwrap(), &4);
    assert!(
        matches!(results[1], Err(Error::Decode(_))),
        "trailing bytes: {:?}",
        results[1]
    );
    assert!(matches!(results[2], Err(Error::FrameTooLarge { .. })));
}
