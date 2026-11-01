//! Pinned mutual TLS over TCP with multiplexed streams, on localhost.

use std::net::SocketAddr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use transport::{
    Conn, Error, GeneratedIdentity, Identity, Listener, TransportKind, generate, read_frame,
    read_payload, write_frame,
};
use transport_tcp::{TcpTlsListener, tcp_connect, tcp_listen};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Ping {
    n: u32,
    text: String,
}

fn identity(g: &GeneratedIdentity) -> Identity {
    Identity::from_pem(&g.cert_pem, &g.key_pem).unwrap()
}

struct Setup {
    server: Arc<TcpTlsListener>,
    addr: SocketAddr,
    server_id: GeneratedIdentity,
    client_id: GeneratedIdentity,
}

async fn setup() -> Setup {
    let server_id = generate("receiver").unwrap();
    let client_id = generate("sender").unwrap();
    let server = Arc::new(
        tcp_listen(
            "127.0.0.1:0".parse().unwrap(),
            identity(&server_id),
            vec![identity(&client_id).cert],
        )
        .await
        .unwrap(),
    );
    let addr = server.local_addr().unwrap();
    Setup {
        server,
        addr,
        server_id,
        client_id,
    }
}

async fn dial(
    s: &Setup,
    client: &GeneratedIdentity,
    pinned: &GeneratedIdentity,
) -> transport::Result<Conn> {
    tcp_connect(s.addr, identity(client), vec![identity(pinned).cert]).await
}

fn spawn_accept(server: Arc<TcpTlsListener>) -> tokio::task::JoinHandle<transport::Result<Conn>> {
    tokio::spawn(async move { server.accept().await.unwrap().handshake.await })
}

#[tokio::test]
async fn mutual_tls_round_trip_and_streams() {
    let s = setup().await;
    let accept = spawn_accept(s.server.clone());
    let client = dial(&s, &s.client_id, &s.server_id).await.unwrap();
    let server = accept.await.unwrap().unwrap();
    assert_eq!(client.kind(), TransportKind::TlsTcp);
    assert_eq!(
        server.peer_certificate().unwrap().as_ref(),
        identity(&s.client_id).cert.as_ref()
    );

    // Bidirectional echo.
    let echo = tokio::spawn({
        let server = server.clone();
        async move {
            let (mut send, mut recv) = server.accept_bi().await.unwrap();
            let m: Ping = read_frame(&mut recv, 1024).await.unwrap().unwrap();
            write_frame(&mut send, &m).await.unwrap();
            send.shutdown().await.unwrap();
        }
    });
    let (mut send, mut recv) = client.open_bi().await.unwrap();
    let ping = Ping {
        n: 7,
        text: "hëllo".into(),
    };
    write_frame(&mut send, &ping).await.unwrap();
    assert_eq!(
        read_frame::<Ping>(&mut recv, 1024).await.unwrap().unwrap(),
        ping
    );
    assert!(
        read_frame::<Ping>(&mut recv, 1024).await.unwrap().is_none(),
        "clean EOF"
    );
    echo.await.unwrap();

    // Many concurrent unidirectional streams, in both directions, carrying distinct payloads.
    let n = 16;
    let receive = tokio::spawn({
        let server = server.clone();
        async move {
            let mut tasks = Vec::new();
            for _ in 0..n {
                let mut r = server.accept_uni().await.unwrap();
                tasks.push(tokio::spawn(async move {
                    let mut buf = Vec::new();
                    r.read_to_end(&mut buf).await.unwrap();
                    buf
                }));
            }
            let mut out = Vec::new();
            for t in tasks {
                out.push(t.await.unwrap());
            }
            out.sort();
            out
        }
    });
    let mut expected = Vec::new();
    let mut sends = Vec::new();
    for i in 0..n {
        let payload = vec![i as u8; 1024 * 1024 + i];
        expected.push(payload.clone());
        let client = client.clone();
        sends.push(tokio::spawn(async move {
            let mut w = client.open_uni().await.unwrap();
            w.write_all(&payload).await.unwrap();
            w.shutdown().await.unwrap();
        }));
    }
    for t in sends {
        t.await.unwrap();
    }
    expected.sort();
    assert_eq!(receive.await.unwrap(), expected);
    assert!(server.rx_progress() >= 16 * 1024 * 1024);
    assert!(client.wire_stats().tx >= 16 * 1024 * 1024);

    // Server-opened stream toward the client (needed for pull).
    let mut w = server.open_uni().await.unwrap();
    w.write_all(b"from server").await.unwrap();
    w.shutdown().await.unwrap();
    let mut r = client.accept_uni().await.unwrap();
    let mut got = Vec::new();
    r.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, b"from server");

    client.close(0, b"done");
    tokio::time::timeout(std::time::Duration::from_secs(5), server.closed())
        .await
        .expect("server should see the close");
}

#[tokio::test]
async fn client_rejects_unpinned_server() {
    let s = setup().await;
    let _accept = spawn_accept(s.server.clone());
    let impostor = generate("impostor").unwrap();
    let Err(err) = dial(&s, &s.client_id, &impostor).await else {
        panic!("connected to an unpinned server")
    };
    assert!(matches!(err, Error::Connection(_)), "{err:?}");
}

#[tokio::test]
async fn server_rejects_unauthorized_client() {
    let s = setup().await;
    let accept = spawn_accept(s.server.clone());
    let stranger = generate("stranger").unwrap();
    // With TLS 1.3 the client may finish its side before the server rejects its certificate,
    // so the failure can surface at connect or at the first stream operation; either way the
    // connection reports why.
    let (mut connected, mut reason) = (false, None);
    let outcome = async {
        let conn = dial(&s, &stranger, &s.server_id).await?;
        connected = true;
        let r = async {
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
        reason = conn.rejection().await;
        r
    }
    .await;
    assert!(
        !matches!(outcome, Ok(Some(_))),
        "unauthorized client got a reply: {outcome:?}"
    );
    if connected {
        let why = reason.expect("a client refused after connecting learns why");
        assert!(why.contains("rejected"), "{why}");
    } else {
        assert!(matches!(&outcome, Err(Error::Connection(_))), "{outcome:?}");
    }
    // The listener completes handshakes itself and yields only accepted sessions.
    let yielded = tokio::time::timeout(std::time::Duration::from_secs(2), accept).await;
    assert!(
        !matches!(yielded, Ok(Ok(Ok(_)))),
        "server must not accept the client"
    );
}

#[tokio::test]
async fn framing_limits_hold_over_tcp() {
    let s = setup().await;
    let accept = spawn_accept(s.server.clone());
    let client = dial(&s, &s.client_id, &s.server_id).await.unwrap();
    let server = accept.await.unwrap().unwrap();

    let mut w = client.open_uni().await.unwrap();
    write_frame(
        &mut w,
        &Ping {
            n: 1,
            text: "x".repeat(100),
        },
    )
    .await
    .unwrap();
    w.shutdown().await.unwrap();
    let mut r = server.accept_uni().await.unwrap();
    assert!(matches!(
        read_frame::<Ping>(&mut r, 16).await,
        Err(Error::FrameTooLarge { .. })
    ));

    let mut w = client.open_uni().await.unwrap();
    w.write_all(b"abcdef").await.unwrap();
    w.shutdown().await.unwrap();
    let mut r = server.accept_uni().await.unwrap();
    assert!(matches!(
        read_payload(&mut r, 4, 100).await,
        Err(Error::Decode(_))
    ));
}

#[tokio::test]
async fn close_is_immediate_even_with_unread_data() {
    let s = setup().await;
    let accept = spawn_accept(s.server.clone());
    let client = dial(&s, &s.client_id, &s.server_id).await.unwrap();
    let server = accept.await.unwrap().unwrap();

    // The server stops reading a large stream; the client then closes.
    let writer = tokio::spawn({
        let client = client.clone();
        async move {
            let mut w = client.open_uni().await.unwrap();
            let _ = w.write_all(&vec![0u8; 64 * 1024 * 1024]).await;
        }
    });
    let mut r = server.accept_uni().await.unwrap();
    let mut first = [0u8; 1024];
    r.read_exact(&mut first).await.unwrap();
    client.close(1, b"abort");
    drop(client);

    let t = std::time::Instant::now();
    tokio::time::timeout(std::time::Duration::from_secs(5), server.closed())
        .await
        .expect("server must see an abrupt close promptly");
    let mut rest = Vec::new();
    assert!(
        r.read_to_end(&mut rest).await.is_err() || rest.len() < 64 * 1024 * 1024,
        "stream must not complete after an abort"
    );
    assert!(t.elapsed() < std::time::Duration::from_secs(5));
    writer.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn lanes_join_one_session_and_carry_streams() {
    let s = setup().await;
    let accept = spawn_accept(s.server.clone());
    let client = transport_tcp::tcp_connect_lanes(
        s.addr,
        identity(&s.client_id),
        vec![identity(&s.server_id).cert],
        4,
    )
    .await
    .unwrap();
    let server = accept.await.unwrap().unwrap();
    // Only one session comes out of the listener, however many lanes join it.
    let more = spawn_accept(s.server.clone());

    // Streams opened before and after the lanes are up all arrive, in both directions.
    let send = |c: Conn, n: usize| async move {
        for i in 0..n {
            let mut w = c.open_uni().await.unwrap();
            w.write_all(&vec![i as u8; 64 * 1024]).await.unwrap();
            w.shutdown().await.unwrap();
        }
    };
    let recv = |c: Conn, n: usize| async move {
        let mut total = 0;
        for _ in 0..n {
            let mut r = c.accept_uni().await.unwrap();
            let mut buf = Vec::new();
            r.read_to_end(&mut buf).await.unwrap();
            total += buf.len();
        }
        total
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let (_, got) = tokio::join!(send(client.clone(), 32), recv(server.clone(), 32));
    assert_eq!(got, 32 * 64 * 1024);
    let (_, got) = tokio::join!(send(server.clone(), 8), recv(client.clone(), 8));
    assert_eq!(got, 8 * 64 * 1024);
    // Several TCP connections carried the traffic: the server counts them all.
    assert!(server.wire_stats().rx >= 32 * 64 * 1024);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), more)
            .await
            .is_err(),
        "lanes must not appear as new sessions"
    );

    client.close(0, b"done");
    tokio::time::timeout(std::time::Duration::from_secs(5), server.closed())
        .await
        .expect("server should see the close");
}
