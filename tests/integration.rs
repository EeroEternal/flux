//! End-to-end integration tests for xFlux.
//!
//! These spin up mock gateway backends, start a real xFlux server on an
//! ephemeral port, and exercise the behaviors the design doc calls out:
//! pass-through, half-close, long streams, connection reuse, sudden backend
//! death and least-connections scheduling.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::{self, pki_types::ServerName};
use tokio_rustls::TlsConnector;
use xflux::config::Config;
use xflux::server::XFluxServer;

type Handler = Arc<dyn Fn(TcpStream) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Spawn a raw TCP backend. `conns` counts accepted connections.
async fn spawn_backend(handler: Handler, conns: Arc<AtomicUsize>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            conns.fetch_add(1, Ordering::SeqCst);
            let handler = handler.clone();
            tokio::spawn(async move { handler(socket).await });
        }
    });
    addr
}

async fn start_xflux(mode: &str, backends: &[SocketAddr]) -> XFluxServer {
    start_xflux_cfg(mode, backends, "leastconn", 0).await
}

async fn start_xflux_cfg(
    mode: &str,
    backends: &[SocketAddr],
    algorithm: &str,
    max_retries: u32,
) -> XFluxServer {
    let mut servers = String::new();
    for addr in backends {
        servers.push_str(&format!(
            "[[backends.servers]]\naddr = \"{addr}\"\nweight = 1\n"
        ));
    }
    let text = format!(
        r#"
[server]
bind = "127.0.0.1:0"
mode = "{mode}"

[timeouts]
connect = "2s"
idle = "3s"
total = "10s"
fin = "1s"

[pool]
max_idle_per_backend = 8
idle_timeout = "5s"

[backends]
algorithm = "{algorithm}"
health_interval = "0s"

{servers}

[retry]
max_retries = {max_retries}

[metrics]
enabled = false
"#
    );
    let config = Config::from_toml(&text).unwrap();
    XFluxServer::start(config).await.unwrap()
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// A backend that echoes every byte it receives.
fn echo_backend() -> Handler {
    Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = [0u8; 1024];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        if socket.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                }
            }
        })
    })
}

fn write_temp_file(name: &str, contents: &str) -> String {
    let dir = std::env::temp_dir().join(format!("xflux-it-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    path.to_string_lossy().into_owned()
}

async fn start_xflux_tls(backend: SocketAddr, cert: &str, key: &str) -> XFluxServer {
    let text = format!(
        r#"
[server]
bind = "127.0.0.1:0"
mode = "thin-l7"

[server.tls]
enabled = true
cert = "{cert}"
key = "{key}"

[timeouts]
connect = "2s"
idle = "5s"
total = "20s"
fin = "1s"

[pool]
max_idle_per_backend = 8
idle_timeout = "5s"

[backends]
algorithm = "leastconn"
health_interval = "0s"

[[backends.servers]]
addr = "{backend}"
weight = 1

[retry]
max_retries = 0

[metrics]
enabled = false
"#
    );
    let config = Config::from_toml(&text).unwrap();
    XFluxServer::start(config).await.unwrap()
}

/// Minimal test HTTP client that preserves read-ahead between responses.
struct TestClient {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            buf: Vec::new(),
        }
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
    }

    async fn read_response(&mut self) -> String {
        loop {
            if let Some(end) = find(&self.buf, b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&self.buf[..end]).to_string();
                let content_length = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        if name.eq_ignore_ascii_case("content-length") {
                            value.trim().parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                let total = end + 4 + content_length;
                if self.buf.len() >= total {
                    let response = self.buf.drain(..total).collect::<Vec<_>>();
                    return String::from_utf8_lossy(&response).to_string();
                }
            }
            let mut tmp = [0u8; 4096];
            let n = self.stream.read(&mut tmp).await.unwrap();
            assert!(n > 0, "unexpected eof before a full response");
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }
}

async fn drain_fixed_body(socket: &mut TcpStream, buf: &mut Vec<u8>, head: &[u8]) {
    let head_text = String::from_utf8_lossy(head);
    let len = head_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    while buf.len() < len {
        let mut tmp = [0u8; 1024];
        match socket.read(&mut tmp).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    }
    let take = len.min(buf.len());
    buf.drain(..take);
}

async fn read_http_head(socket: &mut TcpStream, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    loop {
        if let Some(end) = find(buf, b"\r\n\r\n") {
            let head = buf[..end + 4].to_vec();
            buf.drain(..end + 4);
            return Some(head);
        }
        let mut tmp = [0u8; 1024];
        match socket.read(&mut tmp).await {
            Ok(0) => return None,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => return None,
        }
    }
}

fn http_backend(body: &'static str, keep_alive: bool) -> Handler {
    Arc::new(move |mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            loop {
                if read_http_head(&mut socket, &mut buf).await.is_none() {
                    return;
                }
                let connection = if keep_alive { "keep-alive" } else { "close" };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n{}",
                    body.len(),
                    connection,
                    body
                );
                if socket.write_all(response.as_bytes()).await.is_err() {
                    return;
                }
                if !keep_alive {
                    let _ = socket.shutdown().await;
                    return;
                }
            }
        })
    })
}

#[tokio::test]
async fn l4_passes_bytes_through() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = [0u8; 5];
            if socket.read_exact(&mut buf).await.is_err() {
                return;
            }
            let _ = socket.write_all(b"world").await;
            let _ = socket.shutdown().await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux("l4", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client.write_all(b"hello").await.unwrap();
    let mut out = Vec::new();
    client.read_to_end(&mut out).await.unwrap();
    assert_eq!(&out, b"world");
    server.stop().await;
}

#[tokio::test]
async fn l4_supports_half_close() {
    let conns = Arc::new(AtomicUsize::new(0));
    // Backend only replies after it sees the client's FIN (read_to_end).
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            if socket.read_to_end(&mut buf).await.is_err() {
                return;
            }
            let _ = socket.write_all(&buf).await;
            let _ = socket.shutdown().await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux("l4", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client.write_all(b"ping").await.unwrap();
    client.shutdown().await.unwrap();
    let mut out = Vec::new();
    client.read_to_end(&mut out).await.unwrap();
    assert_eq!(&out, b"ping");
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_forwards_and_reuses_backend_connection() {
    let conns = Arc::new(AtomicUsize::new(0));
    let backend = spawn_backend(http_backend("pong", true), conns.clone()).await;
    let server = start_xflux("thin-l7", &[backend]).await;

    let mut client = TestClient::connect(server.addr).await;
    client.send(b"GET /a HTTP/1.1\r\nHost: xflux\r\n\r\n").await;
    let first = client.read_response().await;
    assert!(first.contains("200 OK"));
    assert!(first.ends_with("pong"));

    client.send(b"GET /b HTTP/1.1\r\nHost: xflux\r\n\r\n").await;
    let second = client.read_response().await;
    assert!(second.contains("200 OK"));
    assert!(second.ends_with("pong"));

    // Two requests, one backend connection.
    assert_eq!(conns.load(Ordering::SeqCst), 1);
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_handles_pipelined_requests() {
    let conns = Arc::new(AtomicUsize::new(0));
    let backend = spawn_backend(http_backend("ok", true), conns.clone()).await;
    let server = start_xflux("thin-l7", &[backend]).await;

    let mut client = TestClient::connect(server.addr).await;
    client
        .send(b"GET /1 HTTP/1.1\r\nHost: xflux\r\n\r\nGET /2 HTTP/1.1\r\nHost: xflux\r\n\r\n")
        .await;
    let first = client.read_response().await;
    let second = client.read_response().await;
    assert!(first.ends_with("ok"));
    assert!(second.ends_with("ok"));
    assert_eq!(conns.load(Ordering::SeqCst), 1);
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_streams_chunked_response_without_buffering() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            if read_http_head(&mut socket, &mut buf).await.is_none() {
                return;
            }
            let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n";
            if socket.write_all(head.as_bytes()).await.is_err() {
                return;
            }
            for i in 0..5u32 {
                let chunk = format!("data: {i}\n\n");
                let frame = format!("{:x}\r\n{}\r\n", chunk.len(), chunk);
                if socket.write_all(frame.as_bytes()).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
            let _ = socket.write_all(b"0\r\n\r\n").await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"POST /v1/chat HTTP/1.1\r\nHost: xflux\r\nContent-Length: 2\r\n\r\n{}")
        .await
        .unwrap();

    let started = Instant::now();
    let mut received = Vec::new();
    let mut buf = [0u8; 4096];
    let mut first_byte_at = None;
    loop {
        let n = client.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        if first_byte_at.is_none() {
            first_byte_at = Some(started.elapsed());
        }
        received.extend_from_slice(&buf[..n]);
        if find(&received, b"\r\n\r\n").is_some() && received.ends_with(b"0\r\n\r\n") {
            break;
        }
    }
    let elapsed = started.elapsed();
    let text = String::from_utf8_lossy(&received);
    for i in 0..5 {
        assert!(text.contains(&format!("data: {i}")));
    }
    // The first byte must arrive well before the full stream (5 * 40ms).
    assert!(first_byte_at.unwrap() < Duration::from_millis(35 + 60));
    assert!(elapsed >= Duration::from_millis(150));
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_survives_backend_disconnect() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            if read_http_head(&mut socket, &mut buf).await.is_none() {
                return;
            }
            // Promise 100 bytes but only deliver 4, then die.
            let head = "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(b"oops").await;
            let _ = socket.shutdown().await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: xflux\r\n\r\n")
        .await
        .unwrap();
    let mut out = Vec::new();
    // The proxy must not hang: it detects truncation and closes.
    let read = tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut out))
        .await
        .expect("proxy hung on truncated response")
        .unwrap();
    assert!(read >= 4);
    assert!(String::from_utf8_lossy(&out).ends_with("oops"));
    server.stop().await;
}

#[tokio::test]
async fn l4_schedules_least_connections() {
    let conns = Arc::new(AtomicUsize::new(0));
    // A backend that holds the connection open for a while.
    let slow: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = [0u8; 1];
            let _ = socket.read(&mut buf).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = socket.write_all(b"slow").await;
        })
    });
    // A backend that answers immediately.
    let fast: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = [0u8; 1];
            let _ = socket.read(&mut buf).await;
            let _ = socket.write_all(b"fast").await;
        })
    });
    let slow_addr = spawn_backend(slow, conns.clone()).await;
    let fast_addr = spawn_backend(fast, conns.clone()).await;
    let server = start_xflux("l4", &[slow_addr, fast_addr]).await;

    // First connection is pinned to whichever backend leastconn picks first.
    let mut a = TcpStream::connect(server.addr).await.unwrap();
    a.write_all(b"x").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second connection must go to the idle backend and answer immediately.
    let mut b = TcpStream::connect(server.addr).await.unwrap();
    b.write_all(b"y").await.unwrap();
    let mut buf = [0u8; 4];
    let n = tokio::time::timeout(Duration::from_millis(300), b.read(&mut buf))
        .await
        .expect("second stream was scheduled onto the busy backend")
        .unwrap();
    assert_eq!(&buf[..n], b"fast");

    let mut slow_buf = [0u8; 4];
    let n = a.read(&mut slow_buf).await.unwrap();
    assert_eq!(&slow_buf[..n], b"slow");
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_falls_back_on_http2_preface() {
    let conns = Arc::new(AtomicUsize::new(0));
    let backend = spawn_backend(echo_backend(), conns.clone()).await;
    let server = start_xflux("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    let preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
    client.write_all(preface).await.unwrap();
    client.shutdown().await.unwrap();
    let mut out = Vec::new();
    client.read_to_end(&mut out).await.unwrap();
    assert_eq!(&out, preface);
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_falls_back_on_binary_protocol() {
    let conns = Arc::new(AtomicUsize::new(0));
    let backend = spawn_backend(echo_backend(), conns.clone()).await;
    let server = start_xflux("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    // Looks like a TLS record header, definitely not an HTTP method.
    let payload = [0x16u8, 0x03, 0x01, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05];
    client.write_all(&payload).await.unwrap();
    client.shutdown().await.unwrap();
    let mut out = Vec::new();
    client.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, payload);
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_falls_back_on_upgrade() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let response = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\nhi";
            let _ = socket.write_all(response).await;
            let _ = socket.shutdown().await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(
            b"GET /ws HTTP/1.1\r\nHost: xflux\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
        )
        .await
        .unwrap();
    let mut out = Vec::new();
    let mut tmp = [0u8; 512];
    loop {
        let n = client.read(&mut tmp).await.unwrap();
        if n == 0 {
            break;
        }
        out.extend_from_slice(&tmp[..n]);
    }
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("101 Switching Protocols"));
    assert!(text.ends_with("hi"));
    server.stop().await;
}

const CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDSTCCAjGgAwIBAgIUDtiHzyE20GEWTuT6zNUexhA+NrkwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MTAxNDEyMjUwMloXDTM2MTAx
MTEyMjUwMlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAzJj8NJ3OgEKyQ7lQnXgsFEI2EGmguQcBEFX9GlbQ75/4
WgjFv9RGkVEmWlYX399/9MLx7XVHjrAhAPV2Vyd6wyhGn04VDMtSNTu9JheF3Ltm
vNjv5UNjepLdW0a8J8d9GcQx12NWT3KPAKyYW8i2CrCAMMzv/+tmHhiV+NdgPv9g
ExVAvdVLNfIe9KyPcOG+4BolLT7leO65bQ/nW35q+/NuhfVNFkZ0WqhmMtld4FNQ
T2FvfZYTeT1YvDg7n5wFDTuQk48JxkydQbva9cBQDUiTUwrSOXkvB8583Cq3F0vV
NSf/iR8dw4fRT0om7aCeYDAeT3IN4T6nRBq2RGRoSQIDAQABo4GSMIGPMB0GA1Ud
DgQWBBR6akSUxLU85oh5Kll6tQ2RaoYeTzAfBgNVHSMEGDAWgBR6akSUxLU85oh5
Kll6tQ2RaoYeTzAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwDAYDVR0TAQH/
BAIwADAOBgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDQYJKoZI
hvcNAQELBQADggEBAAUBUT3iscZzZ6228q9e3fz9KTU+DtMgWUpn04CRNrBNRmjd
mpD89sRSk8egL+XJ9ZZVmkLatYJqjTdfxeEsupaRfjKS4B7OiaEwxdogHSwchyd3
7bGhg7eU4KkvvYd0nQubac0UKq7wt0SnbCP+HDsIThUbHCSO11nMX0T44hwvsEZq
5iRq8GaNypKoRppBjyKhyV3tRaHIJaRqnZd6DkiB6u4oxLFEUBB/PLHx3fQM/1HP
NrJUxpaVyCSxNzQMzmuPciN+8dxM9DhjanmD4rIaNIu+9dSrXJhE8Zj4XhM0IfYM
IExpanPDRqSn6CaiHaggIXW5sWo8Fh4FdrBBDMM=
-----END CERTIFICATE-----
"#;

const KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDMmPw0nc6AQrJD
uVCdeCwUQjYQaaC5BwEQVf0aVtDvn/haCMW/1EaRUSZaVhff33/0wvHtdUeOsCEA
9XZXJ3rDKEafThUMy1I1O70mF4Xcu2a82O/lQ2N6kt1bRrwnx30ZxDHXY1ZPco8A
rJhbyLYKsIAwzO//62YeGJX412A+/2ATFUC91Us18h70rI9w4b7gGiUtPuV47rlt
D+dbfmr7826F9U0WRnRaqGYy2V3gU1BPYW99lhN5PVi8ODufnAUNO5CTjwnGTJ1B
u9r1wFANSJNTCtI5eS8HznzcKrcXS9U1J/+JHx3Dh9FPSibtoJ5gMB5Pcg3hPqdE
GrZEZGhJAgMBAAECggEAFd6tB91N+WsXtdJJZhm2QYCd2ZWG5PxVDmdYp5/7079y
a0JYjAOveuFVI48kHG2+Nl4/wMmaaWvVjU4PtSt+zX1gxi7RJHXX3uGYu4mEaTa9
ecaJJyUzzSKFh5gZLPnfmxlcGAlR7Kpi05g/bYE4B068VysS5PTnTKSFZGz2SGlw
CeoQHa6icdoOgExJZPWeI37earGXyDZCuY10hoB+50B28GUmC0o9Ag6QJFSkeLSr
0772yGq8b4aqr4SoDVAvrD7jtCXkZYRcaIHf+Ln+q45qyhI6FPHZf30j7WZ9U+T0
Gpa3Lh4SsUGFG78azdvFNQ5xrdxirTsrrooR6wS3wQKBgQD3oO61MuFT6cyDgUkP
LqWGZomHuZ6zuDOHDH/EWkD4CTJHO7Ib0ZzdkZpcptbYRdDnb1YfEUEwGdd2pRGa
Qs/bu8b+OzqzHuz/cJ+hd9YF8I7jqBBdwnpZpMceI6oNmzcb0vXZCtMqDDbTwQaH
m6GMlHYFzGvrOkoVtg5blImPCQKBgQDTg6WEu3EDRT164Ky/7l4lxQgAOLaCcEi1
buB7zoo3ZXgOIWrgTwHy+TdlqbWPRS8/3OLh5vA/oWjq7C+tQ1YanwCvc1FaoGu8
5tteuKVsOBDTLxPUJuUXQMYBkMvzihzKigVkp7fuUFyC2gPs5zkaQLcKU4+XhKa5
qaXHUD0fQQKBgH3yf5bvwwWAE8ILHtK3Af+C5Y5/zYVROfwFUnLXCNZfLxBl/2gA
zA0xwlY/kWS81czwptf0WbiSXDF/MZ3JP5leP48NJwT1xMsDSF3C6rtop/SIhZJ6
mOpBUfk1YIe9G2FughabV926CqTXN2//q0IqPIEkgPtKUWYLi+/uc6ypAoGAVDgT
/NDhw+qa+0Vgo5+XDzTv5Z6vVSq+WLfIXKO1ty8I6Z92vc/Xt9Zxr2Iur9hdYvFh
yY851CxB6WCX70Zjj4AGNtYU65/6piHJ8WJWQ8vjLyOMoqnGCEc7UD5GZzd0FVV1
pkl93D9VRqjmbTr4KrLf7xWslCk2dbDOGJmjbMECgYEA3gPEiHj2ykGh+xL/iX/i
o0C8WK9xtrKCoVio4pTk9x94PpvdSo9DVhACnh13ez2d4pTDlaymxWFVlakevoqE
djUZI5wZQvb14+4jLJ4Q1EjK+iy8T7X90Anum37I9JTSNsYunyYBajmAD86AmHP4
0jJbpriZvMqyNI9YRl4YGH4=
-----END PRIVATE KEY-----
"#;

#[tokio::test]
async fn tls_termination_serves_http_and_reuses_backend() {
    let conns = Arc::new(AtomicUsize::new(0));
    let backend = spawn_backend(http_backend("secure", true), conns.clone()).await;
    let cert_path = write_temp_file("cert.pem", CERT_PEM);
    let key_path = write_temp_file("key.pem", KEY_PEM);
    let server = start_xflux_tls(backend, &cert_path, &key_path).await;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut std::io::BufReader::new(CERT_PEM.as_bytes())) {
        roots.add(cert.unwrap()).unwrap();
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(server.addr).await.unwrap();
    let domain = ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(domain, tcp).await.unwrap();

    tls.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    loop {
        let mut tmp = [0u8; 1024];
        let n = tls.read(&mut tmp).await.unwrap();
        if n == 0 {
            break;
        }
        response.extend_from_slice(&tmp[..n]);
        if find(&response, b"secure").is_some() {
            break;
        }
    }
    let text = String::from_utf8_lossy(&response);
    assert!(text.contains("200 OK"), "unexpected response: {text}");
    assert!(text.ends_with("secure"));
    assert_eq!(conns.load(Ordering::SeqCst), 1);
    server.stop().await;
}

fn chunk(data: &str) -> Vec<u8> {
    let mut out = format!("{:x}\r\n", data.len()).into_bytes();
    out.extend_from_slice(data.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

async fn read_until(stream: &mut TcpStream, needle: &[u8]) -> Vec<u8> {
    let mut received = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf))
            .await
            .expect("timed out waiting for backend bytes")
            .unwrap();
        assert!(n > 0, "eof before finding {needle:?}");
        received.extend_from_slice(&buf[..n]);
        if find(&received, needle).is_some() {
            return received;
        }
    }
}

/// `[DONE]` is payload. xFlux must keep splicing until the HTTP chunked terminator.
#[tokio::test]
async fn thin_l7_treats_sse_done_as_opaque_payload() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut leftover = Vec::new();
            for i in 0..2 {
                let req_head = match read_http_head(&mut socket, &mut leftover).await {
                    Some(head) => head,
                    None => return,
                };
                drain_fixed_body(&mut socket, &mut leftover, &req_head).await;
                if i == 0 {
                    let head = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n";
                    if socket.write_all(head).await.is_err() {
                        return;
                    }
                    for part in [
                        "data: hello\n\n",
                        "data: [DONE]\n\n",
                        "data: still-going\n\n",
                    ] {
                        if socket.write_all(&chunk(part)).await.is_err() {
                            return;
                        }
                    }
                    let _ = socket.write_all(b"0\r\n\r\n").await;
                } else {
                    let body = b"ok";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                        body.len(),
                        std::str::from_utf8(body).unwrap()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                }
            }
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"POST /v1/chat HTTP/1.1\r\nHost: xflux\r\nContent-Length: 2\r\n\r\n{}")
        .await
        .unwrap();
    let first = read_until(&mut client, b"0\r\n\r\n").await;
    let text = String::from_utf8_lossy(&first);
    assert!(text.contains("data: hello"), "{text}");
    assert!(text.contains("data: [DONE]"), "{text}");
    assert!(
        text.contains("data: still-going"),
        "[DONE] must not end the HTTP message: {text}"
    );

    client
        .write_all(b"GET /next HTTP/1.1\r\nHost: xflux\r\n\r\n")
        .await
        .unwrap();
    let second = read_until(&mut client, b"ok").await;
    assert!(String::from_utf8_lossy(&second).contains("ok"));
    assert_eq!(
        conns.load(Ordering::SeqCst),
        1,
        "pooling must wait for the HTTP chunked terminator, not [DONE]"
    );
    server.stop().await;
}

/// Request JSON is forwarded verbatim and must not affect backend selection.
#[tokio::test]
async fn thin_l7_forwards_json_body_and_ignores_it_for_scheduling() {
    let a_hits = Arc::new(AtomicUsize::new(0));
    let b_hits = Arc::new(AtomicUsize::new(0));
    let make = |hits: Arc<AtomicUsize>| -> Handler {
        Arc::new(move |mut socket| {
            let hits = hits.clone();
            Box::pin(async move {
                let mut buf = Vec::new();
                let head = match read_http_head(&mut socket, &mut buf).await {
                    Some(head) => head,
                    None => return,
                };
                drain_fixed_body(&mut socket, &mut buf, &head).await;
                hits.fetch_add(1, Ordering::SeqCst);
                let body = b"ok";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    std::str::from_utf8(body).unwrap()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            })
        })
    };
    let backend_a = spawn_backend(make(a_hits.clone()), Arc::new(AtomicUsize::new(0))).await;
    let backend_b = spawn_backend(make(b_hits.clone()), Arc::new(AtomicUsize::new(0))).await;
    let server = start_xflux_cfg("thin-l7", &[backend_a, backend_b], "roundrobin", 0).await;

    for model in ["alpha", "beta"] {
        let mut client = TcpStream::connect(server.addr).await.unwrap();
        let json = format!(r#"{{"model":"{model}","stream":true}}"#);
        let req = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: xflux\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            json.len(),
            json
        );
        client.write_all(req.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut out))
            .await
            .expect("proxy hung");
        assert!(String::from_utf8_lossy(&out).contains("ok"));
    }

    assert_eq!(
        a_hits.load(Ordering::SeqCst),
        1,
        "roundrobin should split regardless of JSON model"
    );
    assert_eq!(
        b_hits.load(Ordering::SeqCst),
        1,
        "roundrobin should split regardless of JSON model"
    );
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_echoes_request_body_verbatim() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            let head = match read_http_head(&mut socket, &mut buf).await {
                Some(head) => head,
                None => return,
            };
            let head_text = String::from_utf8_lossy(&head);
            let len = head_text
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("content-length") {
                        value.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            while buf.len() < len {
                let mut tmp = [0u8; 1024];
                let n = socket.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            buf.truncate(len);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                buf.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.write_all(&buf).await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux("thin-l7", &[backend]).await;

    let payload = br#"{"model":"should-not-parse","messages":[{"role":"user","content":"data: [DONE]\n\n"}]}"#;
    let mut client = TcpStream::connect(server.addr).await.unwrap();
    let req = format!(
        "POST /v1/chat HTTP/1.1\r\nHost: xflux\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    client.write_all(req.as_bytes()).await.unwrap();
    client.write_all(payload).await.unwrap();
    let mut out = Vec::new();
    client.read_to_end(&mut out).await.unwrap();
    let idx = find(&out, b"\r\n\r\n").expect("missing head");
    assert_eq!(&out[idx + 4..], payload);
    server.stop().await;
}

/// A truncated response after the first byte must not be retried on another backend.
#[tokio::test]
async fn thin_l7_does_not_retry_after_first_response_byte() {
    let a_hits = Arc::new(AtomicUsize::new(0));
    let b_hits = Arc::new(AtomicUsize::new(0));
    let dying: Handler = {
        let a_hits = a_hits.clone();
        Arc::new(move |mut socket| {
            let a_hits = a_hits.clone();
            Box::pin(async move {
                let mut buf = Vec::new();
                if read_http_head(&mut socket, &mut buf).await.is_none() {
                    return;
                }
                a_hits.fetch_add(1, Ordering::SeqCst);
                let head =
                    b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\nConnection: close\r\n\r\npartial";
                let _ = socket.write_all(head).await;
                let _ = socket.shutdown().await;
            })
        })
    };
    let healthy: Handler = {
        let b_hits = b_hits.clone();
        Arc::new(move |mut socket| {
            let b_hits = b_hits.clone();
            Box::pin(async move {
                let mut buf = Vec::new();
                if read_http_head(&mut socket, &mut buf).await.is_none() {
                    return;
                }
                b_hits.fetch_add(1, Ordering::SeqCst);
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nfull")
                    .await;
            })
        })
    };
    let a = spawn_backend(dying, Arc::new(AtomicUsize::new(0))).await;
    let b = spawn_backend(healthy, Arc::new(AtomicUsize::new(0))).await;
    let server = start_xflux_cfg("thin-l7", &[a, b], "roundrobin", 2).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: xflux\r\n\r\n")
        .await
        .unwrap();
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut out))
        .await
        .expect("proxy hung on truncated response");
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("partial"), "{text}");
    assert!(!text.contains("full"), "retried after first byte: {text}");
    assert_eq!(a_hits.load(Ordering::SeqCst), 1);
    assert_eq!(
        b_hits.load(Ordering::SeqCst),
        0,
        "must not redispatch after response bytes have flowed"
    );
    server.stop().await;
}

/// Connect failures may redispatch to another backend before any response byte.
#[tokio::test]
async fn thin_l7_redispatches_on_connect_failure() {
    let live_hits = Arc::new(AtomicUsize::new(0));
    let live: Handler = {
        let live_hits = live_hits.clone();
        Arc::new(move |mut socket| {
            let live_hits = live_hits.clone();
            Box::pin(async move {
                let mut buf = Vec::new();
                if read_http_head(&mut socket, &mut buf).await.is_none() {
                    return;
                }
                live_hits.fetch_add(1, Ordering::SeqCst);
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .await;
            })
        })
    };
    let dead_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let live_addr = spawn_backend(live, Arc::new(AtomicUsize::new(0))).await;
    let server = start_xflux_cfg("thin-l7", &[dead, live_addr], "leastconn", 2).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: xflux\r\n\r\n")
        .await
        .unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut out))
        .await
        .expect("did not redispatch to the live backend")
        .unwrap();
    assert!(
        String::from_utf8_lossy(&out).contains("ok"),
        "{}",
        String::from_utf8_lossy(&out)
    );
    assert_eq!(live_hits.load(Ordering::SeqCst), 1);
    server.stop().await;
}

/// 1xx is HTTP framing and must be forwarded; the connection is still reusable.
#[tokio::test]
async fn thin_l7_forwards_informational_then_final() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut leftover = Vec::new();
            for i in 0..2 {
                let req_head = match read_http_head(&mut socket, &mut leftover).await {
                    Some(head) => head,
                    None => return,
                };
                drain_fixed_body(&mut socket, &mut leftover, &req_head).await;
                if i == 0 {
                    let _ = socket.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await;
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\ndone")
                        .await;
                } else {
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: keep-alive\r\n\r\nbye")
                        .await;
                }
            }
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"POST /upload HTTP/1.1\r\nHost: xflux\r\nContent-Length: 4\r\n\r\nbody")
        .await
        .unwrap();
    let first = read_until(&mut client, b"done").await;
    let text = String::from_utf8_lossy(&first);
    assert!(text.contains("100 Continue"), "{text}");
    assert!(text.contains("200 OK"), "{text}");
    assert!(text.contains("done"), "{text}");

    client
        .write_all(b"GET /next HTTP/1.1\r\nHost: xflux\r\n\r\n")
        .await
        .unwrap();
    let second = read_until(&mut client, b"bye").await;
    assert!(String::from_utf8_lossy(&second).contains("bye"));
    assert_eq!(conns.load(Ordering::SeqCst), 1);
    server.stop().await;
}

async fn start_xflux_with_timeouts(
    mode: &str,
    backend: SocketAddr,
    first_byte: &str,
    idle: &str,
    total: &str,
) -> XFluxServer {
    let text = format!(
        r#"
[server]
bind = "127.0.0.1:0"
mode = "{mode}"

[timeouts]
connect = "50ms"
first_byte = "{first_byte}"
idle = "{idle}"
total = "{total}"
fin = "500ms"

[pool]
max_idle_per_backend = 4
idle_timeout = "5s"

[backends]
algorithm = "leastconn"
health_interval = "0s"

[[backends.servers]]
addr = "{backend}"
weight = 1
"#
    );
    let config = Config::from_toml(&text).unwrap();
    XFluxServer::start(config).await.unwrap()
}

#[tokio::test]
async fn thin_l7_enforces_first_byte_timeout_and_returns_504() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            let _ = read_http_head(&mut socket, &mut buf).await;
            // Stall before sending any response byte.
            tokio::time::sleep(Duration::from_secs(2)).await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux_with_timeouts("thin-l7", backend, "80ms", "50ms", "80ms").await;

    let started = Instant::now();
    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"POST /v1/chat HTTP/1.1\r\nHost: xflux\r\nContent-Length: 2\r\n\r\n{}")
        .await
        .unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut out))
        .await
        .expect("first_byte timeout did not fire before 1s")
        .unwrap();
    let elapsed = started.elapsed();
    let text = String::from_utf8_lossy(&out);
    assert!(
        text.starts_with("HTTP/1.1 504 Gateway Timeout"),
        "expected 504 Gateway Timeout even when first_byte == total, got: {text:?}"
    );
    assert!(
        elapsed < Duration::from_millis(800),
        "expected fast first_byte abort, took {elapsed:?}"
    );
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_allows_slow_first_byte_then_enforces_tight_chunk_idle() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            if read_http_head(&mut socket, &mut buf).await.is_none() {
                return;
            }
            // Prefill takes 150ms (> idle 60ms, but < first_byte 500ms).
            tokio::time::sleep(Duration::from_millis(150)).await;
            let head = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n";
            if socket.write_all(head).await.is_err() {
                return;
            }
            if socket.write_all(&chunk("data: first\n\n")).await.is_err() {
                return;
            }
            // Mid-stream stall for 2s (> idle 60ms): xFlux must abort the stream quickly.
            tokio::time::sleep(Duration::from_secs(2)).await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux_with_timeouts("thin-l7", backend, "500ms", "60ms", "5s").await;

    let started = Instant::now();
    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"GET /stream HTTP/1.1\r\nHost: xflux\r\n\r\n")
        .await
        .unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut out))
        .await
        .expect("chunk idle timeout did not abort stalled stream")
        .unwrap();
    let elapsed = started.elapsed();
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("200 OK"), "{text}");
    assert!(text.contains("data: first"), "{text}");
    assert!(
        elapsed < Duration::from_millis(800),
        "expected chunk idle abort well under 1s, took {elapsed:?}"
    );
    server.stop().await;
}

#[tokio::test]
async fn l4_enforces_first_byte_timeout_before_backend_responds() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = [0u8; 64];
            let _ = socket.read(&mut buf).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xflux_with_timeouts("l4", backend, "60ms", "2s", "5s").await;

    let started = Instant::now();
    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client.write_all(b"ping").await.unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut out))
        .await
        .expect("l4 first_byte timeout did not close connection")
        .unwrap();
    assert!(out.is_empty());
    assert!(started.elapsed() < Duration::from_millis(800));
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_max_conns_per_backend_queues_and_reuses_single_connection() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            while let Some(head) = read_http_head(&mut socket, &mut buf).await {
                drain_fixed_body(&mut socket, &mut buf, &head).await;
                tokio::time::sleep(Duration::from_millis(80)).await;
                let response =
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok";
                if socket.write_all(response).await.is_err() {
                    return;
                }
            }
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let text = format!(
        r#"
[server]
bind = "127.0.0.1:0"
mode = "thin-l7"

[timeouts]
connect = "1s"
idle = "2s"
total = "5s"
fin = "500ms"

[pool]
max_idle_per_backend = 4
max_conns_per_backend = 1
acquire_timeout = "1s"
idle_timeout = "5s"

[backends]
algorithm = "leastconn"
health_interval = "0s"

[[backends.servers]]
addr = "{backend}"
weight = 1
"#
    );
    let server = XFluxServer::start(Config::from_toml(&text).unwrap())
        .await
        .unwrap();

    let send_one = |addr: SocketAddr| async move {
        let mut client = TestClient::connect(addr).await;
        client.send(b"GET / HTTP/1.1\r\nHost: xflux\r\n\r\n").await;
        client.read_response().await
    };

    let (r1, r2) = tokio::join!(send_one(server.addr), send_one(server.addr));
    assert!(r1.contains("200 OK"), "{r1}");
    assert!(r2.contains("200 OK"), "{r2}");
    assert_eq!(
        conns.load(Ordering::SeqCst),
        1,
        "second concurrent request should wait for permit and reuse the single backend connection"
    );
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_max_conns_per_backend_rejects_with_503_when_acquire_times_out() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            if read_http_head(&mut socket, &mut buf).await.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let text = format!(
        r#"
[server]
bind = "127.0.0.1:0"
mode = "thin-l7"

[timeouts]
connect = "1s"
idle = "2s"
total = "5s"
fin = "500ms"

[pool]
max_idle_per_backend = 4
max_conns_per_backend = 1
acquire_timeout = "60ms"
idle_timeout = "5s"

[backends]
algorithm = "leastconn"
health_interval = "0s"

[[backends.servers]]
addr = "{backend}"
weight = 1
"#
    );
    let server = XFluxServer::start(Config::from_toml(&text).unwrap())
        .await
        .unwrap();

    let mut first = TcpStream::connect(server.addr).await.unwrap();
    first
        .write_all(b"GET /slow HTTP/1.1\r\nHost: xflux\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut second = TcpStream::connect(server.addr).await.unwrap();
    second
        .write_all(b"GET /overflow HTTP/1.1\r\nHost: xflux\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut second_out = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), second.read_to_end(&mut second_out))
        .await
        .expect("second request did not time out on acquire_timeout")
        .unwrap();
    let second_text = String::from_utf8_lossy(&second_out);
    assert!(
        second_text.starts_with("HTTP/1.1 503 Service Unavailable"),
        "expected 503 Service Unavailable, got: {second_text:?}"
    );

    let mut first_out = Vec::new();
    first.read_to_end(&mut first_out).await.unwrap();
    assert!(String::from_utf8_lossy(&first_out).contains("200 OK"));
    assert_eq!(conns.load(Ordering::SeqCst), 1);
    server.stop().await;
}
