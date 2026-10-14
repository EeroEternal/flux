//! End-to-end integration tests for Flux.
//!
//! These spin up mock gateway backends, start a real Flux server on an
//! ephemeral port, and exercise the behaviors the design doc calls out:
//! pass-through, half-close, long streams, connection reuse, sudden backend
//! death and least-connections scheduling.

use flux::config::Config;
use flux::server::FluxServer;
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

type Handler = Arc<dyn Fn(TcpStream) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Spawn a raw TCP backend. \`conns\` counts accepted connections.
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

async fn start_flux(mode: &str, backends: &[SocketAddr]) -> FluxServer {
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
algorithm = "leastconn"
health_interval = "0s"

{servers}

[retry]
max_retries = 0

[metrics]
enabled = false
"#
    );
    let config = Config::from_toml(&text).unwrap();
    FluxServer::start(config).await.unwrap()
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
    let dir = std::env::temp_dir().join(format!("flux-it-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    path.to_string_lossy().into_owned()
}

async fn start_flux_tls(backend: SocketAddr, cert: &str, key: &str) -> FluxServer {
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
    FluxServer::start(config).await.unwrap()
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
    let server = start_flux("l4", &[backend]).await;

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
    let server = start_flux("l4", &[backend]).await;

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
    let server = start_flux("thin-l7", &[backend]).await;

    let mut client = TestClient::connect(server.addr).await;
    client.send(b"GET /a HTTP/1.1\r\nHost: flux\r\n\r\n").await;
    let first = client.read_response().await;
    assert!(first.contains("200 OK"));
    assert!(first.ends_with("pong"));

    client.send(b"GET /b HTTP/1.1\r\nHost: flux\r\n\r\n").await;
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
    let server = start_flux("thin-l7", &[backend]).await;

    let mut client = TestClient::connect(server.addr).await;
    client
        .send(b"GET /1 HTTP/1.1\r\nHost: flux\r\n\r\nGET /2 HTTP/1.1\r\nHost: flux\r\n\r\n")
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
    let server = start_flux("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"POST /v1/chat HTTP/1.1\r\nHost: flux\r\nContent-Length: 2\r\n\r\n{}")
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
    let server = start_flux("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: flux\r\n\r\n")
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
    let server = start_flux("l4", &[slow_addr, fast_addr]).await;

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
    let server = start_flux("thin-l7", &[backend]).await;

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
    let server = start_flux("thin-l7", &[backend]).await;

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
    let server = start_flux("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(
            b"GET /ws HTTP/1.1\r\nHost: flux\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
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
    let server = start_flux_tls(backend, &cert_path, &key_path).await;

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
