//! End-to-end integration tests for xLB.
//!
//! These spin up mock gateway backends, start a real xLB server on an
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
use xlb::config::Config;
use xlb::server::XlbServer;

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

async fn start_xlb(mode: &str, backends: &[SocketAddr]) -> XlbServer {
    start_xlb_cfg(mode, backends, "leastconn", 0).await
}

async fn start_xlb_cfg(
    mode: &str,
    backends: &[SocketAddr],
    algorithm: &str,
    max_retries: u32,
) -> XlbServer {
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
    XlbServer::start(config).await.unwrap()
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
    let dir = std::env::temp_dir().join(format!("xlb-it-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    path.to_string_lossy().into_owned()
}

async fn start_xlb_tls(backend: SocketAddr, cert: &str, key: &str) -> XlbServer {
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
    XlbServer::start(config).await.unwrap()
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
    let server = start_xlb("l4", &[backend]).await;

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
    let server = start_xlb("l4", &[backend]).await;

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
    let server = start_xlb("thin-l7", &[backend]).await;

    let mut client = TestClient::connect(server.addr).await;
    client.send(b"GET /a HTTP/1.1\r\nHost: xlb\r\n\r\n").await;
    let first = client.read_response().await;
    assert!(first.contains("200 OK"));
    assert!(first.ends_with("pong"));

    client.send(b"GET /b HTTP/1.1\r\nHost: xlb\r\n\r\n").await;
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
    let server = start_xlb("thin-l7", &[backend]).await;

    let mut client = TestClient::connect(server.addr).await;
    client
        .send(b"GET /1 HTTP/1.1\r\nHost: xlb\r\n\r\nGET /2 HTTP/1.1\r\nHost: xlb\r\n\r\n")
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
    let server = start_xlb("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}")
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
    let server = start_xlb("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: xlb\r\n\r\n")
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

/// A gateway whose health endpoint requires auth (e.g. LiteLLM `/health` with a
/// master key) is only schedulable when `backends.health_check_headers` carries
/// the credential; without it the probe gets 401 and the backend is ejected.
#[tokio::test]
async fn health_probe_sends_configured_headers() {
    let conns = Arc::new(AtomicUsize::new(0));
    let handler: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            let Some(head) = read_http_head(&mut socket, &mut buf).await else {
                return;
            };
            let text = String::from_utf8_lossy(&head).into_owned();
            let authed = text.contains("Authorization: Bearer sk-probe");
            let response = if text.starts_with("GET /health") && !authed {
                "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n"
            } else {
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
            };
            let _ = socket.write_all(response.as_bytes()).await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;

    let start = |headers: &str| {
        let text = format!(
            r#"
[server]
bind = "127.0.0.1:0"
mode = "thin-l7"

[backends]
health_check_path = "/health"
health_interval = "1s"
health_check_timeout = "1s"
unhealthy_threshold = 2
healthy_threshold = 1
{headers}
[[backends.servers]]
addr = "{backend}"
weight = 1

[metrics]
enabled = false
"#
        );
        Config::from_toml(&text).unwrap()
    };

    async fn status(addr: SocketAddr) -> String {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /v1/models HTTP/1.1\r\nHost: xlb\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        let head = tokio::time::timeout(
            Duration::from_secs(3),
            read_http_head(&mut client, &mut buf),
        )
        .await
        .expect("proxy hung")
        .expect("no response");
        String::from_utf8_lossy(&head)
            .lines()
            .next()
            .unwrap_or_default()
            .to_string()
    }

    // Probe credential configured: stays schedulable across several probe rounds.
    let server = XlbServer::start(start(
        "health_check_headers = [\"Authorization: Bearer sk-probe\"]",
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(2600)).await;
    assert!(status(server.addr).await.contains(" 200 "));
    server.stop().await;

    // Without it the 401 probes eject the backend and clients see 503.
    let server = XlbServer::start(start("health_check_headers = []"))
        .await
        .unwrap();
    let mut statuses = String::new();
    for _ in 0..40 {
        statuses = status(server.addr).await;
        if statuses.contains(" 503 ") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert!(
        statuses.contains(" 503 "),
        "expected ejection, got {statuses}"
    );
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
    let server = start_xlb("l4", &[slow_addr, fast_addr]).await;

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
    let server = start_xlb("thin-l7", &[backend]).await;

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
    let server = start_xlb("thin-l7", &[backend]).await;

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
    let server = start_xlb("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(
            b"GET /ws HTTP/1.1\r\nHost: xlb\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
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
    let server = start_xlb_tls(backend, &cert_path, &key_path).await;

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

/// `[DONE]` is payload. xLB must keep splicing until the HTTP chunked terminator.
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
    let server = start_xlb("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}")
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
        .write_all(b"GET /next HTTP/1.1\r\nHost: xlb\r\n\r\n")
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
    let server = start_xlb_cfg("thin-l7", &[backend_a, backend_b], "roundrobin", 0).await;

    for model in ["alpha", "beta"] {
        let mut client = TcpStream::connect(server.addr).await.unwrap();
        let json = format!(r#"{{"model":"{model}","stream":true}}"#);
        let req = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: xlb\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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
    let server = start_xlb("thin-l7", &[backend]).await;

    let payload = br#"{"model":"should-not-parse","messages":[{"role":"user","content":"data: [DONE]\n\n"}]}"#;
    let mut client = TcpStream::connect(server.addr).await.unwrap();
    let req = format!(
        "POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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
    let server = start_xlb_cfg("thin-l7", &[a, b], "roundrobin", 2).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: xlb\r\n\r\n")
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
    let server = start_xlb_cfg("thin-l7", &[dead, live_addr], "leastconn", 2).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: xlb\r\n\r\n")
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
    let server = start_xlb("thin-l7", &[backend]).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"POST /upload HTTP/1.1\r\nHost: xlb\r\nContent-Length: 4\r\n\r\nbody")
        .await
        .unwrap();
    let first = read_until(&mut client, b"done").await;
    let text = String::from_utf8_lossy(&first);
    assert!(text.contains("100 Continue"), "{text}");
    assert!(text.contains("200 OK"), "{text}");
    assert!(text.contains("done"), "{text}");

    client
        .write_all(b"GET /next HTTP/1.1\r\nHost: xlb\r\n\r\n")
        .await
        .unwrap();
    let second = read_until(&mut client, b"bye").await;
    assert!(String::from_utf8_lossy(&second).contains("bye"));
    assert_eq!(conns.load(Ordering::SeqCst), 1);
    server.stop().await;
}

async fn start_xlb_with_timeouts(
    mode: &str,
    backend: SocketAddr,
    first_byte: &str,
    idle: &str,
    total: &str,
) -> XlbServer {
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
    XlbServer::start(config).await.unwrap()
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
    let server = start_xlb_with_timeouts("thin-l7", backend, "80ms", "50ms", "80ms").await;

    let started = Instant::now();
    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}")
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
            // Mid-stream stall for 2s (> idle 60ms): xLB must abort the stream quickly.
            tokio::time::sleep(Duration::from_secs(2)).await;
        })
    });
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xlb_with_timeouts("thin-l7", backend, "500ms", "60ms", "5s").await;

    let started = Instant::now();
    let mut client = TcpStream::connect(server.addr).await.unwrap();
    client
        .write_all(b"GET /stream HTTP/1.1\r\nHost: xlb\r\n\r\n")
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
    let server = start_xlb_with_timeouts("l4", backend, "60ms", "2s", "5s").await;

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
    let server = XlbServer::start(Config::from_toml(&text).unwrap())
        .await
        .unwrap();

    let send_one = |addr: SocketAddr| async move {
        let mut client = TestClient::connect(addr).await;
        client.send(b"GET / HTTP/1.1\r\nHost: xlb\r\n\r\n").await;
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
    let server = XlbServer::start(Config::from_toml(&text).unwrap())
        .await
        .unwrap();

    let mut first = TcpStream::connect(server.addr).await.unwrap();
    first
        .write_all(b"GET /slow HTTP/1.1\r\nHost: xlb\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut second = TcpStream::connect(server.addr).await.unwrap();
    second
        .write_all(b"GET /overflow HTTP/1.1\r\nHost: xlb\r\nConnection: close\r\n\r\n")
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

#[tokio::test]
async fn thin_l7_recovers_from_stale_pooled_keepalive_close() {
    let conns = Arc::new(AtomicUsize::new(0));
    let conn_seq = Arc::new(AtomicUsize::new(0));
    let handler: Handler = {
        let conn_seq = conn_seq.clone();
        Arc::new(move |mut socket| {
            let which_conn = conn_seq.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                let mut buf = Vec::new();
                if which_conn == 0 {
                    // First connection: serve the first request with keep-alive,
                    // then wait until the second request arrives on this pooled
                    // socket and immediately close without sending a single byte
                    // (simulating an HTTP/1.1 keep-alive timeout race).
                    let head = match read_http_head(&mut socket, &mut buf).await {
                        Some(h) => h,
                        None => return,
                    };
                    drain_fixed_body(&mut socket, &mut buf, &head).await;
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nfirst",
                        )
                        .await;
                    let _ = read_http_head(&mut socket, &mut buf).await;
                    let _ = socket.shutdown().await;
                } else {
                    // Freshly redialed connection: serve the replayed request.
                    let head = match read_http_head(&mut socket, &mut buf).await {
                        Some(h) => h,
                        None => return,
                    };
                    drain_fixed_body(&mut socket, &mut buf, &head).await;
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: keep-alive\r\n\r\nsecond",
                        )
                        .await;
                }
            })
        })
    };
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xlb("thin-l7", &[backend]).await;

    let mut client = TestClient::connect(server.addr).await;
    client
        .send(b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}")
        .await;
    let r1 = client.read_response().await;
    assert!(r1.ends_with("first"), "{r1}");

    // Second request checks out the pooled socket, hits the simulated keep-alive
    // close race, and must transparently redial a fresh backend socket.
    client
        .send(b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}")
        .await;
    let r2 = client.read_response().await;
    assert!(r2.ends_with("second"), "{r2}");
    assert_eq!(conns.load(Ordering::SeqCst), 2);
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_passively_ejects_dead_backend_on_dial_failure() {
    let live_hits = Arc::new(AtomicUsize::new(0));
    let live: Handler = {
        let live_hits = live_hits.clone();
        Arc::new(move |mut socket| {
            let live_hits = live_hits.clone();
            Box::pin(async move {
                let mut buf = Vec::new();
                while let Some(head) = read_http_head(&mut socket, &mut buf).await {
                    drain_fixed_body(&mut socket, &mut buf, &head).await;
                    live_hits.fetch_add(1, Ordering::SeqCst);
                    if socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            })
        })
    };
    let dead_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let live_addr = spawn_backend(live, Arc::new(AtomicUsize::new(0))).await;

    let text = format!(
        r#"
[server]
bind = "127.0.0.1:0"
mode = "thin-l7"

[timeouts]
connect = "500ms"
idle = "2s"
total = "5s"
fin = "500ms"

[pool]
max_idle_per_backend = 4
idle_timeout = "5s"

[backends]
algorithm = "leastconn"
health_interval = "0s"
passive_unhealthy_threshold = 1
eject_cooldown = "5s"

[[backends.servers]]
addr = "{dead}"
weight = 1

[[backends.servers]]
addr = "{live_addr}"
weight = 1

[retry]
max_retries = 1
"#
    );
    let server = XlbServer::start(Config::from_toml(&text).unwrap())
        .await
        .unwrap();

    // First request tries `dead`, passively ejects it after 1 dial failure, and
    // succeeds on `live_addr`. Subsequent requests go straight to `live_addr`.
    let mut client = TestClient::connect(server.addr).await;
    for _ in 0..3 {
        client.send(b"GET / HTTP/1.1\r\nHost: xlb\r\n\r\n").await;
        let resp = client.read_response().await;
        assert!(resp.contains("200 OK"), "{resp}");
    }
    assert_eq!(live_hits.load(Ordering::SeqCst), 3);
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_propagates_client_abort_during_silent_prefill_and_stream_pause() {
    let conns = Arc::new(AtomicUsize::new(0));
    let (closed_tx, mut closed_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
    let req_seq = Arc::new(AtomicUsize::new(0));
    let handler: Handler = {
        let req_seq = req_seq.clone();
        Arc::new(move |mut socket| {
            let closed_tx = closed_tx.clone();
            let seq = req_seq.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                let mut buf = Vec::new();
                let Some(head) = read_http_head(&mut socket, &mut buf).await else {
                    return;
                };
                drain_fixed_body(&mut socket, &mut buf, &head).await;
                if seq == 0 {
                    // Phase 1: simulate a long-context prefill silence (no response head yet).
                    // Wait until xLB closes the backend socket when the client aborts.
                    let mut probe = [0u8; 16];
                    let _ = socket.read(&mut probe).await;
                    let _ = closed_tx.send(0);
                } else {
                    // Phase 2: send response head + first SSE chunk, then pause silently
                    // (simulating a reasoning pause mid-stream).
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n6\r\ndata:1\r\n",
                        )
                        .await;
                    let mut probe = [0u8; 16];
                    let _ = socket.read(&mut probe).await;
                    let _ = closed_tx.send(1);
                }
            })
        })
    };
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xlb("thin-l7", &[backend]).await;

    // 1. Client aborts during silent prefill (before first response byte).
    let mut client1 = TcpStream::connect(server.addr).await.unwrap();
    client1
        .write_all(
            b"POST /v1/chat/completions HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    drop(client1);

    let which = tokio::time::timeout(Duration::from_millis(500), closed_rx.recv())
        .await
        .expect("backend socket must close immediately when client aborts during prefill")
        .unwrap();
    assert_eq!(which, 0);

    // 2. Client aborts mid-stream during a silent reasoning pause.
    let mut client2 = TcpStream::connect(server.addr).await.unwrap();
    client2
        .write_all(
            b"POST /v1/chat/completions HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}",
        )
        .await
        .unwrap();
    let mut first_bytes = [0u8; 64];
    let n = client2.read(&mut first_bytes).await.unwrap();
    assert!(n > 0);
    drop(client2);

    let which2 = tokio::time::timeout(Duration::from_millis(500), closed_rx.recv())
        .await
        .expect("backend socket must close immediately when client aborts mid-stream")
        .unwrap();
    assert_eq!(which2, 1);
    assert_eq!(conns.load(Ordering::SeqCst), 2);

    server.stop().await;
}

#[tokio::test]
async fn thin_l7_streams_large_context_request_body_verbatim() {
    let conns = Arc::new(AtomicUsize::new(0));
    let received_len = Arc::new(AtomicUsize::new(0));
    let handler: Handler = {
        let received_len = received_len.clone();
        Arc::new(move |mut socket| {
            let received_len = received_len.clone();
            Box::pin(async move {
                let mut buf = Vec::new();
                while let Some(head) = read_http_head(&mut socket, &mut buf).await {
                    let head_text = String::from_utf8_lossy(&head);
                    let len: usize = head_text
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    while buf.len() < len {
                        let mut tmp = vec![0u8; 32 * 1024];
                        match socket.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&tmp[..n]),
                        }
                    }
                    assert!(buf[..len].iter().all(|&b| b == b'x'));
                    buf.drain(..len);
                    received_len.store(len, Ordering::SeqCst);
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                        len,
                        "y".repeat(len)
                    );
                    if socket.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            })
        })
    };
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xlb("thin-l7", &[backend]).await;

    // 256 KiB payload exercises the 64 KiB long-context streaming window and pool reuse.
    let body_size = 256 * 1024;
    let payload = "x".repeat(body_size);
    let mut client = TestClient::connect(server.addr).await;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: xlb\r\nContent-Length: {body_size}\r\n\r\n{payload}"
    );
    client.send(req.as_bytes()).await;
    let resp = client.read_response().await;
    assert!(resp.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(received_len.load(Ordering::SeqCst), body_size);
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_reuses_backend_pool_even_when_client_sends_connection_close() {
    let conns = Arc::new(AtomicUsize::new(0));
    let seen_keepalive = Arc::new(AtomicUsize::new(0));
    let handler: Handler = {
        let seen_keepalive = seen_keepalive.clone();
        Arc::new(move |mut socket| {
            let seen_keepalive = seen_keepalive.clone();
            Box::pin(async move {
                let mut buf = Vec::new();
                while let Some(head) = read_http_head(&mut socket, &mut buf).await {
                    let head_text = String::from_utf8_lossy(&head).to_ascii_lowercase();
                    if head_text.contains("connection: keep-alive") {
                        seen_keepalive.fetch_add(1, Ordering::SeqCst);
                    }
                    drain_fixed_body(&mut socket, &mut buf, &head).await;
                    let response =
                        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\npong";
                    if socket.write_all(response).await.is_err() {
                        return;
                    }
                }
            })
        })
    };
    let backend = spawn_backend(handler, conns.clone()).await;
    let server = start_xlb("thin-l7", &[backend]).await;

    for _ in 0..4 {
        let mut client = TcpStream::connect(server.addr).await.unwrap();
        client
            .write_all(
                b"POST /v1/chat/completions HTTP/1.1\r\nHost: xlb\r\nConnection: close\r\nContent-Length: 2\r\n\r\n{}",
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        client.read_to_end(&mut out).await.unwrap();
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("200 OK"), "{text}");
        assert!(text.ends_with("pong"), "{text}");
    }

    assert_eq!(
        conns.load(Ordering::SeqCst),
        1,
        "4 sequential Connection: close client requests must reuse 1 warm backend socket"
    );
    assert_eq!(seen_keepalive.load(Ordering::SeqCst), 4);
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_fails_over_to_healthy_backend_when_pooled_backend_dies_pre_first_byte() {
    let a_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a_addr = a_listener.local_addr().unwrap();
    tokio::spawn(async move {
        // Accept exactly one connection on backend A, serve the first request
        // with keep-alive, then drop the listener and close the socket as soon
        // as the second request arrives (simulating a backend crash mid-prefill).
        let (mut socket, _) = a_listener.accept().await.unwrap();
        drop(a_listener);
        let mut buf = Vec::new();
        let Some(head1) = read_http_head(&mut socket, &mut buf).await else {
            return;
        };
        drain_fixed_body(&mut socket, &mut buf, &head1).await;
        let _ = socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nfromA",
            )
            .await;
        let _ = read_http_head(&mut socket, &mut buf).await;
        drop(socket);
    });

    let b_conns = Arc::new(AtomicUsize::new(0));
    let b_addr = spawn_backend(http_backend("fromB", true), b_conns.clone()).await;

    let server = start_xlb_cfg("thin-l7", &[a_addr, b_addr], "roundrobin", 2).await;

    let mut client = TestClient::connect(server.addr).await;
    // Request 1 lands on backend A and warms A's idle pool.
    client
        .send(b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}")
        .await;
    let r1 = client.read_response().await;
    assert!(r1.ends_with("fromA"), "{r1}");

    // Request 2 lands on backend B (round-robin).
    client
        .send(b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}")
        .await;
    let r2 = client.read_response().await;
    assert!(r2.ends_with("fromB"), "{r2}");

    // Request 3 lands on backend A again, checks out the warm pooled socket,
    // hits pre-first-byte EOF when A crashes, fails to redial A (listener closed),
    // and must transparently fail over to backend B instead of returning 503.
    client
        .send(b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}")
        .await;
    let r3 = client.read_response().await;
    assert!(
        r3.contains("200 OK") && r3.ends_with("fromB"),
        "expected transparent failover to backend B, got: {r3}"
    );
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_reroutes_on_404_and_502_across_asymmetric_backends_and_reuses_pool() {
    let a_conns = Arc::new(AtomicUsize::new(0));
    let b_conns = Arc::new(AtomicUsize::new(0));

    // Backend A only serves `/v1/deepseek`; returns 404 for `/v1/mimo`.
    let handler_a: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            loop {
                let Some(head) = read_http_head(&mut socket, &mut buf).await else {
                    return;
                };
                let head_str = String::from_utf8_lossy(&head);
                while buf.len() < 16 {
                    let mut tmp = [0u8; 64];
                    match socket.read(&mut tmp).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&tmp[..n]),
                    }
                }
                buf.drain(..16);
                if head_str.contains("/v1/deepseek") {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nfrom-deepseek",
                        )
                        .await;
                } else {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 15\r\nConnection: keep-alive\r\n\r\nmodel-not-found",
                        )
                        .await;
                }
            }
        })
    });

    // Backend B only serves `/v1/mimo`; returns 404 for `/v1/deepseek`.
    let handler_b: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            loop {
                let Some(head) = read_http_head(&mut socket, &mut buf).await else {
                    return;
                };
                let head_str = String::from_utf8_lossy(&head);
                while buf.len() < 16 {
                    let mut tmp = [0u8; 64];
                    match socket.read(&mut tmp).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&tmp[..n]),
                    }
                }
                buf.drain(..16);
                if head_str.contains("/v1/mimo") {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: keep-alive\r\n\r\nfrom-mimo",
                        )
                        .await;
                } else {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 15\r\nConnection: keep-alive\r\n\r\nmodel-not-found",
                        )
                        .await;
                }
            }
        })
    });

    let a_addr = spawn_backend(handler_a, a_conns.clone()).await;
    let b_addr = spawn_backend(handler_b, b_conns.clone()).await;

    // Omit `mode` and `[retry]` so default `mode = "thin-l7"` and `max_retries = -1` are exercised.
    let text = format!(
        r#"
[server]
bind = "127.0.0.1:0"

[backends]
algorithm = "roundrobin"
health_interval = "0s"

[[backends.servers]]
addr = "{a_addr}"
weight = 1

[[backends.servers]]
addr = "{b_addr}"
weight = 1
"#
    );
    let server = XlbServer::start(Config::from_toml(&text).unwrap())
        .await
        .unwrap();

    let mut client = TestClient::connect(server.addr).await;
    // Request 1 hits Backend A first (round-robin idx 0), which returns 404 for /v1/mimo,
    // and xLB transparently reroutes to Backend B -> 200 OK `from-mimo`.
    client
        .send(b"POST /v1/mimo HTTP/1.1\r\nHost: xlb\r\nContent-Length: 16\r\n\r\n{\"model\":\"mimo\"}")
        .await;
    let r1 = client.read_response().await;
    assert!(r1.contains("200 OK") && r1.ends_with("from-mimo"), "{r1}");

    // Request 2 hits Backend B first (round-robin idx 1), which returns 404 for /v1/deepseek,
    // and xLB transparently reroutes to Backend A (reusing Backend A's pooled socket!) -> 200 OK `from-deepseek`.
    client
        .send(b"POST /v1/deepseek HTTP/1.1\r\nHost: xlb\r\nContent-Length: 16\r\n\r\n{\"model\":\"dsk!\"}")
        .await;
    let r2 = client.read_response().await;
    assert!(
        r2.contains("200 OK") && r2.ends_with("from-deepseek"),
        "{r2}"
    );

    // Request 3 asks for `/v1/unknown` which returns 404 on both A and B -> xLB returns the final 404 to client.
    client
        .send(b"POST /v1/unknown HTTP/1.1\r\nHost: xlb\r\nContent-Length: 16\r\n\r\n{\"model\":\"none\"}")
        .await;
    let r3 = client.read_response().await;
    assert!(
        r3.contains("404 Not Found") && r3.ends_with("model-not-found"),
        "{r3}"
    );

    // Both Backend A and Backend B kept their single pooled TCP connection across all 3 requests!
    assert_eq!(a_conns.load(Ordering::SeqCst), 1);
    assert_eq!(b_conns.load(Ordering::SeqCst), 1);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_reroutes_long_context_sse_request_larger_than_max_header_bytes() {
    // 128 KiB prompt body (> 64 KiB `server.max_header_bytes`) to verify long-context
    // prompts are buffered up to `retry.max_body_bytes` and replayed across backends
    // before streaming SSE chunks back to the client.
    const BODY_LEN: usize = 128 * 1024;
    let a_conns = Arc::new(AtomicUsize::new(0));
    let b_conns = Arc::new(AtomicUsize::new(0));

    let handler_a: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            let Some(head) = read_http_head(&mut socket, &mut buf).await else {
                return;
            };
            drain_fixed_body(&mut socket, &mut buf, &head).await;
            let _ = socket
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 15\r\nConnection: keep-alive\r\n\r\nmodel-not-found",
                )
                .await;
        })
    });

    let handler_b: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            let Some(head) = read_http_head(&mut socket, &mut buf).await else {
                return;
            };
            drain_fixed_body(&mut socket, &mut buf, &head).await;
            let _ = socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n",
                )
                .await;
            let _ = socket
                .write_all(b"15\r\ndata: {\"chunk\":\"1\"}\n\n\r\n")
                .await;
            let _ = socket
                .write_all(b"e\r\ndata: [DONE]\n\n\r\n0\r\n\r\n")
                .await;
        })
    });

    let a_addr = spawn_backend(handler_a, a_conns.clone()).await;
    let b_addr = spawn_backend(handler_b, b_conns.clone()).await;

    let server = start_xlb_cfg("thin-l7", &[a_addr, b_addr], "roundrobin", 1).await;

    let mut client = TcpStream::connect(server.addr).await.unwrap();
    let body = vec![b'x'; BODY_LEN];
    let req_head = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: xlb\r\nContent-Length: {BODY_LEN}\r\n\r\n"
    );
    client.write_all(req_head.as_bytes()).await.unwrap();
    client.write_all(&body).await.unwrap();
    let mut received = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = client.read(&mut buf).await.unwrap();
        assert!(n > 0, "unexpected eof before chunked terminator");
        received.extend_from_slice(&buf[..n]);
        if find(&received, b"\r\n\r\n").is_some() && received.ends_with(b"0\r\n\r\n") {
            break;
        }
    }
    let resp = String::from_utf8_lossy(&received);
    assert!(
        resp.contains("200 OK")
            && resp.contains("data: {\"chunk\":\"1\"}")
            && resp.contains("data: [DONE]"),
        "expected long-context SSE response after 404 reroute, got: {resp}"
    );
    assert_eq!(a_conns.load(Ordering::SeqCst), 1);
    assert_eq!(b_conns.load(Ordering::SeqCst), 1);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn thin_l7_distinguishes_non_idempotent_post_from_safe_or_idempotency_key_reroutes() {
    let b_hits = Arc::new(AtomicUsize::new(0));

    // Backend A returns 429 on `/v1/refused` (pre-execution refusal) and 502 on
    // all other paths (in-doubt error that could happen after upstream acceptance).
    let handler_a: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            while let Some(head) = read_http_head(&mut socket, &mut buf).await {
                let head_str = String::from_utf8_lossy(&head).into_owned();
                drain_fixed_body(&mut socket, &mut buf, &head).await;
                let resp: &[u8] = if head_str.contains("/v1/refused") {
                    b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nrate-limit"
                } else {
                    b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 8\r\nConnection: keep-alive\r\n\r\nin-doubt"
                };
                if socket.write_all(resp).await.is_err() {
                    return;
                }
            }
        })
    });

    let handler_b: Handler = {
        let b_hits = b_hits.clone();
        Arc::new(move |mut socket| {
            let b_hits = b_hits.clone();
            Box::pin(async move {
                let mut buf = Vec::new();
                while let Some(head) = read_http_head(&mut socket, &mut buf).await {
                    drain_fixed_body(&mut socket, &mut buf, &head).await;
                    b_hits.fetch_add(1, Ordering::SeqCst);
                    if socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: keep-alive\r\n\r\nfrom-b!",
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            })
        })
    };

    let a_addr = spawn_backend(handler_a, Arc::new(AtomicUsize::new(0))).await;
    let b_addr = spawn_backend(handler_b, Arc::new(AtomicUsize::new(0))).await;

    // Case 1: `respect_idempotency_key = false` (default) and
    // `non_idempotent_retry_statuses = [402, 404, 429, 503]`.
    let cfg_no_idem = format!(
        r#"
[server]
bind = "127.0.0.1:0"
mode = "thin-l7"

[backends]
algorithm = "roundrobin"
health_interval = "0s"

[[backends.servers]]
addr = "{a_addr}"
weight = 1

[[backends.servers]]
addr = "{b_addr}"
weight = 1

[retry]
max_retries = 1
retry_statuses = [402, 404, 429, 500, 502, 503, 504]
non_idempotent_retry_statuses = [402, 404, 429, 503]
respect_idempotency_key = false
"#
    );
    let server = XlbServer::start(Config::from_toml(&cfg_no_idem).unwrap())
        .await
        .unwrap();
    let mut client = TestClient::connect(server.addr).await;

    // 1a. RR index 0 -> Backend A: POST hitting 502 must NOT replay on Backend B.
    client
        .send(
            b"POST /v1/chat/completions HTTP/1.1\r\nHost: xlb\r\nX-Request-Id: sk-secret-123\r\nContent-Length: 2\r\n\r\n{}",
        )
        .await;
    let r1 = client.read_response().await;
    assert!(
        r1.contains("502 Bad Gateway") && r1.ends_with("in-doubt"),
        "expected in-doubt 502 to return without replay, got: {r1}"
    );
    assert_eq!(b_hits.load(Ordering::SeqCst), 0);

    // Advance RR index 1 (Backend B) with a normal request so next lands on A again.
    client
        .send(b"GET /v1/health HTTP/1.1\r\nHost: xlb\r\n\r\n")
        .await;
    let _ = client.read_response().await;
    assert_eq!(b_hits.load(Ordering::SeqCst), 1);

    // 1b. RR index 0 -> Backend A: POST with Idempotency-Key still does NOT replay
    // on 502 when `respect_idempotency_key = false`.
    client
        .send(
            b"POST /v1/chat/completions HTTP/1.1\r\nHost: xlb\r\nIdempotency-Key: idem-1\r\nContent-Length: 2\r\n\r\n{}",
        )
        .await;
    let r2 = client.read_response().await;
    assert!(r2.contains("502 Bad Gateway"), "{r2}");
    assert_eq!(b_hits.load(Ordering::SeqCst), 1);

    // Advance RR index 1 (Backend B).
    client
        .send(b"GET /v1/health HTTP/1.1\r\nHost: xlb\r\n\r\n")
        .await;
    let _ = client.read_response().await;
    assert_eq!(b_hits.load(Ordering::SeqCst), 2);

    // 1c. RR index 0 -> Backend A: POST hitting 429 (pre-execution refusal) DOES
    // reroute to Backend B.
    client
        .send(
            b"POST /v1/refused HTTP/1.1\r\nHost: xlb\r\nX-Request-Id: req-429\r\nContent-Length: 2\r\n\r\n{}",
        )
        .await;
    let r3 = client.read_response().await;
    assert!(
        r3.contains("200 OK") && r3.ends_with("from-b!"),
        "expected pre-execution 429 on POST to reroute to Backend B, got: {r3}"
    );
    assert_eq!(b_hits.load(Ordering::SeqCst), 3);

    // 1d. Next RR lands on Backend A (since reroute used select_skipping without
    // advancing RR cursor, or let's verify GET /v1/models hitting 502 reroutes to B):
    client
        .send(b"GET /v1/models HTTP/1.1\r\nHost: xlb\r\n\r\n")
        .await;
    let r4 = client.read_response().await;
    assert!(
        r4.contains("200 OK") && r4.ends_with("from-b!"),
        "expected safe GET hitting 502 to reroute to Backend B, got: {r4}"
    );
    drop(client);
    server.stop().await;

    // Case 2: `respect_idempotency_key = true` allows POST with Idempotency-Key
    // to reroute on 502.
    b_hits.store(0, Ordering::SeqCst);
    let cfg_with_idem = format!(
        r#"
[server]
bind = "127.0.0.1:0"
mode = "thin-l7"

[backends]
algorithm = "roundrobin"
health_interval = "0s"

[[backends.servers]]
addr = "{a_addr}"
weight = 1

[[backends.servers]]
addr = "{b_addr}"
weight = 1

[retry]
max_retries = 1
retry_statuses = [402, 404, 429, 500, 502, 503, 504]
non_idempotent_retry_statuses = [402, 404, 429, 503]
respect_idempotency_key = true
"#
    );
    let server2 = XlbServer::start(Config::from_toml(&cfg_with_idem).unwrap())
        .await
        .unwrap();
    let mut client2 = TestClient::connect(server2.addr).await;
    client2
        .send(
            b"POST /v1/chat/completions HTTP/1.1\r\nHost: xlb\r\nIdempotency-Key: idem-shared-ledger\r\nContent-Length: 2\r\n\r\n{}",
        )
        .await;
    let r5 = client2.read_response().await;
    assert!(
        r5.contains("200 OK") && r5.ends_with("from-b!"),
        "expected POST with Idempotency-Key to reroute on 502 when respect_idempotency_key=true, got: {r5}"
    );
    assert_eq!(b_hits.load(Ordering::SeqCst), 1);
    drop(client2);
    server2.stop().await;
}

#[tokio::test]
async fn thin_l7_enforces_retry_timeout_ceiling_before_cross_node_reroute() {
    let b_hits = Arc::new(AtomicUsize::new(0));

    // Backend A sleeps 90ms before returning 503 Service Unavailable.
    let handler_a: Handler = Arc::new(|mut socket| {
        Box::pin(async move {
            let mut buf = Vec::new();
            if let Some(head) = read_http_head(&mut socket, &mut buf).await {
                drain_fixed_body(&mut socket, &mut buf, &head).await;
                tokio::time::sleep(Duration::from_millis(90)).await;
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 9\r\nConnection: keep-alive\r\n\r\ntoo-late!",
                    )
                    .await;
            }
        })
    });

    let handler_b: Handler = {
        let b_hits = b_hits.clone();
        Arc::new(move |mut socket| {
            let b_hits = b_hits.clone();
            Box::pin(async move {
                let mut buf = Vec::new();
                if let Some(head) = read_http_head(&mut socket, &mut buf).await {
                    drain_fixed_body(&mut socket, &mut buf, &head).await;
                    b_hits.fetch_add(1, Ordering::SeqCst);
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: keep-alive\r\n\r\nfrom-b!",
                        )
                        .await;
                }
            })
        })
    };

    let a_addr = spawn_backend(handler_a, Arc::new(AtomicUsize::new(0))).await;
    let b_addr = spawn_backend(handler_b, Arc::new(AtomicUsize::new(0))).await;

    let cfg = format!(
        r#"
[server]
bind = "127.0.0.1:0"
mode = "thin-l7"

[timeouts]
connect = "500ms"
idle = "2s"
total = "5s"
fin = "500ms"

[backends]
algorithm = "roundrobin"
health_interval = "0s"

[[backends.servers]]
addr = "{a_addr}"
weight = 1

[[backends.servers]]
addr = "{b_addr}"
weight = 1

[retry]
max_retries = 1
timeout = "40ms"
retry_statuses = [503]
"#
    );
    let server = XlbServer::start(Config::from_toml(&cfg).unwrap())
        .await
        .unwrap();
    let mut client = TestClient::connect(server.addr).await;
    client
        .send(b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nContent-Length: 2\r\n\r\n{}")
        .await;
    let resp = client.read_response().await;
    assert!(
        resp.contains("503 Service Unavailable") && resp.ends_with("too-late!"),
        "expected 503 from Backend A when retry.timeout (40ms) has already elapsed, got: {resp}"
    );
    assert_eq!(
        b_hits.load(Ordering::SeqCst),
        0,
        "must not reroute to Backend B after retry.timeout ceiling"
    );
    drop(client);
    server.stop().await;
}
