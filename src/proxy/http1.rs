//! Minimal HTTP/1.1 framing helpers.
//!
//! xLB never aggregates bodies. These helpers only locate message boundaries
//! so that (a) request/response heads can be forwarded verbatim, (b) bodies can
//! be streamed through without buffering, and (c) a backend connection can be
//! safely returned to the pool once a response has been fully consumed.
//!
//! Payload bytes are opaque: this module does not inspect SSE, NDJSON or JSON.

use super::{read_with_idle, write_all_with_idle};
use std::io;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

/// Find the end of a header block (the position just past `\r\n\r\n`).
pub fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

/// A growable read buffer with a consume cursor.
///
/// Bytes that have been consumed are compacted away, so leftover bytes after a
/// message head remain available as the start of the body.
pub struct ConnBuffer {
    buf: Vec<u8>,
    pos: usize,
}

impl Default for ConnBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnBuffer {
    /// Create an empty buffer.
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            pos: 0,
        }
    }

    /// Bytes not yet consumed.
    pub fn available(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    /// True when no unconsumed bytes remain.
    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    /// Append bytes.
    pub fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Mark `n` bytes as consumed.
    pub fn consume(&mut self, n: usize) {
        self.pos = (self.pos + n).min(self.buf.len());
        self.compact();
    }

    fn compact(&mut self) {
        if self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        } else if self.pos >= 8192 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
    }

    /// Consume the buffer and return whatever bytes have not been processed.
    ///
    /// Used by the l7 → l4 fallback: bytes already read (for example an HTTP/2
    /// preface or an upgrade request) must be forwarded to the backend verbatim
    /// before the raw splice begins.
    pub fn into_available(self) -> Vec<u8> {
        self.buf[self.pos..].to_vec()
    }
}

/// Read a complete message head into the buffer and return it (including the
/// terminating blank line). Returns `Ok(None)` on a clean EOF before any byte.
pub async fn read_head<R: AsyncRead + Unpin>(
    reader: &mut R,
    cb: &mut ConnBuffer,
    max_head_bytes: usize,
    idle: Duration,
    deadline: Instant,
) -> io::Result<Option<Vec<u8>>> {
    loop {
        if let Some(end) = find_headers_end(cb.available()) {
            let head = cb.available()[..end].to_vec();
            cb.consume(end);
            return Ok(Some(head));
        }
        if cb.available().len() > max_head_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message head too large",
            ));
        }
        let mut tmp = [0u8; 8192];
        let n = read_with_idle(reader, &mut tmp, idle, deadline).await?;
        if n == 0 {
            return if cb.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "eof in message head",
                ))
            };
        }
        cb.push(&tmp[..n]);
    }
}

/// Read a response head from `backend` while simultaneously watching `client`
/// for premature disconnect (EOF/RST) when `client_buf` has no pipelined bytes.
///
/// During long-context prefill or reasoning silent phases (tens of seconds with
/// 0 upstream bytes), this ensures a client abort immediately tears down the
/// backend connection instead of waiting for the next token or `first_byte`
/// timeout. Any pipelined bytes that arrive from the client are preserved in
/// `client_buf`.
pub async fn read_response_head_or_client_abort<R, C>(
    backend: &mut R,
    cb: &mut ConnBuffer,
    client: &mut C,
    client_buf: &mut ConnBuffer,
    max_head_bytes: usize,
    idle: Duration,
    deadline: Instant,
) -> io::Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
    C: AsyncRead + Unpin,
{
    loop {
        if let Some(end) = find_headers_end(cb.available()) {
            let head = cb.available()[..end].to_vec();
            cb.consume(end);
            return Ok(Some(head));
        }
        if cb.available().len() > max_head_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message head too large",
            ));
        }
        let mut tmp = [0u8; 8192];
        let n = read_backend_or_client_eof(backend, &mut tmp, client, client_buf, idle, deadline)
            .await?;
        if n == 0 {
            return if cb.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "eof in message head",
                ))
            };
        }
        cb.push(&tmp[..n]);
    }
}

async fn read_backend_or_client_eof<R, C>(
    backend: &mut R,
    buf: &mut [u8],
    client: &mut C,
    client_buf: &mut ConnBuffer,
    idle: Duration,
    deadline: Instant,
) -> io::Result<usize>
where
    R: AsyncRead + Unpin,
    C: AsyncRead + Unpin,
{
    // If the client already sent pipelined bytes for a subsequent request, avoid
    // reading more from the client until that pipelined request is processed.
    if !client_buf.is_empty() {
        return read_with_idle(backend, buf, idle, deadline).await;
    }
    let step_deadline = (Instant::now() + idle).min(deadline);
    let mut client_probe = [0u8; 1024];
    tokio::select! {
        biased;
        res = read_with_idle(backend, buf, idle, deadline) => res,
        client_res = client.read(&mut client_probe) => {
            match client_res {
                Ok(0) => Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "client closed connection while waiting for backend stream",
                )),
                Ok(n) => {
                    client_buf.push(&client_probe[..n]);
                    let rem_idle = step_deadline.saturating_duration_since(Instant::now());
                    read_with_idle(backend, buf, rem_idle, deadline).await
                }
                Err(e) => Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!("client connection error during stream: {e}"),
                )),
            }
        }
    }
}

/// How the body of a message is delimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyKind {
    /// No body at all.
    None,
    /// Exactly `n` bytes.
    Length(u64),
    /// HTTP/1.1 chunked transfer coding.
    Chunked,
    /// The body ends when the peer closes the connection.
    UntilClose,
}

/// A parsed HTTP request line plus the framing headers xLB cares about.
#[derive(Debug, Clone)]
pub struct RequestHead {
    /// Request method.
    pub method: String,
    /// Request target.
    pub path: String,
    /// HTTP minor version (0 = HTTP/1.0, 1 = HTTP/1.1).
    pub version: u8,
    /// Request body framing.
    pub body: BodyKind,
    /// Whether the client wants the connection kept alive.
    pub keep_alive: bool,
    /// Whether the request asks for a protocol upgrade (WebSocket, h2c, ...).
    pub has_upgrade: bool,
}

/// A parsed HTTP response status line plus framing headers.
#[derive(Debug, Clone)]
pub struct ResponseHead {
    /// Status code.
    pub status: u16,
    /// HTTP minor version of the response.
    pub version: u8,
    /// Response body framing.
    pub body: BodyKind,
    /// Whether the backend is willing to keep the connection alive.
    pub keep_alive: bool,
}

fn header<'a>(headers: &'a [httparse::Header<'a>], name: &str) -> Option<&'a [u8]> {
    headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value)
}

fn contains_token(value: &[u8], token: &str) -> bool {
    String::from_utf8_lossy(value)
        .split(',')
        .any(|part| part.trim().eq_ignore_ascii_case(token))
}

fn headers_contain_token(headers: &[httparse::Header<'_>], name: &str, token: &str) -> bool {
    headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case(name))
        .any(|h| contains_token(h.value, token))
}

fn parse_content_length(value: &[u8]) -> Option<u64> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
}

/// Parse a request head. The input must include the terminating blank line.
pub fn parse_request(head: &[u8]) -> Result<RequestHead, String> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut headers);
    match req.parse(head) {
        Ok(httparse::Status::Complete(_)) => {}
        Ok(httparse::Status::Partial) => return Err("incomplete request head".into()),
        Err(e) => return Err(format!("invalid request head: {e}")),
    }
    let method = req.method.ok_or("missing method")?.to_string();
    let path = req.path.ok_or("missing request target")?.to_string();
    let version = req.version.unwrap_or(1);

    let chunked = headers_contain_token(req.headers, "transfer-encoding", "chunked");
    let content_length = header(req.headers, "content-length").and_then(parse_content_length);
    let close = headers_contain_token(req.headers, "connection", "close");
    let keep_alive_header = headers_contain_token(req.headers, "connection", "keep-alive");
    let has_upgrade = header(req.headers, "upgrade").is_some();
    let keep_alive = if version >= 1 {
        !close
    } else {
        keep_alive_header && !close
    };

    let body = if chunked {
        BodyKind::Chunked
    } else if let Some(n) = content_length {
        BodyKind::Length(n)
    } else {
        BodyKind::None
    };

    Ok(RequestHead {
        method,
        path,
        version,
        body,
        keep_alive,
        has_upgrade,
    })
}

/// Rewrite a parsed HTTP/1.x request head so the backend leg stays eligible for
/// connection pooling even when the downstream client sent `Connection: close`.
///
/// Only non-upgrade HTTP/1.1 requests with `Connection: close` are modified:
/// the `close` token is replaced with `keep-alive` (preserving any additional
/// connection tokens on the header) and `req.keep_alive` is updated to `true`
/// for the backend leg.
pub fn rewrite_request_for_backend_pool(head: &[u8], req: &mut RequestHead) -> Vec<u8> {
    if req.keep_alive || req.has_upgrade || req.version < 1 {
        return head.to_vec();
    }
    let Some(first_crlf) = head.windows(2).position(|w| w == b"\r\n") else {
        return head.to_vec();
    };
    let mut out = Vec::with_capacity(head.len() + 16);
    out.extend_from_slice(&head[..first_crlf + 2]);
    let mut pos = first_crlf + 2;
    let mut wrote_keep_alive = false;
    while pos < head.len() {
        let rem = &head[pos..];
        if rem.starts_with(b"\r\n") {
            if !wrote_keep_alive {
                out.extend_from_slice(b"Connection: keep-alive\r\n");
                wrote_keep_alive = true;
            }
            out.extend_from_slice(b"\r\n");
            break;
        }
        let line_len = rem
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|p| p + 2)
            .unwrap_or(rem.len());
        let line = &rem[..line_len];
        let conn_colon = line
            .iter()
            .position(|&b| b == b':')
            .filter(|&idx| line[..idx].trim_ascii().eq_ignore_ascii_case(b"connection"));
        if let Some(colon_idx) = conn_colon {
            let raw_val = trim_line(&line[colon_idx + 1..]);
            let extra_tokens: Vec<&[u8]> = raw_val
                .split(|&b| b == b',')
                .map(|t| t.trim_ascii())
                .filter(|t| {
                    !t.is_empty()
                        && !t.eq_ignore_ascii_case(b"close")
                        && !t.eq_ignore_ascii_case(b"keep-alive")
                })
                .collect();
            if !wrote_keep_alive {
                out.extend_from_slice(b"Connection: keep-alive");
                for tok in extra_tokens {
                    out.extend_from_slice(b", ");
                    out.extend_from_slice(tok);
                }
                out.extend_from_slice(b"\r\n");
                wrote_keep_alive = true;
            } else if !extra_tokens.is_empty() {
                out.extend_from_slice(b"Connection: ");
                for (i, tok) in extra_tokens.iter().enumerate() {
                    if i > 0 {
                        out.extend_from_slice(b", ");
                    }
                    out.extend_from_slice(tok);
                }
                out.extend_from_slice(b"\r\n");
            }
        } else {
            out.extend_from_slice(line);
        }
        pos += line_len;
    }
    if wrote_keep_alive {
        req.keep_alive = true;
    }
    out
}

/// Parse a response head. The input must include the terminating blank line.
pub fn parse_response(head: &[u8]) -> Result<ResponseHead, String> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut resp = httparse::Response::new(&mut headers);
    match resp.parse(head) {
        Ok(httparse::Status::Complete(_)) => {}
        Ok(httparse::Status::Partial) => return Err("incomplete response head".into()),
        Err(e) => return Err(format!("invalid response head: {e}")),
    }
    let status = resp.code.ok_or("missing status code")?;
    let version = resp.version.unwrap_or(1);

    let chunked = headers_contain_token(resp.headers, "transfer-encoding", "chunked");
    let content_length = header(resp.headers, "content-length").and_then(parse_content_length);
    let close = headers_contain_token(resp.headers, "connection", "close");
    let keep_alive_header = headers_contain_token(resp.headers, "connection", "keep-alive");
    let keep_alive = if version >= 1 {
        !close
    } else {
        keep_alive_header && !close
    };

    let body = if status == 204 || status == 304 || (100..200).contains(&status) {
        BodyKind::None
    } else if status == 101 {
        BodyKind::UntilClose
    } else if chunked {
        BodyKind::Chunked
    } else if let Some(n) = content_length {
        BodyKind::Length(n)
    } else {
        BodyKind::UntilClose
    };

    Ok(ResponseHead {
        status,
        version,
        body,
        keep_alive,
    })
}

/// Detects the end of an HTTP/1.1 chunked body without decoding it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkState {
    Size,
    Data,
    DataCrlf,
    Trailer,
    Done,
}

/// Scans raw chunked bytes and reports when the terminating chunk has passed.
pub struct ChunkScanner {
    state: ChunkState,
    remaining: u64,
    line: Vec<u8>,
}

impl Default for ChunkScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkScanner {
    /// Create a scanner positioned at the first chunk-size line.
    pub fn new() -> Self {
        Self {
            state: ChunkState::Size,
            remaining: 0,
            line: Vec::new(),
        }
    }

    /// True once the terminating chunk and any trailers have been consumed.
    pub fn is_done(&self) -> bool {
        self.state == ChunkState::Done
    }

    /// Consume bytes from `data`, returning how many were consumed. Stops as
    /// soon as the body is complete, so bytes after the body are left alone.
    pub fn consume(&mut self, data: &[u8]) -> usize {
        let mut i = 0;
        while i < data.len() {
            match self.state {
                ChunkState::Done => break,
                ChunkState::Size | ChunkState::Trailer => {
                    let byte = data[i];
                    i += 1;
                    self.line.push(byte);
                    if byte == b'\n' {
                        let line = trim_line(&self.line);
                        if self.state == ChunkState::Size {
                            let size = line.split(|b| *b == b';').next().unwrap_or(&[]);
                            let size = size.trim_ascii();
                            match std::str::from_utf8(size)
                                .ok()
                                .and_then(|s| u64::from_str_radix(s, 16).ok())
                            {
                                Some(0) => self.state = ChunkState::Trailer,
                                Some(n) => {
                                    self.remaining = n;
                                    self.state = ChunkState::Data;
                                }
                                None => {
                                    // Malformed framing: treat the body as ended
                                    // rather than hanging.
                                    self.line.clear();
                                    self.state = ChunkState::Done;
                                    break;
                                }
                            }
                        } else if line.is_empty() {
                            self.line.clear();
                            self.state = ChunkState::Done;
                            break;
                        }
                        self.line.clear();
                    }
                }
                ChunkState::Data => {
                    let want = self.remaining.min((data.len() - i) as u64) as usize;
                    i += want;
                    self.remaining -= want as u64;
                    if self.remaining == 0 {
                        self.state = ChunkState::DataCrlf;
                    }
                }
                ChunkState::DataCrlf => {
                    if data[i] == b'\n' {
                        self.state = ChunkState::Size;
                    }
                    i += 1;
                }
            }
        }
        i
    }
}

fn trim_line(line: &[u8]) -> &[u8] {
    let mut end = line.len();
    while end > 0 && (line[end - 1] == b'\r' || line[end - 1] == b'\n') {
        end -= 1;
    }
    &line[..end]
}

/// Stream a body verbatim from `src` to `dst`.
///
/// Buffered bytes already read past the head are flushed first; the remainder
/// is copied with a bounded buffer. Nothing is buffered beyond one read.
pub async fn forward_body<R, W>(
    src: &mut R,
    dst: &mut W,
    cb: &mut ConnBuffer,
    kind: BodyKind,
    idle: Duration,
    deadline: Instant,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    match kind {
        BodyKind::None => Ok(()),
        BodyKind::Length(mut remaining) => {
            let take = cb.available().len().min(remaining as usize);
            if take > 0 {
                write_all_with_idle(dst, &cb.available()[..take], idle, deadline).await?;
                cb.consume(take);
                remaining -= take as u64;
            }
            // Use a 64 KiB read window for large long-context bodies (> 64 KiB)
            // to cut syscall churn by 4x while staying strictly bounded.
            let chunk_cap = if remaining > 64 * 1024 {
                64 * 1024
            } else {
                16 * 1024
            };
            let mut buf = vec![0u8; chunk_cap];
            while remaining > 0 {
                let n = read_with_idle(src, &mut buf, idle, deadline).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "eof in fixed-length body",
                    ));
                }
                let take = n.min(remaining as usize);
                write_all_with_idle(dst, &buf[..take], idle, deadline).await?;
                if take < n {
                    cb.push(&buf[take..n]);
                }
                remaining -= take as u64;
            }
            Ok(())
        }
        BodyKind::Chunked => {
            let mut scanner = ChunkScanner::new();
            let mut buf = [0u8; 16 * 1024];
            loop {
                let consumed = {
                    let buffered = cb.available();
                    if buffered.is_empty() {
                        0
                    } else {
                        scanner.consume(buffered)
                    }
                };
                if consumed > 0 {
                    write_all_with_idle(dst, &cb.available()[..consumed], idle, deadline).await?;
                    cb.consume(consumed);
                }
                if scanner.is_done() {
                    return Ok(());
                }
                let n = read_with_idle(src, &mut buf, idle, deadline).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "eof in chunked body",
                    ));
                }
                cb.push(&buf[..n]);
            }
        }
        BodyKind::UntilClose => {
            let mut buf = [0u8; 16 * 1024];
            loop {
                let len = cb.available().len();
                if len > 0 {
                    write_all_with_idle(dst, cb.available(), idle, deadline).await?;
                    cb.consume(len);
                }
                let n = read_with_idle(src, &mut buf, idle, deadline).await?;
                if n == 0 {
                    return Ok(());
                }
                cb.push(&buf[..n]);
            }
        }
    }
}

/// Stream a response body from `backend` to `client` while simultaneously
/// watching `client` for premature disconnect during silent reasoning pauses.
pub async fn forward_response_body_or_client_abort<R, C>(
    backend: &mut R,
    client: &mut C,
    response_buf: &mut ConnBuffer,
    client_buf: &mut ConnBuffer,
    kind: BodyKind,
    idle: Duration,
    deadline: Instant,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    C: AsyncRead + AsyncWrite + Unpin,
{
    match kind {
        BodyKind::None => Ok(()),
        BodyKind::Length(mut remaining) => {
            let take = response_buf.available().len().min(remaining as usize);
            if take > 0 {
                write_all_with_idle(client, &response_buf.available()[..take], idle, deadline)
                    .await?;
                response_buf.consume(take);
                remaining -= take as u64;
            }
            let chunk_cap = if remaining > 64 * 1024 {
                64 * 1024
            } else {
                16 * 1024
            };
            let mut buf = vec![0u8; chunk_cap];
            while remaining > 0 {
                let n = read_backend_or_client_eof(
                    backend, &mut buf, client, client_buf, idle, deadline,
                )
                .await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "eof in fixed-length body",
                    ));
                }
                let take = n.min(remaining as usize);
                write_all_with_idle(client, &buf[..take], idle, deadline).await?;
                if take < n {
                    response_buf.push(&buf[take..n]);
                }
                remaining -= take as u64;
            }
            Ok(())
        }
        BodyKind::Chunked => {
            let mut scanner = ChunkScanner::new();
            let mut buf = [0u8; 16 * 1024];
            loop {
                let consumed = {
                    let buffered = response_buf.available();
                    if buffered.is_empty() {
                        0
                    } else {
                        scanner.consume(buffered)
                    }
                };
                if consumed > 0 {
                    write_all_with_idle(
                        client,
                        &response_buf.available()[..consumed],
                        idle,
                        deadline,
                    )
                    .await?;
                    response_buf.consume(consumed);
                }
                if scanner.is_done() {
                    return Ok(());
                }
                let n = read_backend_or_client_eof(
                    backend, &mut buf, client, client_buf, idle, deadline,
                )
                .await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "eof in chunked body",
                    ));
                }
                response_buf.push(&buf[..n]);
            }
        }
        BodyKind::UntilClose => {
            let mut buf = [0u8; 16 * 1024];
            loop {
                let len = response_buf.available().len();
                if len > 0 {
                    write_all_with_idle(client, response_buf.available(), idle, deadline).await?;
                    response_buf.consume(len);
                }
                let n = read_backend_or_client_eof(
                    backend, &mut buf, client, client_buf, idle, deadline,
                )
                .await?;
                if n == 0 {
                    return Ok(());
                }
                response_buf.push(&buf[..n]);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_header_end() {
        assert_eq!(find_headers_end(b"GET / HTTP/1.1\r\n\r\nbody"), Some(18));
        assert_eq!(find_headers_end(b"GET / HTTP/1.1\r\n"), None);
    }

    #[test]
    fn parses_request_body_framing() {
        let head = b"POST /v1/chat HTTP/1.1\r\nHost: x\r\nContent-Length: 12\r\n\r\n";
        let req = parse_request(head).unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.body, BodyKind::Length(12));
        assert!(req.keep_alive);

        let head = b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n";
        assert_eq!(parse_request(head).unwrap().body, BodyKind::Chunked);

        let head = b"GET / HTTP/1.0\r\n\r\n";
        assert!(!parse_request(head).unwrap().keep_alive);

        let head = b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n";
        assert!(parse_request(head).unwrap().keep_alive);
    }

    #[test]
    fn parses_response_body_framing() {
        let head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        let resp = parse_response(head).unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, BodyKind::Chunked);
        assert!(resp.keep_alive);

        let head = b"HTTP/1.1 204 No Content\r\n\r\n";
        assert_eq!(parse_response(head).unwrap().body, BodyKind::None);

        let head = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\n";
        let resp = parse_response(head).unwrap();
        assert_eq!(resp.body, BodyKind::Length(3));
        assert!(!resp.keep_alive);

        let head = b"HTTP/1.1 200 OK\r\n\r\n";
        assert_eq!(parse_response(head).unwrap().body, BodyKind::UntilClose);
    }

    #[test]
    fn scans_chunked_body() {
        let mut scanner = ChunkScanner::new();
        let body = b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\nNEXT";
        let consumed = scanner.consume(body);
        assert!(scanner.is_done());
        assert_eq!(consumed, body.len() - 4);
        assert_eq!(&body[consumed..], b"NEXT");
    }

    #[test]
    fn scans_chunked_body_with_extensions_and_trailers() {
        let mut scanner = ChunkScanner::new();
        let body = b"4;foo=bar\r\nWiki\r\n0\r\nX-Trace: abc\r\n\r\n";
        let consumed = scanner.consume(body);
        assert!(scanner.is_done());
        assert_eq!(consumed, body.len());
    }

    #[test]
    fn chunk_scanner_ignores_sse_done_inside_payload() {
        let mut scanner = ChunkScanner::new();
        let event = b"data: [DONE]\n\n";
        let mut body = format!("{:x}\r\n", event.len()).into_bytes();
        body.extend_from_slice(event);
        body.extend_from_slice(b"\r\n5\r\nstill\r\n0\r\n\r\n");
        let consumed = scanner.consume(&body);
        assert!(scanner.is_done());
        assert_eq!(consumed, body.len());
    }

    #[test]
    fn scans_chunked_body_byte_by_byte() {
        let mut scanner = ChunkScanner::new();
        let body = b"3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n";
        for (i, byte) in body.iter().enumerate() {
            let consumed = scanner.consume(std::slice::from_ref(byte));
            assert_eq!(consumed, 1, "byte {i}");
        }
        assert!(scanner.is_done());
    }

    #[test]
    fn rewrites_connection_close_for_backend_pool() {
        let raw = b"POST /v1/chat/completions HTTP/1.1\r\nHost: xlb\r\nConnection: close\r\nContent-Length: 2\r\n\r\n";
        let mut req = parse_request(raw).unwrap();
        assert!(!req.keep_alive);
        let rewritten = rewrite_request_for_backend_pool(raw, &mut req);
        assert!(req.keep_alive);
        let reparsed = parse_request(&rewritten).unwrap();
        assert!(reparsed.keep_alive);
        assert_eq!(reparsed.body, BodyKind::Length(2));
        let text = String::from_utf8_lossy(&rewritten);
        assert!(text.contains("Connection: keep-alive\r\n"), "{text}");
        assert!(!text.contains("Connection: close"), "{text}");

        // Multi-token Connection header preserves non-close tokens.
        let multi_tok =
            b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nConnection: close, te\r\nContent-Length: 2\r\n\r\n";
        let mut req2 = parse_request(multi_tok).unwrap();
        assert!(!req2.keep_alive);
        let rewritten2 = rewrite_request_for_backend_pool(multi_tok, &mut req2);
        assert!(req2.keep_alive);
        let text2 = String::from_utf8_lossy(&rewritten2);
        assert!(text2.contains("Connection: keep-alive, te\r\n"), "{text2}");

        // Multiple Connection header lines where the second has close.
        let multi_hdr = b"POST /v1/chat HTTP/1.1\r\nHost: xlb\r\nConnection: keep-alive\r\nConnection: close\r\n\r\n";
        let mut req3 = parse_request(multi_hdr).unwrap();
        assert!(!req3.keep_alive);
        let rewritten3 = rewrite_request_for_backend_pool(multi_hdr, &mut req3);
        assert!(req3.keep_alive);
        let reparsed3 = parse_request(&rewritten3).unwrap();
        assert!(reparsed3.keep_alive);
    }
}
