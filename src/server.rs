//! Listener, accept loop, graceful shutdown and the metrics endpoint.

use crate::backend::Scheduler;
use crate::config::{Config, TlsConfig};
use crate::health;
use crate::metrics::Metrics;
use crate::proxy;
use socket2::{Domain, Protocol, Socket, Type};
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;
use tokio_rustls::rustls;
use tokio_rustls::TlsAcceptor;

/// Shared, immutable state handed to every connection task.
pub struct AppState {
    /// Effective configuration.
    pub config: Config,
    /// Backend scheduler.
    pub scheduler: Scheduler,
    /// Prometheus metrics.
    pub metrics: Arc<Metrics>,
    /// Optional TLS acceptor for client connections.
    pub tls: Option<TlsAcceptor>,
}

/// A running xLB instance.
pub struct XlbServer {
    /// The address xLB is listening on.
    pub addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    handles: Vec<JoinHandle<()>>,
}

impl XlbServer {
    /// Bind, start accepting and start background tasks.
    pub async fn start(config: Config) -> anyhow::Result<Self> {
        config.validate()?;
        let metrics = Metrics::new()?;
        let scheduler = Scheduler::new(&config, metrics.clone())?;
        let listener = bind(&config)?;
        let addr = listener.local_addr()?;

        let state = Arc::new(AppState {
            config: config.clone(),
            scheduler,
            metrics: metrics.clone(),
            tls: load_tls(&config.server.tls)?,
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut handles = Vec::new();

        handles.push(tokio::spawn(accept_loop(
            listener,
            state.clone(),
            shutdown_rx.clone(),
        )));

        handles.extend(health::spawn_all(
            state.scheduler.backends().to_vec(),
            config.backends.health_check_path.clone(),
            config.backends.health_check_headers.clone(),
            config.backends.health_interval,
            config.backends.health_check_timeout,
            metrics.clone(),
            shutdown_rx.clone(),
        ));

        if config.metrics.enabled {
            handles.push(tokio::spawn(metrics_server(
                config.metrics.bind.clone(),
                metrics.clone(),
                shutdown_rx.clone(),
            )));
        }

        Ok(Self {
            addr,
            shutdown: shutdown_tx,
            handles,
        })
    }

    /// Stop accepting, notify background tasks and wait for them to finish.
    pub async fn stop(self) {
        let _ = self.shutdown.send(true);
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}

fn bind(config: &Config) -> anyhow::Result<TcpListener> {
    let addr = config.bind_addr()?;
    let domain = Domain::for_address(addr);
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    if config.server.reuseport {
        #[cfg(unix)]
        socket.set_reuse_port(true)?;
        #[cfg(not(unix))]
        tracing::warn!("SO_REUSEPORT requested but unsupported on this platform");
    }
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let std_listener: std::net::TcpListener = socket.into();
    Ok(TcpListener::from_std(std_listener)?)
}

fn load_tls(config: &TlsConfig) -> anyhow::Result<Option<TlsAcceptor>> {
    if !config.enabled {
        return Ok(None);
    }
    let cert_path = config
        .cert
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("server.tls.cert is required when TLS is enabled"))?;
    let key_path = config
        .key
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("server.tls.key is required when TLS is enabled"))?;

    let cert_file = File::open(cert_path)
        .map_err(|e| anyhow::anyhow!("cannot open TLS cert {cert_path}: {e}"))?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("cannot parse TLS cert {cert_path}: {e}"))?;
    if certs.is_empty() {
        anyhow::bail!("no certificates found in {cert_path}");
    }

    let key_file =
        File::open(key_path).map_err(|e| anyhow::anyhow!("cannot open TLS key {key_path}: {e}"))?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(|e| anyhow::anyhow!("cannot parse TLS key {key_path}: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {key_path}"))?;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    // Advertise HTTP/1.1 only; xLB does not proxy HTTP/2 in thin-l7.
    server_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    tracing::info!(cert = %cert_path, "TLS termination enabled");
    Ok(Some(TlsAcceptor::from(Arc::new(server_config))))
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<AppState>,
    mut shutdown: watch::Receiver<bool>,
) {
    let semaphore = Arc::new(Semaphore::new(state.config.server.max_connections.max(1)));
    let mut tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        if state.config.server.tcp_nodelay {
                            let _ = stream.set_nodelay(true);
                        }
                        crate::pool::configure_tcp_keepalive(&stream);
                        let permit = match semaphore.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                state.metrics.rejected("queue_full");
                                drop(stream);
                                continue;
                            }
                        };
                        state.metrics.connections_total.inc();
                        state.metrics.active_connections.inc();
                        let state = state.clone();
                        tasks.spawn(async move {
                            let outcome = if let Some(acceptor) = state.tls.clone() {
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        proxy::handle(tls_stream, peer, state.clone()).await
                                    }
                                    Err(e) => {
                                        tracing::debug!(%peer, error = %e, "tls handshake failed");
                                        Ok(())
                                    }
                                }
                            } else {
                                proxy::handle(stream, peer, state.clone()).await
                            };
                            if let Err(e) = outcome {
                                tracing::debug!(%peer, error = %e, "connection finished with error");
                            }
                            state.metrics.active_connections.dec();
                            drop(permit);
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
                // Reap finished connection tasks so the JoinSet does not grow
                // without bound.
                while tasks.try_join_next().is_some() {}
            }
        }
    }

    let drain = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(30), drain)
        .await
        .is_err()
    {
        tracing::warn!("graceful drain timed out; aborting remaining connections");
        tasks.abort_all();
    }
}

async fn metrics_server(bind: String, metrics: Arc<Metrics>, mut shutdown: watch::Receiver<bool>) {
    let addr = match crate::config::resolve_addr(&bind) {
        Some(addr) => addr,
        None => {
            tracing::warn!(bind, "cannot resolve metrics bind address");
            return;
        }
    };
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::warn!(bind, error = %e, "cannot bind metrics endpoint");
            return;
        }
    };
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            result = listener.accept() => {
                if let Ok((stream, _)) = result {
                    tokio::spawn(serve_metrics(stream, metrics.clone()));
                }
            }
        }
    }
}

async fn serve_metrics(mut stream: TcpStream, metrics: Arc<Metrics>) {
    let mut buf = [0u8; 1024];
    let n = match stream.read(&mut buf).await {
        Ok(n) => n,
        Err(_) => return,
    };
    let request = String::from_utf8_lossy(&buf[..n]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let response = if path == "/health" {
        "ok\n".to_string()
    } else {
        metrics.encode()
    };
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        response.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}
