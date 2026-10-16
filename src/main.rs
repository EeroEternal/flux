use anyhow::Context;
use xlb::config::Config;
use xlb::server::XlbServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let options = parse_args();
    if options.help {
        print_help();
        return Ok(());
    }
    init_tracing();

    let config = Config::load(&options.config)
        .with_context(|| format!("failed to load config {}", options.config))?;

    if options.check {
        println!("configuration OK: {}", options.config);
        return Ok(());
    }

    let server = XlbServer::start(config).await?;
    tracing::info!(
        addr = %server.addr,
        "xlb listening (press Ctrl-C to stop)"
    );

    wait_for_shutdown().await;
    tracing::info!("shutdown signal received");
    server.stop().await;
    tracing::info!("xlb stopped");
    Ok(())
}

struct Options {
    config: String,
    check: bool,
    help: bool,
}

fn parse_args() -> Options {
    let mut options = Options {
        config: "config.toml".to_string(),
        check: false,
        help: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-c" | "--config" => {
                if let Some(path) = args.next() {
                    options.config = path;
                }
            }
            "--check" => options.check = true,
            "-h" | "--help" => options.help = true,
            other => options.config = other.to_string(),
        }
    }
    options
}

fn print_help() {
    println!(
        "xlb - streaming-first AI load balancer\n\n\
         USAGE:\n  xlb [OPTIONS] [CONFIG]\n\n\
         OPTIONS:\n  -c, --config <PATH>  Path to the TOML config (default: config.toml)\n      --check          Validate the config and exit\n  -h, --help           Print help\n\n\
         ENV:\n  RUST_LOG             Log filter (default: xlb=info)\n  XLB_LOG_JSON       Set to 1 for JSON logs"
    );
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("xlb=info"));
    let json = std::env::var("XLB_LOG_JSON")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if json {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init();
    } else {
        let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    }
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
