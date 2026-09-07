use feed_mux::{init_tracing, run, config::Config};

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring crypto provider");

    init_tracing();
    let config = Config::from_env();
    if let Err(e) = run(config).await {
        tracing::error!(error = %e, "feed-mux exited with error");
        std::process::exit(1);
    }
}
