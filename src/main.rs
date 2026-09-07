use feed_mux::{config::Config, init_tracing, run};

fn init_rustls_crypto_provider() {
    // rustls 0.23 requires an explicit process-level CryptoProvider before any TLS.
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
}

#[tokio::main]
async fn main() {
    init_rustls_crypto_provider();
    init_tracing();
    let config = Config::from_env();
    if let Err(e) = run(config).await {
        tracing::error!(error = %e, "feed-mux exited with error");
        std::process::exit(1);
    }
}
