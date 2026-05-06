use tracing::{info};
use tracing_subscriber;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    cache_size_mb: usize,
    listen_addr: String,
    replication_factor: u8,
}


fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO) //Включаем логирование
        .init();

    info!("Application started");

    let config = Config {
        cache_size_mb: 1024,
        listen_addr: "127.0.0.1:8080".to_string(),
        replication_factor: 3,
    };

    info!(?config, "Configuration loaded");
}
