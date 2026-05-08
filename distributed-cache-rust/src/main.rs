pub mod resp;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber;

async fn handle_connection(mut stream: tokio::net::TcpStream) {
    let peer_addr = match stream.peer_addr() {
        Ok(addr) => addr,
        Err(_) => {
            error!("Failed to get peer address");
            return;
        }
    };

    info!("New connection from: {}", peer_addr);

    let mut buf = [0u8; 1024];

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => {
                info!("Connection closed by peer: {}", peer_addr);
                break;
            }
            Ok(n) => {
                info!(bytes = n, peer = %peer_addr, "Received data, responding PONG");
                if let Err(e) = stream.write_all(b"+PONG\r\n").await {
                    error!(peer = %peer_addr, error = %e, "Failed to send PONG");
                    break;
                }
            }
            Err(e) => {
                error!(peer = %peer_addr, error = %e, "Error reading from connection");
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let bind_addr = "127.0.0.1:8080";

    let listener = TcpListener::bind(bind_addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind to {}: {}", bind_addr, e));

    info!("Server listening on {}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("Accepted connection from: {}", addr);
                tokio::spawn(async move {
                    handle_connection(stream).await;
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
