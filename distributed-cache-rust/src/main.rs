pub mod cmd;
pub mod resp;
pub mod store;

use cmd::{execute_command, parse_command};
use resp::parser::RespBuffer;
use store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info};

async fn handle_connection(mut stream: tokio::net::TcpStream, store: Store) {
    let peer_addr = match stream.peer_addr() {
        Ok(addr) => addr,
        Err(_) => {
            error!("Failed to get peer address");
            return;
        }
    };

    info!("New connection from: {}", peer_addr);

    let mut buf = [0u8; 4096];
    let mut resp_buf = RespBuffer::new();

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => {
                info!("Connection closed by peer: {}", peer_addr);
                break;
            }
            Ok(n) => {
                resp_buf.feed(&buf[..n]);

                // Extract all complete frames from the buffer.
                loop {
                    match resp_buf.try_parse() {
                        Ok(Some(frame)) => {
                            // Expect an array of arguments (the command).
                            let args = match &frame {
                                resp::parser::RespValue::Array(Some(items)) => items.clone(),
                                other => {
                                    let err =
                                        resp::parser::RespValue::Error(format!(
                                            "ERR expected array, got {:?}",
                                            other
                                        ));
                                    if let Err(e) = stream.write_all(&err.encode()).await {
                                        error!(peer = %peer_addr, error = %e, "write error");
                                    }
                                    continue;
                                }
                            };

                            // Parse & execute.
                            let response = match parse_command(&args) {
                                Ok(cmd) => {
                                    info!(peer = %peer_addr, command = ?cmd, "executing");
                                    execute_command(&cmd, &store)
                                }
                                Err(err_val) => err_val.encode(),
                            };

                            if let Err(e) = stream.write_all(&response).await {
                                error!(peer = %peer_addr, error = %e, "write error");
                                break;
                            }
                        }
                        Ok(None) => {
                            // Need more data.
                            break;
                        }
                        Err(e) => {
                            error!(peer = %peer_addr, error = %e, "parse error, skipping");
                            // RespBuffer already advanced past the garbage.
                        }
                    }
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

    let store = Store::new();

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("Accepted connection from: {}", addr);
                let store = store.clone();
                tokio::spawn(async move {
                    handle_connection(stream, store).await;
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
    use super::*;

    #[tokio::test]
    async fn integration_set_get() {
        let store = Store::new();
        let bind_addr = "127.0.0.1:0"; // OS picks port
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        let port = listener.local_addr().unwrap().port();

        // Spawn server on random port.
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let store = store.clone();
                        tokio::spawn(async move {
                            handle_connection(stream, store).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        // Let server start.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect a TCP client.
        let mut client = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        // --- PING ---
        client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut buf = [0u8; 32];
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buf[..n], b"+PONG\r\n", "PING should return +PONG");

        // --- SET ---
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n", "SET should return +OK");

        // --- GET ---
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buf[..n], b"$5\r\nhello\r\n", "GET should return bulk string");

        // --- DEL ---
        client
            .write_all(b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buf[..n], b":1\r\n", "DEL should return :1");

        // --- GET after DEL → nil ---
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buf[..n], b"$-1\r\n", "GET after DEL should return nil");

        // --- EXISTS ---
        client
            .write_all(b"*2\r\n$6\r\nEXISTS\r\n$3\r\nkey\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buf[..n], b":0\r\n", "EXISTS should return :0");
    }
}
