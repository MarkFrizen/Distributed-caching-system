pub mod cmd;
pub mod resp;
pub mod store;

use cmd::{execute_command, parse_command};
use resp::parser::RespBuffer;
use store::{Store, StoreConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};
use tracing::{error, info};

async fn handle_connection(mut stream: tokio::net::TcpStream, store: Store) {
    let peer_addr = match stream.peer_addr() {
        Ok(addr) => addr,
        Err(_) => {
            error!("Не удалось получить адрес клиента");
            return;
        }
    };

    info!("Новое соединение от: {}", peer_addr);

    let mut buf = [0u8; 4096];
    let mut resp_buf = RespBuffer::new();

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => {
                info!("Соединение закрыто клиентом: {}", peer_addr);
                break;
            }
            Ok(n) => {
                resp_buf.feed(&buf[..n]);

                // Извлекаем все полные фреймы из буфера.
                loop {
                    match resp_buf.try_parse() {
                        Ok(Some(frame)) => {
                            // Ожидаем массив аргументов (команду).
                            let args = match &frame {
                                resp::parser::RespValue::Array(Some(items)) => items.clone(),
                                other => {
                                    let err =
                                        resp::parser::RespValue::Error(format!(
                                            "ERR expected array, got {:?}",
                                            other
                                        ));
                                    if let Err(e) = stream.write_all(&err.encode()).await {
                                        error!(peer = %peer_addr, error = %e, "Ошибка записи");
                                    }
                                    continue;
                                }
                            };

                            // Разбираем и выполняем.
                            let response = match parse_command(&args) {
                                Ok(cmd) => {
                                    info!(peer = %peer_addr, command = ?cmd, "Выполнение команды");
                                    execute_command(&cmd, &store)
                                }
                                Err(err_val) => err_val.encode(),
                            };

                            if let Err(e) = stream.write_all(&response).await {
                                error!(peer = %peer_addr, error = %e, "Ошибка записи");
                                break;
                            }
                        }
                        Ok(None) => {
                            // Нужно больше данных.
                            break;
                        }
                        Err(e) => {
                            error!(peer = %peer_addr, error = %e, "Ошибка разбора, пропускаем");
                            // RespBuffer уже продвинулся дальше мусора.
                        }
                    }
                }
            }
            Err(e) => {
                error!(peer = %peer_addr, error = %e, "Ошибка чтения из соединения");
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
        .unwrap_or_else(|e| panic!("Не удалось привязаться к {}: {}", bind_addr, e));

    info!("Сервер слушает на {}", bind_addr);

    let config = StoreConfig {
        max_memory_mb: 128,
    };
    let store = Store::with_config(config);

    // Фоновая задача: раз в 100 мс очищает случайную выборку просроченных TTL-ключей.
    let cleanup_store = store.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(100)).await;
            let removed = cleanup_store.clean_expired();
            if removed > 0 {
                info!(removed, "Фоновая очистка TTL: удалено ключей");
            }
        }
    });

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("Принято соединение от: {}", addr);
                let store = store.clone();
                tokio::spawn(async move {
                    handle_connection(stream, store).await;
                });
            }
            Err(e) => {
                error!("Не удалось принять соединение: {}", e);
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
        // Запуск сервера на случайном порту
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

        // Даем серверу время для запуска.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Подключаем TCP-клиент.
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
        assert_eq!(&buf[..n], b"+PONG\r\n", "PING должен возвращать +PONG");

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
        assert_eq!(&buf[..n], b"+OK\r\n", "SET должен возвращать +OK");

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
        assert_eq!(&buf[..n], b"$5\r\nhello\r\n", "GET должен возвращать строку-батон");

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
        assert_eq!(&buf[..n], b":1\r\n", "DEL должен возвращать :1");

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
        assert_eq!(&buf[..n], b"$-1\r\n", "GET после DEL должен возвращать nil");

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
        assert_eq!(&buf[..n], b":0\r\n", "EXISTS должен возвращать :0");
    }
}
