pub mod cmd;
pub mod persistence;
pub mod resp;
pub mod store;

use cmd::{execute_command, parse_command, Command};
use persistence::{Persistence, PersistenceConfig};
use resp::parser::RespBuffer;
use store::{Store, StoreConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};
use tracing::{error, info};

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    store: Store,
    persistence: Persistence,
) {
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
                                    let err = resp::parser::RespValue::Error(format!(
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

                                    // BGSAVE обрабатывается через Persistence.
                                    if matches!(cmd, Command::Bgsave) {
                                        persistence.save_snapshot(&store).await
                                    } else {
                                        // AOF: логируем изменяющие команды.
                                        if cmd.is_modifying() {
                                            persistence.append_command(&frame).await;
                                        }
                                        execute_command(&cmd, &store)
                                    }
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

    // Адрес привязки: из переменной окружения CACHE_BIND или по умолчанию 127.0.0.1:8080.
    let bind_addr = std::env::var("CACHE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into());

    let listener = TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| panic!("Не удалось привязаться к {}: {}", bind_addr, e));

    info!("Сервер слушает на {}", bind_addr);

    // Инициализация хранилища.
    let store_config = StoreConfig {
        max_memory_mb: 128,
    };
    let store = Store::with_config(store_config);

    // Инициализация персистентности.
    let persistence_config = PersistenceConfig {
        data_dir: "./data".into(),
        rdb_filename: "dump.rdb".into(),
        aof_filename: "appendonly.aof".into(),
        snapshot_interval_secs: 60,
        aof_enabled: true,
    };
    let persistence = Persistence::new(persistence_config);

    // Создаём директорию данных и загружаем существующие данные.
    if let Err(e) = persistence.init().await {
        error!("Ошибка инициализации директории данных: {}", e);
    }
    persistence.load(&store).await;

    // --- Фоновые задачи ---

    // 1. Фоновая очистка просроченных TTL-ключей (каждые 100 мс).
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

    // 2. Периодический RDB-снимок (каждые 60 секунд).
    let snapshot_persistence = persistence.clone();
    let snapshot_store = store.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(60)).await;
            info!("Фоновый RDB-снимок");
            snapshot_persistence.save_snapshot(&snapshot_store).await;
        }
    });

    // 3. Периодический сброс AOF-буфера на диск (каждые 2 секунды).
    let flush_persistence = persistence.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(2)).await;
            flush_persistence.flush_aof().await;
        }
    });

    // --- Приём соединений ---
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("Принято соединение от: {}", addr);
                let store = store.clone();
                let persistence = persistence.clone();
                tokio::spawn(async move {
                    handle_connection(stream, store, persistence).await;
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

    /// Интеграционный тест: SET → BGSAVE → новое хранилище → GET.
    #[tokio::test]
    async fn integration_bgsave_and_restore() {
        // Настраиваем временную директорию для теста.
        let test_dir = std::env::temp_dir().join("cache_int_bgsave");
        let _ = std::fs::remove_dir_all(&test_dir);

        let store = Store::new();
        let persistence = Persistence::new(PersistenceConfig {
            data_dir: test_dir.clone(),
            aof_enabled: false,
            snapshot_interval_secs: 0,
            ..Default::default()
        });
        persistence.init().await.unwrap();

        // Запускаем сервер на случайном порту.
        let bind_addr = "127.0.0.1:0";
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_store = store.clone();
        let server_persistence = persistence.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let s = server_store.clone();
                        let p = server_persistence.clone();
                        tokio::spawn(async move {
                            handle_connection(stream, s, p).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Подключаемся и выполняем SET.
        let mut client = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        // Принудительный сброс, чтобы команды не склеились.
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$5\r\nhello\r\n")
            .await
            .unwrap();
        client.flush().await.unwrap();
        let mut buf = [0u8; 32];
        let n = tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n", "SET должен возвращать +OK");

        // Небольшая пауза между командами для синхронизации.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Выполняем BGSAVE.
        client
            .write_all(b"*1\r\n$6\r\nBGSAVE\r\n")
            .await
            .unwrap();
        client.flush().await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n", "BGSAVE должен возвращать +OK");

        // Закрываем соединение.
        let _ = client.shutdown().await;
        // Даём время на завершение записи.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Создаём новое хранилище и загружаем RDB.
        let store2 = Store::new();
        persistence.load(&store2).await;

        // Проверяем, что данные восстановлены.
        assert_eq!(
            store2.get(b"k1"),
            resp::parser::RespValue::BulkString(Some(b"hello".to_vec())).encode()
        );

        // Очистка.
        let _ = std::fs::remove_dir_all(&test_dir);
    }

    /// Интеграционный тест: проверка AOF-логирования через handle_connection.
    #[tokio::test]
    async fn integration_aof_logging() {
        let test_dir = std::env::temp_dir().join("cache_int_aof");
        let _ = std::fs::remove_dir_all(&test_dir);

        let store = Store::new();
        let persistence = Persistence::new(PersistenceConfig {
            data_dir: test_dir.clone(),
            aof_enabled: true,
            snapshot_interval_secs: 0,
            ..Default::default()
        });
        persistence.init().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_store = store.clone();
        let server_persistence = persistence.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let s = server_store.clone();
                        let p = server_persistence.clone();
                        tokio::spawn(async move {
                            handle_connection(stream, s, p).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        // Принудительный сброс, чтобы команды не склеились.
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$3\r\nval\r\n")
            .await
            .unwrap();
        client.flush().await.unwrap();
        let mut buf = [0u8; 32];
        let _ = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\ny\r\n$3\r\nval\r\n")
            .await
            .unwrap();
        client.flush().await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();

        // Сбрасываем AOF.
        persistence.flush_aof().await;

        // Закрываем соединение.
        let _ = client.shutdown().await;

        // Проверяем, что AOF-файл существует и непуст.
        let aof_path = test_dir.join("appendonly.aof");
        assert!(aof_path.exists(), "AOF-файл должен существовать");
        let meta = std::fs::metadata(&aof_path).unwrap();
        assert!(meta.len() > 0, "AOF-файл не должен быть пустым");

        // Создаём новое хранилище и проигрываем AOF.
        let store2 = Store::new();
        persistence.load(&store2).await;

        assert_eq!(
            store2.get(b"x"),
            resp::parser::RespValue::BulkString(Some(b"val".to_vec())).encode()
        );
        assert_eq!(
            store2.get(b"y"),
            resp::parser::RespValue::BulkString(Some(b"val".to_vec())).encode()
        );

        let _ = std::fs::remove_dir_all(&test_dir);
    }

    /// Интеграционный тест: базовый SET/GET/DEL/EXISTS/PING через сокет.
    #[tokio::test]
    async fn integration_set_get() {
        let test_dir = std::env::temp_dir().join("cache_int_basic");
        let _ = std::fs::remove_dir_all(&test_dir);

        let store = Store::new();
        let persistence = Persistence::new(PersistenceConfig {
            data_dir: test_dir.clone(),
            aof_enabled: false,
            snapshot_interval_secs: 0,
            ..Default::default()
        });
        persistence.init().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let s = store.clone();
                        let p = persistence.clone();
                        tokio::spawn(async move {
                            handle_connection(stream, s, p).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        // --- PING ---
        client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut buf = [0u8; 32];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"+PONG\r\n", "PING должен возвращать +PONG");

        // --- SET ---
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n", "SET должен возвращать +OK");

        // --- GET ---
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"$5\r\nhello\r\n", "GET должен возвращать строку-батон");

        // --- DEL ---
        client
            .write_all(b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b":1\r\n", "DEL должен возвращать :1");

        // --- GET after DEL -> nil ---
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"$-1\r\n", "GET после DEL должен возвращать nil");

        // --- EXISTS ---
        client
            .write_all(b"*2\r\n$6\r\nEXISTS\r\n$3\r\nkey\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b":0\r\n", "EXISTS должен возвращать :0");

        let _ = client.shutdown().await;
        let _ = std::fs::remove_dir_all(&test_dir);
    }

    /// Интеграционный тест: PING не должен логироваться в AOF.
    #[tokio::test]
    async fn integration_ping_not_logged() {
        let test_dir = std::env::temp_dir().join("cache_int_ping_aof");
        let _ = std::fs::remove_dir_all(&test_dir);

        let store = Store::new();
        let persistence = Persistence::new(PersistenceConfig {
            data_dir: test_dir.clone(),
            aof_enabled: true,
            snapshot_interval_secs: 0,
            ..Default::default()
        });
        persistence.init().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_store = store.clone();
        let server_persistence = persistence.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let s = server_store.clone();
                        let p = server_persistence.clone();
                        tokio::spawn(async move {
                            handle_connection(stream, s, p).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 32];
        let _ = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();

        persistence.flush_aof().await;
        let _ = client.shutdown().await;

        // AOF-файл должен быть пустым (PING не логируется).
        let aof_path = test_dir.join("appendonly.aof");
        if aof_path.exists() {
            let meta = std::fs::metadata(&aof_path).unwrap();
            assert_eq!(meta.len(), 0, "AOF должен быть пустым после PING");
        }

        let _ = std::fs::remove_dir_all(&test_dir);
    }
}
