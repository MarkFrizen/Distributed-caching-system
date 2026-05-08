//! Модуль репликации master‑slave.
//!
//! # Схема работы
//!
//! * **Master**: при подключении реплики (команда `SYNC`) отправляет снимок всех
//!   данных как последовательность RESP2-команд SET, затем подписывается на
//!   `broadcast::Sender` и транслирует все изменяющие команды в реальном времени.
//! * **Replica**: фоновый клиент, который подключается к мастеру, отправляет
//!   `SYNC`, получает снимок и поток живых команд, применяя их к своему хранилищу.
//!   При разрыве соединения автоматически переподключается.
//!
//! # Конфигурация (переменные окружения)
//!
//! * `CACHE_ROLE` — `"master"` или `"replica"`. По умолчанию репликация отключена.
//! * `CACHE_MASTER` — адрес мастера (только для реплики), напр. `"127.0.0.1:8080"`.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::cmd;
use crate::resp::parser::{RespBuffer, RespValue};
use crate::store::Store;

// ---------------------------------------------------------------------------
// Конфигурация
// ---------------------------------------------------------------------------

/// Роль узла в репликации.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationRole {
    /// Узел‑мастер: принимает SYNC‑соединения от реплик.
    Master,
    /// Узел‑реплика: подключается к мастеру и синхронизируется.
    Replica { master_addr: String },
}

/// Конфигурация репликации, прочитанная из переменных окружения.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub role: ReplicationRole,
}

impl ReplicationConfig {
    /// Читает `CACHE_ROLE` (и `CACHE_MASTER` для реплики) из окружения.
    ///
    /// Если `CACHE_ROLE` не задана — возвращается `None` (репликация отключена).
    pub fn from_env() -> Option<Self> {
        let role = std::env::var("CACHE_ROLE").ok()?.to_lowercase();
        match role.as_str() {
            "master" => Some(ReplicationConfig {
                role: ReplicationRole::Master,
            }),
            "replica" => {
                let master_addr = std::env::var("CACHE_MASTER")
                    .unwrap_or_else(|_| "127.0.0.1:8080".into());
                Some(ReplicationConfig {
                    role: ReplicationRole::Replica { master_addr },
                })
            }
            other => {
                warn!("Неизвестная роль '{}' в CACHE_ROLE, репликация отключена", other);
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Master — состояние для рассылки команд репликам
// ---------------------------------------------------------------------------

/// Состояние мастера: широковещательный канал для отправки изменяющих команд
/// всем подключённым репликам.
#[derive(Clone)]
pub struct MasterState {
    sender: broadcast::Sender<Vec<u8>>,
}

impl MasterState {
    /// Создаёт новый `MasterState` с каналом ёмкостью 1024.
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1024);
        MasterState { sender }
    }

    /// Возвращает клон отправителя для вставки изменяющих команд.
    pub fn sender(&self) -> broadcast::Sender<Vec<u8>> {
        self.sender.clone()
    }

    /// Обрабатывает SYNC-запрос от реплики.
    ///
    /// 1. Отправляет снимок текущих данных (RESP2-команды SET).
    /// 2. Подписывается на `broadcast::Receiver` и транслирует живые команды.
    ///
    /// Принимает владение `stream` — после вызова соединение считается
    /// захваченным репликационным протоколом.
    pub async fn handle_sync(&self, mut stream: TcpStream, store: &Store) {
        let peer = match stream.peer_addr() {
            Ok(a) => a.to_string(),
            Err(_) => "unknown".into(),
        };
        info!(peer = %peer, "Начало SYNC с репликой");

        // 1. Снимок данных: все живые ключи → SET-команды.
        let entries = store.snapshot_entries();
        info!(peer = %peer, count = entries.len(), "Отправка снимка данных реплике");
        for (key, value, ttl_secs) in &entries {
            let cmd_frame = if let Some(ttl) = ttl_secs {
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"SET".to_vec())),
                    RespValue::BulkString(Some(key.clone())),
                    RespValue::BulkString(Some(value.clone())),
                    RespValue::BulkString(Some(b"EX".to_vec())),
                    RespValue::BulkString(Some(ttl.to_string().into_bytes())),
                ]))
            } else {
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"SET".to_vec())),
                    RespValue::BulkString(Some(key.clone())),
                    RespValue::BulkString(Some(value.clone())),
                ]))
            };
            if let Err(e) = stream.write_all(&cmd_frame.encode()).await {
                warn!(peer = %peer, error = %e, "Ошибка записи снимка реплике");
                return;
            }
        }

        // Маркер окончания снимка: специальная строка `+SYNC_DONE\r\n`.
        if let Err(e) = stream.write_all(b"+SYNC_DONE\r\n").await {
            warn!(peer = %peer, error = %e, "Ошибка записи маркера SYNC_DONE");
            return;
        }
        info!(peer = %peer, "Снимок отправлен, переход к трансляции живых команд");

        // 2. Подписываемся на широковещательный канал и транслируем.
        let mut rx = self.sender.subscribe();
        loop {
            match rx.recv().await {
                Ok(frame_bytes) => {
                    if let Err(e) = stream.write_all(&frame_bytes).await {
                        warn!(peer = %peer, error = %e, "Реплика отключилась от потока команд");
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(peer = %peer, skipped, "Реплика отстала, пропущено команд");
                    // Продолжаем — следующие команды будут доставлены.
                }
                Err(broadcast::error::RecvError::Closed) => {
                    info!(peer = %peer, "Канал трансляции закрыт");
                    break;
                }
            }
        }
    }
}

impl Default for MasterState {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Replica — фоновый клиент, подключающийся к мастеру
// ---------------------------------------------------------------------------

/// Запускает фоновую задачу реплики.
///
/// Подключается к мастеру по адресу `master_addr`, отправляет `SYNC`,
/// получает снимок данных, затем транслирует живые команды, применяя их
/// к `store`.
///
/// При обрыве соединения автоматически переподключается после паузы в 1 с.
pub fn spawn_replica_client(master_addr: String, store: Store) {
    tokio::spawn(async move {
        loop {
            info!(master = %master_addr, "Реплика: попытка подключения к мастеру");

            match TcpStream::connect(&master_addr).await {
                Ok(mut stream) => {
                    info!(master = %master_addr, "Реплика: подключена к мастеру");

                    // Отправляем SYNC.
                    if let Err(e) = stream
                        .write_all(b"*1\r\n$4\r\nSYNC\r\n")
                        .await
                    {
                        warn!(master = %master_addr, error = %e, "Реплика: ошибка отправки SYNC");
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    // Читаем ответы от мастера.
                    let mut buf = [0u8; 65536];
                    let mut resp_buf = RespBuffer::new();

                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) => {
                                warn!(master = %master_addr, "Реплика: мастер закрыл соединение");
                                break;
                            }
                            Ok(n) => {
                                resp_buf.feed(&buf[..n]);

                                loop {
                                    match resp_buf.try_parse() {
                                        Ok(Some(RespValue::SimpleString(s)))
                                            if s == "SYNC_DONE" =>
                                        {
                                            info!("Реплика: снимок получен, переход к живым командам");
                                        }
                                        Ok(Some(RespValue::Array(Some(items)))) => {
                                            // Применяем команду к локальному хранилищу.
                                            if let Ok(cmd) = cmd::parse_command(&items)
                                                && cmd.is_modifying()
                                            {
                                                cmd::execute_command(&cmd, &store);
                                            }
                                        }
                                        Ok(Some(_)) => {
                                            // Прочие RESP-значения (например, +OK) — игнорируем.
                                        }
                                        Ok(None) => break, // Нужно больше данных.
                                        Err(e) => {
                                            warn!(error = %e, "Реплика: ошибка разбора RESP");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(master = %master_addr, error = %e, "Реплика: ошибка чтения");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(master = %master_addr, error = %e, "Реплика: не удалось подключиться");
                }
            }

            info!(master = %master_addr, "Реплика: переподключение через 1 с");
            sleep(Duration::from_secs(1)).await;
        }
    });
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{Persistence, PersistenceConfig};
    use crate::resp::parser::RespValue;

    /// Интеграционный тест: мастер + реплика.
    ///
    /// 1. Запускает мастер на случайном порту.
    /// 2. Запускает реплику, которая подключается к мастеру через SYNC.
    /// 3. SET на мастере → проверяет GET на реплике.
    /// 4. DEL на мастере → проверяет GET на реплике (nil).
    #[tokio::test]
    async fn integration_master_replica_sync() {
        let test_dir = std::env::temp_dir().join("cache_int_replication");
        let _ = std::fs::remove_dir_all(&test_dir);

        // --- Мастер ---
        let master_store = Store::new();
        let master_persistence = Persistence::new(PersistenceConfig {
            data_dir: test_dir.join("master"),
            aof_enabled: false,
            ..Default::default()
        });
        master_persistence.init().await.unwrap();
        let master_state = MasterState::new();

        let master_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let master_port = master_listener.local_addr().unwrap().port();
        let master_addr = format!("127.0.0.1:{}", master_port);

        let m_store = master_store.clone();
        let m_pers = master_persistence.clone();
        let m_state = master_state.clone();
        let cluster = crate::cluster::ClusterState::disabled();
        tokio::spawn(async move {
            loop {
                match master_listener.accept().await {
                    Ok((stream, _)) => {
                        let s = m_store.clone();
                        let p = m_pers.clone();
                        let m = m_state.clone();
                        let c = cluster.clone();
                        tokio::spawn(async move {
                            crate::handle_connection(stream, s, p, c, Some(m)).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        // --- Реплика ---
        let replica_store = Store::new();
        let _replica_task = spawn_replica_client(master_addr.clone(), replica_store.clone());

        // Ждём, пока реплика подключится и синхронизируется.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // --- SET на мастере ---
        let mut master_client = tokio::net::TcpStream::connect(&master_addr)
            .await
            .unwrap();
        master_client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 32];
        let n = tokio::time::timeout(Duration::from_secs(3), master_client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n", "SET на мастере должен вернуть +OK");

        // Ждём репликацию.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // --- GET на реплике ---
        let get_resp = replica_store.get(b"key");
        assert_eq!(
            get_resp,
            RespValue::BulkString(Some(b"value".to_vec())).encode(),
            "Реплика должна вернуть значение, установленное на мастере"
        );

        // --- DEL на мастере ---
        master_client
            .write_all(b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n")
            .await
            .unwrap();
        let n = tokio::time::timeout(Duration::from_secs(3), master_client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b":1\r\n", "DEL на мастере должен вернуть :1");

        // Ждём репликацию DEL.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // --- GET на реплике (должен быть nil) ---
        let get_resp = replica_store.get(b"key");
        assert_eq!(
            get_resp,
            RespValue::BulkString(None).encode(),
            "Реплика должна отразить DEL: ключ должен отсутствовать"
        );

        // --- Очистка ---
        let _ = master_client.shutdown().await;
        let _ = std::fs::remove_dir_all(&test_dir);
    }

    /// Тест: SYNC с предварительно заполненными данными.
    #[tokio::test]
    async fn integration_replica_gets_snapshot() {
        let test_dir = std::env::temp_dir().join("cache_int_repl_snapshot");
        let _ = std::fs::remove_dir_all(&test_dir);

        // --- Мастер с предзаполненными данными ---
        let master_store = Store::new();
        master_store.set(b"k1", b"v1", None);
        master_store.set(b"k2", b"v2", Some(3600));
        let master_persistence = Persistence::new(PersistenceConfig {
            data_dir: test_dir.join("master"),
            aof_enabled: false,
            ..Default::default()
        });
        master_persistence.init().await.unwrap();
        let master_state = MasterState::new();

        let master_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let master_port = master_listener.local_addr().unwrap().port();
        let master_addr = format!("127.0.0.1:{}", master_port);

        let m_store = master_store.clone();
        let m_pers = master_persistence.clone();
        let m_state = master_state.clone();
        let cluster = crate::cluster::ClusterState::disabled();
        tokio::spawn(async move {
            loop {
                match master_listener.accept().await {
                    Ok((stream, _)) => {
                        let s = m_store.clone();
                        let p = m_pers.clone();
                        let m = m_state.clone();
                        let c = cluster.clone();
                        tokio::spawn(async move {
                            crate::handle_connection(stream, s, p, c, Some(m)).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        // --- Реплика ---
        let replica_store = Store::new();
        let _replica_task = spawn_replica_client(master_addr.clone(), replica_store.clone());

        // Ждём синхронизацию.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // --- Проверка снимка ---
        assert_eq!(
            replica_store.get(b"k1"),
            RespValue::BulkString(Some(b"v1".to_vec())).encode(),
            "Реплика должна получить k1 из снимка"
        );
        assert_eq!(
            replica_store.get(b"k2"),
            RespValue::BulkString(Some(b"v2".to_vec())).encode(),
            "Реплика должна получить k2 из снимка"
        );

        let _ = std::fs::remove_dir_all(&test_dir);
    }
}
