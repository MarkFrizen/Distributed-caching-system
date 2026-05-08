//! Модуль персистентности: RDB-снимки (BGSAVE / по таймеру) и AOF-лог.
//!
//! ## RDB
//! Снимок — это последовательность RESP2-команд `SET key value EX <ttl>`.
//! Запись ведётся во временный файл, затем атомарно переименовывается.
//! При старте сервер читает снимок как поток RESP2 и восстанавливает данные.
//!
//! ## AOF
//! Каждая изменяющая команда (SET, DEL) дописывается в лог-файл.
//! Сброс на диск выполняется с интервалом 2 секунды.
//! При старте сервер проигрывает AOF для восстановления.
//!
//! ## Порядок загрузки
//! Если AOF включён — загружается только AOF (он точнее).
//! Иначе загружается RDB-снимок (если существует).

use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::cmd;
use crate::resp::parser::{RespBuffer, RespValue};
use crate::store::Store;

/// Конфигурация персистентности.
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Директория для хранения файлов данных.
    pub data_dir: PathBuf,
    /// Имя RDB-файла (снимок).
    pub rdb_filename: String,
    /// Имя AOF-файла.
    pub aof_filename: String,
    /// Интервал фоновых снимков в секундах (0 = отключено).
    pub snapshot_interval_secs: u64,
    /// Включить AOF.
    pub aof_enabled: bool,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        PersistenceConfig {
            data_dir: PathBuf::from("./data"),
            rdb_filename: "dump.rdb".into(),
            aof_filename: "appendonly.aof".into(),
            snapshot_interval_secs: 60,
            aof_enabled: true,
        }
    }
}

/// Управление персистентностью: RDB-снимки и AOF-лог.
///
/// Клонирование безопасно: `aof_file` разделяется через `Arc<Mutex<...>>`.
#[derive(Clone)]
pub struct Persistence {
    config: PersistenceConfig,
    rdb_path: PathBuf,
    aof_path: PathBuf,
    /// AOF-файл, защищённый мьютексом для конкурентного доступа.
    aof_file: Arc<Mutex<Option<tokio::fs::File>>>,
}

impl Persistence {
    /// Создаёт новый экземпляр с заданной конфигурацией.
    pub fn new(config: PersistenceConfig) -> Self {
        let rdb_path = config.data_dir.join(&config.rdb_filename);
        let aof_path = config.data_dir.join(&config.aof_filename);
        Persistence {
            config,
            rdb_path,
            aof_path,
            aof_file: Arc::new(Mutex::new(None)),
        }
    }

    /// Создаёт директорию для данных (если не существует).
    pub async fn init(&self) -> Result<(), std::io::Error> {
        tokio::fs::create_dir_all(&self.config.data_dir).await
    }

    /// Загружает данные из AOF или RDB при старте сервера.
    ///
    /// Приоритет:
    /// 1. Если AOF включён — проигрывается AOF (он точнее).
    /// 2. Иначе загружается RDB-снимок (если существует).
    pub async fn load(&self, store: &Store) {
        if self.config.aof_enabled {
            if self.aof_path.exists() {
                info!("Загрузка из AOF: {}", self.aof_path.display());
                match self.replay_aof(store) {
                    Ok(count) => info!("AOF загружен: {} команд", count),
                    Err(e) => warn!("Ошибка загрузки AOF (пропускаем): {}", e),
                }
            } else {
                info!("AOF-файл не найден, пропускаем");
            }
        } else if self.rdb_path.exists() {
            info!("Загрузка из RDB: {}", self.rdb_path.display());
            match self.load_rdb(store) {
                Ok(count) => info!("RDB загружен: {} записей", count),
                Err(e) => warn!("Ошибка загрузки RDB (пропускаем): {}", e),
            }
        } else {
            info!("Файлы данных не найдены, чистая инициализация");
        }
    }

    // -----------------------------------------------------------------------
    // RDB — сохранение снимка
    // -----------------------------------------------------------------------

    /// Сохраняет RDB-снимок: итерирует все записи из `store`, записывает их
    /// как RESP2-команды SET во временный файл, затем атомарно переименовывает.
    ///
    /// Возвращает RESP-ответ `+OK\r\n` при успехе или `-ERR ...\r\n`.
    pub async fn save_snapshot(&self, store: &Store) -> Vec<u8> {
        let tmp_path = self.rdb_path.with_extension("rdb.tmp");

        // Собираем записи.
        let entries = store.snapshot_entries();
        info!(count = entries.len(), "Сохранение RDB-снимка");

        // Сериализуем в RESP2-команды.
        let mut buf = Vec::new();
        for (key, value, ttl_remaining) in &entries {
            let cmd = if let Some(ttl) = ttl_remaining {
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
            buf.extend_from_slice(&cmd.encode());
        }

        // Пишем во временный файл.
        match tokio::fs::write(&tmp_path, &buf).await {
            Ok(_) => {}
            Err(e) => {
                error!("Ошибка записи RDB-снимка: {}", e);
                return RespValue::Error(format!("ERR saving snapshot: {}", e)).encode();
            }
        }

        // Атомарно переименовываем.
        match tokio::fs::rename(&tmp_path, &self.rdb_path).await {
            Ok(_) => {
                info!("RDB-снимок сохранён: {}", self.rdb_path.display());
                b"+OK\r\n".to_vec()
            }
            Err(e) => {
                error!("Ошибка переименования RDB-файла: {}", e);
                // Пытаемся удалить временный файл.
                let _ = tokio::fs::remove_file(&tmp_path).await;
                RespValue::Error(format!("ERR renaming snapshot: {}", e)).encode()
            }
        }
    }

    /// Загружает RDB-снимок: читает файл, разбирает RESP2-фреймы,
    /// выполняет команды SET для восстановления.
    fn load_rdb(&self, store: &Store) -> Result<usize, String> {
        let data = std::fs::read(&self.rdb_path)
            .map_err(|e| format!("RDB read error: {}", e))?;

        let mut buf = RespBuffer::new();
        buf.feed(&data);
        let mut count = 0;

        loop {
            match buf.try_parse() {
                Ok(Some(RespValue::Array(Some(items)))) => {
                    if let Ok(cmd) = cmd::parse_command(&items) {
                        cmd::execute_command(&cmd, store);
                        count += 1;
                    }
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(e) => return Err(format!("RDB parse error: {}", e)),
            }
        }

        Ok(count)
    }

    // -----------------------------------------------------------------------
    // AOF — запись команд
    // -----------------------------------------------------------------------

    /// Дописывает изменяющую команду в AOF-лог.
    /// `frame` — полный RESP2-фрейм исходной команды (должен быть массивом).
    pub async fn append_command(&self, frame: &RespValue) {
        if !self.config.aof_enabled {
            return;
        }

        let data = frame.encode();

        let mut guard = self.aof_file.lock().await;
        let file = match guard.as_mut() {
            Some(f) => f,
            None => {
                // Ленивое открытие AOF-файла при первой записи.
                match tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.aof_path)
                    .await
                {
                    Ok(f) => guard.insert(f),
                    Err(e) => {
                        error!("Ошибка открытия AOF-файла: {}", e);
                        return;
                    }
                }
            }
        };

        if let Err(e) = file.write_all(&data).await {
            error!("Ошибка записи в AOF: {}", e);
        }
    }

    /// Принудительный сброс AOF-буфера на диск.
    pub async fn flush_aof(&self) {
        if !self.config.aof_enabled {
            return;
        }
        let mut guard = self.aof_file.lock().await;
        if let Some(file) = guard.as_mut()
            && let Err(e) = file.flush().await
        {
            error!("Ошибка сброса AOF: {}", e);
        }
    }

    /// Проигрывает AOF-файл: читает, разбирает RESP2-фреймы и выполняет команды.
    fn replay_aof(&self, store: &Store) -> Result<usize, String> {
        let data = std::fs::read(&self.aof_path)
            .map_err(|e| format!("AOF read error: {}", e))?;

        if data.is_empty() {
            return Ok(0);
        }

        let mut buf = RespBuffer::new();
        buf.feed(&data);
        let mut count = 0;

        loop {
            match buf.try_parse() {
                Ok(Some(RespValue::Array(Some(items)))) => {
                    if let Ok(cmd) = cmd::parse_command(&items) {
                        cmd::execute_command(&cmd, store);
                        count += 1;
                    }
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(e) => return Err(format!("AOF parse error: {}", e)),
            }
        }

        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StoreConfig;
    use std::time::Duration;

    /// Вспомогательная: временная директория для тестов.
    fn tmp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cache_test_{}", name));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[tokio::test]
    async fn rdb_save_and_load() {
        let dir = tmp_dir("rdb_save_load");
        let config = PersistenceConfig {
            data_dir: dir.clone(),
            aof_enabled: false,
            ..Default::default()
        };
        let persistence = Persistence::new(config);
        persistence.init().await.unwrap();

        let store = Store::with_config(StoreConfig { max_memory_mb: 100 });
        store.set(b"key1", b"value1", None);
        store.set(b"key2", b"value2", Some(3600)); // с TTL

        // Сохраняем снимок.
        let resp = persistence.save_snapshot(&store).await;
        assert_eq!(resp, b"+OK\r\n", "save_snapshot должен вернуть +OK");

        // Создаём новое хранилище и загружаем.
        let store2 = Store::with_config(StoreConfig { max_memory_mb: 100 });
        persistence.load(&store2).await;

        // Проверяем восстановление.
        assert_eq!(
            store2.get(b"key1"),
            RespValue::BulkString(Some(b"value1".to_vec())).encode()
        );
        assert_eq!(
            store2.get(b"key2"),
            RespValue::BulkString(Some(b"value2".to_vec())).encode()
        );

        // Очистка.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rdb_skips_expired_keys() {
        let dir = tmp_dir("rdb_skip_expired");
        let config = PersistenceConfig {
            data_dir: dir.clone(),
            aof_enabled: false,
            ..Default::default()
        };
        let persistence = Persistence::new(config);
        persistence.init().await.unwrap();

        let store = Store::with_config(StoreConfig { max_memory_mb: 100 });
        store.set(b"permanent", b"stay", None);
        store.set(b"gone", b"away", Some(0)); // истекает сразу
        tokio::time::sleep(Duration::from_millis(50)).await;
        store.clean_expired(); // убираем истёкший

        let resp = persistence.save_snapshot(&store).await;
        assert_eq!(resp, b"+OK\r\n");

        let store2 = Store::with_config(StoreConfig { max_memory_mb: 100 });
        persistence.load(&store2).await;

        // Истёкший ключ не должен восстановиться.
        assert_eq!(
            store2.get(b"permanent"),
            RespValue::BulkString(Some(b"stay".to_vec())).encode()
        );
        assert_eq!(
            store2.get(b"gone"),
            RespValue::BulkString(None).encode()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn aof_append_and_replay() {
        let dir = tmp_dir("aof_replay");
        let config = PersistenceConfig {
            data_dir: dir.clone(),
            aof_enabled: true,
            ..Default::default()
        };
        let persistence = Persistence::new(config);
        persistence.init().await.unwrap();

        let _store = Store::with_config(StoreConfig { max_memory_mb: 100 });
        let set1 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"1".to_vec())),
        ]));
        let set2 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
            RespValue::BulkString(Some(b"2".to_vec())),
            RespValue::BulkString(Some(b"EX".to_vec())),
            RespValue::BulkString(Some(b"100".to_vec())),
        ]));
        let del = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"DEL".to_vec())),
            RespValue::BulkString(Some(b"a".to_vec())),
        ]));

        persistence.append_command(&set1).await;
        persistence.append_command(&set2).await;
        persistence.append_command(&del).await;
        persistence.flush_aof().await;

        // Новое хранилище — проигрываем AOF.
        let store2 = Store::with_config(StoreConfig { max_memory_mb: 100 });
        persistence.load(&store2).await;

        // Ключ 'a' был удалён — не должен быть.
        assert_eq!(
            store2.get(b"a"),
            RespValue::BulkString(None).encode()
        );
        // Ключ 'b' должен быть с TTL.
        assert_eq!(
            store2.get(b"b"),
            RespValue::BulkString(Some(b"2".to_vec())).encode()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn load_empty_files() {
        // Загрузка при отсутствии файлов не должна падать.
        let dir = tmp_dir("load_empty");
        let config = PersistenceConfig {
            data_dir: dir.clone(),
            aof_enabled: true,
            ..Default::default()
        };
        let persistence = Persistence::new(config);
        persistence.init().await.unwrap();

        let store = Store::new();
        persistence.load(&store).await; // не должно panicked

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn save_snapshot_returns_err_on_missing_dir() {
        // Сохранение в несуществующую директорию (без init) должно вернуть ERR, не панику.
        let dir = tmp_dir("snapshot_missing_dir");
        // Сознательно НЕ вызываем init() — директория не создана.
        let config = PersistenceConfig {
            data_dir: dir.clone(),
            aof_enabled: false,
            ..Default::default()
        };
        let persistence = Persistence::new(config);

        let store = Store::new();
        store.set(b"key", b"val", None);

        let resp = persistence.save_snapshot(&store).await;
        // Должна вернуть -ERR ..., а не упасть с паникой.
        assert!(resp.starts_with(b"-ERR"), "save_snapshot должен вернуть ERR, получено: {:?}", String::from_utf8_lossy(&resp));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn load_handles_corrupted_rdb() {
        // Повреждённый RDB-файл — загрузка не должна паниковать.
        let dir = tmp_dir("corrupted_rdb");
        let config = PersistenceConfig {
            data_dir: dir.clone(),
            aof_enabled: false,
            ..Default::default()
        };
        let persistence = Persistence::new(config);
        persistence.init().await.unwrap();

        // Пишем мусор в RDB-файл.
        let rdb_path = dir.join("dump.rdb");
        std::fs::write(&rdb_path, b"\xff\xfe\xfd\xfc\x00GARBAGE\x01\x02\x03").unwrap();

        let store = Store::new();
        // Не должно panicked.
        persistence.load(&store).await;

        // Хранилище должно остаться пустым.
        assert_eq!(store.len(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn load_handles_corrupted_aof() {
        // Повреждённый AOF-файл — загрузка не должна паниковать.
        let dir = tmp_dir("corrupted_aof");
        let config = PersistenceConfig {
            data_dir: dir.clone(),
            aof_enabled: true,
            ..Default::default()
        };
        let persistence = Persistence::new(config);
        persistence.init().await.unwrap();

        // Пишем мусор в AOF-файл.
        let aof_path = dir.join("appendonly.aof");
        std::fs::write(&aof_path, b"\x00CORRUPTED\xff\xfeAOF\x01").unwrap();

        let store = Store::new();
        // Не должно panicked.
        persistence.load(&store).await;

        // Хранилище должно остаться пустым.
        assert_eq!(store.len(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn append_command_noop_when_aof_disabled() {
        // При выключенном AOF append_command не должен создавать файл.
        let dir = tmp_dir("aof_disabled");
        let config = PersistenceConfig {
            data_dir: dir.clone(),
            aof_enabled: false,
            ..Default::default()
        };
        let persistence = Persistence::new(config);
        persistence.init().await.unwrap();

        let set = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"k".to_vec())),
            RespValue::BulkString(Some(b"v".to_vec())),
        ]));

        persistence.append_command(&set).await;
        persistence.flush_aof().await;

        // AOF-файл не должен существовать.
        let aof_path = dir.join("appendonly.aof");
        assert!(!aof_path.exists(), "AOF-файл не должен создаваться при aof_enabled=false");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
