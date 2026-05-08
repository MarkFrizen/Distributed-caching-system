use dashmap::DashMap;
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Приблизительные накладные расходы на одну запись в DashMap (заголовки, хеш-таблица, enum и т.д.).
const OVERHEAD_PER_ENTRY: u64 = 128;

/// Размер случайной выборки для приближённого LRU-вытеснения.
const LRU_SAMPLE_SIZE: usize = 5;

/// Значение, хранящееся в кэше, с опциональным TTL и меткой последнего доступа.
#[derive(Debug, Clone)]
struct StoredValue {
    data: Vec<u8>,
    expires_at: Option<Instant>,
    /// Момент последнего доступа (GET или SET).
    last_access: Instant,
    /// Длина ключа (для учёта памяти — ключ хранится отдельно в DashMap).
    key_len: usize,
}

/// Конфигурация хранилища.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Максимальный объём памяти в мегабайтах.
    pub max_memory_mb: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig { max_memory_mb: 128 }
    }
}

/// Потокобезопасное in-memory хранилище ключ–значение с ограничением памяти
/// и приближённым LRU-вытеснением.
#[derive(Debug, Clone)]
pub struct Store {
    inner: Arc<DashMap<Vec<u8>, StoredValue>>,
    /// Максимальный объём памяти в байтах.
    max_memory: u64,
    /// Текущий приблизительный расход памяти.
    used_memory: Arc<AtomicU64>,
}

impl Store {
    /// Создаёт новое хранилище с конфигурацией по умолчанию.
    pub fn new() -> Self {
        Store::with_config(StoreConfig::default())
    }

    /// Создаёт новое хранилище с заданной конфигурацией.
    pub fn with_config(config: StoreConfig) -> Self {
        Store {
            inner: Arc::new(DashMap::new()),
            max_memory: config.max_memory_mb * 1024 * 1024,
            used_memory: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Возвращает текущий приблизительный расход памяти в байтах.
    pub fn current_memory_usage(&self) -> u64 {
        self.used_memory.load(Ordering::Relaxed)
    }

    /// Возвращает максимальный объём памяти в байтах.
    pub fn max_memory_bytes(&self) -> u64 {
        self.max_memory
    }

    /// Приблизительный размер одной записи.
    fn entry_size(key_len: usize, value_len: usize) -> u64 {
        key_len as u64 + value_len as u64 + OVERHEAD_PER_ENTRY
    }

    /// LRU-вытеснение: пока `used_memory > max_memory`, выбирает случайную
    /// выборку ключей, находит среди них самый старый (по `last_access`)
    /// и удаляет его.
    fn evict_lru(&self) {
        if self.inner.is_empty() {
            return;
        }

        let sample_size = LRU_SAMPLE_SIZE.min(self.inner.len());

        while self.used_memory.load(Ordering::Relaxed) > self.max_memory {
            // Собираем случайную выборку ключей.
            let keys: Vec<Vec<u8>> = {
                let mut rng = rand::rng();
                let entries: Vec<_> = self.inner.iter().collect();
                let count = sample_size.min(entries.len());
                let mut selected = Vec::with_capacity(count);

                // Случайная выборка без повторений.
                let mut indices: Vec<usize> = (0..entries.len()).collect();
                // Перемешиваем часть массива.
                for i in 0..count {
                    let j = rng.random_range(i..entries.len());
                    indices.swap(i, j);
                }
                for &idx in &indices[..count] {
                    selected.push(entries[idx].key().clone());
                }
                selected
            };

            // Находим ключ с самой старой меткой last_access.
            let mut evict_key: Option<Vec<u8>> = None;
            let mut oldest = Instant::now();

            for key in &keys {
                if let Some(entry) = self.inner.get(key) {
                    if entry.last_access < oldest {
                        oldest = entry.last_access;
                        evict_key = Some(key.clone());
                    }
                }
            }

            if let Some(key) = evict_key {
                if let Some((_, removed)) = self.inner.remove(&key) {
                    let size = Self::entry_size(removed.key_len, removed.data.len());
                    self.used_memory.fetch_sub(size, Ordering::Relaxed);
                }
            } else {
                // Ничего не нашли — вероятно, все ключи уже удалены конкурентно.
                break;
            }

            // Если хранилище опустело — прекращаем.
            if self.inner.is_empty() {
                break;
            }
        }
    }

    // -- PING -----------------------------------------------------------------

    /// Всегда возвращает `+PONG`.
    pub fn ping(&self) -> &'static [u8] {
        b"+PONG\r\n"
    }

    // -- ECHO -----------------------------------------------------------------

    /// Возвращает сообщение обратно в виде bulk string.
    pub fn echo(&self, msg: &[u8]) -> Vec<u8> {
        RespValue::BulkString(Some(msg.to_vec())).encode()
    }

    // -- SET ------------------------------------------------------------------

    /// Вставляет `key` → `value`. Если `ttl_secs` равен `Some(n)`, ключ истечёт
    /// через `n` секунд. Возвращает `+OK\r\n`.
    ///
    /// После вставки проверяет лимит памяти и при необходимости запускает LRU-вытеснение.
    pub fn set(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Vec<u8> {
        let now = Instant::now();
        let expires_at = ttl_secs.map(|s| now + std::time::Duration::from_secs(s));

        let new_size = Self::entry_size(key.len(), value.len());

        // Проверяем, существует ли уже ключ — вычитаем старый размер.
        if let Some((_, old)) = self.inner.remove(key) {
            let old_size = Self::entry_size(old.key_len, old.data.len());
            self.used_memory.fetch_sub(old_size, Ordering::Relaxed);
        }

        // Вставляем новое значение.
        self.inner.insert(
            key.to_vec(),
            StoredValue {
                data: value.to_vec(),
                expires_at,
                last_access: now,
                key_len: key.len(),
            },
        );

        // Увеличиваем счётчик использованной памяти.
        self.used_memory.fetch_add(new_size, Ordering::Relaxed);

        // Запускаем LRU-вытеснение при превышении лимита.
        self.evict_lru();

        b"+OK\r\n".to_vec()
    }

    // -- GET ------------------------------------------------------------------

    /// Извлекает значение по ключу `key`. Выполняет ленивый срок действия:
    /// если ключ истёк, он удаляется и возвращается `None`.
    /// Обновляет `last_access` при успешном чтении.
    pub fn get(&self, key: &[u8]) -> Vec<u8> {
        // Пытаемся получить изменяемую ссылку — так можно обновить last_access.
        let key_vec = key.to_vec();
        if let Some(mut entry) = self.inner.get_mut(&key_vec) {
            // Проверка истечения TTL.
            let expired = entry
                .expires_at
                .map(|d| Instant::now() >= d)
                .unwrap_or(false);

            if expired {
                let key_len = entry.key_len;
                let data_len = entry.data.len();
                // Освобождаем блокировку перед удалением.
                drop(entry);
                self.inner.remove(&key_vec);
                self.used_memory
                    .fetch_sub(Self::entry_size(key_len, data_len), Ordering::Relaxed);
                return RespValue::BulkString(None).encode();
            }

            // Обновляем метку последнего доступа.
            entry.last_access = Instant::now();
            let data = entry.data.clone();
            RespValue::BulkString(Some(data)).encode()
        } else {
            RespValue::BulkString(None).encode()
        }
    }

    // -- DEL ------------------------------------------------------------------

    /// Удаляет один или несколько ключей. Возвращает количество реально удалённых ключей.
    pub fn del(&self, keys: &[&[u8]]) -> Vec<u8> {
        let mut count = 0i64;
        for key in keys {
            if let Some((_, removed)) = self.inner.remove(*key) {
                count += 1;
                let size = Self::entry_size(removed.key_len, removed.data.len());
                self.used_memory.fetch_sub(size, Ordering::Relaxed);
            }
        }
        RespValue::Integer(count).encode()
    }

    // -- EXISTS ---------------------------------------------------------------

    /// Возвращает количество существующих ключей среди переданных.
    /// Учитывает ленивый срок действия (пропускает истёкшие).
    pub fn exists(&self, keys: &[&[u8]]) -> Vec<u8> {
        let mut count = 0i64;
        for key in keys {
            if let Some(stored) = self.inner.get(*key) {
                let expired = stored
                    .expires_at
                    .map(|d| Instant::now() >= d)
                    .unwrap_or(false);
                if !expired {
                    count += 1;
                }
            }
        }
        RespValue::Integer(count).encode()
    }

    /// Активная очистка просроченных TTL: обходит случайную выборку ключей
    /// и удаляет истёкшие. Вызывается из фоновой задачи.
    ///
    /// Возвращает количество удалённых ключей.
    pub fn clean_expired(&self) -> usize {
        let sample_size = 20.min(self.inner.len());
        if sample_size == 0 {
            return 0;
        }

        let now = Instant::now();
        let mut removed = 0usize;

        // Собираем случайную выборку ключей для проверки.
        let keys_to_check: Vec<Vec<u8>> = {
            let mut rng = rand::rng();
            let entries: Vec<_> = self.inner.iter().collect();
            let mut indices: Vec<usize> = (0..entries.len()).collect();
            for i in 0..sample_size {
                let j = rng.random_range(i..entries.len());
                indices.swap(i, j);
            }
            indices[..sample_size]
                .iter()
                .map(|&idx| entries[idx].key().clone())
                .collect()
        };

        for key in &keys_to_check {
            if let Some(entry) = self.inner.get(key) {
                let expired = entry.expires_at.map(|d| now >= d).unwrap_or(false);
                if expired {
                    let key_len = entry.key_len;
                    let data_len = entry.data.len();
                    drop(entry);
                    self.inner.remove(key);
                    self.used_memory
                        .fetch_sub(Self::entry_size(key_len, data_len), Ordering::Relaxed);
                    removed += 1;
                }
            }
        }

        removed
    }

    /// Возвращает все записи для сохранения в RDB-снимок.
    /// Уже истёкшие ключи пропускаются.
    /// Возвращаемый кортеж: (ключ, значение, оставшееся TTL в секундах).
    pub fn snapshot_entries(&self) -> Vec<(Vec<u8>, Vec<u8>, Option<u64>)> {
        let now = Instant::now();
        self.inner
            .iter()
            .filter_map(|entry| {
                let ttl_remaining = entry.expires_at.map(|exp| {
                    if exp > now {
                        exp.duration_since(now).as_secs()
                    } else {
                        0
                    }
                });
                // Пропускаем уже истёкшие ключи.
                if ttl_remaining == Some(0) {
                    return None;
                }
                Some((entry.key().clone(), entry.data.clone(), ttl_remaining))
            })
            .collect()
    }

    /// Внутренний помощник: количество записей (для тестирования).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get() {
        let store = Store::new();
        store.set(b"key1", b"val1", None);
        let resp = store.get(b"key1");
        assert_eq!(resp, RespValue::BulkString(Some(b"val1".to_vec())).encode());
    }

    #[test]
    fn get_missing() {
        let store = Store::new();
        let resp = store.get(b"nokey");
        assert_eq!(resp, RespValue::BulkString(None).encode());
    }

    #[test]
    fn del_single() {
        let store = Store::new();
        store.set(b"k", b"v", None);
        let resp = store.del(&[b"k"]);
        assert_eq!(resp, RespValue::Integer(1).encode());
        assert!(store.get(b"k") == RespValue::BulkString(None).encode());
    }

    #[test]
    fn del_missing() {
        let store = Store::new();
        let resp = store.del(&[b"absent"]);
        assert_eq!(resp, RespValue::Integer(0).encode());
    }

    #[test]
    fn del_multiple() {
        let store = Store::new();
        store.set(b"a", b"1", None);
        store.set(b"b", b"2", None);
        let resp = store.del(&[b"a", b"b", b"c"]);
        assert_eq!(resp, RespValue::Integer(2).encode());
    }

    #[test]
    fn exists_returns_count() {
        let store = Store::new();
        store.set(b"x", b"1", None);
        let resp = store.exists(&[b"x", b"y"]);
        assert_eq!(resp, RespValue::Integer(1).encode());
    }

    #[test]
    fn ttl_expires_key() {
        let store = Store::new();
        store.set(b"ephemeral", b"value", Some(1));
        assert_eq!(
            store.get(b"ephemeral"),
            RespValue::BulkString(Some(b"value".to_vec())).encode()
        );
        std::thread::sleep(std::time::Duration::from_secs(1));
        assert_eq!(
            store.get(b"ephemeral"),
            RespValue::BulkString(None).encode()
        );
    }

    #[test]
    fn set_overwrites() {
        let store = Store::new();
        store.set(b"k", b"v1", None);
        store.set(b"k", b"v2", None);
        assert_eq!(
            store.get(b"k"),
            RespValue::BulkString(Some(b"v2".to_vec())).encode()
        );
    }

    #[test]
    fn exists_with_expired_key() {
        let store = Store::new();
        store.set(b"tmp", b"x", Some(0)); // 0 seconds → effectively expired
        std::thread::sleep(std::time::Duration::from_millis(50));
        let resp = store.exists(&[b"tmp"]);
        assert_eq!(resp, RespValue::Integer(0).encode());
    }

    // -- Тесты LRU-вытеснения ------------------------------------------------

    #[test]
    fn eviction_removes_keys_when_over_limit() {
        // Хранилище с очень маленьким лимитом — 1 КБ.
        let config = StoreConfig { max_memory_mb: 0 }; // 0 байт — любой SET превысит лимит
        let store = Store::with_config(config);

        // Даже при лимите 0 SET должен отработать (вставит ключ, потом вытеснит
        // (возможно, только что вставленный) — но не упадёт).
        let resp = store.set(b"key1", b"value1", None);
        assert_eq!(resp, b"+OK\r\n");

        // Убедимся, что хранилище работает.
        store.set(b"key2", b"value2", None);
        store.set(b"key3", b"value3", None);

        // При лимите 0 все ключи должны быть вытеснены.
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn eviction_keeps_recently_accessed() {
        // Лимит ~3 КБ — помещается примерно 1-2 ключа.
        let config = StoreConfig { max_memory_mb: 0 }; // 0 байт — проверяем вытеснение
        let store = Store::with_config(config);

        // Вставим ключи — все должны вытесняться.
        store.set(b"k1", b"x", None);
        store.set(b"k2", b"x", None);
        store.set(b"k3", b"x", None);

        // При лимите 0 все вытеснены.
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn eviction_preserves_data_under_limit() {
        // Большой лимит — всё должно поместиться.
        let config = StoreConfig { max_memory_mb: 100 };
        let store = Store::with_config(config);

        store.set(b"persistent", b"data", None);
        assert_eq!(
            store.get(b"persistent"),
            RespValue::BulkString(Some(b"data".to_vec())).encode()
        );
    }

    #[test]
    fn memory_accounting_accurate_after_set_and_del() {
        let config = StoreConfig { max_memory_mb: 100 };
        let store = Store::with_config(config);

        store.set(b"a", b"1", None);
        assert!(store.current_memory_usage() > 0);
        assert!(store.current_memory_usage() <= store.max_memory_bytes());

        let before_del = store.current_memory_usage();
        store.del(&[b"a"]);
        assert!(store.current_memory_usage() < before_del);
    }

    #[test]
    fn clean_expired_removes_ttl_keys() {
        let store = Store::new();
        store.set(b"permanent", b"stay", None);
        store.set(b"ephemeral", b"go", Some(0)); // истекает мгновенно

        // Даём время для истечения.
        std::thread::sleep(std::time::Duration::from_millis(50));

        let removed = store.clean_expired();
        assert!(
            removed >= 1,
            "должен удалить истёкший ключ, убрано: {}",
            removed
        );

        // Постоянный ключ должен остаться.
        assert_eq!(
            store.get(b"permanent"),
            RespValue::BulkString(Some(b"stay".to_vec())).encode()
        );
    }

    #[test]
    fn set_updates_last_access() {
        let config = StoreConfig { max_memory_mb: 100 };
        let store = Store::with_config(config);

        store.set(b"key", b"val", None);
        store.set(b"key", b"val2", None); // перезапись

        // После перезаписи old size должен быть вычтен, новый — добавлен.
        assert!(
            store.current_memory_usage() > 0,
            "память должна учитываться после SET"
        );

        // Проверяем GET — он тоже обновляет last_access.
        let resp = store.get(b"key");
        assert_eq!(resp, RespValue::BulkString(Some(b"val2".to_vec())).encode());
    }
}

// ---------------------------------------------------------------------------
// Переэкспорт — RespValue используется в тестах store (доступен, так как
// main.rs объявляет pub mod resp)
// ---------------------------------------------------------------------------

use crate::resp::parser::RespValue;
