use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;

/// A value stored in the cache with an optional TTL expiry.
#[derive(Debug, Clone)]
struct StoredValue {
    data: Vec<u8>,
    expires_at: Option<Instant>,
}

/// Thread-safe, in-memory key–value store.
///
/// Wraps an `Arc<DashMap>` so it can be cheaply cloned and shared across
/// async tasks (one per TCP connection).
#[derive(Debug, Clone)]
pub struct Store {
    inner: Arc<DashMap<Vec<u8>, StoredValue>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            inner: Arc::new(DashMap::new()),
        }
    }

    // -- PING -----------------------------------------------------------------

    /// Always returns `+PONG`.
    pub fn ping(&self) -> &'static [u8] {
        b"+PONG\r\n"
    }

    // -- ECHO -----------------------------------------------------------------

    /// Returns the message back as a bulk string.
    pub fn echo(&self, msg: &[u8]) -> Vec<u8> {
        RespValue::BulkString(Some(msg.to_vec())).encode()
    }

    // -- SET ------------------------------------------------------------------

    /// Insert `key` → `value`. If `ttl_secs` is `Some(n)`, the key will expire
    /// after `n` seconds. Returns `+OK\r\n`.
    pub fn set(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Vec<u8> {
        let expires_at = ttl_secs.map(|s| Instant::now() + std::time::Duration::from_secs(s));
        self.inner.insert(
            key.to_vec(),
            StoredValue {
                data: value.to_vec(),
                expires_at,
            },
        );
        b"+OK\r\n".to_vec()
    }

    // -- GET ------------------------------------------------------------------

    /// Retrieve the value for `key`. Performs lazy expiry: if the key has
    /// expired it is removed and `None` is returned.
    pub fn get(&self, key: &[u8]) -> Vec<u8> {
        let entry = self.inner.get(key);

        if entry.is_none() {
            return RespValue::BulkString(None).encode();
        }

        let expired = entry
            .as_ref()
            .unwrap()
            .expires_at
            .map(|d| Instant::now() >= d)
            .unwrap_or(false);

        if expired {
            // Release the read lock before removing.
            drop(entry);
            self.inner.remove(key);
            return RespValue::BulkString(None).encode();
        }

        let data = entry.as_ref().unwrap().data.clone();
        RespValue::BulkString(Some(data)).encode()
    }

    // -- DEL ------------------------------------------------------------------

    /// Remove one or more keys. Returns the number of keys that were actually
    /// removed.
    pub fn del(&self, keys: &[&[u8]]) -> Vec<u8> {
        let mut count = 0i64;
        for key in keys {
            if self.inner.remove(*key).is_some() {
                count += 1;
            }
        }
        RespValue::Integer(count).encode()
    }

    // -- EXISTS ---------------------------------------------------------------

    /// Returns the number of existing keys among the given ones.
    pub fn exists(&self, keys: &[&[u8]]) -> Vec<u8> {
        let mut count = 0i64;
        for key in keys {
            // Also check lazy expiry.
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

    /// Internal helper: number of entries (for testing).
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
// Tests
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
        // TTL = 1 second.
        store.set(b"ephemeral", b"value", Some(1));
        assert_eq!(
            store.get(b"ephemeral"),
            RespValue::BulkString(Some(b"value".to_vec())).encode()
        );
        std::thread::sleep(std::time::Duration::from_secs(1));
        // Should be gone now.
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
        // exists should count it as nonexistent
        let resp = store.exists(&[b"tmp"]);
        assert_eq!(resp, RespValue::Integer(0).encode());
    }
}

// ---------------------------------------------------------------------------
// Wire helper — re-export RespValue so we don't need to import resp::parser
// in the store tests (already available because main.rs declares pub mod resp)
// ---------------------------------------------------------------------------

use crate::resp::parser::RespValue;
