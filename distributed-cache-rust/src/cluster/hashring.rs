use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::collections::HashMap;

/// Кольцо consistent hashing на основе SHA-256 (младшие 64 бита).
///
/// Каждый реальный узел представлен `num_vnodes` виртуальными узлами,
/// равномерно распределёнными по кольцу диапазона `[0, 2^64)`.
#[derive(Debug, Clone)]
pub struct HashRing {
    /// Кольцо: хеш → адрес узла.
    ring: BTreeMap<u64, String>,
    /// Для каждого узла — список его виртуальных хешей (нужен для быстрого удаления).
    vnodes: HashMap<String, Vec<u64>>,
}

impl HashRing {
    /// Создаёт пустое кольцо.
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
            vnodes: HashMap::new(),
        }
    }

    /// Добавляет узел с `num_vnodes` виртуальными узлами.
    pub fn add_node(&mut self, address: &str, num_vnodes: u32) {
        let address = address.to_string();
        let mut hashes = Vec::with_capacity(num_vnodes as usize);

        for i in 0..num_vnodes {
            let hash = hash_vnode(&address, i);
            hashes.push(hash);
            self.ring.insert(hash, address.clone());
        }

        self.vnodes.insert(address, hashes);
    }

    /// Удаляет узел и все его виртуальные узлы из кольца.
    pub fn remove_node(&mut self, address: &str) {
        if let Some(hashes) = self.vnodes.remove(address) {
            for h in hashes {
                self.ring.remove(&h);
            }
        }
    }

    /// Возвращает адрес узла, ответственного за ключ.
    ///
    /// Хеширует ключ через SHA-256 и находит первый виртуальный узел
    /// на кольце (с wrap-around).
    pub fn get_node(&self, key: &[u8]) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = hash_key(key);
        // Ищем первый элемент с ключом >= hash (lower_bound).
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.first_key_value())
            .map(|(_, addr)| addr.as_str())
    }

    /// Возвращает количество реальных узлов в кольце.
    pub fn len(&self) -> usize {
        self.vnodes.len()
    }

    /// Возвращает `true`, если кольцо пустое.
    pub fn is_empty(&self) -> bool {
        self.vnodes.is_empty()
    }

    /// Возвращает список адресов всех узлов.
    pub fn addresses(&self) -> Vec<&str> {
        self.vnodes.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for HashRing {
    fn default() -> Self {
        Self::new()
    }
}

// -- SHA-256 helpers ---------------------------------------------------------

/// SHA-256 хеш ключа (младшие 64 бита).
fn hash_key(data: &[u8]) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    // Берём младшие 8 байт (64 бита) из 32-байтового хеша.
    u64::from_be_bytes(result[24..32].try_into().unwrap())
}

/// SHA-256 хеш виртуального узла: `"addr:vnode_index"`.
fn hash_vnode(address: &str, index: u32) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(address.as_bytes());
    hasher.update(b":");
    hasher.update(index.to_le_bytes());
    let result = hasher.finalize();
    u64::from_be_bytes(result[24..32].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ring_returns_none() {
        let ring = HashRing::new();
        assert!(ring.get_node(b"anything").is_none());
    }

    #[test]
    fn single_node_all_keys_map_to_it() {
        let mut ring = HashRing::new();
        ring.add_node("127.0.0.1:8080", 256);

        let keys: &[&[u8]] = &[b"a", b"b", b"c", b"hello", b"world", b"test_key_123"];
        for key in keys {
            assert_eq!(ring.get_node(key), Some("127.0.0.1:8080"));
        }
    }

    #[test]
    fn two_nodes_distribute_keys() {
        let mut ring = HashRing::new();
        ring.add_node("node1:8080", 256);
        ring.add_node("node2:8080", 256);

        // Проверяем, что оба узла используются.
        let mut node1_count = 0;
        let mut node2_count = 0;
        let keys: Vec<Vec<u8>> = (0..1000)
            .map(|i| format!("key{}", i).into_bytes())
            .collect();

        for key in &keys {
            match ring.get_node(key) {
                Some("node1:8080") => node1_count += 1,
                Some("node2:8080") => node2_count += 1,
                other => panic!("Неизвестный узел: {:?}", other),
            }
        }

        // Каждый узел должен получить хотя бы 20% ключей.
        assert!(
            node1_count > 200,
            "node1 получил всего {} ключей",
            node1_count
        );
        assert!(
            node2_count > 200,
            "node2 получил всего {} ключей",
            node2_count
        );

        // Итого все ключи распределены.
        assert_eq!(node1_count + node2_count, 1000);
    }

    #[test]
    fn remove_node_redirects_keys() {
        let mut ring = HashRing::new();
        ring.add_node("node1:8080", 256);
        ring.add_node("node2:8080", 256);

        let key = b"mykey";
        let before = ring.get_node(key).unwrap().to_string();

        ring.remove_node(&before);
        let after = ring.get_node(key).unwrap();

        assert_ne!(
            before, after,
            "После удаления узла ключ должен перенаправиться"
        );
    }

    #[test]
    fn reinsert_node_is_stable() {
        let mut ring = HashRing::new();
        ring.add_node("a:8080", 256);
        ring.add_node("b:8080", 256);

        let keys: Vec<Vec<u8>> = (0..500).map(|i| format!("k{}", i).into_bytes()).collect();

        // Запоминаем распределение.
        let mapping: Vec<String> = keys
            .iter()
            .map(|k| ring.get_node(k).unwrap().to_string())
            .collect();

        // Удаляем и добавляем тот же узел.
        ring.remove_node("a:8080");
        ring.add_node("a:8080", 256);

        // Распределение должно быть идентичным (детерминированные хеши).
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(
                ring.get_node(k).unwrap(),
                mapping[i],
                "Распределение должно быть стабильным после reinsert для ключа k{}",
                i
            );
        }
    }

    #[test]
    fn different_key_hashes_differ() {
        let h1 = hash_key(b"key1");
        let h2 = hash_key(b"key2");
        assert_ne!(h1, h2, "Хеши разных ключей должны различаться");
    }

    #[test]
    fn same_key_always_same_hash() {
        let h1 = hash_key(b"deterministic");
        let h2 = hash_key(b"deterministic");
        assert_eq!(h1, h2);
    }

    #[test]
    fn ring_len_and_addresses() {
        let mut ring = HashRing::new();
        assert_eq!(ring.len(), 0);
        assert!(ring.is_empty());

        ring.add_node("x:8080", 256);
        ring.add_node("y:8080", 256);
        assert_eq!(ring.len(), 2);
        assert!(!ring.is_empty());

        let mut addrs: Vec<&str> = ring.addresses();
        addrs.sort();
        assert_eq!(addrs, vec!["x:8080", "y:8080"]);
    }

    #[test]
    fn remove_non_existent_node() {
        let mut ring = HashRing::new();
        ring.add_node("a:8080", 256);
        // Удаление несуществующего узла не должно паниковать.
        ring.remove_node("non-existent");
        assert_eq!(ring.len(), 1);
    }

    #[test]
    fn add_twice_is_idempotent() {
        let mut ring = HashRing::new();
        ring.add_node("x:8080", 256);

        // Повторное добавление того же узла добавляет ещё одну порцию vnodes.
        ring.add_node("x:8080", 256);

        // Ключи всё ещё мапятся к этому узлу.
        let keys: &[&[u8]] = &[b"a", b"b", b"c"];
        for key in keys {
            assert_eq!(ring.get_node(key), Some("x:8080"));
        }
    }

    #[test]
    fn hash_range_is_uniform() {
        let mut ring = HashRing::new();
        ring.add_node("a:8080", 256);
        ring.add_node("b:8080", 256);
        ring.add_node("c:8080", 256);

        let total = 3000;
        let mut dist = std::collections::HashMap::new();
        for i in 0..total {
            let key = format!("uniform_key_{}", i);
            let node = ring.get_node(key.as_bytes()).unwrap().to_string();
            *dist.entry(node).or_insert(0) += 1;
        }

        // Каждый узел должен получить 33% ± 10%.
        for (node, count) in &dist {
            let pct = *count as f64 / total as f64;
            assert!(
                (pct - 0.333).abs() < 0.1,
                "Узел {} получил {:.1}% (ожидалось ~33%)",
                node,
                pct * 100.0
            );
        }
    }
}
