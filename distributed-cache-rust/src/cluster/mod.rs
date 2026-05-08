pub mod hashring;

use hashring::HashRing;
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

/// Конфигурация одного узла кластера.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ClusterNodeConfig {
    pub address: String,
    pub vnodes: u32,
}

/// Конфигурация кластера.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Список всех узлов кластера.
    pub nodes: Vec<ClusterNodeConfig>,
    /// Адрес текущего узла (должен совпадать с одним из `nodes`).
    pub current_addr: String,
    /// Количество виртуальных узлов по умолчанию, если не указано в конкретном узле.
    pub default_vnodes: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            current_addr: "127.0.0.1:8080".into(),
            default_vnodes: 256,
        }
    }
}

/// Состояние кластера для одного узла.
#[derive(Debug, Clone)]
pub struct ClusterState {
    /// Адрес текущего узла.
    pub current_addr: String,
    /// Хеш-кольцо (разделяемый доступ через RwLock).
    pub ring: Arc<RwLock<HashRing>>,
    /// Включена ли кластеризация.
    pub enabled: bool,
}

impl ClusterState {
    /// Создаёт `ClusterState` из конфигурации: строит кольцо по списку узлов.
    pub fn from_config(config: &ClusterConfig) -> Self {
        let mut ring = HashRing::new();
        for node in &config.nodes {
            let vnodes = if node.vnodes > 0 {
                node.vnodes
            } else {
                config.default_vnodes
            };
            ring.add_node(&node.address, vnodes);
            info!(
                address = %node.address,
                vnodes,
                "Узел добавлен в хеш-кольцо"
            );
        }

        info!(
            nodes = ring.len(),
            current_addr = %config.current_addr,
            "Кластер инициализирован"
        );

        Self {
            current_addr: config.current_addr.clone(),
            ring: Arc::new(RwLock::new(ring)),
            enabled: !config.nodes.is_empty(),
        }
    }

    /// Создаёт выключенный кластер (режим single-node).
    pub fn disabled() -> Self {
        Self {
            current_addr: String::new(),
            ring: Arc::new(RwLock::new(HashRing::new())),
            enabled: false,
        }
    }

    /// Определяет, какому узлу принадлежит ключ.
    ///
    /// Возвращает `Some(addr)`, если ключ должен быть обработан другим узлом,
    /// и `None`, если текущий узел — владелец (или кластеризация отключена).
    pub fn route_key(&self, key: &[u8]) -> Option<String> {
        if !self.enabled {
            return None;
        }
        let ring = self.ring.read().unwrap();
        match ring.get_node(key) {
            Some(addr) if addr != self.current_addr => {
                info!(key = %String::from_utf8_lossy(key), target = %addr, "Маршрутизация ключа на другой узел");
                Some(addr.to_string())
            }
            _ => None,
        }
    }
}

/// Проксирует RESP2-запрос на указанный узел и возвращает ответ.
pub async fn proxy_request(frame_bytes: &[u8], target_addr: &str) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{timeout, Duration};

    match timeout(Duration::from_secs(5), TcpStream::connect(target_addr)).await {
        Ok(Ok(mut stream)) => {
            // Отправляем запрос.
            if let Err(e) = timeout(Duration::from_secs(5), stream.write_all(frame_bytes)).await
            {
                let msg = format!("ERR proxy write timeout to {}: {}", target_addr, e);
                warn!("{}", msg);
                return format!("-{}\r\n", msg).into_bytes();
            }
            if let Err(e) = timeout(Duration::from_secs(5), stream.flush()).await {
                let msg = format!("ERR proxy flush timeout to {}: {}", target_addr, e);
                warn!("{}", msg);
                return format!("-{}\r\n", msg).into_bytes();
            }

            // Читаем ответ (одна RESP2-команда).
            let mut buf = vec![0u8; 4096];
            match timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
                Ok(Ok(n)) => {
                    buf.truncate(n);
                    buf
                }
                Ok(Err(e)) => {
                    let msg = format!("ERR proxy read error from {}: {}", target_addr, e);
                    warn!("{}", msg);
                    format!("-{}\r\n", msg).into_bytes()
                }
                Err(_) => {
                    let msg = format!("ERR proxy read timeout from {}", target_addr);
                    warn!("{}", msg);
                    format!("-{}\r\n", msg).into_bytes()
                }
            }
        }
        Ok(Err(e)) => {
            let msg = format!("ERR cannot connect to {}: {}", target_addr, e);
            warn!("{}", msg);
            format!("-{}\r\n", msg).into_bytes()
        }
        Err(_) => {
            let msg = format!("ERR connect timeout to {}", target_addr);
            warn!("{}", msg);
            format!("-{}\r\n", msg).into_bytes()
        }
    }
}
