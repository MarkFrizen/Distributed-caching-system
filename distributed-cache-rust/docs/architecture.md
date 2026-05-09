# Архитектура

## Общая схема

```mermaid
graph TB
    Client1[Клиент / redis-cli]
    Client2[Клиент / benchmark]
    
    subgraph "Сервер distributed-cache-rust"
        Listener[TCP Listener<br/>tokio::net::TcpListener]
        
        subgraph "Обработка соединения"
            RespBuffer[RespBuffer<br/>Потоковый парсер RESP2]
            Router{Маршрутизация<br/>по ключу}
            CmdDispatch[Диспетчеризация команд]
        end
        
        subgraph "Хранилище"
            Store[Store<br/>DashMap + LRU]
            Eviction[Фоновое LRU<br/>вытеснение]
            TTL[Фоновая очистка<br/>просроченных TTL]
        end
        
        subgraph "Персистентность"
            RDB[RDB-снимки]
            AOF[AOF-лог]
            BGSave[BGSAVE]
        end
        
        subgraph "Кластеризация"
            HashRing[HashRing<br/>consistent hashing]
            Proxy[Проксирование<br/>на другой узел]
        end
        
        subgraph "Репликация"
            MasterState[MasterState<br/>broadcast]
            ReplicaClient[ReplicaClient<br/>SYNC + live commands]
        end
    end
    
    Client1 --> Listener
    Client2 --> Listener
    Listener --> RespBuffer
    RespBuffer --> CmdDispatch
    
    CmdDispatch --> Router
    Router -->|ключ локальный| Store
    Router -->|ключ удалённый| Proxy
    Proxy -->|прокси на другой узел| OtherNode[Другой узел кластера]
    
    Store --> Eviction
    Store --> TTL
    Store --> RDB
    Store --> AOF
    BGSave --> RDB
    
    CmdDispatch -->|SET/DEL| MasterState
    MasterState -->|broadcast| ReplicaClient
    
    ReplicaClient -->|SYNC| MasterState
    
    style Client1 fill:#bbf
    style Client2 fill:#bbf
    style OtherNode fill:#fbb
```

## Поток выполнения запроса

```mermaid
sequenceDiagram
    participant C as Клиент
    participant B as RespBuffer
    participant P as Парсер (nom)
    participant R as Router
    participant S as Store
    participant Per as Persistence
    participant M as MasterState
    
    C->>B: TCP stream (байты)
    B->>P: parse_frame()
    P-->>B: RespValue::Array
    B-->>C: frame ready
    
    C->>R: parse_command() + route_key()
    
    alt ключ удалённый (кластер)
        R->>C: proxy_request() → ответ от другого узла
    else ключ локальный
        R->>S: execute_command()
        
        alt команда изменяющая (SET/DEL)
            S->>Per: append_command() (AOF)
            S->>M: sender.send(frame) (репликация)
        end
        
        S-->>C: RESP-ответ
    end
```

## Структура модулей

```
src/
├── main.rs                 # Точка входа, настройка сервера, фоновые задачи
├── cmd.rs                  # Парсинг и выполнение команд (Command enum)
├── store/
│   └── mod.rs              # In-memory хранилище (DashMap + LRU + TTL)
├── persistence/
│   └── mod.rs              # RDB-снимки и AOF-лог
├── cluster/
│   ├── mod.rs              # ClusterState, проксирование
│   └── hashring.rs         # Consistent hashing (HashRing)
├── replication/
│   └── mod.rs              # MasterState, ReplicaClient, ReplicationConfig
├── resp/
│   ├── mod.rs              # Реэкспорт парсера
│   └── parser.rs           # RESP2 парсер (nom) + RespBuffer
└── bin/
    └── benchmark.rs        # Нагрузочный клиент
```

## Используемые технологии

| Компонент | Технология |
|---|---|
| Асинхронный I/O | Tokio (tokio::net, tokio::io, tokio::spawn) |
| In-memory хранилище | DashMap (concurrent HashMap) |
| Парсинг RESP2 | nom 8 (streaming-парсер) |
| Кластеризация | SHA-256 (через крейт sha2) |
| Логирование | tracing + tracing-subscriber |
| Сериализация JSON | serde + serde_json |
| Случайные числа | rand 0.9 |
| Контейнеризация | Docker (многоэтапная сборка, debian:bookworm-slim) |

## Ключевые архитектурные решения

1. **Асинхронная модель Tokio** — каждое соединение обрабатывается в отдельной задаче
   `tokio::spawn`, планировщик Tokio распределяет нагрузку между ядрами CPU.

2. **Приближённый LRU** — вместо точного LRU (дорогого в concurrent среде) используется
   случайная выборка из 5 ключей, из которых удаляется самый старый. Это даёт
   производительность, близкую к оптимальной, с константной сложностью.

3. **RESP2-формат для RDB** — снимки хранятся как последовательность RESP2-команд SET,
   что позволяет использовать тот же парсер для загрузки (избегая дублирования логики).

4. **Consistent hashing с 256 vnodes** — равномерное распределение ключей (разброс < 10%)
   без централизованного координатора.

5. **Broadcast-репликация** — мастер транслирует изменяющие команды всем репликам
   через `tokio::sync::broadcast`. Реплики применяют команды в том же порядке.
