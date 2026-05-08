# Distributed Caching System

Распределённая система кэширования на Rust, совместимая с Redis-клиентами (RESP2).

## Статус проекта

В разработке. Реализованы этапы:

- **Этап 1** — Базовый асинхронный TCP-сервер (PING/PONG)
- **Этап 2** — Парсинг RESP2 (nom-парсер, потоковый буфер)
- **Этап 3** — In-memory хранилище и основные команды (SET, GET, DEL, EXISTS, ECHO, PING, TTL)
- **Этап 4** — Ограничение памяти и вытеснение (LRU)
- **Этап 5** — Персистентность (RDB / AOF) ✅

## Стек

- Rust (edition 2024)
- Tokio (асинхронный I/O)
- DashMap (конкурентное in-memory хранилище)
- Nom (парсинг RESP2)
- Tracing (логирование)

## Быстрый старт

```bash
cd distributed-cache-rust
cargo run
```

Сервер запускается на `127.0.0.1:8080`.

Проверка через redis-cli:

```bash
redis-cli -p 8080 PING
# Ожидаемый ответ: PONG
```
