# ============================================================================
# Этап 1: сборка
# ============================================================================
FROM rust:1.86-slim-bookworm AS builder

WORKDIR /app

# Кэширование зависимостей: копируем только манифесты
COPY Cargo.toml Cargo.lock ./

# Создаём фиктивный main.rs, чтобы собрать только зависимости
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>&1 && rm -rf src

# Копируем настоящий исходный код
COPY src ./src

# Изменения в файле main.rs, чтобы пересобрать только его
RUN touch src/main.rs && cargo build --release

# ============================================================================
# Этап 2: минимальный runtime-образ
# ============================================================================
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    socat \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/distributed-cache-rust /usr/local/bin/

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD sh -c 'PORT=${CACHE_BIND#*:} && printf "*1\r\n\$4\r\nPING\r\n" | timeout 3 socat - TCP:127.0.0.1:$PORT | grep -q "+PONG" || exit 1'

# По умолчанию слушаем на всех интерфейсах для Docker.
ENV CACHE_BIND=0.0.0.0:8080

EXPOSE 8080

CMD ["distributed-cache-rust"]
