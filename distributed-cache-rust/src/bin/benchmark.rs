//! Нагрузочный клиент для измерения производительности сервера.
//!
//! Использование:
//!   cargo run --bin benchmark -- --addr 127.0.0.1:8080 --connections 50 --requests 1000
//!
//! Измеряет QPS, P50, P99 для SET/GET/MIXED команд.

use rand::SeedableRng;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Конфигурация
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Config {
    addr: String,
    connections: usize,
    requests_per_connection: usize,
    command: CommandType,
    value_size: usize,
    csv: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum CommandType {
    Set,
    Get,
    Mixed,
}

impl Config {
    fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut c = Config {
            addr: "127.0.0.1:8080".into(),
            connections: 50,
            requests_per_connection: 1000,
            command: CommandType::Mixed,
            value_size: 256,
            csv: false,
        };
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--addr" => {
                    i += 1;
                    c.addr = args[i].clone();
                }
                "--connections" => {
                    i += 1;
                    c.connections = args[i].parse().expect("--connections must be integer");
                }
                "--requests" => {
                    i += 1;
                    c.requests_per_connection =
                        args[i].parse().expect("--requests must be integer");
                }
                "--command" => {
                    i += 1;
                    c.command = match args[i].to_lowercase().as_str() {
                        "set" => CommandType::Set,
                        "get" => CommandType::Get,
                        "mixed" => CommandType::Mixed,
                        other => panic!("unknown command type: {other} (use set/get/mixed)"),
                    };
                }
                "--value-size" => {
                    i += 1;
                    c.value_size = args[i].parse().expect("--value-size must be integer");
                }
                "--csv" => c.csv = true,
                "--microbench" => {
                    run_microbenchmarks(c.value_size);
                    std::process::exit(0);
                }
                "--help" | "-h" => {
                    println!(
                        "\
Usage: benchmark [OPTIONS]

Options:
  --addr <ADDR>               Server address [default: 127.0.0.1:8080]
  --connections <N>           Parallel connections [default: 50]
  --requests <N>              Requests per connection [default: 1000]
  --command <set|get|mixed>   Command type [default: mixed]
  --value-size <BYTES>        Value size for SET [default: 256]
  --csv                       Output as CSV
  --help, -h                  Show this help"
                    );
                    std::process::exit(0);
                }
                _ => eprintln!("ignoring unknown arg: {}", args[i]),
            }
            i += 1;
        }
        c
    }
}

// ---------------------------------------------------------------------------
// Минимальный RESP-верификатор
// ---------------------------------------------------------------------------

/// Проверяет, что ответ начинается с `+OK\r\n` (может содержать префикс +PONG etc.)
fn verify_ok(resp: &[u8]) -> bool {
    resp.len() >= 5 && &resp[..5] == b"+OK\r\n" || resp.len() >= 7 && &resp[..7] == b"+PONG\r\n"
}

/// Проверяет, что ответ — Bulk String (для GET), возвращает true если получили не nil.
fn verify_bulk(resp: &[u8]) -> bool {
    if resp.len() < 4 {
        return false;
    }
    // $<len>\r\n...
    if resp[0] != b'$' {
        return false;
    }
    // $-1\r\n = nil → false
    if resp.len() >= 5 && &resp[..5] == b"$-1\r\n" {
        return false;
    }
    // Ищем \r\n после длины
    if let Some(pos) = resp[1..].windows(2).position(|w| w == b"\r\n") {
        let len_str = std::str::from_utf8(&resp[1..=pos]);
        if let Ok(s) = len_str {
            let len: i64 = s.parse().unwrap_or(-1);
            len >= 0
        } else {
            false
        }
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// Загрузчик
// ---------------------------------------------------------------------------

async fn worker(cfg: &Config, worker_id: usize, rng_seed: u64) -> (Vec<f64>, usize, usize) {
    use rand::Rng;
    let mut rng = rand::rngs::SmallRng::seed_from_u64(rng_seed);

    let mut latencies = Vec::with_capacity(cfg.requests_per_connection);
    let mut success = 0usize;
    let mut errors = 0usize;

    let stream = TcpStream::connect(&cfg.addr).await;
    let mut stream = match stream {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[worker {worker_id}] connect error: {e}");
            return (latencies, 0, cfg.requests_per_connection);
        }
    };

    // Генерация значения для SET
    let value: Vec<u8> = (0..cfg.value_size)
        .map(|_| rng.random_range(b'a'..=b'z'))
        .collect();

    for req_id in 0..cfg.requests_per_connection {
        let is_set = match cfg.command {
            CommandType::Set => true,
            CommandType::Get => false,
            CommandType::Mixed => req_id % 2 == 0,
        };

        let key = format!("bench:{}:{}", worker_id, req_id / 10);

        let (cmd_bytes, expect_ok) = if is_set {
            let mut bytes = Vec::with_capacity(64 + value.len());
            bytes.extend_from_slice(b"*3\r\n$3\r\nSET\r\n");
            bytes.extend_from_slice(format!("${}\r\n", key.len()).as_bytes());
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(b"\r\n");
            bytes.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
            bytes.extend_from_slice(&value);
            bytes.extend_from_slice(b"\r\n");
            (bytes, true)
        } else {
            let mut bytes = Vec::with_capacity(64);
            bytes.extend_from_slice(b"*2\r\n$3\r\nGET\r\n");
            bytes.extend_from_slice(format!("${}\r\n", key.len()).as_bytes());
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(b"\r\n");
            (bytes, false)
        };

        let start = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            // Отправляем
            stream.write_all(&cmd_bytes).await?;
            stream.flush().await?;

            // Читаем ответ
            let mut buf = vec![0u8; 8192];
            let n = stream.read(&mut buf).await?;
            buf.truncate(n);
            Ok::<_, std::io::Error>(buf)
        })
        .await;

        match result {
            Ok(Ok(resp)) => {
                let elapsed = start.elapsed().as_secs_f64() * 1000.0; // ms
                latencies.push(elapsed);

                let valid = if expect_ok {
                    verify_ok(&resp)
                } else {
                    verify_bulk(&resp)
                };
                if valid {
                    success += 1;
                } else {
                    errors += 1;
                }
            }
            Ok(Err(_e)) => {
                errors += 1;
            }
            Err(_) => {
                // timeout
                errors += 1;
            }
        }
    }

    let _ = stream.shutdown().await;
    (latencies, success, errors)
}

// ---------------------------------------------------------------------------
// Статистика
// ---------------------------------------------------------------------------

struct Summary {
    qps: f64,
    p50: f64,
    p99: f64,
    p999: f64,
    min: f64,
    max: f64,
    avg: f64,
    total_requests: usize,
    success: usize,
    errors: usize,
}

fn compute_stats(
    all_latencies: &[f64],
    duration: Duration,
    success: usize,
    errors: usize,
) -> Summary {
    let total = success + errors;
    let qps = if duration.as_secs_f64() > 0.0 {
        total as f64 / duration.as_secs_f64()
    } else {
        0.0
    };

    let mut sorted = all_latencies.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p = |perc: f64| -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let idx = ((perc / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    };

    let avg = if !sorted.is_empty() {
        sorted.iter().sum::<f64>() / sorted.len() as f64
    } else {
        0.0
    };

    Summary {
        qps,
        p50: p(50.0),
        p99: p(99.0),
        p999: p(99.9),
        min: sorted.first().copied().unwrap_or(0.0),
        max: sorted.last().copied().unwrap_or(0.0),
        avg,
        total_requests: total,
        success,
        errors,
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Микробенчмарки (in-process, без сети)
// ---------------------------------------------------------------------------

fn run_microbenchmarks(value_size: usize) {
    use std::collections::HashMap;

    println!();
    println!("===== MICROBENCHMARKS =====");
    println!();

    // --- RESP парсинг ---
    let inputs: &[(&str, &[u8])] = &[
        ("simple_string", b"+OK\r\n"),
        ("bulk_string", b"$5\r\nhello\r\n"),
        ("array_ping", b"*1\r\n$4\r\nPING\r\n"),
        (
            "array_set",
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        ),
    ];

    for (name, input) in inputs {
        let n = 100_000;
        let start = Instant::now();
        for _ in 0..n {
            // Простейший RESP-парсинг на лету
            let _ = simple_resp_parse(input);
        }
        let elapsed = start.elapsed();
        let ns_per_op = elapsed.as_nanos() as f64 / n as f64;
        println!(
            "  resp_parse_{:<20} {:>8.0} ns/op  ({:.0} ops/s)",
            name,
            ns_per_op,
            1_000_000_000.0 / ns_per_op
        );
    }

    // --- RESP кодирование ---
    let n = 100_000;
    let start = Instant::now();
    for _ in 0..n {
        let _ = "+OK\r\n".to_string();
    }
    let elapsed = start.elapsed();
    let ns_per_op = elapsed.as_nanos() as f64 / n as f64;
    println!(
        "  resp_encode_simple_string   {:>8.0} ns/op  ({:.0} ops/s)",
        ns_per_op,
        1_000_000_000.0 / ns_per_op
    );

    let payload = "x".repeat(value_size);
    let n = 50_000;
    let start = Instant::now();
    for _ in 0..n {
        let _ = format!("${}\r\n{}\r\n", payload.len(), payload);
    }
    let elapsed = start.elapsed();
    let ns_per_op = elapsed.as_nanos() as f64 / n as f64;
    println!(
        "  resp_encode_bulk_string    {:>8.0} ns/op  ({:.0} ops/s)",
        ns_per_op,
        1_000_000_000.0 / ns_per_op
    );

    // --- HashMap (симуляция Store) ---
    let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let n = 50_000;
    let start = Instant::now();
    for i in 0..n {
        let key = format!("key{}", i).into_bytes();
        let val = format!("val{}", i).into_bytes();
        map.insert(key, val);
    }
    let elapsed = start.elapsed();
    let ns_per_op = elapsed.as_nanos() as f64 / n as f64;
    println!(
        "  hashmap_insert             {:>8.0} ns/op  ({:.0} ops/s)",
        ns_per_op,
        1_000_000_000.0 / ns_per_op
    );

    let n = 50_000;
    let start = Instant::now();
    for i in 0..n {
        let key = format!("key{}", i).into_bytes();
        let _ = map.get(&key);
    }
    let elapsed = start.elapsed();
    let ns_per_op = elapsed.as_nanos() as f64 / n as f64;
    println!(
        "  hashmap_get                {:>8.0} ns/op  ({:.0} ops/s)",
        ns_per_op,
        1_000_000_000.0 / ns_per_op
    );

    println!();
    println!("===== MICROBENCHMARKS DONE =====");
}

/// Минимальный RESP-парсер для микробенчмарков.
fn simple_resp_parse(input: &[u8]) -> Option<usize> {
    if input.is_empty() {
        return None;
    }
    match input[0] {
        b'+' | b'-' | b':' => {
            // Simple string, Error, Integer: до \r\n
            input[1..]
                .windows(2)
                .position(|w| w == b"\r\n")
                .map(|pos| pos + 3)
        }
        b'$' => {
            // Bulk string: $<len>\r\n<data>\r\n
            let rest = &input[1..];
            let crlf = rest.windows(2).position(|w| w == b"\r\n")?;
            let len_str = std::str::from_utf8(&rest[..crlf]).ok()?;
            let len: i64 = len_str.parse().ok()?;
            if len == -1 {
                Some(crlf + 5) // $-1\r\n = 5 bytes
            } else if len >= 0 {
                let total = crlf + 2 + (len as usize) + 2;
                if total <= input.len() {
                    Some(total)
                } else {
                    None
                }
            } else {
                None
            }
        }
        b'*' => {
            // Array: *<count>\r\n...
            let rest = &input[1..];
            let crlf = rest.windows(2).position(|w| w == b"\r\n")?;
            let cnt_str = std::str::from_utf8(&rest[..crlf]).ok()?;
            let cnt: i64 = cnt_str.parse().ok()?;
            if cnt == -1 {
                Some(crlf + 5) // *-1\r\n = 5 bytes
            } else if cnt == 0 {
                Some(crlf + 4) // *0\r\n = 4 bytes
            } else {
                let mut offset = crlf + 2;
                for _ in 0..cnt {
                    if offset >= input.len() {
                        return None;
                    }
                    let item_len = simple_resp_parse(&input[offset..])?;
                    offset += item_len;
                }
                Some(offset)
            }
        }
        _ => None,
    }
}

#[tokio::main]
async fn main() {
    let cfg = Config::from_args();

    eprintln!(
        "Benchmark: {} connections x {} requests each = {} total\n  addr={}, command={:?}, value_size={}",
        cfg.connections,
        cfg.requests_per_connection,
        cfg.connections * cfg.requests_per_connection,
        cfg.addr,
        cfg.command,
        cfg.value_size,
    );

    let start = Instant::now();

    // Запускаем воркеров
    let mut handles = Vec::with_capacity(cfg.connections);
    for i in 0..cfg.connections {
        let cfg = cfg.clone();
        handles.push(tokio::spawn(async move {
            worker(&cfg, i, i as u64 * 12345 + 42).await
        }));
    }

    let mut all_latencies = Vec::new();
    let mut total_success = 0usize;
    let mut total_errors = 0usize;

    for h in handles {
        let (lats, ok, err) = h.await.unwrap();
        all_latencies.extend(lats);
        total_success += ok;
        total_errors += err;
    }

    let duration = start.elapsed();
    let stats = compute_stats(&all_latencies, duration, total_success, total_errors);

    if cfg.csv {
        println!(
            "command,connections,requests,qps,p50_ms,p99_ms,p999_ms,min_ms,max_ms,avg_ms,success,errors"
        );
        println!(
            "{:?},{},{},{:.0},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{},{}",
            cfg.command,
            cfg.connections,
            stats.total_requests,
            stats.qps,
            stats.p50,
            stats.p99,
            stats.p999,
            stats.min,
            stats.max,
            stats.avg,
            stats.success,
            stats.errors,
        );
    } else {
        println!();
        println!("===== RESULTS =====");
        println!("  QPS:          {:>10.0} req/s", stats.qps);
        println!("  Total:        {:>10} requests", stats.total_requests);
        println!("  Success:      {:>10}", stats.success);
        println!("  Errors:       {:>10}", stats.errors);
        println!("  Duration:     {:>10.2} s", duration.as_secs_f64());
        println!();
        println!("  Latency (ms):");
        println!("    P50:        {:>10.3}", stats.p50);
        println!("    P99:        {:>10.3}", stats.p99);
        println!("    P99.9:      {:>10.3}", stats.p999);
        println!("    Min:        {:>10.3}", stats.min);
        println!("    Max:        {:>10.3}", stats.max);
        println!("    Avg:        {:>10.3}", stats.avg);
        println!();
    }
}
