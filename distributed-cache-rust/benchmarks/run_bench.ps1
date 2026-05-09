<#
.SYNOPSIS
    Скрипт нагрузочного тестирования для distributed-cache-rust.
.DESCRIPTION
    Запускает сервер в Docker, выполняет серию бенчмарков через Rust-клиент
    и redis-benchmark, собирает результаты в CSV.
.PARAMETER SkipDocker
    Пропустить сборку Docker-образа и использовать уже запущенный сервер.
.PARAMETER SkipRedisBench
    Пропустить redis-benchmark тесты (если redis:alpine недоступен).
#>

param(
    [switch]$SkipDocker,
    [switch]$SkipRedisBench
)

$ErrorActionPreference = "Stop"
$ROOT = Split-Path -Parent $PSScriptRoot
cd $ROOT

# ---- 1. Сборка Rust-бенчмарка ----
Write-Host "`n=== Сборка benchmark бинарника ===" -ForegroundColor Cyan
cargo build --bin benchmark --release
if ($LASTEXITCODE -ne 0) { throw "Сборка benchmark провалилась" }

$BENCH_BIN = ".\target\release\benchmark.exe"

# ---- 2. Запуск сервера в Docker ----
if (-not $SkipDocker) {
    Write-Host "`n=== Сборка Docker-образа ===" -ForegroundColor Cyan
    docker build -t distributed-cache-rust:bench -f .\Dockerfile .
    if ($LASTEXITCODE -ne 0) { throw "Сборка Docker провалилась" }

    Write-Host "`n=== Запуск контейнера ===" -ForegroundColor Cyan
    docker rm -f cache-bench 2>$null
    docker run -d --name cache-bench `
        -e CACHE_BIND=0.0.0.0:8080 `
        -p 8080:8080 `
        distributed-cache-rust:bench

    # Ждём healthcheck
    Write-Host "Ожидание сервера..." -ForegroundColor Yellow
    Start-Sleep -Seconds 6

    # Проверка PING
    $RESP = "dummy"
    $attempts = 0
    while ($RESP -notmatch '\+PONG' -and $attempts -lt 10) {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient('127.0.0.1', 8080)
            $stream = $tcp.GetStream()
            $data = [System.Text.Encoding]::ASCII.GetBytes("*1`r`n`$4`r`nPING`r`n")
            $stream.Write($data, 0, $data.Length)
            Start-Sleep -Milliseconds 200
            $buffer = New-Object byte[] 1024
            $read = $stream.Read($buffer, 0, $buffer.Length)
            $RESP = [System.Text.Encoding]::ASCII.GetString($buffer, 0, $read)
            $tcp.Close()
        } catch {
            $RESP = "error"
        }
        if ($RESP -notmatch '\+PONG') {
            Start-Sleep -Seconds 1
            $attempts++
        }
    }
    if ($RESP -notmatch '\+PONG') {
        docker logs cache-bench
        throw "Сервер не отвечает на PING"
    }
    Write-Host "Сервер готов!" -ForegroundColor Green
} else {
    Write-Host "`n=== Пропуск Docker, используем существующий сервер ===" -ForegroundColor Yellow
}

# ---- 3. Микробенчмарки (in-process) ----
Write-Host "`n`n=== MICROBENCHMARKS ===" -ForegroundColor Cyan
& $BENCH_BIN --microbench

# ---- 4. Нагрузочные тесты через Rust-клиент ----
Write-Host "`n`n=== НАГРУЗОЧНЫЕ ТЕСТЫ (Rust-клиент) ===" -ForegroundColor Cyan

$RESULTS_DIR = ".\benchmarks\results"
New-Item -ItemType Directory -Force -Path $RESULTS_DIR | Out-Null
$CSV_FILE = "$RESULTS_DIR\benchmark_results.csv"
"command,connections,requests,qps,p50_ms,p99_ms,p999_ms,min_ms,max_ms,avg_ms,success,errors" | Set-Content $CSV_FILE

# Тестовые сценарии
$scenarios = @(
    @{ command = "mixed"; connections = 1;   requests = 1000  },
    @{ command = "mixed"; connections = 50;  requests = 5000  },
    @{ command = "mixed"; connections = 100; requests = 5000  },
    @{ command = "mixed"; connections = 200; requests = 5000  },
    @{ command = "set";   connections = 50;  requests = 5000  },
    @{ command = "get";   connections = 50;  requests = 5000  }
)

foreach ($s in $scenarios) {
    $cmd = $s.command
    $conn = $s.connections
    $req = $s.requests
    $label = "${cmd}_c${conn}_r${req}"

    Write-Host "`n--- $label ---" -ForegroundColor Green
    $output = & $BENCH_BIN --addr 127.0.0.1:8080 --command $cmd --connections $conn --requests $req --csv 2>&1

    # Сохраняем сырой вывод
    $output | Out-File -FilePath "$RESULTS_DIR\$label.txt" -Encoding utf8

    # Извлекаем CSV-строку
    foreach ($line in $output) {
        if ($line -match '^[a-zA-Z]') {
            Add-Content $CSV_FILE $line
            Write-Host "  OK: $line" -ForegroundColor Gray
        }
    }
}

# ---- 5. redis-benchmark (сравнение с эталоном) ----
if (-not $SkipRedisBench) {
    Write-Host "`n`n=== REDIS-BENCHMARK (через Docker) ===" -ForegroundColor Cyan

    # redis-benchmark для SET
    Write-Host "`n--- redis-benchmark SET (50 conn, 100000 req) ---" -ForegroundColor Green
    docker run --rm --network host redis:alpine redis-benchmark `
        -h 127.0.0.1 -p 8080 -t set -n 100000 -c 50 -d 256 `
        2>&1 | Out-File -FilePath "$RESULTS_DIR\redis_bench_set.txt" -Encoding utf8

    # redis-benchmark для GET
    Write-Host "`n--- redis-benchmark GET (50 conn, 100000 req) ---" -ForegroundColor Green
    docker run --rm --network host redis:alpine redis-benchmark `
        -h 127.0.0.1 -p 8080 -t get -n 100000 -c 50 -d 256 `
        2>&1 | Out-File -FilePath "$RESULTS_DIR\redis_bench_get.txt" -Encoding utf8

    # redis-benchmark PING
    Write-Host "`n--- redis-benchmark PING (50 conn, 100000 req) ---" -ForegroundColor Green
    docker run --rm --network host redis:alpine redis-benchmark `
        -h 127.0.0.1 -p 8080 -t ping -n 100000 -c 50 `
        2>&1 | Out-File -FilePath "$RESULTS_DIR\redis_bench_ping.txt" -Encoding utf8

    Write-Host "`nredis-benchmark результаты сохранены в $RESULTS_DIR" -ForegroundColor Green
}

# ---- 6. Остановка Docker ----
if (-not $SkipDocker) {
    Write-Host "`n=== Остановка контейнера ===" -ForegroundColor Cyan
    docker stop cache-bench
    docker rm cache-bench
}

Write-Host "`n`n========================================" -ForegroundColor Cyan
Write-Host "  БЕНЧМАРК ЗАВЕРШЁН" -ForegroundColor Green
Write-Host "  Результаты: $CSV_FILE" -ForegroundColor White
Write-Host "  Подробно:   $RESULTS_DIR" -ForegroundColor White
Write-Host "========================================" -ForegroundColor Cyan
