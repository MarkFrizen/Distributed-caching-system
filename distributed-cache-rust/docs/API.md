# API сервера

Сервер реализует подмножество протокола **RESP2** (Redis Serialization Protocol v2).
Клиенты могут подключаться любым TCP-клиентом, поддерживающим RESP2 (включая `redis-cli`).

---

## Формат протокола (RESP2)

| Тип | Префикс | Пример |
|---|---|---|
| **Simple String** | `+` | `+OK\r\n` |
| **Error** | `-` | `-ERR unknown command\r\n` |
| **Integer** | `:` | `:1\r\n` |
| **Bulk String** | `$` | `$5\r\nhello\r\n` |
| **Null Bulk String** | `$-1` | `$-1\r\n` |
| **Array** | `*` | `*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n` |
| **Null Array** | `*-1` | `*-1\r\n` |

---

## Поддерживаемые команды

### PING

Проверка доступности сервера.

```
PING → +PONG
PING "hello" → $5\r\nhello\r\n
```

**RESP2:**
```
*1\r\n$4\r\nPING\r\n
*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n
```

---

### ECHO

Возвращает переданное сообщение.

```
ECHO "hello" → $5\r\nhello\r\n
```

**RESP2:**
```
*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
```

---

### SET

Устанавливает значение ключа. Поддерживает опциональный TTL в секундах (через `EX`).

```
SET foo bar → +OK
SET foo bar EX 10 → +OK
```

**RESP2 (без TTL):**
```
*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
```

**RESP2 (с TTL):**
```
*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nEX\r\n$2\r\n10\r\n
```

---

### GET

Возвращает значение ключа. Если ключ не найден или истёк TTL — возвращает Null Bulk String.

```
GET foo → $3\r\nbar\r\n
GET missing → $-1\r\n
```

**RESP2:**
```
*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
```

---

### DEL

Удаляет один или несколько ключей. Возвращает количество удалённых ключей.

```
DEL foo → :1
DEL foo bar → :2
```

**RESP2:**
```
*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n
*3\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
```

---

### EXISTS

Проверяет существование одного или нескольких ключей. Возвращает количество существующих ключей
(с учётом истекших TTL — они не учитываются).

```
EXISTS foo → :1
EXISTS foo bar → :2
```

**RESP2:**
```
*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n
```

---

### BGSAVE

Запускает сохранение RDB-снимка на диск. Возвращает `+OK` после завершения.

```
BGSAVE → +OK
```

**RESP2:**
```
*1\r\n$6\r\nBGSAVE\r\n
```

---

### SYNC

Протокол репликации (только для мастер-узла). Реплика подключается и отправляет `SYNC`,
получает полный снимок данных + поток изменяющих команд в реальном времени.

```
SYNC → <SET ...>\r\n<SET ...>\r\n+SYNC_DONE\r\n<streaming commands...>
```

**RESP2:**
```
*1\r\n$4\r\nSYNC\r\n
```

> **Примечание:** Обычные клиенты не должны использовать SYNC. Эта команда предназначена
> только для реплик, настроенных через переменные окружения `CACHE_ROLE` / `CACHE_MASTER`.

---

## Коды ошибок

| Ситуация | Ответ |
|---|---|
| Неизвестная команда | `-ERR unknown command 'XXX'` |
| Неверное число аргументов SET | `-ERR wrong number of arguments for 'SET' command` |
| Неверное число аргументов GET | `-ERR wrong number of arguments for 'GET' command` |
| SYNC на не-мастере | `-ERR SYNC not allowed: node is not a master` |
| Значение EX не число | `-ERR value is not an integer or out of range` |
| Таймаут прокси (кластер) | `-ERR proxy write timeout to <addr>: ...` |

---

## Ограничения

- Максимальный размер значения: не ограничен явно, но ограничен доступной памятью
- Максимальный размер запроса: буфер 4 КБ на чтение (обрабатывается потоково при помощи `RespBuffer`)
- TTL: точность до секунды, максимальное значение — `u64::MAX`
- Репликация: полный resync при переподключении (PSYNC не реализован)
