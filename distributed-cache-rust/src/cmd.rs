use crate::resp::parser::RespValue;
use crate::store::Store;

/// Разобранная Redis-команда, готовая к выполнению.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Ping(Option<Vec<u8>>),
    Echo(Vec<u8>),
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    },
    Get(Vec<u8>),
    Del(Vec<Vec<u8>>),
    Exists(Vec<Vec<u8>>),
    /// Сохранить RDB-снимок на диск (BGSAVE).
    Bgsave,
}

impl Command {
    /// Возвращает `true`, если команда изменяет данные (SET, DEL).
    /// Используется для AOF-логирования.
    pub fn is_modifying(&self) -> bool {
        matches!(self, Command::Set { .. } | Command::Del(_))
    }

    /// Возвращает ключ для маршрутизации (первый ключ команды).
    ///
    /// Для multi-key команд (DEL, EXISTS) возвращается первый ключ;
    /// остальные должны принадлежать тому же узлу (как в Redis Cluster).
    pub fn routing_key(&self) -> Option<&[u8]> {
        match self {
            Command::Set { key, .. } => Some(key),
            Command::Get(key) => Some(key),
            Command::Del(keys) => keys.first().map(|k| k.as_slice()),
            Command::Exists(keys) => keys.first().map(|k| k.as_slice()),
            _ => None,
        }
    }
}

/// Попытка преобразовать массив RESP в `Command`.
///
/// Возвращает `Err(resp_error)`, если команда неизвестна или имеет неправильное
/// количество аргументов — вызывающий код может отправить эту ошибку напрямую клиенту.
pub fn parse_command(args: &[RespValue]) -> Result<Command, RespValue> {
    if args.is_empty() {
        return Err(RespValue::Error("ERR empty command".into()));
    }

    let raw_name = match &args[0] {
        RespValue::BulkString(Some(bytes)) => bytes,
        RespValue::SimpleString(s) => s.as_bytes(),
        other => {
            return Err(RespValue::Error(format!(
                "ERR expected command name, got {:?}",
                other
            )));
        }
    };

    // Case-insensitive ASCII comparison.
    let name = String::from_utf8_lossy(raw_name).to_uppercase();

    match name.as_str() {
        "PING" => {
            let msg = args.get(1).and_then(|v| match v {
                RespValue::BulkString(Some(b)) => Some(b.clone()),
                _ => None,
            });
            Ok(Command::Ping(msg))
        }

        "ECHO" => {
            let msg = args.get(1).and_then(|v| match v {
                RespValue::BulkString(Some(b)) => Some(b.clone()),
                _ => None,
            });
            match msg {
                Some(m) => Ok(Command::Echo(m)),
                None => Err(RespValue::Error(
                    "ERR wrong number of arguments for 'ECHO' command".into(),
                )),
            }
        }

        "SET" => {
            if args.len() < 3 {
                return Err(RespValue::Error(
                    "ERR wrong number of arguments for 'SET' command".into(),
                ));
            }
            let key = get_bulk(&args[1])?;
            let value = get_bulk(&args[2])?;

            let ttl_secs = if args.len() >= 4 {
                let opt_raw = get_bulk(&args[3])?;
                let opt_upper = String::from_utf8_lossy(&opt_raw).to_uppercase();
                if opt_upper == "EX" {
                    if args.len() < 5 {
                        return Err(RespValue::Error(
                            "ERR wrong number of arguments for 'SET' command".into(),
                        ));
                    }
                    let secs_raw = get_bulk(&args[4])?;
                    let secs_str = String::from_utf8_lossy(&secs_raw);
                    let secs: u64 = secs_str
                        .parse()
                        .map_err(|_| RespValue::Error("ERR value is not an integer or out of range".into()))?;
                    Some(secs)
                } else {
                    return Err(RespValue::Error(format!(
                        "ERR unknown option '{}' for 'SET' command",
                        opt_upper
                    )));
                }
            } else {
                None
            };

            Ok(Command::Set {
                key,
                value,
                ttl_secs,
            })
        }

        "GET" => {
            let key = args
                .get(1)
                .and_then(|v| get_bulk(v).ok())
                .ok_or_else(|| {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'GET' command".into(),
                    )
                })?;
            Ok(Command::Get(key))
        }

        "DEL" => {
            if args.len() < 2 {
                return Err(RespValue::Error(
                    "ERR wrong number of arguments for 'DEL' command".into(),
                ));
            }
            let mut keys = Vec::new();
            for arg in &args[1..] {
                keys.push(get_bulk(arg)?);
            }
            Ok(Command::Del(keys))
        }

        "EXISTS" => {
            if args.len() < 2 {
                return Err(RespValue::Error(
                    "ERR wrong number of arguments for 'EXISTS' command".into(),
                ));
            }
            let mut keys = Vec::new();
            for arg in &args[1..] {
                keys.push(get_bulk(arg)?);
            }
            Ok(Command::Exists(keys))
        }

        "BGSAVE" => {
            if args.len() > 1 {
                return Err(RespValue::Error(
                    "ERR BGSAVE does not accept arguments".into(),
                ));
            }
            Ok(Command::Bgsave)
        }

        _ => Err(RespValue::Error(format!(
            "ERR unknown command '{}'",
            name
        ))),
    }
}

/// Выполняет разобранную `Command` на хранилище и возвращает ответные байты в кодировке RESP.
pub fn execute_command(cmd: &Command, store: &Store) -> Vec<u8> {
    match cmd {
        Command::Ping(msg) => match msg {
            Some(m) => RespValue::BulkString(Some(m.clone())).encode(),
            None => RespValue::SimpleString("PONG".into()).encode(),
        },
        Command::Echo(msg) => RespValue::BulkString(Some(msg.clone())).encode(),
        Command::Set { key, value, ttl_secs } => store.set(key, value, *ttl_secs),
        Command::Get(key) => store.get(key),
        Command::Del(keys) => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            store.del(&refs)
        }
        Command::Exists(keys) => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            store.exists(&refs)
        }
        Command::Bgsave => {
            // Обработка BGSAVE выполняется на уровне handle_connection,
            // где доступен Persistence. Здесь — заглушка.
            b"+OK\r\n".to_vec()
        }
    }
}

// -- Вспомогательные функции --------------------------------------------------

/// Извлекает байты из `RespValue`, который должен быть `BulkString`.
fn get_bulk(v: &RespValue) -> Result<Vec<u8>, RespValue> {
    match v {
        RespValue::BulkString(Some(b)) => Ok(b.clone()),
        other => Err(RespValue::Error(format!(
            "ERR expected bulk string, got {:?}",
            other
        ))),
    }
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(data: &[u8]) -> RespValue {
        RespValue::BulkString(Some(data.to_vec()))
    }

    // -- parse_command -------------------------------------------------------

    #[test]
    fn parse_ping_no_args() {
        let cmd = parse_command(&[bs(b"PING")]).unwrap();
        assert_eq!(cmd, Command::Ping(None));
    }

    #[test]
    fn parse_ping_with_msg() {
        let cmd = parse_command(&[bs(b"PING"), bs(b"hello")]).unwrap();
        assert_eq!(cmd, Command::Ping(Some(b"hello".to_vec())));
    }

    #[test]
    fn parse_echo() {
        let cmd = parse_command(&[bs(b"ECHO"), bs(b"hello")]).unwrap();
        assert_eq!(cmd, Command::Echo(b"hello".to_vec()));
    }

    #[test]
    fn parse_set_no_ttl() {
        let cmd = parse_command(&[bs(b"SET"), bs(b"key"), bs(b"value")]).unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                ttl_secs: None
            }
        );
    }

    #[test]
    fn parse_set_with_ttl() {
        let cmd = parse_command(&[bs(b"SET"), bs(b"k"), bs(b"v"), bs(b"EX"), bs(b"10")])
            .unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_secs: Some(10)
            }
        );
    }

    #[test]
    fn parse_get() {
        let cmd = parse_command(&[bs(b"GET"), bs(b"mykey")]).unwrap();
        assert_eq!(cmd, Command::Get(b"mykey".to_vec()));
    }

    #[test]
    fn parse_del_one() {
        let cmd = parse_command(&[bs(b"DEL"), bs(b"k1")]).unwrap();
        assert_eq!(cmd, Command::Del(vec![b"k1".to_vec()]));
    }

    #[test]
    fn parse_del_multi() {
        let cmd = parse_command(&[bs(b"DEL"), bs(b"a"), bs(b"b"), bs(b"c")]).unwrap();
        assert_eq!(
            cmd,
            Command::Del(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );
    }

    #[test]
    fn parse_exists() {
        let cmd = parse_command(&[bs(b"EXISTS"), bs(b"k")]).unwrap();
        assert_eq!(cmd, Command::Exists(vec![b"k".to_vec()]));
    }

    #[test]
    fn parse_unknown_command() {
        let result = parse_command(&[bs(b"UNKNOWN")]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty() {
        let result = parse_command(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_set_missing_args() {
        let result = parse_command(&[bs(b"SET")]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_bgsave() {
        let cmd = parse_command(&[bs(b"BGSAVE")]).unwrap();
        assert_eq!(cmd, Command::Bgsave);
    }

    #[test]
    fn parse_bgsave_with_args_fails() {
        let result = parse_command(&[bs(b"BGSAVE"), bs(b"extra")]);
        assert!(result.is_err());
    }

    // -- execute_command -----------------------------------------------------

    #[test]
    fn exec_ping() {
        let store = Store::new();
        let resp = execute_command(&Command::Ping(None), &store);
        assert_eq!(resp, b"+PONG\r\n");
    }

    #[test]
    fn exec_ping_with_msg() {
        let store = Store::new();
        let resp = execute_command(&Command::Ping(Some(b"hi".to_vec())), &store);
        assert_eq!(resp, b"$2\r\nhi\r\n");
    }

    #[test]
    fn exec_echo() {
        let store = Store::new();
        let resp = execute_command(&Command::Echo(b"hello".to_vec()), &store);
        assert_eq!(resp, b"$5\r\nhello\r\n");
    }

    #[test]
    fn exec_set_get() {
        let store = Store::new();
        let set_resp = execute_command(
            &Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_secs: None,
            },
            &store,
        );
        assert_eq!(set_resp, b"+OK\r\n");

        let get_resp = execute_command(&Command::Get(b"k".to_vec()), &store);
        assert_eq!(get_resp, b"$1\r\nv\r\n");
    }

    #[test]
    fn exec_del() {
        let store = Store::new();
        store.set(b"k", b"v", None);
        let resp = execute_command(&Command::Del(vec![b"k".to_vec()]), &store);
        assert_eq!(resp, b":1\r\n");
    }

    #[test]
    fn exec_exists() {
        let store = Store::new();
        store.set(b"x", b"1", None);
        let resp = execute_command(&Command::Exists(vec![b"x".to_vec()]), &store);
        assert_eq!(resp, b":1\r\n");
    }
}
