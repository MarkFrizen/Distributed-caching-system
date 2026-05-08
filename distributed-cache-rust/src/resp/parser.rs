use nom::{
    IResult,
    branch::alt,
    bytes::streaming::{take, take_until},
    character::streaming::{char, crlf, digit1},
    combinator::{opt, recognize},
    multi::count,
    sequence::{preceded, terminated},
    Parser,
};

// ---------------------------------------------------------------------------
// Типы значений RESP2
// ---------------------------------------------------------------------------

/// Представляет любое значение в протоколе сериализации Redis (RESP2).
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// `+<строка>\r\n`
    SimpleString(String),
    /// `-<строка>\r\n`
    Error(String),
    /// `:<целое>\r\n`
    Integer(i64),
    /// `$<длина>\r\n<данные>\r\n` — `None` представляет собой пустую строку-батон (`$-1\r\n`).
    BulkString(Option<Vec<u8>>),
    /// `*<количество>\r\n...` — `None` представляет собой пустой массив (`*-1\r\n`).
    Array(Option<Vec<RespValue>>),
}

// ---------------------------------------------------------------------------
// Вспомогательные функции кодирования (RespValue → bytes, полезны для тестов)
// ---------------------------------------------------------------------------

impl RespValue {
    /// Кодирует это значение в строку байтов RESP2.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => {
                let mut buf = b"+".to_vec();
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf
            }
            RespValue::Error(s) => {
                let mut buf = b"-".to_vec();
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf
            }
            RespValue::Integer(n) => format!(":{}\r\n", n).into_bytes(),
            RespValue::BulkString(None) => b"$-1\r\n".to_vec(),
            RespValue::BulkString(Some(data)) => {
                let mut buf = format!("${}\r\n", data.len()).into_bytes();
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
                buf
            }
            RespValue::Array(None) => b"*-1\r\n".to_vec(),
            RespValue::Array(Some(items)) => {
                let mut buf = format!("*{}\r\n", items.len()).into_bytes();
                for item in items {
                    buf.extend_from_slice(&item.encode());
                }
                buf
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Потоковый буфер — накапливает данные сокета и извлекает из них полные фреймы
// ---------------------------------------------------------------------------

/// Буфер байтов, используемый для накопления частичных данных и извлечения из них
/// полных фреймов RESP2.
#[derive(Debug)]
pub struct RespBuffer {
    buf: Vec<u8>,
}

impl RespBuffer {
    pub fn new() -> Self {
        RespBuffer { buf: Vec::new() }
    }

    /// Добавляет вновь полученные байты в буфер.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Пытается разобрать один полный фрейм с начала буфера.
    ///
    /// - `Ok(Some(value))` — фрейм успешно разобран; потреблённые байты
    ///   удаляются из буфера, и вызывающий код должен обработать `value`.
    /// - `Ok(None)` — недостаточно данных (nom вернул `Incomplete`).
    /// - `Err(msg)` — буфер содержит недопустимые данные RESP2.
    pub fn try_parse(&mut self) -> Result<Option<RespValue>, String> {
        let result = parse_frame(&self.buf);
        match result {
            Ok((remaining, value)) => {
                let consumed = self.buf.len() - remaining.len();
                self.buf.drain(..consumed);
                Ok(Some(value))
            }
            Err(nom::Err::Incomplete(_)) => Ok(None),
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
                let code = e.code;
                let consumed = self.buf.len() - e.input.len();
                if consumed > 0 {
                    self.buf.drain(..consumed);
                } else {
                    // Ничего не было потреблено → пропускаем один байт, чтобы выйти из тупика.
                    self.buf.drain(..1);
                }
                Err(format!("RESP parse error: {:?}", code))
            }
        }
    }

    /// Возвращает ссылку на базовый буфер (для отладки).
    pub fn pending(&self) -> &[u8] {
        &self.buf
    }
}

impl Default for RespBuffer {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Парсеры RESP2 на основе Nom (потоковый режим)
// ---------------------------------------------------------------------------

/// Разбирает один полный фрейм RESP2 из `input`.
///
/// Возвращает оставшиеся (необработанные) байты и разобранное значение.
pub fn parse_frame(input: &[u8]) -> IResult<&[u8], RespValue> {
    alt((
        parse_simple_string,
        parse_error,
        parse_integer,
        parse_bulk_string,
        parse_array,
    ))
    .parse(input)
}

// -- Leaf parsers -----------------------------------------------------------

fn parse_simple_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    preceded(char('+'), terminated(take_until("\r\n"), crlf))
        .map(|s: &[u8]| RespValue::SimpleString(String::from_utf8_lossy(s).to_string()))
        .parse(input)
}

fn parse_error(input: &[u8]) -> IResult<&[u8], RespValue> {
    preceded(char('-'), terminated(take_until("\r\n"), crlf))
        .map(|s: &[u8]| RespValue::Error(String::from_utf8_lossy(s).to_string()))
        .parse(input)
}

fn parse_integer(input: &[u8]) -> IResult<&[u8], RespValue> {
    preceded(char(':'), terminated(parse_signed_int, crlf))
        .map(RespValue::Integer)
        .parse(input)
}

/// Parse a possibly-negative integer encoded as decimal text.
fn parse_signed_int(input: &[u8]) -> IResult<&[u8], i64> {
    recognize((opt(char('-')), digit1))
        .map_res(|bytes: &[u8]| {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| nom::error::Error::<&[u8]>::new(bytes, nom::error::ErrorKind::Fail))?;
            s.parse::<i64>()
                .map_err(|_| nom::error::Error::<&[u8]>::new(bytes, nom::error::ErrorKind::Fail))
        })
        .parse(input)
}

fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, len) =
        preceded(char('$'), terminated(parse_signed_int, crlf)).parse(input)?;

    if len == -1 {
        return Ok((input, RespValue::BulkString(None)));
    }

    let len = len as usize;
    let (input, data) = take(len).parse(input)?;
    let (input, _) = crlf.parse(input)?;

    Ok((input, RespValue::BulkString(Some(data.to_vec()))))
}

fn parse_array(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, cnt) =
        preceded(char('*'), terminated(parse_signed_int, crlf)).parse(input)?;

    if cnt == -1 {
        return Ok((input, RespValue::Array(None)));
    }

    let (input, items) = count(parse_frame, cnt as usize).parse(input)?;

    Ok((input, RespValue::Array(Some(items))))
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- SimpleString -----------------------------------------------------

    #[test]
    fn parse_simple_string_ok() {
        let input = b"+OK\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::SimpleString("OK".into()));
    }

    #[test]
    fn parse_simple_string_with_spaces() {
        let input = b"+hello world\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::SimpleString("hello world".into()));
    }

    #[test]
    fn encode_simple_string_roundtrip() {
        let v = RespValue::SimpleString("PONG".into());
        assert_eq!(v.encode(), b"+PONG\r\n");
        let (_, parsed) = parse_frame(&v.encode()).unwrap();
        assert_eq!(parsed, v);
    }

    // ---- Error ------------------------------------------------------------

    #[test]
    fn parse_error_ok() {
        let input = b"-ERR\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Error("ERR".into()));
    }

    #[test]
    fn parse_error_with_message() {
        let input =
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(
            val,
            RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
                    .into()
            )
        );
    }

    #[test]
    fn encode_error_roundtrip() {
        let v = RespValue::Error("ERR unknown command".into());
        assert_eq!(v.encode(), b"-ERR unknown command\r\n");
        let (_, parsed) = parse_frame(&v.encode()).unwrap();
        assert_eq!(parsed, v);
    }

    // ---- Integer ----------------------------------------------------------

    #[test]
    fn parse_integer_positive() {
        let input = b":1000\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Integer(1000));
    }

    #[test]
    fn parse_integer_zero() {
        let input = b":0\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Integer(0));
    }

    #[test]
    fn parse_integer_negative() {
        let input = b":-1\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Integer(-1));
    }

    #[test]
    fn encode_integer_roundtrip() {
        let v = RespValue::Integer(-42);
        assert_eq!(v.encode(), b":-42\r\n");
        let (_, parsed) = parse_frame(&v.encode()).unwrap();
        assert_eq!(parsed, v);
    }

    // ---- BulkString -------------------------------------------------------

    #[test]
    fn parse_bulk_string() {
        let input = b"$5\r\nhello\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[test]
    fn parse_null_bulk_string() {
        let input = b"$-1\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::BulkString(None));
    }

    #[test]
    fn parse_empty_bulk_string() {
        let input = b"$0\r\n\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::BulkString(Some(b"".to_vec())));
    }

    #[test]
    fn parse_bulk_string_binary() {
        // Bulk strings can contain \r\n inside the data itself
        let data = b"he\r\nllo";
        let input = format!("${}\r\n", data.len());
        let mut full = input.into_bytes();
        full.extend_from_slice(data);
        full.extend_from_slice(b"\r\n");

        let (rem, val) = parse_frame(&full).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::BulkString(Some(data.to_vec())));
    }

    #[test]
    fn encode_bulk_string_roundtrip() {
        let v = RespValue::BulkString(Some(b"world".to_vec()));
        assert_eq!(v.encode(), b"$5\r\nworld\r\n");
        let (_, parsed) = parse_frame(&v.encode()).unwrap();
        assert_eq!(parsed, v);
    }

    #[test]
    fn encode_null_bulk_string_roundtrip() {
        let v = RespValue::BulkString(None);
        assert_eq!(v.encode(), b"$-1\r\n");
        let (_, parsed) = parse_frame(&v.encode()).unwrap();
        assert_eq!(parsed, v);
    }

    // ---- Array ------------------------------------------------------------

    #[test]
    fn parse_array_two_simple_strings() {
        let input = b"*2\r\n+OK\r\n+NO\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::SimpleString("OK".into()),
                RespValue::SimpleString("NO".into()),
            ]))
        );
    }

    #[test]
    fn parse_null_array() {
        let input = b"*-1\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Array(None));
    }

    #[test]
    fn parse_empty_array() {
        let input = b"*0\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Array(Some(vec![])));
    }

    #[test]
    fn parse_array_mixed_types() {
        // *3\r\n+OK\r\n:42\r\n$-1\r\n
        let input = b"*3\r\n+OK\r\n:42\r\n$-1\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::SimpleString("OK".into()),
                RespValue::Integer(42),
                RespValue::BulkString(None),
            ]))
        );
    }

    #[test]
    fn parse_nested_array() {
        // *2\r\n*1\r\n+OK\r\n+NO\r\n
        let input = b"*2\r\n*1\r\n+OK\r\n+NO\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::Array(Some(vec![RespValue::SimpleString("OK".into())])),
                RespValue::SimpleString("NO".into()),
            ]))
        );
    }

    #[test]
    fn parse_array_bulk_strings() {
        // *2\r\n$3\r\nSET\r\n$5\r\nmykey\r\n
        let input = b"*2\r\n$3\r\nSET\r\n$5\r\nmykey\r\n";
        let (rem, val) = parse_frame(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"mykey".to_vec())),
            ]))
        );
    }

    #[test]
    fn encode_array_roundtrip() {
        let v = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"GET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
        ]));
        assert_eq!(v.encode(), b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        let (_, parsed) = parse_frame(&v.encode()).unwrap();
        assert_eq!(parsed, v);
    }

    #[test]
    fn encode_null_array() {
        let v = RespValue::Array(None);
        assert_eq!(v.encode(), b"*-1\r\n");
    }

    // ---- Edge cases -------------------------------------------------------

    #[test]
    fn empty_input_returns_incomplete() {
        let result = parse_frame(b"");
        assert!(matches!(result, Err(nom::Err::Incomplete(_))));
    }

    #[test]
    fn partial_input_returns_incomplete() {
        // Only "$5\r\nhel" — not enough data
        let result = parse_frame(b"$5\r\nhel");
        assert!(matches!(result, Err(nom::Err::Incomplete(_))));
    }

    #[test]
    fn invalid_first_byte_returns_error() {
        let result = parse_frame(b"~hello\r\n");
        assert!(result.is_err());
    }

    // ---- Streaming buffer tests -------------------------------------------

    #[test]
    fn resp_buffer_accumulates_and_extracts() {
        let mut buf = RespBuffer::new();

        // Feed partial data
        buf.feed(b"+OK\r");
        assert!(buf.try_parse().unwrap().is_none());

        // Now feed the rest
        buf.feed(b"\n");
        let val = buf.try_parse().unwrap().expect("should parse now");
        assert_eq!(val, RespValue::SimpleString("OK".into()));
        assert!(buf.pending().is_empty());
    }

    #[test]
    fn resp_buffer_multiple_frames() {
        let mut buf = RespBuffer::new();
        buf.feed(b"+OK\r\n+GOOD\r\n");

        let v1 = buf.try_parse().unwrap().expect("first frame");
        assert_eq!(v1, RespValue::SimpleString("OK".into()));

        let v2 = buf.try_parse().unwrap().expect("second frame");
        assert_eq!(v2, RespValue::SimpleString("GOOD".into()));

        assert!(buf.pending().is_empty());
    }

    #[test]
    fn resp_buffer_handles_garbage() {
        let mut buf = RespBuffer::new();
        buf.feed(b"~garbage\r\n+OK\r\n");

        // Каждый вызов пропускает 1 байт — продолжаем вызывать, пока не появится валидный фрейм.
        let val = loop {
            match buf.try_parse() {
                Ok(Some(v)) => break v,
                Err(_) => continue, // draining garbage
                Ok(None) => panic!("unexpected incomplete frame"),
            }
        };

        assert_eq!(val, RespValue::SimpleString("OK".into()));
    }

    #[test]
    fn resp_buffer_keeps_remaining() {
        let mut buf = RespBuffer::new();
        buf.feed(b"+A\r\n+");

        let val = buf.try_parse().unwrap().expect("first frame");
        assert_eq!(val, RespValue::SimpleString("A".into()));

        // Buffer should now contain "+", and more data is needed
        assert!(buf.try_parse().unwrap().is_none());
    }
}
