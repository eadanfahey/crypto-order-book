use super::{errors::Error, parse_util::parse_int};

pub struct ByteSliceIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> ByteSliceIterator<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        ByteSliceIterator { data, pos: 0 }
    }

    pub fn is_empty(&self) -> bool {
        return self.pos >= self.data.len();
    }

    pub fn consume_byte(&mut self) -> u8 {
        let byte = self.data[self.pos];
        self.pos += 1;
        return byte;
    }

    pub fn expect_byte(&mut self, byte: u8) -> Result<u8, Error> {
        if self.pos >= self.data.len() {
            let msg = format!(
                "expected '{}' at position {} but found EOF",
                byte as char, self.pos
            );
            return Err(Error::ParseError(msg));
        }
        let b = self.data[self.pos];
        if b == byte {
            self.pos += 1;
            return Ok(b);
        }
        let msg = format!(
            "expected '{}' at position {} but found '{}'",
            byte as char, self.pos, b as char
        );
        return Err(Error::ParseError(msg));
    }

    pub fn expect_key(&mut self, key: &str) -> Result<(), Error> {
        let pos = self.pos;
        self.expect_byte(b'"')?;
        self.expect_bytes(key.as_bytes()).map_err(|_| {
            let msg = format!("expected key '{}' as position {}", key, pos);
            Error::ParseError(msg)
        })?;
        self.expect_byte(b'"')?;
        self.expect_byte(b':')?;
        Ok(())
    }

    pub fn expect_single_byte_key(&mut self, key: u8) -> Result<(), Error> {
        let res: Result<(), Error> = {
            self.expect_byte(b'"')?;
            let k = self.consume_byte();
            if k != key {
                let msg = format!("expected key '{}' but found {}", key as char, k as char);
                return Err(Error::ParseError(msg));
            }
            self.expect_byte(b'"')?;
            self.expect_byte(b':')?;
            Ok(())
        };
        res.map_err(|e| Error::ParseError(format!("parsing object key: {}", e.to_string())))
    }

    pub fn expect_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self.pos + bytes.len() >= self.data.len() {
            let msg = "unexpected  EOF".to_owned();
            return Err(Error::ParseError(msg));
        }
        for (i, &b) in bytes.iter().enumerate() {
            let test = self.data[self.pos + i];
            if b != test {
                let msg = format!(
                    "expected char '{}' at position {} but found '{}'",
                    b as char,
                    self.pos + i,
                    test as char
                );
                return Err(Error::ParseError(msg));
            }
        }
        self.pos += bytes.len();
        Ok(())
    }

    pub fn peek_byte(&mut self) -> u8 {
        return self.data[self.pos];
    }

    pub fn consume_string(&mut self) -> Result<&'a [u8], Error> {
        self.expect_byte(b'"')?;
        let s = self.consume_until_byte(b'"');
        self.expect_byte(b'"')?;
        Ok(s)
    }

    pub fn consume_int(&mut self) -> Result<u64, Error> {
        let pos = self.pos;
        let s = self.consume_until_filter(|b| b.is_ascii_digit());
        parse_int(s).map_err(|e| Error::ParseError(format!("position {}: {}", pos, e)))
    }

    pub fn consume_until_byte(&mut self, byte: u8) -> &'a [u8] {
        self.consume_until_filter(|b| b != byte)
    }

    pub fn consume_until_filter<F: Fn(u8) -> bool>(&mut self, filter: F) -> &'a [u8] {
        let start = self.pos;
        let mut len = 0;
        for b in self.data[self.pos..].iter() {
            if filter(*b) {
                len += 1;
            } else {
                break;
            }
        }
        let slice = &self.data[start..start + len];
        self.pos += len;
        return slice;
    }
}
