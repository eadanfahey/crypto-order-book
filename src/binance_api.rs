use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Deserializer};
use std::str::from_utf8;
use std::time::{Duration, Instant};
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::errors::Error;

const BASE_API_ENDPOINT: &'static str = "https://api.binance.com/api/v3";
const BASE_STREAM_ENDPOINT: &'static str = "wss://stream.binance.com:9443/ws";

fn create_api_url(endpoint: &'static str) -> Url {
    return Url::parse(&(BASE_API_ENDPOINT.to_owned() + endpoint)).unwrap();
}

fn create_stream_url(endpoint: &str) -> Url {
    return Url::parse(&(BASE_STREAM_ENDPOINT.to_owned() + endpoint)).unwrap();
}

pub enum Symbol {
    BtcUsdt,
}

struct SymbolScales {
    price: u32,
    quantity: u32,
}

impl Symbol {
    fn api_string(&self) -> &'static str {
        match self {
            Self::BtcUsdt => "BTCUSDT",
        }
    }

    fn stream_string(&self) -> &'static str {
        match self {
            Self::BtcUsdt => "btcusdt",
        }
    }

    fn scales(&self) -> SymbolScales {
        match self {
            Self::BtcUsdt => SymbolScales {
                price: 2,
                quantity: 8,
            },
        }
    }
}

#[allow(dead_code)]
fn array_of_price_levels<'de, D>(deserializer: D) -> Result<Vec<PriceLevel>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Vec<[String; 2]> = Deserialize::deserialize(deserializer)?;
    let res = s
        .iter()
        .map(|arr| {
            let price = parse_scaled_number(&arr[0], 2);
            let quantity = parse_scaled_number(&arr[1], 8);
            PriceLevel { price, quantity }
        })
        .collect();
    Ok(res)
}

#[allow(dead_code)]
async fn get_request<T>(url: Url) -> Result<T, Error>
where
    T: serde::de::DeserializeOwned,
{
    let path = url.path().to_owned();
    let resp = reqwest::get(url).await?;
    let status = resp.status();
    if status != reqwest::StatusCode::OK {
        return Err(Error::ApiBadStatus(status.as_u16() as u32, path));
    }

    resp.json::<T>().await.map_err(Error::ApiRequestError)
}

#[derive(Debug, Clone, PartialEq)]
pub struct PriceLevel {
    pub price: u64,
    pub quantity: u64,
}

#[derive(Debug, PartialEq)]
pub struct OrderBook {
    pub last_update_id: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

/// Get a snapshot of the orderbook for a trading pair up to a given depth. The maximum
/// depth that Binance returns is 5000.
pub async fn get_order_book_snapshot(symbol: Symbol, depth: u32) -> Result<OrderBook, Error> {
    let mut url = create_api_url("/depth");
    url.query_pairs_mut()
        .append_pair("symbol", symbol.api_string())
        .append_pair("limit", &depth.to_string());

    let path = url.path().to_owned();
    let resp = reqwest::get(url).await?;
    let status = resp.status();
    if status != reqwest::StatusCode::OK {
        return Err(Error::ApiBadStatus(status.as_u16() as u32, path));
    }

    let data = resp.bytes().await.map_err(Error::ApiRequestError)?;
    let scales = symbol.scales();

    parse_order_book(&data, &scales)
}

#[derive(Debug, PartialEq)]
pub struct OrderBookDiff {
    pub event_time: u64,
    pub symbol: String,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

/// Start a websocket stream of order book diffs for a symbol from the Binance market
/// data streams. Each order book diff and the duration to parse its message will be
/// sent to the provided `channel`. Once connected to the stream, this function spawns
/// a background task to send the diffs. The task listens to the `shutdown_recv` channel
/// to be notified when it should close the stream.
pub async fn start_stream_order_book_diffs(
    symbol: Symbol,
    channel: UnboundedSender<(OrderBookDiff, Duration)>,
    mut shutdown_recv: UnboundedReceiver<()>,
) -> JoinHandle<Result<(), Error>> {
    let url = create_stream_url(&format!("/{}@depth@100ms", symbol.stream_string()));

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let scales = symbol.scales();
    let pong = Message::Pong("pong".as_bytes().to_vec());

    let handle = tokio::spawn(async move {
        loop {
            select! {
                _ = shutdown_recv.recv() => {
                    ws_sender.send(Message::Close(None)).await.map_err(Error::StreamError)?;
                    return Ok(());
                },
                Some(msg) = ws_receiver.next() => {
                    let msg = msg.unwrap();
                    if msg.is_ping() {
                        ws_sender.send(pong.clone()).await.map_err(Error::StreamError)?;
                        continue;
                    }
                    if msg.is_ping() {
                        continue;
                    }
                    if msg.is_close() {
                        return Ok(());
                    }
                    let start = Instant::now();
                    let data = msg.into_data();
                    let diff = parse_diff_depth_stream_event(&data, &scales)?;
                    channel.send((diff, start.elapsed())).unwrap();
                }
                else => {
                    return Ok(());
                },
            }
        }
    });

    handle
}

struct ByteSliceIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> ByteSliceIterator<'a> {
    fn new(data: &'a [u8]) -> Self {
        ByteSliceIterator { data, pos: 0 }
    }

    fn is_empty(&self) -> bool {
        return self.pos >= self.data.len();
    }

    fn consume_byte(&mut self) -> u8 {
        let byte = self.data[self.pos];
        self.pos += 1;
        return byte;
    }

    fn expect_byte(&mut self, byte: u8) -> Result<u8, Error> {
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

    fn peek_byte(&mut self) -> u8 {
        return self.data[self.pos];
    }

    fn consume_until_byte(&mut self, byte: u8) -> &'a [u8] {
        self.consume_until_filter(|b| b != byte)
    }

    fn consume_until_filter<F: Fn(u8) -> bool>(&mut self, filter: F) -> &'a [u8] {
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

/// Parses a decimal number represented as a string to a scaled `u64`.
/// Example: parse_scaled_number("24765.230000", 2) -> 2476523.
fn parse_scaled_number(s: &str, scale: u32) -> u64 {
    let dot_idx = s.find('.');
    if let Some(i) = dot_idx {
        let whole = s[..i].parse::<u64>().unwrap();
        let fractional = if scale > 0 {
            let start = i + 1;
            let end = std::cmp::min(start + scale as usize, s.len());
            let part = &s[start..end];
            let mut fractional = part.parse::<u64>().unwrap();
            if part.len() < scale as usize {
                let d = scale - part.len() as u32;
                fractional *= (10 as u64).pow(d);
            }
            fractional
        } else {
            0
        };
        if whole == 0 && fractional == 0 {
            return 0;
        }
        return whole * (10 as u64).pow(scale) + fractional;
    } else {
        return s.parse::<u64>().unwrap() * (10 as u64).pow(scale);
    }
}

fn parse_price_levels(
    bytes: &mut ByteSliceIterator,
    scales: &SymbolScales,
) -> Result<Vec<PriceLevel>, Error> {
    bytes.expect_byte(b'[')?;

    // Check if the array is empty
    if bytes.peek_byte() == b']' {
        bytes.consume_byte();
        return Ok(vec![]);
    }

    let mut levels: Vec<PriceLevel> = Vec::new();
    loop {
        bytes.expect_byte(b'[')?;

        // price
        bytes.expect_byte(b'"')?;
        let price = from_utf8(bytes.consume_until_byte(b'"')).unwrap();
        bytes.expect_byte(b'"')?;

        bytes.expect_byte(b',')?;

        // quantity
        bytes.expect_byte(b'"')?;
        let quantity = from_utf8(bytes.consume_until_byte(b'"')).unwrap();
        bytes.expect_byte(b'"')?;

        bytes.expect_byte(b']')?;

        let price_level = PriceLevel {
            price: parse_scaled_number(price, scales.price),
            quantity: parse_scaled_number(quantity, scales.quantity),
        };
        levels.push(price_level);

        let b = bytes.peek_byte();
        if b == b']' {
            bytes.consume_byte();
            break;
        }
        bytes.expect_byte(b',')?;
    }

    Ok(levels)
}

fn parse_single_byte_object_key(bytes: &mut ByteSliceIterator, key: u8) -> Result<u8, Error> {
    let res: Result<u8, Error> = {
        bytes.expect_byte(b'"')?;
        let k = bytes.consume_byte();
        if k != key {
            let msg = format!("expected key '{}' but found {}", key as char, k as char);
            return Err(Error::ParseError(msg));
        }
        bytes.expect_byte(b'"')?;
        bytes.expect_byte(b':')?;
        Ok(k)
    };
    res.map_err(|e| Error::ParseError(format!("parsing object key: {}", e.to_string())))
}

/// Handcoded parser for the Binance order book diff stream messages.
/// See: https://binance-docs.github.io/apidocs/spot/en/#partial-book-depth-streams
fn parse_diff_depth_stream_event(
    data: &[u8],
    scales: &SymbolScales,
) -> Result<OrderBookDiff, Error> {
    let mut bytes = ByteSliceIterator::new(data);

    // Opening curly bracket
    bytes.expect_byte(b'{')?;

    // Event type key
    parse_single_byte_object_key(&mut bytes, b'e')?;

    // Event type value
    bytes.expect_byte(b'"')?;
    bytes.consume_until_byte(b'"');
    bytes.expect_byte(b'"')?;
    bytes.expect_byte(b',')?;

    // Event time key
    parse_single_byte_object_key(&mut bytes, b'E')?;

    // Event time value
    let event_time_s = from_utf8(bytes.consume_until_byte(b',')).unwrap();
    let event_time = event_time_s.parse::<u64>().unwrap();
    bytes.expect_byte(b',')?;

    // Symbol key
    parse_single_byte_object_key(&mut bytes, b's')?;

    // Symbol value
    bytes.expect_byte(b'"')?;
    let symbol = from_utf8(bytes.consume_until_byte(b'"')).unwrap();
    bytes.expect_byte(b'"')?;
    bytes.expect_byte(b',')?;

    // First update ID key
    parse_single_byte_object_key(&mut bytes, b'U')?;

    // First update ID value
    let first_update_id = from_utf8(bytes.consume_until_byte(b','))
        .map(|s| s.parse::<u64>().unwrap())
        .unwrap();
    bytes.expect_byte(b',')?;

    // Final update ID key
    parse_single_byte_object_key(&mut bytes, b'u')?;

    // Final update ID value
    let final_update_id = from_utf8(bytes.consume_until_byte(b','))
        .map(|s| s.parse::<u64>().unwrap())
        .unwrap();
    bytes.expect_byte(b',')?;

    // Bids key
    parse_single_byte_object_key(&mut bytes, b'b')?;

    // Bids value
    let bids = parse_price_levels(&mut bytes, scales)?;
    bytes.expect_byte(b',')?;

    // Asks key
    parse_single_byte_object_key(&mut bytes, b'a')?;

    // Asks value
    let asks = parse_price_levels(&mut bytes, scales)?;

    // Closing curly bracket
    bytes.expect_byte(b'}')?;

    if !bytes.is_empty() {
        return Err(Error::ParseError("expected EOF".to_string()));
    }

    Ok(OrderBookDiff {
        event_time,
        symbol: symbol.to_string(),
        first_update_id,
        final_update_id,
        bids,
        asks,
    })
}

/// Handcoded parser for the Binance order book snapshot.
/// See: https://binance-docs.github.io/apidocs/spot/en/#order-book
fn parse_order_book(data: &[u8], scales: &SymbolScales) -> Result<OrderBook, Error> {
    let mut bytes = ByteSliceIterator::new(data);

    // Opening curly brace
    bytes.expect_byte(b'{')?;

    // lastUpdateId key
    bytes.consume_until_byte(b':');
    bytes.expect_byte(b':')?;

    // lastUpdateId value
    let last_update_id = from_utf8(bytes.consume_until_byte(b','))
        .unwrap()
        .parse::<u64>()
        .unwrap();
    bytes.expect_byte(b',')?;

    // bids key
    bytes.expect_byte(b'"')?;
    let bids_key = from_utf8(bytes.consume_until_byte(b'"')).unwrap();
    if bids_key != "bids" {
        let msg = format!("expected key 'bids' but found '{}'", bids_key);
        return Err(Error::ParseError(msg));
    }
    bytes.expect_byte(b'"')?;
    bytes.expect_byte(b':')?;

    // bids value
    let bids = parse_price_levels(&mut bytes, scales)?;
    bytes.expect_byte(b',')?;

    // asks key
    bytes.expect_byte(b'"')?;
    let asks_key = from_utf8(bytes.consume_until_byte(b'"')).unwrap();
    if asks_key != "asks" {
        let msg = format!("expected key 'asks' but found '{}'", asks_key);
        return Err(Error::ParseError(msg));
    }
    bytes.expect_byte(b'"')?;
    bytes.expect_byte(b':')?;

    // asks value
    let asks = parse_price_levels(&mut bytes, scales)?;

    // Closing curly brace
    bytes.expect_byte(b'}')?;

    if !bytes.is_empty() {
        return Err(Error::ParseError("expected EOF".to_string()));
    }

    Ok(OrderBook {
        last_update_id,
        bids,
        asks,
    })
}

#[cfg(test)]
mod tests {
    use crate::binance_api::{parse_scaled_number, SymbolScales};

    use super::{parse_diff_depth_stream_event, parse_order_book, OrderBookDiff, PriceLevel};

    #[test]
    fn test_parser() {
        // {
        //   "e":"depthUpdate",
        //   "E":1679024268761,
        //   "s":"BTCUSDT",
        //   "U":35286408260,
        //   "u":35286409112,
        //   "b":[["25700.51000000","0.00000000"],["25700.12000000","0.10277000"],["25700.11000000","0.00000000"],["25700.08000000","0.00403000"],["25700.04000000","0.00430000"]]
        //   "a":[["25701.43000000","0.00000000"],["25701.44000000","0.00203000"],["25701.47000000","0.00064000"],["25701.50000000","0.00000000"]]
        // }

        let data = r#"{"e":"depthUpdate","E":1679024268761,"s":"BTCUSDT","U":35286408260,"u":35286409112,"b":[["25700.51000000","0.00000000"],["25700.12000000","0.10277000"],["25700.11000000","0.00000000"],["25700.08000000","0.00403000"],["25700.04000000","0.00430000"]],"a":[["25701.43000000","0.00000000"],["25701.44000000","0.00203000"],["25701.47000000","0.00064000"],["25701.50000000","0.00000000"]]}"#;
        let scales = SymbolScales {
            price: 2,
            quantity: 8,
        };
        let diff = parse_diff_depth_stream_event(data.as_bytes(), &scales).unwrap();
        let expected_diff = OrderBookDiff {
            event_time: 1679024268761,
            symbol: "BTCUSDT".to_owned(),
            first_update_id: 35286408260,
            final_update_id: 35286409112,
            bids: vec![
                PriceLevel {
                    price: 2570051,
                    quantity: 00000000,
                },
                PriceLevel {
                    price: 2570012,
                    quantity: 10277000,
                },
                PriceLevel {
                    price: 2570011,
                    quantity: 00000000,
                },
                PriceLevel {
                    price: 2570008,
                    quantity: 00403000,
                },
                PriceLevel {
                    price: 2570004,
                    quantity: 00430000,
                },
            ],
            asks: vec![
                PriceLevel {
                    price: 2570143,
                    quantity: 00000000,
                },
                PriceLevel {
                    price: 2570144,
                    quantity: 00203000,
                },
                PriceLevel {
                    price: 2570147,
                    quantity: 00064000,
                },
                PriceLevel {
                    price: 2570150,
                    quantity: 00000000,
                },
            ],
        };
        assert_eq!(&diff, &expected_diff);
    }

    #[test]
    fn test_parse_diff_depth_empty() {
        // let data =
        // {
        //   "e":"depthUpdate",
        //   "E":1679634678423,
        //   "s":"BTCUSDT",
        //   "U":35843600705,
        //   "u":35843600709,
        //   "b":[["28174.25000000","8.62292000"],["28128.00000000","0.67862000"]],
        //   "a":[]
        // }"
        let data = r#"{"e":"depthUpdate","E":1679634678423,"s":"BTCUSDT","U":35843600705,"u":35843600709,"b":[["28174.25000000","8.62292000"],["28128.00000000","0.67862000"]],"a":[]}"#;
        let scales = SymbolScales {
            price: 2,
            quantity: 8,
        };
        let diff = parse_diff_depth_stream_event(data.as_bytes(), &scales).unwrap();
        let expected_diff = OrderBookDiff {
            event_time: 1679634678423,
            symbol: "BTCUSDT".to_owned(),
            first_update_id: 35843600705,
            final_update_id: 35843600709,
            bids: vec![
                PriceLevel {
                    price: 2817425,
                    quantity: 862292000,
                },
                PriceLevel {
                    price: 2812800,
                    quantity: 67862000,
                },
            ],
            asks: vec![],
        };
        assert_eq!(&diff, &expected_diff);
    }

    #[test]
    fn test_parse_order_book() {
        // {
        //    "lastUpdateId":35357397801,
        //    "bids":[["26458.89000000","0.19892000"],["26458.86000000","0.97602000"]],
        //    "asks":[["26460.33000000","0.00864000"],["26460.74000000","0.00070000"]]
        // }
        let data = r#"{"lastUpdateId":35357397801,"bids":[["26458.89000000","0.19892000"],["26458.86000000","0.97602000"]],"asks":[["26460.33000000","0.00864000"],["26460.74000000","0.00070000"]]}"#;
        let scales = SymbolScales {
            price: 2,
            quantity: 8,
        };
        parse_order_book(data.as_bytes(), &scales).unwrap();
    }

    #[test]
    fn test_parse_scaled_number() {
        assert_eq!(parse_scaled_number("25700.51000000", 2), 2570051);
        assert_eq!(parse_scaled_number("25700.51000000", 4), 257005100);
        assert_eq!(parse_scaled_number("25700.51000000", 0), 25700);
        assert_eq!(parse_scaled_number("25700.51000000", 1), 257005);
        assert_eq!(parse_scaled_number("0.00000000", 1), 0);
        assert_eq!(parse_scaled_number("0.00000000", 8), 0);
        assert_eq!(parse_scaled_number("0.00064000", 8), 64000);
        assert_eq!(parse_scaled_number("0.00064000", 2), 0);
        assert_eq!(parse_scaled_number("0.00064000", 10), 6400000);
    }
}
