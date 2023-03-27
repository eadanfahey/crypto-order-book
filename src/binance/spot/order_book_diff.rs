use super::{
    snapshot::{parse_price_levels, PriceLevel},
    url_util::create_stream_url,
    Symbol,
};
use crate::binance::{
    errors::Error, json_parser::ByteSliceIterator, stream::start_market_data_stream,
};
use serde::{Deserialize, Serialize};
use std::str::from_utf8;
use tokio::{
    sync::{broadcast, mpsc::UnboundedSender},
    task::JoinHandle,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]

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
    channel: UnboundedSender<OrderBookDiff>,
    shutdown_recv: broadcast::Receiver<()>,
) -> Result<JoinHandle<Result<(), Error>>, Error> {
    let url = create_stream_url(&format!("/{}@depth@100ms", symbol.stream_string()));
    let handle =
        start_market_data_stream(url, channel, shutdown_recv, parse_diff_depth_stream_event).await;
    handle
}

/// Handcoded parser for the Binance order book diff stream messages.
/// See: https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream
pub fn parse_diff_depth_stream_event(data: &[u8]) -> Result<OrderBookDiff, Error> {
    let mut bytes = ByteSliceIterator::new(data);

    // Opening curly bracket
    bytes.expect_byte(b'{')?;

    // Event type key
    bytes.expect_single_byte_key(b'e')?;

    // Event type value
    bytes.consume_string()?;
    bytes.expect_byte(b',')?;

    // Event time key
    bytes.expect_single_byte_key(b'E')?;

    // Event time value
    let event_time = bytes.consume_int()?;
    bytes.expect_byte(b',')?;

    // Symbol key
    bytes.expect_single_byte_key(b's')?;

    // Symbol value
    let symbol = from_utf8(bytes.consume_string()?).unwrap();
    bytes.expect_byte(b',')?;

    // First update ID key
    bytes.expect_single_byte_key(b'U')?;

    // First update ID value
    let first_update_id = bytes.consume_int()?;
    bytes.expect_byte(b',')?;

    // Final update ID key
    bytes.expect_single_byte_key(b'u')?;

    // Final update ID value
    let final_update_id = bytes.consume_int()?;
    bytes.expect_byte(b',')?;

    // Bids key
    bytes.expect_single_byte_key(b'b')?;

    // Bids value
    let bids = parse_price_levels(&mut bytes)?;
    bytes.expect_byte(b',')?;

    // Asks key
    bytes.expect_single_byte_key(b'a')?;

    // Asks value
    let asks = parse_price_levels(&mut bytes)?;

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

#[cfg(test)]
mod tests {
    use super::{parse_diff_depth_stream_event, OrderBookDiff, PriceLevel};

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
        let diff = parse_diff_depth_stream_event(data.as_bytes()).unwrap();
        let expected_diff = OrderBookDiff {
            event_time: 1679024268761,
            symbol: "BTCUSDT".to_owned(),
            first_update_id: 35286408260,
            final_update_id: 35286409112,
            bids: vec![
                PriceLevel {
                    price: 2570051000000,
                    quantity: 0,
                },
                PriceLevel {
                    price: 2570012000000,
                    quantity: 10277000,
                },
                PriceLevel {
                    price: 2570011000000,
                    quantity: 0,
                },
                PriceLevel {
                    price: 2570008000000,
                    quantity: 403000,
                },
                PriceLevel {
                    price: 2570004000000,
                    quantity: 430000,
                },
            ],
            asks: vec![
                PriceLevel {
                    price: 2570143000000,
                    quantity: 0,
                },
                PriceLevel {
                    price: 2570144000000,
                    quantity: 203000,
                },
                PriceLevel {
                    price: 2570147000000,
                    quantity: 64000,
                },
                PriceLevel {
                    price: 2570150000000,
                    quantity: 0,
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
        let diff = parse_diff_depth_stream_event(data.as_bytes()).unwrap();
        let expected_diff = OrderBookDiff {
            event_time: 1679634678423,
            symbol: "BTCUSDT".to_owned(),
            first_update_id: 35843600705,
            final_update_id: 35843600709,
            bids: vec![
                PriceLevel {
                    price: 2817425000000,
                    quantity: 862292000,
                },
                PriceLevel {
                    price: 2812800000000,
                    quantity: 67862000,
                },
            ],
            asks: vec![],
        };
        assert_eq!(&diff, &expected_diff);
    }
}
