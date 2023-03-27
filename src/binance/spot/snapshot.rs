use super::{url_util::create_api_url, Symbol};
use crate::binance::{
    errors::Error, json_parser::ByteSliceIterator, parse_util::parse_scaled_number,
};
use serde::{Deserialize, Serialize};
use std::str::from_utf8;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: u64,
    pub quantity: u64,
}

#[derive(Debug, PartialEq)]
pub struct OrderBookSnapshot {
    pub last_update_id: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

/// Get a snapshot of the orderbook for a trading pair up to a given depth. The maximum
/// depth that Binance returns is 5000.
pub async fn get_order_book_snapshot(
    symbol: Symbol,
    depth: u32,
) -> Result<OrderBookSnapshot, Error> {
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

    parse_order_book(&data)
}

pub fn parse_price_levels(bytes: &mut ByteSliceIterator) -> Result<Vec<PriceLevel>, Error> {
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
        let price = from_utf8(bytes.consume_string()?).unwrap();
        bytes.expect_byte(b',')?;

        // quantity
        let quantity = from_utf8(bytes.consume_string()?).unwrap();

        bytes.expect_byte(b']')?;

        let price_level = PriceLevel {
            price: parse_scaled_number(price, 8),
            quantity: parse_scaled_number(quantity, 8),
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

/// Handcoded parser for the Binance order book snapshot.
/// See: https://binance-docs.github.io/apidocs/spot/en/#order-book
pub fn parse_order_book(data: &[u8]) -> Result<OrderBookSnapshot, Error> {
    let mut bytes = ByteSliceIterator::new(data);

    // Opening curly brace
    bytes.expect_byte(b'{')?;

    // lastUpdateId key
    bytes.expect_key("lastUpdateId")?;

    // lastUpdateId value
    let last_update_id = bytes.consume_int()?;
    bytes.expect_byte(b',')?;

    // bids key
    bytes.expect_key("bids")?;

    // bids value
    let bids = parse_price_levels(&mut bytes)?;
    bytes.expect_byte(b',')?;

    // asks key
    bytes.expect_key("asks")?;

    // asks value
    let asks = parse_price_levels(&mut bytes)?;

    // Closing curly brace
    bytes.expect_byte(b'}')?;

    if !bytes.is_empty() {
        return Err(Error::ParseError("expected EOF".to_string()));
    }

    Ok(OrderBookSnapshot {
        last_update_id,
        bids,
        asks,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_order_book;

    #[test]
    fn test_parse_order_book() {
        // {
        //    "lastUpdateId":35357397801,
        //    "bids":[["26458.89000000","0.19892000"],["26458.86000000","0.97602000"]],
        //    "asks":[["26460.33000000","0.00864000"],["26460.74000000","0.00070000"]]
        // }
        let data = r#"{"lastUpdateId":35357397801,"bids":[["26458.89000000","0.19892000"],["26458.86000000","0.97602000"]],"asks":[["26460.33000000","0.00864000"],["26460.74000000","0.00070000"]]}"#;
        parse_order_book(data.as_bytes()).unwrap();
    }
}
