use crate::binance_api::{self, PriceLevel};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Order book datastructure backed by a `BTreeMap`.
#[derive(Serialize, Deserialize)]
pub struct OrderBook {
    last_update_id: u64,
    bids: BTreeMap<u64, u64>,
    asks: BTreeMap<u64, u64>,
    last_updated_time: Option<u64>,
}

impl OrderBook {
    /// Create a new `OrderBook` with an initial snapshot from the Binance API.
    pub fn new(init: binance_api::OrderBook) -> Self {
        let mut asks = BTreeMap::new();
        let mut bids = BTreeMap::new();

        for level in init.bids.iter() {
            bids.insert(level.price, level.quantity);
        }

        for level in init.asks.iter() {
            asks.insert(level.price, level.quantity);
        }

        OrderBook {
            last_update_id: init.last_update_id,
            bids: bids,
            asks: asks,
            last_updated_time: None,
        }
    }

    fn update_asks(&mut self, levels: &[PriceLevel]) {
        // Can ignore any change beyond the maximum ask in the book.
        for level in levels.iter() {
            if level.quantity == 0 {
                self.asks.remove(&level.price);
            } else {
                self.asks.insert(level.price, level.quantity);
            }
        }
    }

    fn update_bids(&mut self, levels: &[PriceLevel]) {
        for level in levels.iter() {
            if level.quantity == 0 {
                self.bids.remove(&level.price);
            } else {
                self.bids.insert(level.price, level.quantity);
            }
        }
    }

    /// Update the order book with a diff received from the Binance order book diff
    /// stream. The order book is managed according to the description here:
    /// https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
    pub fn update(&mut self, diff: &binance_api::OrderBookDiff) {
        if self.last_updated_time.is_none() {
            if diff.final_update_id <= self.last_update_id {
                return;
            }
        } else {
            assert!(diff.first_update_id == self.last_update_id + 1);
        }
        self.update_asks(&diff.asks);
        self.update_bids(&diff.bids);
        self.last_update_id = diff.final_update_id;
        self.last_updated_time = Some(diff.event_time);
    }

    pub fn last_updated_time(&self) -> Option<u64> {
        self.last_updated_time
    }

    #[allow(dead_code)]
    pub fn best_bid(&self) -> Option<PriceLevel> {
        if let Some((&price, &quantity)) = self.bids.last_key_value() {
            return Some(PriceLevel { price, quantity });
        }
        None
    }

    #[allow(dead_code)]
    pub fn best_ask(&self) -> Option<PriceLevel> {
        if let Some((&price, &quantity)) = self.asks.first_key_value() {
            return Some(PriceLevel { price, quantity });
        }
        None
    }
}
