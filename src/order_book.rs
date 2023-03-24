use crate::binance_api::{self, PriceLevel};
use std::collections::BTreeMap;

/// Order book datastructure backed by a `BTreeMap`.
pub struct OrderBook {
    last_update_id: u64,
    bids: BTreeMap<u64, u64>,
    asks: BTreeMap<u64, u64>,
    first_diff_processed: bool,
}

impl OrderBook {
    /// Create a new `OrderBook` with an initial snapshot from the Binance API.
    pub fn new(init: binance_api::OrderBook) -> Self {
        let mut asks = BTreeMap::new();
        let mut bids = BTreeMap::new();
        for level in init.asks.iter() {
            asks.insert(level.price, level.quantity);
        }
        for level in init.bids.iter() {
            bids.insert(level.price, level.quantity);
        }
        OrderBook {
            last_update_id: init.last_update_id,
            asks,
            bids,
            first_diff_processed: false,
        }
    }

    fn apply_diff_to(side: &mut BTreeMap<u64, u64>, levels: &[PriceLevel]) -> u64 {
        let mut updates = 0;
        for level in levels.iter() {
            if level.quantity == 0 {
                if let Some(_) = side.remove(&level.price) {
                    updates += 1;
                }
            } else {
                side.insert(level.price, level.quantity);
                updates += 1;
            }
        }
        return updates;
    }

    /// Update the order book with a diff received from the Binance order book diff
    /// stream. The order book is managed according to the description here:
    /// https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
    pub fn update(&mut self, diff: binance_api::OrderBookDiff) -> u64 {
        if !self.first_diff_processed {
            if diff.final_update_id <= self.last_update_id {
                return 0;
            }
        } else {
            assert!(diff.first_update_id == self.last_update_id + 1);
        }
        let ask_updates = Self::apply_diff_to(&mut self.asks, &diff.asks);
        let bid_updates = Self::apply_diff_to(&mut self.bids, &diff.bids);
        self.last_update_id = diff.final_update_id;
        return ask_updates + bid_updates;
    }
}
