mod binance_api;
mod errors;
mod order_book;

use std::time::{Duration, Instant};
use tokio::sync::mpsc::unbounded_channel;
use tokio::signal;
use binance_api::Symbol;
use crate::order_book::OrderBook;
use crate::binance_api::OrderBookDiff;


#[tokio::main]
async fn main() {

    // Start the websocket order book diff stream.
    let (shutdown_send, shutdown_recv) = unbounded_channel::<()>();
    let (tx, mut rx) = unbounded_channel::<(OrderBookDiff, Duration)>();
    binance_api::start_stream_order_book_diffs(Symbol::BtcUsdt, tx, shutdown_recv).await;

    let ob_handle = tokio::spawn(async move {
        let init = binance_api::get_order_book_snapshot(Symbol::BtcUsdt, 5000).await.unwrap();
        let mut order_book = OrderBook::new(init);
        while let Some((diff, parse_duration)) = rx.recv().await {
            let num_updates = diff.asks.len() + diff.bids.len();
            let start = Instant::now();
            order_book.update(diff);

            let total_us = (start.elapsed() + parse_duration).as_micros();
            let rate = (num_updates as f64 / total_us as f64) as f64; 
            println!("Updated order book in {}us ({:.2}M updates/s)", total_us, rate);
        }
    });

    match signal::ctrl_c().await {
        Ok(()) => {
            shutdown_send.send(()).unwrap();
            ob_handle.await.unwrap();
        },
        Err(_) => {
            panic!("Failed to listen for ctrl-c signal");
        }
    };
}