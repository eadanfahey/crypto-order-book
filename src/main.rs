mod binance_api;
mod diff_file;
mod errors;
mod order_book;

use std::time::Instant;
use tokio::sync::mpsc::unbounded_channel;
use tokio::{io::AsyncWriteExt, signal};

use crate::binance_api::{OrderBookDiff, Symbol};
use crate::diff_file::{save_orderbook_snapshot, OrderBookDiffFile};
use crate::order_book::OrderBook;

#[tokio::main]
async fn main() {
    // Start the websocket order book diff stream.
    let (shutdown_send, shutdown_recv) = unbounded_channel::<()>();
    let (tx, mut rx) = unbounded_channel::<(OrderBookDiff, Instant)>();
    binance_api::start_stream_order_book_diffs(Symbol::BtcUsdt, tx, shutdown_recv).await;

    let ob_handle = tokio::spawn(async move {
        let symbol = Symbol::BtcUsdt;
        let mut stdout = tokio::io::stdout();

        // Initialize the orderbook with a snapshot from the API
        let init = binance_api::get_order_book_snapshot(symbol, 1000)
            .await
            .unwrap();
        let mut order_book = OrderBook::new(init);

        let mut diff_file = OrderBookDiffFile::new(symbol);

        let mut i = 0;
        let mut snapshot_saved = false;
        let mut latencies: Vec<u128> = Vec::with_capacity(100);
        let mut started_new_snapshot = Instant::now();
        while let Some((diff, start)) = rx.recv().await {
            i += 1;
            order_book.update(&diff);
            latencies.push(start.elapsed().as_micros());

            if i % 100 == 0 {
                latencies.sort();
                let median = latencies[50];
                let min = latencies.first().unwrap();
                let max = latencies.last().unwrap();
                let msg = format!("{}us [{}, {}]us\n", median, min, max);
                stdout.write_all(msg.as_bytes()).await.unwrap();
                stdout.flush().await.unwrap();
                latencies.clear();
            }

            if !snapshot_saved && order_book.last_updated_time().is_some() {
                snapshot_saved = true;
                save_orderbook_snapshot(symbol, &order_book).await;
            }

            if snapshot_saved {
                diff_file.write(&diff).await;
            }

            // Save a new snapshot every 10 minutes
            if started_new_snapshot.elapsed().as_secs() >= 60 * 10 {
                diff_file.close().await;
                diff_file = OrderBookDiffFile::new(symbol);
                started_new_snapshot = Instant::now();
                snapshot_saved = false;
            }
        }

        diff_file.close().await;
    });

    match signal::ctrl_c().await {
        Ok(()) => {
            shutdown_send.send(()).unwrap();
            ob_handle.await.unwrap();
        }
        Err(_) => {
            panic!("Failed to listen for ctrl-c signal");
        }
    };
}
