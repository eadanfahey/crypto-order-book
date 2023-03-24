mod binance_api;
mod errors;
mod files;
mod order_book;

use std::time::Instant;
use tokio::sync::{broadcast, mpsc::unbounded_channel};
use tokio::task::JoinHandle;
use tokio::{io::AsyncWriteExt, signal};

use crate::binance_api::{OrderBookDiff, Symbol};
use crate::files::{save_orderbook_snapshot, OrderBookDiffFile};
use crate::order_book::OrderBook;

#[tokio::main]
async fn main() {
    let symbols = vec![
        Symbol::BtcUsdt,
        Symbol::EthUsdt,
        Symbol::ArbUsdt,
        Symbol::BtcBusd,
        Symbol::UsdcUsdt,
        Symbol::BusdUsdt,
        Symbol::EthBusd,
        Symbol::XrpUsdt,
        Symbol::BtcTusd,
        Symbol::EthBtc,
        Symbol::LtcUsdt,
        Symbol::SolUsdt,
        Symbol::BnbUsdt,
    ];

    let (shutdown_send, _) = broadcast::channel::<()>(1);
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(symbols.len());

    for symbol in symbols.into_iter() {
        let shutdown_recv = shutdown_send.subscribe();
        let handle = tokio::spawn(async move { record_orderbook(symbol, shutdown_recv).await });
        handles.push(handle);
    }

    match signal::ctrl_c().await {
        Ok(()) => {
            shutdown_send.send(()).unwrap();
            for handle in handles.into_iter() {
                handle.await.unwrap();
            }
        }
        Err(_) => {
            panic!("Failed to listen for ctrl-c signal");
        }
    };
}

async fn record_orderbook(symbol: Symbol, shutdown_recv: broadcast::Receiver<()>) {
    // Start the websocket order book diff stream.
    let (tx, mut rx) = unbounded_channel::<(OrderBookDiff, Instant)>();
    binance_api::start_stream_order_book_diffs(symbol, tx, shutdown_recv).await;

    let mut stdout = tokio::io::stdout();

    // Initialize the orderbook with a snapshot from the API
    let init = binance_api::get_order_book_snapshot(symbol, 1000)
        .await
        .unwrap();
    let mut order_book = OrderBook::new(init);

    let mut diff_file = OrderBookDiffFile::new(symbol);

    // Update the order book with diffs received on the websocket channel
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
            let msg = format!(
                "{} median order book update latency: {}us (min: {}us, max: {}us)\n",
                symbol.to_string(),
                median,
                min,
                max
            );
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
}
