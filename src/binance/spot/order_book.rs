use super::{
    order_book_diff::{start_stream_order_book_diffs, OrderBookDiff},
    snapshot::get_order_book_snapshot,
    Symbol,
};
use crate::{
    binance::errors::Error, msgpack_lines::FileWriter as MsgPackFileWriter, order_book::OrderBook,
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::Instant,
};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    sync::{broadcast, mpsc::unbounded_channel},
};

#[derive(Serialize, Deserialize)]
pub struct SpotOrderBook {
    book: OrderBook,
    depth: u32,
    last_update_id: u64,
    last_updated_time: Option<u64>,
}

impl SpotOrderBook {
    pub fn new(depth: u32) -> Self {
        SpotOrderBook {
            book: OrderBook::new(),
            depth,
            last_update_id: 0,
            last_updated_time: None,
        }
    }

    /// Update the order book with a diff received from the Binance order book diff
    /// stream. The order book is managed according to the description here:
    /// https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
    fn update(&mut self, diff: &OrderBookDiff) {
        if self.last_updated_time.is_none() {
            if diff.final_update_id <= self.last_update_id {
                return;
            }
        } else {
            assert!(diff.first_update_id == self.last_update_id + 1);
        }
        for level in diff.asks.iter() {
            self.book.update_ask_level(level.price, level.quantity);
        }
        for level in diff.bids.iter() {
            self.book.update_bid_level(level.price, level.quantity);
        }
        self.last_update_id = diff.final_update_id;
        self.last_updated_time = Some(diff.event_time);
    }

    /// Streams live updates from the Binance API spot order book on a trading pair to
    /// file.
    pub async fn stream_to_file(
        &mut self,
        symbol: Symbol,
        data_dir: &str,
        shutdown_recv: broadcast::Receiver<()>,
    ) -> Result<(), Error> {
        // Start streaming order book diffs from the API
        let (tx, mut rx) = unbounded_channel::<OrderBookDiff>();
        start_stream_order_book_diffs(symbol, tx, shutdown_recv).await?;

        // Get a snapshot of the book from the API and update our book
        let snapshot = get_order_book_snapshot(symbol, self.depth).await?;
        for level in snapshot.asks.iter() {
            self.book.update_ask_level(level.price, level.quantity)
        }
        for level in snapshot.bids.iter() {
            self.book.update_bid_level(level.price, level.quantity)
        }

        let (mut diff_file, mut filepath) = new_diff_file(&data_dir, symbol).await;

        let mut snapshot_saved = false;
        let mut started_new_snapshot = Instant::now();

        // Read the stream of diffs and apply them to our book
        let nrows = 0;
        while let Some(diff) = rx.recv().await {
            self.update(&diff);

            diff_file.write(&diff).await.unwrap();

            // Save the initial snapshot
            // We need to wait until the order book has catched up with the diff stream
            // first by checking when if last_updated_time has been set
            if !snapshot_saved && self.last_updated_time.is_some() {
                let dir = get_order_books_dir(&data_dir, symbol).await;
                let filepath =
                    dir.join(format!("{}_snapshot.json", self.last_updated_time.unwrap()));
                self.save_snapshot(filepath).await?;
                snapshot_saved = true;
            }

            // Save a new snapshot every 10 minutes
            if started_new_snapshot.elapsed().as_secs() >= 60 * 10 {
                diff_file.close().await.unwrap();
                println!("Saved {} with {} rows", filepath.as_os_str().to_string_lossy(), nrows);
                (diff_file, filepath) = new_diff_file(&data_dir, symbol).await;
                started_new_snapshot = Instant::now();
                snapshot_saved = false;
            }
        }

        diff_file.close().await.unwrap();
        println!("Saved {} with {} rows", filepath.as_os_str().to_string_lossy(), nrows);

        Ok(())
    }

    async fn save_snapshot(&self, filepath: PathBuf) -> Result<(), Error> {
        let f = File::create(filepath).await?;
        let mut writer = BufWriter::new(f);
        let s = serde_json::to_string(self).unwrap();
        writer.write_all(s.as_bytes()).await?;
        Ok(())
    }
}

async fn new_diff_file(data_dir: &str, symbol: Symbol) -> (MsgPackFileWriter<OrderBookDiff>, PathBuf) {
    let dir = get_order_books_dir(data_dir, symbol).await;
    let filepath = dir.join(format!("{}_diff.msgpckl", now_millis()));
    let writer = MsgPackFileWriter::<OrderBookDiff>::new(filepath.clone());
    (writer, filepath)
}

async fn get_order_books_dir(data_dir: &str, symbol: Symbol) -> PathBuf {
    let book_dir = Path::new(data_dir)
        .join("binance")
        .join("spot")
        .join("order_books")
        .join(symbol.to_string());

    if !book_dir.try_exists().is_err() {
        tokio::fs::create_dir_all(&book_dir).await.unwrap();
    }

    book_dir
}

fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
