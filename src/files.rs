use crate::binance_api::{OrderBookDiff, Symbol, Trade};
use crate::order_book::OrderBook;
use std::env;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

const MAGIC_NUMBER: u32 = 1315047820;
const BLOCK_SEP: u32 = 0;

async fn get_book_dir(symbol: Symbol) -> PathBuf {
    let data_dir = env::var("DATA_DIR").unwrap();
    let book_dir = Path::new(&data_dir)
        .join("binance")
        .join("order_books")
        .join(symbol.to_string());

    if !book_dir.try_exists().is_err() {
        tokio::fs::create_dir_all(&book_dir).await.unwrap();
    }

    book_dir
}

pub async fn get_trade_dir(symbol: Symbol) -> PathBuf {
    let data_dir = env::var("DATA_DIR").unwrap();
    let book_dir = Path::new(&data_dir)
        .join("binance")
        .join("trades")
        .join(symbol.to_string());

    if !book_dir.try_exists().is_err() {
        tokio::fs::create_dir_all(&book_dir).await.unwrap();
    }

    book_dir
}

pub struct OrderBookDiffFile {
    symbol: Symbol,
    writer: Option<BufWriter<File>>,
}

impl OrderBookDiffFile {
    pub fn new(symbol: Symbol) -> Self {
        return OrderBookDiffFile {
            symbol,
            writer: None,
        };
    }

    pub async fn write(&mut self, diff: &OrderBookDiff) {
        if self.writer.is_none() {
            let book_dir = get_book_dir(self.symbol).await;
            let filename = format!("{}_diff.bin", diff.event_time);
            let filepath = book_dir.join(filename);
            let f = File::create(filepath).await.unwrap();
            let mut writer = BufWriter::new(f);

            writer.write_all(&MAGIC_NUMBER.to_le_bytes()).await.unwrap();
            writer
                .write_all(&self.symbol.id().to_le_bytes())
                .await
                .unwrap();

            self.writer = Some(writer);
        }

        let writer = self.writer.as_mut().unwrap();
        writer
            .write_all(&diff.event_time.to_le_bytes())
            .await
            .unwrap();
        writer
            .write_all(&(diff.bids.len() as u64).to_le_bytes())
            .await
            .unwrap();
        writer
            .write_all(&(diff.asks.len() as u64).to_le_bytes())
            .await
            .unwrap();

        for level in diff.bids.iter() {
            writer.write_all(&level.price.to_le_bytes()).await.unwrap();
            writer
                .write_all(&level.quantity.to_le_bytes())
                .await
                .unwrap();
        }

        for level in diff.asks.iter() {
            writer.write_all(&level.price.to_le_bytes()).await.unwrap();
            writer
                .write_all(&level.quantity.to_le_bytes())
                .await
                .unwrap();
        }

        writer.write_all(&BLOCK_SEP.to_le_bytes()).await.unwrap();
    }

    pub async fn close(self) {
        if let Some(mut writer) = self.writer {
            writer.flush().await.unwrap();
        }
    }
}

pub struct TradesFile {
    symbol: Symbol,
    writer: Option<BufWriter<File>>,
}

impl TradesFile {
    pub fn new(symbol: Symbol) -> Self {
        TradesFile {
            symbol,
            writer: None,
        }
    }

    pub async fn write(&mut self, trade: &Trade) {
        if self.writer.is_none() {
            let trade_dir = get_trade_dir(self.symbol).await;
            let filename = format!("{}_trades.jsonl", trade.trade_time);
            let filepath = trade_dir.join(filename);
            let f = File::create(filepath).await.unwrap();
            let writer = BufWriter::new(f);
            self.writer = Some(writer);
        }

        let writer = self.writer.as_mut().unwrap();
        let data = serde_json::to_string(&trade).unwrap();
        writer.write_all(data.as_bytes()).await.unwrap();
        writer.write_all(&[b'\n']).await.unwrap();
    }

    pub async fn close(self) {
        if let Some(mut writer) = self.writer {
            writer.flush().await.unwrap();
        }
    }
}

pub async fn save_orderbook_snapshot(symbol: Symbol, order_book: &OrderBook) {
    let book_dir = get_book_dir(symbol).await;
    let filename = format!("{}_snapshot.json", order_book.last_updated_time().unwrap());
    let filepath = book_dir.join(filename);
    let data = serde_json::to_string(&order_book).unwrap();
    let mut f = File::create(filepath).await.unwrap();
    f.write_all(data.as_bytes()).await.unwrap();
    f.flush().await.unwrap();
}
