use std::{path::{PathBuf, Path}, time::Instant};
use serde::{Deserialize, Serialize};
use tokio::{sync::broadcast, sync::mpsc::{UnboundedSender, unbounded_channel}, task::JoinHandle, fs::File};
use url::Url;
use crate::binance::{stream::start_market_data_stream, parse_util::deserialize_decimal_e8, Error};
use super::Symbol;

const BASE_STREAM_ENDPOINT: &'static str = "wss://fstream.binance.com/ws";

fn create_stream_url(endpoint: &str) -> Url {
    return Url::parse(&(BASE_STREAM_ENDPOINT.to_owned() + endpoint)).unwrap();
}


fn stream_name(symbol: Symbol) -> &'static str {
    match symbol {
        Symbol::BtcUsdt => "btcusdt",
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct StreamBookTick {
    #[serde(alias = "u")]
    order_book_update_id: u64,
    #[serde(alias = "E")]
    event_time: u64,
    #[serde(alias = "T")]
    transaction_time: u64,
    #[serde(alias = "b", deserialize_with = "deserialize_decimal_e8")]
    best_bid_price: u64,
    #[serde(alias = "B", deserialize_with = "deserialize_decimal_e8")]
    best_bid_quantity: u64,
    #[serde(alias = "a", deserialize_with = "deserialize_decimal_e8")]
    best_ask_price: u64,
    #[serde(alias = "A", deserialize_with = "deserialize_decimal_e8")]
    best_ask_quantity: u64,
}

impl StreamBookTick {
    fn to_book_tick(self) -> BookTick {
        BookTick {
            order_book_update_id: self.order_book_update_id,
            event_time: self.event_time,
            transaction_time: self.transaction_time,
            best_bid_price: self.best_bid_price,
            best_bid_quantity: self.best_bid_quantity,
            best_ask_price: self.best_ask_price,
            best_ask_quantity: self.best_ask_quantity
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BookTick {
    order_book_update_id: u64,
    event_time: u64,
    transaction_time: u64,
    best_bid_price: u64,
    best_bid_quantity: u64,
    best_ask_price: u64,
    best_ask_quantity: u64,

}

impl BookTick {
    fn parse_stream_json(v: &[u8]) -> Result<Self, Error> {
        let tick = serde_json::from_slice(v).map(|s: StreamBookTick| s.to_book_tick())?;
        Ok(tick)
    }
}

pub async fn stream_book_ticks(
    symbol: Symbol,
    channel: UnboundedSender<BookTick>,
    shutdown_recv: broadcast::Receiver<()>,
) -> Result<JoinHandle<Result<(), Error>>, Error> {
    let url = create_stream_url(&format!("/{}@bookTicker", stream_name(symbol)));
    start_market_data_stream(url, channel, shutdown_recv, BookTick::parse_stream_json).await
}

pub async fn stream_book_ticks_to_file(
    symbol: Symbol,
    base_data_dir: String,
    shutdown_recv: broadcast::Receiver<()>,
) -> Result<(), Error> {
    let (tx, mut rx) = unbounded_channel::<BookTick>();
    stream_book_ticks(symbol, tx, shutdown_recv).await?;

    let (mut ticks_file, mut filepath) = new_ticks_file(&base_data_dir, symbol).await;

    let mut started_new_file = Instant::now();

    let mut nrows = 0;
    while let Some(tick) = rx.recv().await {
        ticks_file.serialize(&tick).await.unwrap();
        nrows += 1;

        // Create a new file every 10 minutes
        if started_new_file.elapsed().as_secs() >= 60 * 10 {
            ticks_file.flush().await?;
            println!("Saved {} with {} rows", filepath.as_os_str().to_string_lossy(), nrows);
            (ticks_file, filepath) = new_ticks_file(&base_data_dir, symbol).await;
            started_new_file = Instant::now();
            nrows = 0;
        }
    }

    ticks_file.flush().await.unwrap();
    println!("Saved {} with {} rows", filepath.as_os_str().to_string_lossy(), nrows);

    Ok(())
}

async fn new_ticks_file(base_data_dir: &str, symbol: Symbol) -> (csv_async::AsyncSerializer<File>, PathBuf) {
    let dir = get_book_ticks_dir(base_data_dir, symbol).await;
    let filepath = dir.join(format!("{}_book_ticks.csv", now_millis()));
    let f = File::create(&filepath).await.unwrap();
    let writer = csv_async::AsyncSerializer::from_writer(f);
    (writer, filepath)
}

fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

async fn get_book_ticks_dir(data_dir: &str, symbol: Symbol) -> PathBuf {
    let book_dir = Path::new(&data_dir)
        .join("binance")
        .join("futures")
        .join("book_ticks")
        .join(symbol.to_string());

    if !book_dir.try_exists().is_err() {
        tokio::fs::create_dir_all(&book_dir).await.unwrap();
    }

    book_dir
}
