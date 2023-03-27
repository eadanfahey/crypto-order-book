use std::path::{Path, PathBuf};
use std::time::Instant;

use super::{symbol::Symbol, url_util::create_stream_url};
use crate::binance::{
    errors::Error, parse_util::deserialize_decimal_e8, stream::start_market_data_stream,
};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::{
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedSender},
    },
    task::JoinHandle,
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct StreamTrade {
    #[serde(alias = "E")]
    pub event_time: u64,
    #[serde(alias = "t")]
    pub trade_id: u64,
    #[serde(alias = "p", deserialize_with = "deserialize_decimal_e8")]
    pub price: u64,
    #[serde(alias = "q", deserialize_with = "deserialize_decimal_e8")]
    pub quantity: u64,
    #[serde(alias = "b")]
    pub buyer_order_id: u64,
    #[serde(alias = "a")]
    pub seller_order_id: u64,
    #[serde(alias = "T")]
    pub trade_time: u64,
    #[serde(alias = "m")]
    pub is_market_maker: bool,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Trade {
    pub event_time: u64,
    pub trade_id: u64,
    pub price: u64,
    pub quantity: u64,
    pub buyer_order_id: u64,
    pub seller_order_id: u64,
    pub trade_time: u64,
    pub is_market_maker: bool,
}

impl StreamTrade {
    fn to_trade(self) -> Trade {
        Trade {
            event_time: self.event_time,
            trade_id: self.trade_id,
            price: self.price,
            quantity: self.quantity,
            buyer_order_id: self.buyer_order_id,
            seller_order_id: self.seller_order_id,
            trade_time: self.trade_time,
            is_market_maker: self.is_market_maker,
        }
    }
}

impl Trade {
    fn parse_stream_json(v: &[u8]) -> Result<Self, Error> {
        let trade = serde_json::from_slice(v).map(|s: StreamTrade| s.to_trade())?;
        Ok(trade)
    }
}

pub async fn stream_trades(
    symbol: Symbol,
    channel: UnboundedSender<Trade>,
    shutdown_recv: broadcast::Receiver<()>,
) -> Result<JoinHandle<Result<(), Error>>, Error> {
    let url = create_stream_url(&format!("/{}@trade", symbol.stream_string()));
    println!("{}", url);
    start_market_data_stream(url, channel, shutdown_recv, Trade::parse_stream_json).await
}

pub async fn stream_trades_to_file(
    symbol: Symbol,
    base_data_dir: String,
    shutdown_recv: broadcast::Receiver<()>,
) -> Result<(), Error> {
    let (tx, mut rx) = unbounded_channel::<Trade>();
    let handle = stream_trades(symbol, tx, shutdown_recv).await?;

    let (mut trades_file, mut filepath) = new_trades_file(&base_data_dir, symbol).await;

    let mut started_new_file = Instant::now();

    let mut nrows = 0;
    while let Some(trade) = rx.recv().await {
        trades_file.serialize(&trade).await.unwrap();

        // Create a new file every 10 minutes
        if started_new_file.elapsed().as_secs() >= 60 * 10 {
            trades_file.flush().await.unwrap();
            println!("Saved {} with {} rows", filepath.as_os_str().to_string_lossy(), nrows);
            (trades_file, filepath) = new_trades_file(&base_data_dir, symbol).await;
            started_new_file = Instant::now();
            nrows = 0;
        }
    }

    trades_file.flush().await.unwrap();
    println!("Saved {} with {} rows", filepath.as_os_str().to_string_lossy(), nrows);

    let res = handle.await.unwrap();
    res
}

async fn new_trades_file(base_data_dir: &str, symbol: Symbol) -> (csv_async::AsyncSerializer<File>, PathBuf) {
    let dir = get_trade_dir(base_data_dir, symbol).await;
    let filepath = dir.join(format!("{}_trades.csv", now_millis()));
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

async fn get_trade_dir(data_dir: &str, symbol: Symbol) -> PathBuf {
    let book_dir = Path::new(&data_dir)
        .join("binance")
        .join("spot")
        .join("trades")
        .join(symbol.to_string());

    if !book_dir.try_exists().is_err() {
        tokio::fs::create_dir_all(&book_dir).await.unwrap();
    }

    book_dir
}
