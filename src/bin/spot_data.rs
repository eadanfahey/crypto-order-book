use tokio::{signal, select};
use tokio::sync::broadcast;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::JoinHandle;

use exchange_data::binance;
use exchange_data::binance::spot::Symbol;

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
    let mut handles: Vec<JoinHandle<()>> = vec![];
    
    let (err_sender, mut err_receiver) = unbounded_channel::<Result<(), binance::Error>>();
    let err_senders = (0..symbols.len()).map(|_| err_sender.clone());
    

    for (symbol, errc) in symbols.into_iter().zip(err_senders) {
        let rx1 = shutdown_send.subscribe();
        let rx2 = shutdown_send.subscribe();
        let errc2 = errc.clone();
        let h1 = tokio::spawn(async move {
            let data_dir = std::env::var("DATA_DIR").unwrap();
            let res = binance::spot::stream_trades_to_file(symbol, data_dir.clone(), rx1).await;
            errc.send(res).unwrap();
        });
        let h2 = tokio::spawn(async move {
            let data_dir = std::env::var("DATA_DIR").unwrap();
            let mut order_book = binance::spot::SpotOrderBook::new(1000);
            let res = order_book.stream_to_file(symbol, &data_dir, rx2).await;
            errc2.send(res).unwrap();
        });
        handles.push(h1);
        handles.push(h2);
    }

    loop {
        select! {
            s = signal::ctrl_c() => {
                match s {
                    Ok(()) => {
                        shutdown_send.send(()).unwrap();
                        for handle in handles.into_iter() {
                            handle.await.unwrap();
                        }
                        return;
                    }
                    Err(_) => {
                        panic!("Failed to listen for ctrl-c signal");
                    }
                };
            }
            Some(res) = err_receiver.recv() => {
                if let Err(e) = res {
                    shutdown_send.send(()).unwrap();
                    for handle in handles.into_iter() {
                        handle.await.unwrap();
                    }
                    panic!("{}", e);
                }
            }
        }
    }
}
