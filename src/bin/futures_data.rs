use exchange_data::binance::{self, futures::Symbol};
use tokio::{
    signal,
    sync::broadcast,
    task::JoinHandle,
};

#[tokio::main]
async fn main() {
    let symbols = vec![Symbol::BtcUsdt];

    let (shutdown_send, _) = broadcast::channel::<()>(1);
    let mut handles: Vec<JoinHandle<Result<(), binance::Error>>> = vec![];

    for symbol in symbols.into_iter() {
        let rx1 = shutdown_send.subscribe();

        let h1 = tokio::spawn(async move { 
            let data_dir = std::env::var("DATA_DIR").unwrap();
            binance::futures::stream_book_ticks_to_file(symbol, data_dir, rx1).await
        });
        handles.push(h1);
    }

    match signal::ctrl_c().await {
        Ok(()) => {
            shutdown_send.send(()).unwrap();
            for handle in handles.into_iter() {
                handle.await.unwrap().unwrap();
            }
        }
        Err(_) => {
            panic!("Failed to listen for ctrl-c signal");
        }
    };
}
