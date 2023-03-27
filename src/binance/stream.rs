use futures_util::{SinkExt, StreamExt};
use tokio::{
    select,
    sync::{broadcast, mpsc::UnboundedSender},
    task::JoinHandle,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use super::errors::Error;

pub(crate) async fn start_market_data_stream<
    T: std::fmt::Debug + std::marker::Send + 'static,
    F: for<'a> Fn(&'a [u8]) -> Result<T, Error> + std::marker::Send + 'static,
>(
    url: Url,
    channel: UnboundedSender<T>,
    mut shutdown_recv: broadcast::Receiver<()>,
    deserialize: F,
) -> Result<JoinHandle<Result<(), Error>>, Error> {
    let (ws_stream, _) = connect_async(url).await?;

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let pong = Message::Pong("pong".as_bytes().to_vec());

    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
        loop {
            select! {
                _ = shutdown_recv.recv() => {
                    ws_sender.send(Message::Close(None)).await?;
                    return Ok(());
                },
                Some(msg) = ws_receiver.next() => {
                    let msg = msg.unwrap();
                    if msg.is_ping() {
                        ws_sender.send(pong.clone()).await.map_err(Error::StreamError)?;
                        continue;
                    }
                    if msg.is_pong() {
                        continue;
                    }
                    if msg.is_close() {
                        return Ok(());
                    }
                    let data = msg.into_data();
                    let item: T = deserialize(&data)?;
                    channel.send(item).expect("receive side of channel closed");
                }
                else => {
                    return Ok(());
                },
            }
        }
    });

    Ok(handle)
}
