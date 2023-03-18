# Crypto Market Order Book

This repo contains example code for maintaing a local order book of crypto exchanges.
Currently only Binance is supported.

The order book is implemented in Rust and uses a B-tree datastructure and hand-coded
parsers for the data received from the exchange API.

The order book is capable of updating at a rate of about 2 million updates per second,
including parsing time on a laptop.

Note: rather than storing price levels as strings or decimals, it stores prices and
quantities as scaled `u64`s for performance reasons. See `src/binance_api.rs` for the
scaling factors. For example, for the `BTCUSDT` trading pair, the order book stores the
prices in units of cents (scaling factor 2) and quantities in satoshis (scaling factor 8).
