#[derive(Clone, Copy)]
pub enum Symbol {
    BtcUsdt,
    EthUsdt,
    ArbUsdt,
    BtcBusd,
    UsdcUsdt,
    BusdUsdt,
    EthBusd,
    XrpUsdt,
    BtcTusd,
    EthBtc,
    LtcUsdt,
    SolUsdt,
    BnbUsdt,
}

impl Symbol {
    pub(crate) fn api_string(&self) -> &'static str {
        match self {
            Self::BtcUsdt => "BTCUSDT",
            Self::EthUsdt => "ETHUSDT",
            Self::ArbUsdt => "ARBUSDT",
            Self::BtcBusd => "BTCBUSD",
            Self::UsdcUsdt => "USDCUSDT",
            Self::BusdUsdt => "BUSDUSDT",
            Self::EthBusd => "ETHBUSD",
            Self::XrpUsdt => "XRPUSDT",
            Self::BtcTusd => "BTCTUSD",
            Self::EthBtc => "ETHBTC",
            Self::LtcUsdt => "LTCUSDT",
            Self::SolUsdt => "SOLUSDT",
            Self::BnbUsdt => "BNBUSDT",
        }
    }

    pub(crate) fn stream_string(&self) -> &'static str {
        match self {
            Self::BtcUsdt => "btcusdt",
            Self::EthUsdt => "ethusdt",
            Self::ArbUsdt => "arbusdt",
            Self::BtcBusd => "btcbusd",
            Self::UsdcUsdt => "usdcusdt",
            Self::BusdUsdt => "busdusdt",
            Self::EthBusd => "ethbusd",
            Self::XrpUsdt => "xrpusdt",
            Self::BtcTusd => "btctusd",
            Self::EthBtc => "ethbtc",
            Self::LtcUsdt => "ltcusdt",
            Self::SolUsdt => "solusdt",
            Self::BnbUsdt => "bnbusdt",
        }
    }

    pub(crate) fn to_string(&self) -> &'static str {
        match self {
            Self::BtcUsdt => "BTC-USDT",
            Self::EthUsdt => "ETH-USDT",
            Self::ArbUsdt => "ARB-USDT",
            Self::BtcBusd => "BTC-BUSD",
            Self::UsdcUsdt => "USDC-USDT",
            Self::BusdUsdt => "BUSD-USDT",
            Self::EthBusd => "ETH-BUSD",
            Self::XrpUsdt => "XRP-USDT",
            Self::BtcTusd => "BTC-TUSD",
            Self::EthBtc => "ETH-BTC",
            Self::LtcUsdt => "LTC-USDT",
            Self::SolUsdt => "SOL-USDT",
            Self::BnbUsdt => "BNB-USDT",
        }
    }

    pub fn id(&self) -> u32 {
        match self {
            Self::BtcUsdt => 1,
            Self::EthUsdt => 2,
            Self::ArbUsdt => 3,
            Self::BtcBusd => 4,
            Self::UsdcUsdt => 5,
            Self::BusdUsdt => 6,
            Self::EthBusd => 7,
            Self::XrpUsdt => 8,
            Self::BtcTusd => 9,
            Self::EthBtc => 10,
            Self::LtcUsdt => 11,
            Self::SolUsdt => 12,
            Self::BnbUsdt => 13,
        }
    }
}
