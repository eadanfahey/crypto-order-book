#[derive(Clone, Copy)]
pub enum Symbol {
    BtcUsdt,
}

impl Symbol {
    pub(crate) fn to_string(&self) -> &'static str {
        match self {
            Self::BtcUsdt => "BTC-USDT",
        }
    }

}
