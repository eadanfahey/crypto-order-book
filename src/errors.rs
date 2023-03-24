#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("API request error")]
    ApiRequestError(#[from] reqwest::Error),

    #[error("Unexpected status code: {0} for endpoint {1}")]
    ApiBadStatus(u32, String),

    #[error("error parsing: {0}")]
    ParseError(String),

    #[error("stream error")]
    StreamError(#[from] tokio_tungstenite::tungstenite::Error),
}
