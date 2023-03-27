#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("stream error")]
    StreamError(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("JSON parsing error")]
    JSONParseError(#[from] serde_json::Error),

    #[error("Unexpected status code: {0} for endpoint {1}")]
    ApiBadStatus(u32, String),

    #[error("API request error")]
    ApiRequestError(#[from] reqwest::Error),

    #[error("error parsing: {0}")]
    ParseError(String),

    #[error("IO error")]
    IOError(#[from] std::io::Error),
}
