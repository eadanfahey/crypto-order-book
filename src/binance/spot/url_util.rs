use url::Url;

const BASE_API_ENDPOINT: &'static str = "https://api.binance.com/api/v3";
const BASE_STREAM_ENDPOINT: &'static str = "wss://stream.binance.com:9443/ws";

pub fn create_api_url(endpoint: &'static str) -> Url {
    return Url::parse(&(BASE_API_ENDPOINT.to_owned() + endpoint)).unwrap();
}

pub fn create_stream_url(endpoint: &str) -> Url {
    return Url::parse(&(BASE_STREAM_ENDPOINT.to_owned() + endpoint)).unwrap();
}
