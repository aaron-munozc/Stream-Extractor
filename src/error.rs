#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("URL parsing error: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Ffmpeg processing error: {0}")]
    Ffmpeg(String),
    #[error("Playlist format error: {0}")]
    PlaylistParse(String),
    #[error("The quality index {0} does not exist in this manifest")]
    InvalidQualityIndex(usize),
    #[error("Requested resource not found")]
    NotFound,
    #[error("Invalid URL provided: {0}")]
    InvalidUrl(String),
    #[error("Operation cancelled by user: {0}")]
    Cancelled(String),
    #[error("Time parsing error: {0}")]
    TimeParse(String),
    #[error("API Rate limited after retries")]
    RateLimited,
    #[error("Missing chat or video ID")]
    MissingId,
    #[error("Http error: {0}")]
    Http(String),
}

pub type Result<T> = std::result::Result<T, Error>;
