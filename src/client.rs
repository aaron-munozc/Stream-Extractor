use reqwest::{
    Client,
    cookie::Jar,
    header::{HeaderMap, HeaderValue},
};
use std::sync::Arc;
use url::Url;

use crate::error::Result;

#[derive(Clone, Debug)]
pub struct StreamClient {
    pub inner: Client,
    pub cookie_jar: Arc<Jar>,
}

impl Default for StreamClient {
    fn default() -> Self {
        Self::new().expect("Failed to initialize default StreamClient")
    }
}

impl StreamClient {
    pub fn new() -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"));
        headers.insert("accept", HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"));
        headers.insert("accept-language", HeaderValue::from_static("en;q=0.8"));
        headers.insert("upgrade-insecure-requests", HeaderValue::from_static("1"));
        headers.insert("cache-control", HeaderValue::from_static("max-age=0"));

        let jar = Arc::new(Jar::default());
        let client = Client::builder()
            .default_headers(headers)
            .cookie_provider(jar.clone())
            .http2_adaptive_window(true)
            .build()?;

        Ok(Self {
            inner: client,
            cookie_jar: jar,
        })
    }

    pub fn load_cookies(&self, raw_cookie: &str) {
        if let Ok(url) = Url::parse("https://kick.com") {
            self.cookie_jar.add_cookie_str(raw_cookie, &url);
        }
    }
}
