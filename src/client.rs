use reqwest::{
    Client, ClientBuilder,
    cookie::Jar,
    header::{HeaderMap, HeaderValue},
};
use std::sync::Arc;
use url::Url;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct StreamClient {
    pub(crate) inner: Client,
    pub cookie_jar: Arc<Jar>,
}

impl Default for StreamClient {
    fn default() -> Self {
        Self::new().unwrap_or_else(|_| {
            let jar = Arc::new(Jar::default());
            Self {
                inner: Client::builder()
                    .cookie_provider(Arc::clone(&jar))
                    .build()
                    .unwrap_or_default(),
                cookie_jar: jar,
            }
        })
    }
}

impl StreamClient {
    pub fn new() -> Result<Self> {
        Self::builder().build()
    }

    pub fn builder() -> StreamClientBuilder {
        StreamClientBuilder::new()
    }

    pub fn reqwest_client(&self) -> &Client {
        &self.inner
    }

}

pub struct StreamClientBuilder {
    inner_builder: ClientBuilder,
    cookie_jar: Arc<Jar>,
}

impl StreamClientBuilder {
    fn new() -> Self {
        // Establish the baseline configuration BEFORE exposing it to the user.
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"));
        headers.insert("accept", HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"));
        headers.insert("accept-language", HeaderValue::from_static("en;q=0.8"));
        headers.insert("upgrade-insecure-requests", HeaderValue::from_static("1"));
        headers.insert("cache-control", HeaderValue::from_static("max-age=0"));

        let jar = Arc::new(Jar::default());

        let builder = Client::builder()
            .default_headers(headers)
            .cookie_provider(Arc::clone(&jar))
            .http2_adaptive_window(true);

        Self {
            inner_builder: builder,
            cookie_jar: jar,
        }
    }

    pub fn configure<F>(mut self, f: F) -> Self
    where
        F: FnOnce(ClientBuilder) -> ClientBuilder,
    {
        // The user can now completely overwrite the default headers if they choose to.
        self.inner_builder = f(self.inner_builder);
        self
    }

    pub fn build(self) -> Result<StreamClient> {
        // Build safely; no mandatory overrides happen here.
        let client = self.inner_builder.build()?;
        Ok(StreamClient { inner: client, cookie_jar: self.cookie_jar })
    }
}
