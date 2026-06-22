use crate::error::Result;
use crate::http::{Client, ClientBuilder, Jar, header::{HeaderMap, HeaderValue}};
use std::sync::Arc;

#[derive(Clone)]
pub struct StreamClient {
    pub(crate) inner: Client,
    pub cookie_jar: Arc<Jar>,
}

// Manually implement Debug because wreq::Client does not implement it
impl std::fmt::Debug for StreamClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamClient")
         .field("cookie_jar", &self.cookie_jar)
         .finish_non_exhaustive()
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
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"));
        headers.insert("accept", HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"));
        headers.insert("accept-language", HeaderValue::from_static("en;q=0.8"));
        headers.insert("upgrade-insecure-requests", HeaderValue::from_static("1"));
        headers.insert("cache-control", HeaderValue::from_static("max-age=0"));

        let jar = Arc::new(Jar::default());

        // --- Backend Specific Initialization ---
        #[cfg(feature = "reqwest-backend")]
        let builder = Client::builder()
            .default_headers(headers)
            .cookie_provider(Arc::clone(&jar))
            .http2_adaptive_window(true);

        #[cfg(feature = "wreq-backend")]
        let builder = Client::builder()
            .emulation(wreq_util::Emulation::Chrome126)
            .default_headers(headers)
            .cookie_provider(Arc::clone(&jar));
        // ---------------------------------------

        Self {
            inner_builder: builder,
            cookie_jar: jar,
        }
    }

    pub fn configure<F>(mut self, f: F) -> Self
    where
        F: FnOnce(ClientBuilder) -> ClientBuilder,
    {
        self.inner_builder = f(self.inner_builder);
        self
    }

    pub fn build(self) -> Result<StreamClient> {
        let client = self.inner_builder.build()?;
        Ok(StreamClient {
            inner: client,
            cookie_jar: self.cookie_jar,
        })
    }
}