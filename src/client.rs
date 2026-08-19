use std::sync::Arc;

use crate::error::Result;
use crate::http::{
    Client, ClientBuilder, Jar,
    header::{HeaderMap, HeaderValue},
};

// ---------------------------------------------------------------------------
// Default Configuration Constants
// ---------------------------------------------------------------------------

const DEFAULT_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";

const DEFAULT_ACCEPT: &str =
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8";

// ---------------------------------------------------------------------------
// StreamClient
// ---------------------------------------------------------------------------

/// An HTTP client configured for stream-extraction requests.
///
/// All downloading and metadata functions in this crate accept a
/// `&StreamClient` rather than being methods on it. This keeps the client
/// as a plain transport layer and lets you pass it freely across tasks.
#[derive(Clone)]
pub struct StreamClient {
    pub(crate) inner: Client,
    pub(crate) cookie_jar: Arc<Jar>,
}

impl StreamClient {
    /// Build a client with all standard stream-extractor defaults.
    pub fn new() -> Result<Self> {
        Self::builder().build()
    }

    /// Start a [`StreamClientBuilder`] for fine-grained HTTP configuration.
    ///
    /// ```rust
    /// use stream_extractor::StreamClient;
    ///
    /// let client = StreamClient::builder()
    ///     .configure(|b| b.timeout(std::time::Duration::from_secs(30)))
    ///     .build()?;
    /// ```
    pub fn builder() -> StreamClientBuilder {
        StreamClientBuilder::new()
    }

    /// Construct a `StreamClient` from an existing, user-provided HTTP client.
    ///
    /// This is highly useful if you want to reuse an existing application-wide
    /// `reqwest` or `wreq` client, complete with custom proxies, connection pools,
    /// interceptors, or TLS configurations.
    ///
    /// # Note
    /// You must provide the `Arc<Jar>` that is wired into the provided client
    /// so that `stream_extractor` can independently read and manipulate cookies.
    pub fn from_inner(client: Client, cookie_jar: Arc<Jar>) -> Self {
        Self {
            inner: client,
            cookie_jar,
        }
    }

    /// Access the underlying HTTP client for advanced use-cases.
    pub fn http_client(&self) -> &Client {
        &self.inner
    }

    /// Access the thread-safe cookie jar used by this client.
    pub fn cookie_jar(&self) -> &Arc<Jar> {
        &self.cookie_jar
    }
}

impl Default for StreamClient {
    fn default() -> Self {
        Self::new().expect("failed to build default StreamClient")
    }
}

// Manually implement Debug because the underlying Client doesn't implement it perfectly.
impl std::fmt::Debug for StreamClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamClient")
            .field("cookie_jar", &self.cookie_jar)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// A builder for configuring a [`StreamClient`].
///
/// Obtain one with [`StreamClient::builder()`].
pub struct StreamClientBuilder {
    inner_builder: ClientBuilder,
    cookie_jar: Arc<Jar>,
}

impl StreamClientBuilder {
    /// Creates a new builder populated with the default spoofed browser headers
    /// and a fresh cookie jar.
    pub fn new() -> Self {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static(DEFAULT_USER_AGENT));
        headers.insert("accept", HeaderValue::from_static(DEFAULT_ACCEPT));
        headers.insert("accept-language", HeaderValue::from_static("en;q=0.8"));
        headers.insert("upgrade-insecure-requests", HeaderValue::from_static("1"));
        headers.insert("cache-control", HeaderValue::from_static("max-age=0"));

        let jar = Arc::new(Jar::default());

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

        Self {
            inner_builder: builder,
            cookie_jar: jar,
        }
    }

    /// Apply arbitrary configuration to the underlying [`ClientBuilder`].
    ///
    /// This gives you full access to the underlying HTTP crate's options
    /// without losing the default headers provided by `StreamClient`.
    ///
    /// ```rust
    /// use stream_extractor::StreamClient;
    ///
    /// let client = StreamClient::builder()
    ///     .configure(|b| b.timeout(std::time::Duration::from_secs(60)))
    ///     .build()?;
    /// ```
    #[must_use]
    pub fn configure<F>(mut self, f: F) -> Self
    where
        F: FnOnce(ClientBuilder) -> ClientBuilder,
    {
        self.inner_builder = f(self.inner_builder);
        self
    }

    /// Consumes the builder and returns a functional [`StreamClient`].
    pub fn build(self) -> Result<StreamClient> {
        Ok(StreamClient {
            inner: self.inner_builder.build()?,
            cookie_jar: self.cookie_jar,
        })
    }
}

impl Default for StreamClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
