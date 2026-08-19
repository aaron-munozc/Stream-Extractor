mod chat;
mod client;
#[cfg(feature = "vod")]
mod downloader;
pub mod error;
mod kick;
mod twitch;
mod types;

use log::{debug, info, warn};

use crate::error::Result;

pub use client::{StreamClient, StreamClientBuilder};
pub use error::Error;

// ---------------------------------------------------------------------------
// Compile-time back-end guard
// ---------------------------------------------------------------------------

#[cfg(all(feature = "reqwest-backend", feature = "wreq-backend"))]
compile_error!(
    "Features `reqwest-backend` and `wreq-backend` are mutually exclusive. \
     Enable exactly one."
);

// ---------------------------------------------------------------------------
// HTTP back-end shim
// ---------------------------------------------------------------------------

pub(crate) mod http {
    #[cfg(feature = "reqwest-backend")]
    pub use reqwest::{Client, ClientBuilder, Error, StatusCode, cookie::Jar, header};

    #[cfg(feature = "wreq-backend")]
    pub use wreq::{Client, ClientBuilder, Error, StatusCode, cookie::Jar, header};
}

// ---------------------------------------------------------------------------
// Public type re-exports
// ---------------------------------------------------------------------------

// Always-public types
pub use types::{
    Badge, ChatDownloadOptions, ClipInfo, Identity, KickOptions, LiveInfo, MessageSaved, Platform,
    PlatformChatOptions, ProgressCallback, ProgressPayload, Sender, TwitchOptions, VodInfo,
};

// VOD-feature-gated types
#[cfg(feature = "vod")]
pub use types::{
    QualityPreference, StreamQuality, StreamResolution, VideoFormat, VodDownloadOptions,
};

// ---------------------------------------------------------------------------
// Stream — the primary resolved-stream type
// ---------------------------------------------------------------------------

/// A resolved stream, typed by what kind of content it represents.
///
/// Each variant carries the focused info struct for that content type.
/// Use pattern-matching or the helper methods (`as_vod`, `into_clip`, …) to
/// access the inner data.
///
/// ```rust
/// use stream_extractor::{Stream, StreamClient};
///
/// let client = StreamClient::new()?;
/// match stream_extractor::fetch_stream(&client, url).await? {
///     Stream::Vod(vod)   => println!("VOD: {}", vod.vod_id),
///     Stream::Clip(clip) => println!("Clip: {}", clip.clip_id),
///     Stream::Live(live) => println!("Live: {:?}", live.username),
///     _                  => {} // future variants
/// }
/// ```
///
/// Marked `#[non_exhaustive]` so adding new platforms is not a breaking change.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde-types", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-types", serde(tag = "kind", rename_all = "lowercase"))]
#[non_exhaustive]
pub enum Stream {
    Vod(VodInfo),
    Clip(ClipInfo),
    Live(LiveInfo),
}

impl Stream {
    /// The platform this stream belongs to.
    pub fn platform(&self) -> &Platform {
        match self {
            Stream::Vod(v) => &v.platform,
            Stream::Clip(c) => &c.platform,
            Stream::Live(l) => &l.platform,
        }
    }

    /// Borrow the inner [`VodInfo`], if this is a VOD.
    pub fn as_vod(&self) -> Option<&VodInfo> {
        if let Stream::Vod(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Borrow the inner [`ClipInfo`], if this is a clip.
    pub fn as_clip(&self) -> Option<&ClipInfo> {
        if let Stream::Clip(c) = self {
            Some(c)
        } else {
            None
        }
    }

    /// Borrow the inner [`LiveInfo`], if this is a live channel.
    pub fn as_live(&self) -> Option<&LiveInfo> {
        if let Stream::Live(l) = self {
            Some(l)
        } else {
            None
        }
    }

    /// Consume into the inner [`VodInfo`].
    pub fn into_vod(self) -> Option<VodInfo> {
        if let Stream::Vod(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Consume into the inner [`ClipInfo`].
    pub fn into_clip(self) -> Option<ClipInfo> {
        if let Stream::Clip(c) = self {
            Some(c)
        } else {
            None
        }
    }

    /// Consume into the inner [`LiveInfo`].
    pub fn into_live(self) -> Option<LiveInfo> {
        if let Stream::Live(l) = self {
            Some(l)
        } else {
            None
        }
    }

    /// Returns `true` if this is a [`Stream::Vod`].
    pub fn is_vod(&self) -> bool {
        matches!(self, Stream::Vod(_))
    }

    /// Returns `true` if this is a [`Stream::Clip`].
    pub fn is_clip(&self) -> bool {
        matches!(self, Stream::Clip(_))
    }

    /// Returns `true` if this is a [`Stream::Live`].
    pub fn is_live(&self) -> bool {
        matches!(self, Stream::Live(_))
    }
}

// ---------------------------------------------------------------------------
// fetch_stream — primary public API entry point
// ---------------------------------------------------------------------------

/// Fetch and identify a stream from a URL.
///
/// Returns a typed [`Stream`] variant (VOD, Clip, or Live) depending on the
/// URL structure and the platform's API response.
///
/// # Errors
/// - [`Error::InvalidUrl`] — the URL is not a recognised Twitch or Kick URL.
/// - [`Error::NotFound`] — the API returned no data for the given ID.
/// - Network or deserialisation errors propagated from the HTTP client.
pub async fn fetch_stream(client: &StreamClient, url: &str) -> Result<Stream> {
    info!("Fetching stream metadata for: {}", url);

    let parsed = url::Url::parse(url).map_err(|_| Error::InvalidUrl(url.to_string()))?;
    let host = parsed.host_str().unwrap_or("");

    if host.contains("twitch.tv") || host.contains("clips.twitch.tv") {
        dispatch_twitch(client, url).await
    } else if host.contains("kick.com") {
        dispatch_kick(client, url).await
    } else {
        warn!("Unrecognised URL format: {}", url);
        Err(Error::InvalidUrl(url.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Downloading helpers — free functions so callers aren't locked to methods
// ---------------------------------------------------------------------------

/// Download the video track for a VOD.
///
/// Requires the `vod` feature.
#[cfg(feature = "vod")]
pub async fn download_vod_video(
    client: &StreamClient,
    info: &VodInfo,
    options: types::VodDownloadOptions,
) -> Result<std::path::PathBuf> {
    info!("Starting VOD download on platform: {}", info.platform);
    downloader::download_vod_video(client, info, options).await
}

/// Download the video track for a clip.
///
/// Requires the `vod` feature.
#[cfg(feature = "vod")]
pub async fn download_clip_video(
    client: &StreamClient,
    info: &ClipInfo,
    options: types::VodDownloadOptions,
) -> Result<std::path::PathBuf> {
    info!("Starting clip download on platform: {}", info.platform);
    downloader::download_clip_video(client, info, options).await
}

/// Query the available quality variants for a VOD.
///
/// Requires the `vod` feature.
#[cfg(feature = "vod")]
pub async fn get_vod_qualities(
    client: &StreamClient,
    info: &VodInfo,
) -> Result<Vec<types::StreamQuality>> {
    downloader::get_vod_qualities(client, info).await
}

/// Query the available quality variants for a clip.
///
/// Requires the `vod` feature.
#[cfg(feature = "vod")]
pub async fn get_clip_qualities(
    client: &StreamClient,
    info: &ClipInfo,
) -> Result<Vec<types::StreamQuality>> {
    downloader::get_clip_qualities(client, info).await
}

/// Download the chat log for a VOD.
pub async fn download_vod_chat(
    client: &StreamClient,
    info: &VodInfo,
    options: types::ChatDownloadOptions,
) -> Result<std::path::PathBuf> {
    info!("Starting VOD chat download on platform: {}", info.platform);
    chat::download_vod_chat(client, info, options).await
}

/// Download the chat log for a clip's time window.
pub async fn download_clip_chat(
    client: &StreamClient,
    info: &ClipInfo,
    options: types::ChatDownloadOptions,
) -> Result<std::path::PathBuf> {
    info!("Starting clip chat download on platform: {}", info.platform);
    chat::download_clip_chat(client, info, options).await
}

// ---------------------------------------------------------------------------
// Private dispatch helpers
// ---------------------------------------------------------------------------

async fn dispatch_twitch(client: &StreamClient, url: &str) -> Result<Stream> {
    match twitch::get_twitch_stream_info(url) {
        twitch::TwitchStream::Vod(id) => {
            debug!("Twitch VOD identified, video ID: {}", id);
            twitch::fetch_twitch_vod_metadata(client, &id)
                .await?
                .map(Stream::Vod)
                .ok_or(Error::NotFound)
        }
        twitch::TwitchStream::Clip(id) => {
            debug!("Twitch Clip identified, clip ID: {}", id);
            twitch::fetch_twitch_clip_metadata(client, &id)
                .await?
                .map(Stream::Clip)
                .ok_or(Error::NotFound)
        }
        twitch::TwitchStream::Live(channel) => {
            debug!("Twitch Live channel identified: {}", channel);
            twitch::fetch_twitch_live_metadata(client, &channel)
                .await?
                .map(Stream::Live)
                .ok_or(Error::NotFound)
        }
        twitch::TwitchStream::Invalid => {
            warn!("Invalid Twitch URL structure: {}", url);
            Err(Error::InvalidUrl(url.to_string()))
        }
    }
}

async fn dispatch_kick(client: &StreamClient, url: &str) -> Result<Stream> {
    match kick::get_kick_stream_info(url) {
        kick::KickStream::Vod(uuid) => {
            info!("Kick VOD identified, video ID: {}", uuid);
            kick::fetch_kick_video_api(client, &uuid)
                .await?
                .map(Stream::Vod)
                .ok_or(Error::NotFound)
        }
        kick::KickStream::Clip(clip_id) => {
            info!("Kick Clip identified, clip ID: {}", clip_id);
            kick::fetch_kick_clip_api(client, &clip_id)
                .await?
                .map(Stream::Clip)
                .ok_or(Error::NotFound)
        }
        kick::KickStream::Live(slug) => {
            info!("Kick Live channel identified: {}", slug);
            kick::fetch_kick_channel_api(client, &slug)
                .await?
                .map(Stream::Live)
                .ok_or(Error::NotFound)
        }
        kick::KickStream::Invalid => {
            warn!("Invalid Kick URL structure: {}", url);
            Err(Error::InvalidUrl(url.to_string()))
        }
    }
}
