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
pub use error::Error;
pub use client::StreamClient;

// --- Public type re-exports -------------------------------------------------

#[cfg(feature = "vod")]
pub use types::{
    QualityPreference, StreamQuality, StreamResolution, VideoFormat, VodDownloadOptions,
};

pub use types::{
    Badge, ChatDownloadOptions, ClipInfo, Identity, KickOptions, LiveInfo, MessageSaved, Platform,
    PlatformChatOptions, ProgressCallback, ProgressPayload, Sender, TwitchOptions, VodInfo,
};

#[cfg(all(feature = "reqwest-backend", feature = "wreq-backend"))]
compile_error!("Features `reqwest-backend` and `wreq-backend` are mutually exclusive.");

pub(crate) mod http {
    #[cfg(feature = "reqwest-backend")]
    pub use reqwest::{Client, ClientBuilder, Error, StatusCode, header, cookie::Jar};

    #[cfg(feature = "wreq-backend")]
    pub use wreq::{Client, ClientBuilder, Error, StatusCode, header, cookie::Jar};
}

// ---------------------------------------------------------------------------
// Stream — the primary public type returned by `fetch_stream`
// ---------------------------------------------------------------------------

/// A resolved stream, typed by what kind of content it represents.
///
/// Each variant carries the focused info struct for that content type.
/// Use pattern-matching or the helper methods to access the inner data.
///
/// ```rust
/// match stream {
///     Stream::Vod(vod)   => println!("VOD: {:?}", vod.title),
///     Stream::Clip(clip) => println!("Clip: {:?}", clip.title),
///     Stream::Live(live) => println!("Live: {:?}", live.username),
/// }
/// ```
pub enum Stream {
    Vod(VodStream),
    Clip(ClipStream),
    Live(LiveStream),
}

impl Stream {
    /// Returns the platform this stream belongs to.
    pub fn platform(&self) -> &Platform {
        match self {
            Stream::Vod(v) => &v.info.platform,
            Stream::Clip(c) => &c.info.platform,
            Stream::Live(l) => &l.info.platform,
        }
    }

    /// Borrow the inner `VodInfo`, if this is a VOD.
    pub fn as_vod(&self) -> Option<&VodInfo> {
        if let Stream::Vod(v) = self {
            Some(&v.info)
        } else {
            None
        }
    }

    /// Borrow the inner `ClipInfo`, if this is a clip.
    pub fn as_clip(&self) -> Option<&ClipInfo> {
        if let Stream::Clip(c) = self {
            Some(&c.info)
        } else {
            None
        }
    }

    /// Borrow the inner `LiveInfo`, if this is a live/channel stream.
    pub fn as_live(&self) -> Option<&LiveInfo> {
        if let Stream::Live(l) = self {
            Some(&l.info)
        } else {
            None
        }
    }

    /// Consume into the inner `VodInfo`.
    pub fn into_vod(self) -> Option<VodInfo> {
        if let Stream::Vod(v) = self {
            Some(v.info)
        } else {
            None
        }
    }

    /// Consume into the inner `ClipInfo`.
    pub fn into_clip(self) -> Option<ClipInfo> {
        if let Stream::Clip(c) = self {
            Some(c.info)
        } else {
            None
        }
    }

    /// Consume into the inner `LiveInfo`.
    pub fn into_live(self) -> Option<LiveInfo> {
        if let Stream::Live(l) = self {
            Some(l.info)
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// VodStream
// ---------------------------------------------------------------------------

pub struct VodStream {
    pub info: VodInfo,
    client: StreamClient,
}

impl VodStream {
    pub fn new(info: VodInfo, client: &StreamClient) -> Self {
        Self {
            info,
            client: client.clone(),
        }
    }

    /// Available quality variants for this VOD.
    #[cfg(feature = "vod")]
    pub async fn get_qualities(&self) -> Result<Vec<StreamQuality>> {
        downloader::get_vod_qualities(&self.client, &self.info).await
    }

    /// Download the video track.
    #[cfg(feature = "vod")]
    pub async fn download_video(&self, options: VodDownloadOptions) -> Result<std::path::PathBuf> {
        info!(
            "Starting VOD download on platform: {}",
            self.info.platform
        );
        downloader::download_vod_video(&self.client, &self.info, options).await
    }

    /// Download the chat log.
    pub async fn download_chat(&self, options: ChatDownloadOptions) -> Result<std::path::PathBuf> {
        info!(
            "Starting VOD chat download on platform: {}",
            self.info.platform
        );
        chat::download_vod_chat(&self.client, &self.info, options).await
    }

    /// Consume this wrapper and return the inner `VodInfo`.
    pub fn into_info(self) -> VodInfo {
        self.info
    }
}

impl std::ops::Deref for VodStream {
    type Target = VodInfo;
    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

// ---------------------------------------------------------------------------
// ClipStream
// ---------------------------------------------------------------------------

pub struct ClipStream {
    pub info: ClipInfo,
    client: StreamClient,
}

impl ClipStream {
    pub fn new(info: ClipInfo, client: &StreamClient) -> Self {
        Self {
            info,
            client: client.clone(),
        }
    }

    /// Available quality variants for this clip.
    #[cfg(feature = "vod")]
    pub async fn get_qualities(&self) -> Result<Vec<StreamQuality>> {
        downloader::get_clip_qualities(&self.client, &self.info).await
    }

    /// Download the video track.
    #[cfg(feature = "vod")]
    pub async fn download_video(&self, options: VodDownloadOptions) -> Result<std::path::PathBuf> {
        info!(
            "Starting clip download on platform: {}",
            self.info.platform
        );
        downloader::download_clip_video(&self.client, &self.info, options).await
    }

    /// Download the chat log for the clip's time window.
    pub async fn download_chat(&self, options: ChatDownloadOptions) -> Result<std::path::PathBuf> {
        info!(
            "Starting clip chat download on platform: {}",
            self.info.platform
        );
        chat::download_clip_chat(&self.client, &self.info, options).await
    }

    /// Consume this wrapper and return the inner `ClipInfo`.
    pub fn into_info(self) -> ClipInfo {
        self.info
    }
}

impl std::ops::Deref for ClipStream {
    type Target = ClipInfo;
    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

// ---------------------------------------------------------------------------
// LiveStream
// ---------------------------------------------------------------------------

/// A live (or recently-offline) channel.
///
/// Live streams do not support chat or video downloading through this library
/// — they expose channel metadata only. The `playback_url` in `LiveInfo` can
/// be passed to an external HLS player directly.
pub struct LiveStream {
    pub info: LiveInfo,
    #[allow(dead_code)]
    client: StreamClient,
}

impl LiveStream {
    pub fn new(info: LiveInfo, client: &StreamClient) -> Self {
        Self {
            info,
            client: client.clone(),
        }
    }

    /// Consume this wrapper and return the inner `LiveInfo`.
    pub fn into_info(self) -> LiveInfo {
        self.info
    }
}

impl std::ops::Deref for LiveStream {
    type Target = LiveInfo;
    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

// ---------------------------------------------------------------------------
// fetch_stream — primary public API
// ---------------------------------------------------------------------------

/// Fetch and identify a stream from a URL.
///
/// Returns a typed [`Stream`] variant (VOD, Clip, or Live) depending on the
/// URL structure and the platform's API response.
///
/// # Errors
/// - [`Error::InvalidUrl`] — the URL is not a recognised Twitch or Kick URL.
/// - [`Error::NotFound`] — the API returned no data for the given ID.
/// - Network / deserialisation errors propagated from the HTTP client.
pub async fn fetch_stream(client: &StreamClient, url: &str) -> Result<Stream> {
    info!("Fetching stream metadata for: {}", url);

    let parsed_url = url::Url::parse(url).map_err(|_| Error::InvalidUrl(url.to_string()))?;
    let host = parsed_url.host_str().unwrap_or("");

    if host.contains("twitch.tv") || host.contains("clips.twitch.tv") {
        match twitch::get_twitch_stream_info(url) {
            twitch::TwitchStream::Vod(id) => {
                debug!("Twitch VOD identified, video ID: {}", id);
                match twitch::fetch_twitch_vod_metadata(client, &id).await? {
                    Some(info) => Ok(Stream::Vod(VodStream::new(info, client))),
                    None => Err(Error::NotFound),
                }
            }
            twitch::TwitchStream::Clip(id) => {
                debug!("Twitch Clip identified, clip ID: {}", id);
                match twitch::fetch_twitch_clip_metadata(client, &id).await? {
                    Some(info) => Ok(Stream::Clip(ClipStream::new(info, client))),
                    None => Err(Error::NotFound),
                }
            }
            twitch::TwitchStream::Live(channel) => {
                debug!("Twitch Live channel identified: {}", channel);
                match twitch::fetch_twitch_live_metadata(client, &channel).await? {
                    Some(info) => Ok(Stream::Live(LiveStream::new(info, client))),
                    None => Err(Error::NotFound),
                }
            }
            twitch::TwitchStream::Invalid => {
                warn!("Invalid Twitch URL structure: {}", url);
                Err(Error::InvalidUrl(url.to_string()))
            }
        }
    } else if host.contains("kick.com") {
        match kick::get_kick_stream_info(url) {
            kick::KickStream::Vod(uuid) => {
                info!("Kick VOD identified, video ID: {}", uuid);
                match kick::fetch_kick_video_api(client, &uuid).await? {
                    Some(info) => Ok(Stream::Vod(VodStream::new(info, client))),
                    None => Err(Error::NotFound),
                }
            }
            kick::KickStream::Live(slug) => {
                info!("Kick Live Channel identified, channel: {}", slug);
                match kick::fetch_kick_channel_api(client, &slug).await? {
                    Some(info) => Ok(Stream::Live(LiveStream::new(info, client))),
                    None => Err(Error::NotFound),
                }
            }
            kick::KickStream::Clip(clip_id) => {
                info!("Kick Clip identified, clip ID: {}", clip_id);
                match kick::fetch_kick_clip_api(client, &clip_id).await? {
                    Some(info) => Ok(Stream::Clip(ClipStream::new(info, client))),
                    None => Err(Error::NotFound),
                }
            }
            kick::KickStream::Invalid => {
                warn!("Invalid Kick URL structure: {}", url);
                Err(Error::InvalidUrl(url.to_string()))
            }
        }
    } else {
        warn!("Unrecognised URL format: {}", url);
        Err(Error::InvalidUrl(url.to_string()))
    }
}