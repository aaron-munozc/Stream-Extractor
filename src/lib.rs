mod chat;
mod client;
mod downloader;
pub mod error;
mod kick;
mod twitch;
mod types;

use std::ops::Deref;
use log::{debug, info, warn};

use crate::error::{Result};
pub use error::Error;
pub use client::StreamClient;
pub use types::{
    Badge, ChatOptions, DownloadOptions, Identity, MessageSaved, Platform, ProgressCallback,
    ProgressPayload, QualityPreference, Sender, StreamMetadata, StreamQuality, StreamResolution,
    StreamStatus, VideoFormat,
};

#[cfg(all(feature = "reqwest-backend", feature = "wreq-backend"))]
compile_error!("Features `reqwest-backend` and `wreq-backend` are mutually exclusive.");

pub(crate) mod http {
    #[cfg(feature = "reqwest-backend")]
    pub use reqwest::{Client, ClientBuilder, Error, StatusCode, header, cookie::Jar};

    #[cfg(feature = "wreq-backend")]
    pub use wreq::{Client, ClientBuilder, Error, StatusCode, header, cookie::Jar};
}


pub struct Stream {
    metadata: StreamMetadata,
    client: StreamClient,
}

impl Stream {
    pub fn new(metadata: StreamMetadata, client: &StreamClient) -> Self {
        Self {
            metadata,
            client: client.clone(),
        }
    }

    pub fn into_inner(self) -> StreamMetadata {
        self.metadata
    }

    pub async fn get_qualities(&self) -> Result<Vec<StreamQuality>> {
        let url = self
            .metadata
            .playback_url
            .as_ref()
            .or(self.metadata.source.as_ref())
            .ok_or(Error::NotFound)?;
        downloader::get_qualities_internal(&self.client, url).await
    }

    pub async fn download_video(&self, options: DownloadOptions) -> Result<std::path::PathBuf> {
        info!(
            "Starting video download on platform: {}",
            self.metadata.platform
        );
        downloader::download_vod_internal(&self.client, &self.metadata, options).await
    }

    pub async fn download_chat(&self, options: ChatOptions) -> Result<std::path::PathBuf> {
        info!(
            "Starting chat download on platform: {}",
            self.metadata.platform
        );
        chat::download_chat_internal(&self.client, &self.metadata, options).await
    }
}

impl Deref for Stream {
    type Target = StreamMetadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

pub async fn fetch_stream(client: &StreamClient, url: &str) -> Result<Stream> {
    info!("Fetching stream metadata for: {}", url);

    let parsed_url = url::Url::parse(url).map_err(|_| Error::InvalidUrl(url.to_string()))?;
    let host = parsed_url.host_str().unwrap_or("");

    let meta_opt = if host.contains("twitch.tv") {
        match twitch::get_twitch_stream_info(url) {
            twitch::TwitchStream::Vod(id) => {
                debug!("Twitch VOD identified, video ID: {:?}", id);
                twitch::fetch_twitch_metadata(client, &id).await?
            }
            twitch::TwitchStream::Clip(id) => {
                debug!("Twitch Clip identified, clip ID: {:?}", id);
                twitch::fetch_twitch_clip_metadata(client, &id).await?
            }
            twitch::TwitchStream::Invalid => {
                warn!("Invalid Twitch URL structure: {}", url);
                return Err(Error::InvalidUrl(url.to_string()));
            }
        }
    } else if host.contains("kick.com") {
        match kick::get_kick_stream_info(url) {
            kick::KickStream::Vod(uuid) => {
                info!("Kick VOD identified, video ID: {}", uuid);
                kick::fetch_kick_video_api(client, &uuid).await?
            }
            kick::KickStream::Live(slug) => {
                info!(
                    "Kick Live Channel identified, channel: {}",
                    slug
                );
                kick::fetch_kick_channel_api(client, &slug).await?
            }
            kick::KickStream::Clip(clip_id) => {
                info!("Kick Clip identified, clip ID: {}", clip_id);
                kick::fetch_kick_clip_api(client, &clip_id).await?
            }
            kick::KickStream::Invalid => {
                warn!("Invalid Kick URL structure: {}", url);
                return Err(Error::InvalidUrl(url.to_string()));
            }
        }
    } else {
        warn!("Unrecognized URL format: {}", url);
        return Err(Error::InvalidUrl(url.to_string()));
    };

    match meta_opt {
        Some(meta) => Ok(Stream {
            metadata: meta,
            client: client.clone(),
        }),
        None => Err(Error::NotFound),
    }
}