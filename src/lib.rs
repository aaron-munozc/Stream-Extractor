mod chat;
pub mod client;
mod downloader;
pub mod error;
mod kick;
mod twitch;
pub mod types;
use log::{debug, info, warn};

pub use crate::types::ChatOptions;
pub use client::StreamClient;
pub use error::{Error, Result};
pub use types::{
    DownloadOptions, Platform, ProgressPayload, StreamMetadata, StreamQuality, StreamStatus,
};

pub struct Stream {
    pub metadata: StreamMetadata,
    client: StreamClient,
}

impl Stream {
    pub fn new(metadata: StreamMetadata, client: &StreamClient) -> Self {
        Self {
            metadata,
            client: client.clone(),
        }
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

    pub async fn download_video(&self, options: DownloadOptions) -> Result<()> {
        info!(
            "Beginning resource acquisition pipeline on platform: {}",
            self.metadata.platform
        );
        downloader::download_vod_internal(&self.client, &self.metadata, options).await
    }

    pub async fn download_chat(&self, options: ChatOptions) -> Result<()> {
        log::info!(
            "Beginning chat capture timeline on platform: {}",
            self.metadata.platform
        );
        chat::download_chat_internal(&self.client, &self.metadata, options).await
    }
}

pub async fn fetch_stream(client: &StreamClient, url: &str) -> Result<Stream> {
    info!("Resolving engine metadata rules for target: {}", url);

    match twitch::get_twitch_stream_info(url) {
        twitch::TwitchStream::Vod(id) => {
            debug!("Discovered active Twitch VOD footprint. Sub-ID: {:?}", id);
            if let Some(meta) = twitch::fetch_twitch_metadata(client, &id).await? {
                return Ok(Stream {
                    metadata: meta,
                    client: client.clone(),
                });
            }
            return Err(Error::NotFound);
        }
        twitch::TwitchStream::Clip(id) => {
            debug!("Discovered active Twitch Clip footprint. Clip-ID: {:?}", id);
            if let Some(meta) = twitch::fetch_twitch_clip_metadata(client, &id).await? {
                return Ok(Stream {
                    metadata: meta,
                    client: client.clone(),
                });
            }
            return Err(Error::NotFound);
        }
        twitch::TwitchStream::Invalid => {}
    }

    let meta_opt = match kick::get_kick_stream_info(url) {
        kick::KickStream::Vod(uuid) => {
            info!("Discovered Kick VOD signature. Manifest key: {}", uuid);
            kick::fetch_kick_video_api(client, &uuid).await?
        }
        kick::KickStream::Live(slug) => {
            info!(
                "Discovered Kick Live Channel footprint. Target profile: {}",
                slug
            );
            kick::fetch_kick_channel_api(client, &slug).await?
        }
        kick::KickStream::Clip(clip_id) => {
            info!("Discovered Kick Clip footprint. Clip ID: {}", clip_id);
            kick::fetch_kick_clip_api(client, &clip_id).await?
        }
        kick::KickStream::Invalid => {
            warn!("Target reference structure is un-routable: {}", url);
            return Err(Error::InvalidUrl(url.to_string()));
        }
    };

    match meta_opt {
        Some(meta) => Ok(Stream {
            metadata: meta,
            client: client.clone(),
        }),
        None => Err(Error::NotFound),
    }
}
