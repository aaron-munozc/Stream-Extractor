use reqwest::header::{ACCEPT, REFERER, USER_AGENT};
use url::Url;

use crate::client::StreamClient;
use crate::error::Result;
use crate::types::{
    ChannelField, KickChannelResponse, KickVideoResponse, Platform, StreamMetadata, StreamStatus,
};

#[derive(Debug, PartialEq, Eq)]
pub enum KickStream {
    Live(String),
    Vod(String),
    Invalid,
}

pub fn get_kick_stream_info(url: &str) -> KickStream {
    let parsed = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return KickStream::Invalid,
    };

    match parsed.host_str() {
        Some(h) if h == "kick.com" || h == "www.kick.com" => {}
        _ => return KickStream::Invalid,
    };

    let segments: Vec<&str> = parsed
        .path_segments()
        .map(|s| s.filter(|seg| !seg.is_empty()).collect())
        .unwrap_or_default();

    match segments.as_slice() {
        [_, prefix, uuid, ..] if *prefix == "videos" || *prefix == "video" => {
            KickStream::Vod(uuid.to_string())
        }
        [prefix, uuid, ..] if *prefix == "videos" || *prefix == "video" => {
            KickStream::Vod(uuid.to_string())
        }
        [slug] => KickStream::Live(slug.to_string()),
        _ => KickStream::Invalid,
    }
}

pub async fn fetch_kick_video_api(
    client: &StreamClient,
    uuid: &str,
) -> Result<Option<StreamMetadata>> {
    let api_url = format!("https://kick.com/api/v1/video/{}", uuid);

    let resp = client
        .inner
        .get(&api_url)
        .header(ACCEPT, "application/json")
        .header(REFERER, "https://kick.com/")
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let parsed: KickVideoResponse = resp.json().await?; // Simpler than reading text then parsing

    let mut meta = StreamMetadata::default();
    meta.platform = Platform::Kick;
    meta.stream_status = Some(StreamStatus::Vod);
    meta.vod_uuid = Some(uuid.to_string());
    meta.views = parsed.views;
    meta.source = parsed.source.clone();
    meta.playback_url = parsed.playback_url;

    if let Some(ls) = parsed.livestream {
        meta.title = ls.session_title;
        meta.start_time = ls.start_time;
        meta.duration = ls.duration;
        meta.thumbnail_url = ls.thumbnail;

        if let Some(ch_field) = ls.channel {
            match ch_field {
                ChannelField::Obj(ch) => {
                    meta.username = ch.user.and_then(|u| u.username).or(ch.slug);
                    meta.followers = ch.followers_count;
                    meta.chat_id = ch.chatroom.and_then(|c| c.id).or(ch.id);

                    if meta.playback_url.is_none() {
                        meta.playback_url = ch.playback_url;
                    }
                }
                ChannelField::Id(id) => {
                    meta.chat_id = Some(id);
                }
            }
        }
    }

    if meta.playback_url.is_none() {
        meta.playback_url = meta.source.clone();
    }

    Ok(Some(meta))
}

pub async fn fetch_kick_channel_api(
    client: &StreamClient,
    slug: &str,
) -> Result<Option<StreamMetadata>> {
    let api_url = format!("https://kick.com/api/v1/channels/{}", slug);

    let resp = client
        .inner
        .get(&api_url)
        .header(ACCEPT, "application/json")
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let parsed: KickChannelResponse = resp.json().await?;

    let mut meta = StreamMetadata::default();
    meta.platform = Platform::Kick;
    meta.username = parsed
        .user
        .as_ref()
        .and_then(|u| u.username.clone())
        .or_else(|| Some(slug.to_string()));
    meta.followers = parsed.followers_count;
    meta.playback_url = parsed.playback_url;
    meta.chat_id = parsed.chatroom.and_then(|c| c.id).or(parsed.id);

    if let Some(ls) = parsed.livestream {
        meta.stream_status = Some(StreamStatus::Live);
        meta.title = ls.session_title;
        meta.start_time = ls.start_time;
        meta.viewer_count = ls.viewer_count;
        meta.thumbnail_url = ls.thumbnail;
    } else {
        meta.stream_status = Some(StreamStatus::Offline);
    }

    Ok(Some(meta))
}
