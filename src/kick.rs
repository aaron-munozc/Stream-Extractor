use reqwest::StatusCode;
use reqwest::header::{ACCEPT, REFERER};
use url::Url;

use crate::client::StreamClient;
use crate::error::Result;
use crate::types::{
    ChannelField, KickChannelResponse, KickClipResponse, KickVideoResponse, Platform,
    StreamMetadata, StreamStatus,
};

pub(crate) enum KickStream {
    Vod(String),
    Clip(String),
    Live(String),
    Invalid,
}

pub(crate) fn get_kick_stream_info(url: &str) -> KickStream {
    let parsed = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return KickStream::Invalid,
    };

    if !matches!(parsed.host_str(), Some("kick.com") | Some("www.kick.com")) {
        return KickStream::Invalid;
    }

    let segments: Vec<&str> = parsed
        .path_segments()
        .map(|s| s.filter(|seg| !seg.is_empty()).collect())
        .unwrap_or_default();

    match segments.as_slice() {
        // Matches: kick.com/<username>/video/<uuid> or kick.com/<username>/videos/<uuid>
        [_, "video" | "videos", uuid, ..] => KickStream::Vod(uuid.to_string()),

        // Matches: kick.com/<username>/clips/<clip_id>
        [_, "clips", clip_id, ..] => KickStream::Clip(clip_id.to_string()),

        // Matches: kick.com/<username> (Live channel feed)
        [slug] => KickStream::Live(slug.to_string()),

        _ => KickStream::Invalid,
    }
}
pub(crate) async fn fetch_kick_video_api(
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

    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    let resp = resp.error_for_status()?;

    let parsed: KickVideoResponse = resp.json().await?;

    let mut meta = StreamMetadata {
        platform: Platform::Kick,
        stream_status: Some(StreamStatus::Vod),
        vod_uuid: Some(uuid.to_string()),
        views: parsed.views,
        source: parsed.source.clone(),
        ..Default::default()
    };

    let mut channel_live_fallback_url: Option<String> = None;

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

                    channel_live_fallback_url = ch.playback_url;
                }
                ChannelField::Id(id) => {
                    meta.chat_id = Some(id);
                }
            }
        }
    }

    meta.playback_url = parsed
        .playback_url
        .or(parsed.source)
        .or(channel_live_fallback_url);

    if meta.source.is_none() {
        meta.source = meta.playback_url.clone();
    }

    Ok(Some(meta))
}

pub(crate) async fn fetch_kick_clip_api(
    client: &StreamClient,
    clip_id: &str,
) -> Result<Option<StreamMetadata>> {
    let api_url = format!("https://kick.com/api/v2/clips/{}", clip_id);

    let resp = client
        .inner
        .get(&api_url)
        .header(ACCEPT, "application/json")
        .header(REFERER, "https://kick.com/")
        .send()
        .await?;

    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    let resp = resp.error_for_status()?;

    let parsed: KickClipResponse = resp.json().await?;

    let clip = match parsed.clip {
        Some(data) => data,
        None => return Ok(None),
    };

    let meta = StreamMetadata {
        platform: Platform::Kick,
        stream_status: Some(StreamStatus::Clip),
        vod_uuid: Some(clip_id.to_string()),
        title: clip.title,
        thumbnail_url: clip.thumbnail_url,
        views: clip.views,
        start_time: clip.created_at,
        duration: clip.duration.map(|sec| (sec * 1000.0) as i64),
        source: clip.video_url.clone(),
        playback_url: clip.video_url,
        username: clip.channel.as_ref().and_then(|c| c.username.clone()),
        chat_id: clip.channel.and_then(|c| c.id),
        ..Default::default()
    };

    Ok(Some(meta))
}

pub(crate) async fn fetch_kick_channel_api(
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

    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    let resp = resp.error_for_status()?;

    let parsed: KickChannelResponse = resp.json().await?;

    let (status, title, start, viewers, thumb) = if let Some(ls) = parsed.livestream {
        (
            StreamStatus::Live,
            ls.session_title,
            ls.start_time,
            ls.viewer_count,
            ls.thumbnail,
        )
    } else {
        (StreamStatus::Offline, None, None, None, None)
    };

    let meta = StreamMetadata {
        platform: Platform::Kick,
        username: parsed
            .user
            .as_ref()
            .and_then(|u| u.username.clone())
            .or_else(|| Some(slug.to_string())),
        followers: parsed.followers_count,
        playback_url: parsed.playback_url,
        chat_id: parsed.chatroom.and_then(|c| c.id).or(parsed.id),
        stream_status: Some(status),
        title,
        start_time: start,
        viewer_count: viewers,
        thumbnail_url: thumb,
        ..Default::default()
    };

    Ok(Some(meta))
}
