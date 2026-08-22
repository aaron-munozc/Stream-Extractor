use crate::http::{
    StatusCode,
    header::{ACCEPT, REFERER},
};
use url::Url;

use crate::client::StreamClient;
use crate::error::Result;
use crate::types::{
    ChannelField, ClipInfo, KickChannelResponse, KickClipResponse, KickVideoResponse, LiveInfo,
    Platform, VodInfo, parse_datetime,
};

// ----------------- URL Parser -----------------

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
        // kick.com/<username>/video/<uuid> or kick.com/<username>/videos/<uuid>
        [_, "video" | "videos", uuid, ..] => KickStream::Vod(uuid.to_string()),

        // kick.com/<username>/clips/<clip_id>
        [_, "clips", clip_id, ..] => KickStream::Clip(clip_id.to_string()),

        // kick.com/<username>
        [slug] => KickStream::Live(slug.to_string()),

        _ => KickStream::Invalid,
    }
}

// ----------------- Public fetch functions -----------------

pub(crate) async fn fetch_kick_video_api(
    client: &StreamClient,
    uuid: &str,
) -> Result<Option<VodInfo>> {
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

    let mut info = VodInfo {
        vod_id: uuid.to_string(),
        platform: Platform::Kick,
        views: parsed.views,
        source: parsed.source.clone(),
        ..Default::default()
    };

    let mut channel_live_fallback_url: Option<String> = None;

    if let Some(ls) = parsed.livestream {
        info.title = ls.session_title;
        info.start_time = parse_datetime(ls.start_time);
        info.duration = ls.duration;
        info.thumbnail_url = ls.thumbnail;

        if let Some(ch_field) = ls.channel {
            match ch_field {
                ChannelField::Obj(ch) => {
                    info.username = ch.user.and_then(|u| u.username).or(ch.slug);
                    info.chat_id = ch
                        .chatroom
                        .and_then(|c| c.id)
                        .or(ch.id)
                        .map(|id| id.to_string());
                    channel_live_fallback_url = ch.playback_url;
                }
                ChannelField::Id(id) => {
                    info.chat_id = Some(id.to_string());
                }
            }
        }
    }

    info.playback_url = parsed
        .playback_url
        .or(parsed.source)
        .or(channel_live_fallback_url);

    if info.source.is_none() {
        info.source = info.playback_url.clone();
    }

    Ok(Some(info))
}

pub(crate) async fn fetch_kick_clip_api(
    client: &StreamClient,
    clip_id: &str,
) -> Result<Option<ClipInfo>> {
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

    let username = clip.channel.as_ref().and_then(|c| c.username.clone());

    let chat_id = clip
        .channel
        .as_ref()
        .and_then(|c| c.id)
        .or(clip.channel_id)
        .map(|id| id.to_string());

    Ok(Some(ClipInfo {
        clip_id: clip_id.to_string(),
        platform: Platform::Kick,
        title: clip.title,
        username,
        thumbnail_url: clip.thumbnail_url,
        start_time: parse_datetime(clip.started_at.or(clip.created_at)),
        duration: clip.duration.map(|sec| sec as i64),
        views: clip.views,
        chat_id,
        playback_url: clip.video_url,
    }))
}

pub(crate) async fn fetch_kick_channel_api(
    client: &StreamClient,
    slug: &str,
) -> Result<Option<LiveInfo>> {
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

    let is_live = parsed.livestream.is_some();

    let (title, start_time, viewer_count, thumbnail_url) = if let Some(ls) = parsed.livestream {
        (
            ls.session_title,
            ls.start_time,
            ls.viewer_count,
            ls.thumbnail,
        )
    } else {
        (None, None, None, None)
    };

    // Convert the numeric Kick IDs into Strings
    let channel_id = parsed.id.map(|id| id.to_string());

    let chat_id = parsed
        .chatroom
        .and_then(|c| c.id)
        .or(parsed.id)
        .map(|id| id.to_string());

    Ok(Some(LiveInfo {
        platform: Platform::Kick,
        channel_id,
        username: parsed
            .user
            .as_ref()
            .and_then(|u| u.username.clone())
            .or_else(|| Some(slug.to_string())),
        title,
        thumbnail_url,
        start_time: parse_datetime(start_time),
        viewer_count,
        followers: parsed.followers_count,
        playback_url: parsed.playback_url,
        chat_id,
        is_live,
    }))
}
