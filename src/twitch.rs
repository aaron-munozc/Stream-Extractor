use regex::Regex;
use reqwest::StatusCode;
use serde::Deserialize;
use url::Url;
use urlencoding::encode;

use crate::client::StreamClient;
use crate::error::Result;
use crate::types::{Platform, StreamMetadata, StreamStatus};

// ----------------- Internal Twitch Specific DTOs -----------------

#[derive(Debug, Deserialize)]
pub(crate) struct GqlOwner {
    #[serde(rename = "displayName")]
    pub(crate) display_name: Option<String>,
    pub(crate) login: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GqlVideo {
    pub(crate) title: Option<String>,
    #[serde(rename = "thumbnailURLs")]
    pub(crate) thumbnail_urls: Option<Vec<String>>,
    #[serde(rename = "createdAt")]
    pub(crate) created_at: Option<String>,
    #[serde(rename = "lengthSeconds")]
    pub(crate) length_seconds: Option<i64>,
    pub(crate) owner: Option<GqlOwner>,
    #[serde(rename = "viewCount")]
    pub(crate) view_count: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GqlVideoData {
    pub(crate) video: Option<GqlVideo>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GqlResponse {
    pub(crate) data: Option<GqlVideoData>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TwitchAccessTokenResponse {
    pub(crate) token: String,
    pub(crate) sig: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct VideoPlaybackAccessToken {
    pub(crate) value: String,
    pub(crate) signature: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GqlVideoTokenData {
    #[serde(rename = "videoPlaybackAccessToken")]
    pub(crate) video_playback_access_token: Option<VideoPlaybackAccessToken>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GqlVideoTokenResponse {
    pub(crate) data: Option<GqlVideoTokenData>,
}

// ----------------- Parser & Extraction Logic -----------------

pub(crate) enum TwitchStream {
    Vod(String),
    Clip(String),
    Invalid,
}

pub(crate) fn get_twitch_stream_info(url: &str) -> TwitchStream {
    let parsed = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return TwitchStream::Invalid,
    };

    let host = parsed.host_str();

    let segments: Vec<&str> = parsed
        .path_segments()
        .map(|s| s.filter(|seg| !seg.is_empty()).collect())
        .unwrap_or_default();

    // Helper to strip query params from IDs
    let clean_id = |id: &str| id.split('?').next().unwrap_or(id).to_string();

    match segments.as_slice() {
        ["videos", id, ..] if host.is_some_and(|h| h.ends_with("twitch.tv")) => {
            TwitchStream::Vod(id.to_string())
        }
        [_, "clip", id, ..] if host.is_some_and(|h| h.ends_with("twitch.tv")) => {
            TwitchStream::Clip(clean_id(id))
        }
        [id, ..] if host == Some("clips.twitch.tv") => TwitchStream::Clip(clean_id(id)),
        _ => TwitchStream::Invalid,
    }
}
async fn fetch_twitch_video_graphql(
    client: &StreamClient,
    video_id: &str,
) -> Result<Option<GqlVideo>> {
    let url = "https://gql.twitch.tv/gql";
    let body = serde_json::json!({
        "query": format!("query {{ video(id: \"{}\") {{ title, thumbnailURLs(height: 180, width: 320), createdAt, lengthSeconds, owner {{ id, displayName, login }}, viewCount }} }}", video_id)
    });

    let resp = client
        .inner
        .post(url)
        .header("Client-ID", "kimne78kx3ncx6brgo4mv6wki5h1ko")
        .json(&body)
        .send()
        .await?;

    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    let resp = resp.error_for_status()?;


    let parsed: GqlResponse = resp.json().await?;
    Ok(parsed.data.and_then(|d| d.video))
}
async fn fetch_twitch_access_token(
    client: &StreamClient,
    video_id: &str,
) -> Result<Option<TwitchAccessTokenResponse>> {
    let gql_body = serde_json::json!({
        "operationName": "PlaybackAccessToken_Template",
        "query": "query PlaybackAccessToken_Template($login: String!, $isLive: Boolean!, $vodID: ID!, $isVod: Boolean!, $playerType: String!) {  streamPlaybackAccessToken(channelName: $login, params: {platform: \"web\", playerBackend: \"mediaplayer\", playerType: $playerType}) @include(if: $isLive) {    value    signature    __typename  }  videoPlaybackAccessToken(id: $vodID, params: {platform: \"web\", playerBackend: \"mediaplayer\", playerType: $playerType}) @include(if: $isVod) {    value    signature    __typename  }}",
        "variables": {
            "isLive": false,
            "login": "",
            "isVod": true,
            "vodID": video_id,
            "playerType": "embed"
        }
    });
    let gql_resp = client
        .inner
        .post("https://gql.twitch.tv/gql")
        .header("Client-ID", "kimne78kx3ncx6brgo4mv6wki5h1ko")
        .json(&gql_body)
        .send()
        .await?;

    let response: GqlVideoTokenResponse = gql_resp.json().await?;

    if let Some(token_data) = response.data.and_then(|d| d.video_playback_access_token) {
        return Ok(Some(TwitchAccessTokenResponse {
            token: token_data.value,
            sig: token_data.signature,
        }));
    }

    Ok(None)
}
fn build_twitch_master_m3u8(video_id: &str, token: &str, sig: &str) -> String {
    format!(
        "https://usher.ttvnw.net/vod/{}.m3u8?sig={}&token={}&allow_source=true&allow_audio_only=true&include_unavailable=true&platform=web&player_backend=mediaplayer",
        video_id,
        encode(sig),
        encode(token)
    )
}

pub(crate) async fn fetch_twitch_clip_metadata(
    client: &StreamClient,
    clip_id: &str,
) -> Result<Option<StreamMetadata>> {
    let url = "https://gql.twitch.tv/gql";

    let info_body = serde_json::json!({
        "query": format!("query {{ clip(slug: \"{}\") {{ id, title, durationSeconds, viewCount, createdAt, thumbnailURL, broadcaster {{ displayName, login }} }} }}", clip_id)
    });

    let info_resp = client
        .inner
        .post(url)
        .header("Client-ID", "kimne78kx3ncx6brgo4mv6wki5h1ko")
        .json(&info_body)
        .send()
        .await?;
    
    if info_resp.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let info_resp = info_resp.error_for_status()?;

    let parsed: serde_json::Value = info_resp.json().await?;
    let clip = &parsed["data"]["clip"];
    if clip.is_null() {
        return Ok(None);
    }

    let token_body = serde_json::json!({
        "operationName": "VideoAccessToken_Clip",
        "variables": { "slug": clip_id },
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "36b89d2507fce29e5ca551df756d27c1cfe079e2609642b4390aa4c35796eb11"
            }
        }
    });

    let token_resp = client
        .inner
        .post(url)
        .header("Client-ID", "kimne78kx3ncx6brgo4mv6wki5h1ko")
        .json(&token_body)
        .send()
        .await?;

    let mut mp4_url = String::new();
    if token_resp.status().is_success() {
        let token_val: serde_json::Value = token_resp.json().await?;
        if let Some(qualities) = token_val["data"]["clip"]["videoQualities"].as_array()
            && let Some(best) = qualities.first()
        {
            let source_url = best["sourceURL"].as_str().unwrap_or("");
            let sig = token_val["data"]["clip"]["playbackAccessToken"]["signature"]
                .as_str()
                .unwrap_or("");
            let token = token_val["data"]["clip"]["playbackAccessToken"]["value"]
                .as_str()
                .unwrap_or("");

            if !source_url.is_empty() {
                mp4_url = format!(
                    "{}?sig={}&token={}",
                    source_url,
                    sig,
                    urlencoding::encode(token)
                );
            }
        }
    }

    if mp4_url.is_empty() {
        let thumb = clip["thumbnailURL"].as_str().unwrap_or("");
        if let Some(idx) = thumb.find("-preview") {
            mp4_url = format!("{}.mp4", &thumb[..idx]);
        } else {
            mp4_url = thumb.to_string();
        }
    }

    Ok(Some(StreamMetadata {
        vod_uuid: Some(clip_id.to_string()),
        title: clip["title"].as_str().map(|s| s.to_string()),
        thumbnail_url: clip["thumbnailURL"].as_str().map(|s| s.to_string()),
        duration: clip["durationSeconds"].as_i64(),
        views: clip["viewCount"].as_i64(),
        start_time: clip["createdAt"].as_str().map(|s| s.to_string()),
        username: clip["broadcaster"]["login"].as_str().map(|s| s.to_string()),
        platform: Platform::Twitch,
        stream_status: Some(StreamStatus::Vod),
        source: Some(mp4_url.clone()),
        playback_url: Some(mp4_url),
        ..Default::default()
    }))
}
pub(crate) async fn fetch_twitch_metadata(
    client: &StreamClient,
    video_id: &str,
) -> Result<Option<StreamMetadata>> {
    let gql_video = fetch_twitch_video_graphql(client, video_id).await?;
    let token_response = match fetch_twitch_access_token(client, video_id).await? {
        Some(tok) => tok,
        None => {
            log::error!(
                "Failed to acquire Twitch Video Playback Token for VOD {}",
                video_id
            );
            return Ok(None);
        }
    };

    let master_url = build_twitch_master_m3u8(video_id, &token_response.token, &token_response.sig);
    let master_res = client.inner.get(&master_url).send().await?;

    if master_res.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let master_res = master_res.error_for_status()?;
    let master_text = master_res.text().await?;
    let bandwidth_re = Regex::new(r#"BANDWIDTH=(\d+)"#).unwrap();
    let mut candidate_playlists: Vec<(i64, String)> = Vec::new();
    let mut last_bw: Option<i64> = None;

    for line in master_text.lines() {
        if let Some(caps) = bandwidth_re.captures(line) {
            last_bw = caps.get(1).and_then(|m| m.as_str().parse::<i64>().ok());
        }
        let trimmed = line.trim();
        if trimmed.ends_with(".m3u8") || trimmed.starts_with("http") {
            candidate_playlists.push((last_bw.unwrap_or(0), trimmed.to_string()));
            last_bw = None;
        }
    }
    candidate_playlists.sort_by_key(|(bw, _)| -*bw);

    let chosen_playlist = candidate_playlists
        .first()
        .map(|(_, p)| p.clone())
        .or_else(|| {
            master_text
                .lines()
                .find(|l| l.trim().contains(".m3u8") || l.trim().starts_with("http"))
                .map(|s| s.trim().to_string())
        })
        .map(|rel| {
            if rel.starts_with("http") {
                rel
            } else {
                Url::parse(&master_url)
                    .ok()
                    .and_then(|base| base.join(&rel).ok().map(|u| u.to_string()))
                    .unwrap_or(rel)
            }
        });

    let (title, thumbnail_url, start_time, duration, views, username) = if let Some(g) = gql_video {
        let thumb = g
            .thumbnail_urls
            .and_then(|v| v.into_iter().find(|s| s.starts_with("http")));
        let name = if let Some(owner) = g.owner {
            owner.login.or(owner.display_name)
        } else {
            None
        };
        (
            g.title,
            thumb,
            g.created_at,
            g.length_seconds,
            g.view_count,
            name,
        )
    } else {
        (None, None, None, None, None, None)
    };

    Ok(Some(StreamMetadata {
        vod_uuid: Some(video_id.to_string()),
        title,
        thumbnail_url,
        duration,
        views,
        stream_status: Some(StreamStatus::Vod),
        start_time,
        username,
        platform: Platform::Twitch,
        source: Some(master_url),
        playback_url: chosen_playlist,
        ..Default::default()
    }))
}
