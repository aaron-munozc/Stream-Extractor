use regex::Regex;
use serde::Deserialize;
use url::Url;
use urlencoding::encode;

use crate::client::StreamClient;
use crate::error::Result;
use crate::types::{Platform, StreamMetadata, StreamStatus};

// ----------------- Internal Twitch Specific DTOs -----------------

#[derive(Debug, Deserialize)]
pub(crate) struct GqlOwner {
    pub(crate) displayName: Option<String>,
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

pub fn get_twitch_stream_info(url: &str) -> (bool, Option<String>) {
    let parsed = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return (false, None),
    };

    let is_twitch = parsed
        .host_str()
        .map_or(false, |h| h.ends_with("twitch.tv"));
    if !is_twitch {
        return (false, None);
    }

    let segments: Vec<&str> = parsed
        .path_segments()
        .map(|s| s.filter(|seg| !seg.is_empty()).collect())
        .unwrap_or_default();

    if let Some(pos) = segments.iter().position(|&s| s == "videos") {
        if let Some(id) = segments.get(pos + 1) {
            return (true, Some(id.to_string()));
        }
    }

    (false, None)
}

async fn fetch_twitch_video_graphql(
    client: &StreamClient,
    video_id: &str,
) -> Result<Option<GqlVideo>> {
    let url = "https://gql.twitch.tv/gql";
    let body = format!(
        r#"{{"query":"query{{video(id:\"{}\"){{title,thumbnailURLs(height:180,width:320),createdAt,lengthSeconds,owner{{id,displayName,login}},viewCount}}}}"#,
        video_id
    );

    let resp = client
        .inner
        .post(url)
        .header("Client-ID", "kimne78kx3ncx6brgo4mv6wki5h1ko")
        .body(body)
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let parsed: GqlResponse = resp.json().await?;
    Ok(parsed.data.and_then(|d| d.video))
}

async fn fetch_twitch_access_token(
    client: &StreamClient,
    video_id: &str,
) -> Result<Option<TwitchAccessTokenResponse>> {
    let gql_url = "https://gql.twitch.tv/gql";
    let gql_body = format!(
        r#"{{"operationName":"PlaybackAccessToken_Template","query":"query PlaybackAccessToken_Template($login: String!, $isLive: Boolean!, $vodID: ID!, $isVod: Boolean!, $playerType: String!) {{  videoPlaybackAccessToken(id: $vodID, params: {{platform: \"web\", playerBackend: \"mediaplayer\", playerType: $playerType}}) @include(if: $isVod) {{    value    signature  }} }}","variables":{{"isLive":false,"login":"","isVod":true,"vodID":"{}","playerType":"embed"}}}}"#,
        video_id
    );

    let gql_resp = client
        .inner
        .post(gql_url)
        .header("Client-ID", "kimne78kx3ncx6brgo4mv6wki5h1ko")
        .body(gql_body)
        .send()
        .await?;

    if gql_resp.status().is_success() {
        if let Ok(parsed) = gql_resp.json::<GqlVideoTokenResponse>().await {
            if let Some(tok) = parsed.data.and_then(|d| d.video_playback_access_token) {
                return Ok(Some(TwitchAccessTokenResponse {
                    token: tok.value,
                    sig: tok.signature,
                }));
            }
        }
    }

    // Fallback legacy API endpoint
    let legacy_url = format!("https://api.twitch.tv/api/vods/{}/access_token", video_id);
    let legacy_res = client.inner.get(&legacy_url).send().await?;
    if !legacy_res.status().is_success() {
        return Ok(None);
    }

    let parsed: TwitchAccessTokenResponse = legacy_res.json().await?;
    Ok(Some(parsed))
}

fn build_twitch_master_m3u8(video_id: &str, token: &str, sig: &str) -> String {
    format!(
        "https://usher.ttvnw.net/vod/{}.m3u8?sig={}&token={}&allow_source=true&allow_audio_only=true&include_unavailable=true&platform=web&player_backend=mediaplayer",
        video_id,
        encode(sig),
        encode(token)
    )
}

pub async fn fetch_twitch_metadata(
    client: &StreamClient,
    maybe_vod_id: Option<String>,
) -> Result<Option<StreamMetadata>> {
    let video_id = match maybe_vod_id {
        Some(id) => id,
        None => return Ok(None),
    };

    let gql_video = fetch_twitch_video_graphql(client, &video_id).await?;

    let token_response = match fetch_twitch_access_token(client, &video_id).await? {
        Some(tok) => tok,
        None => return Ok(None),
    };

    let master_url =
        build_twitch_master_m3u8(&video_id, &token_response.token, &token_response.sig);
    let master_res = client.inner.get(&master_url).send().await?;
    if !master_res.status().is_success() {
        return Ok(None);
    }

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
            let bw = last_bw.unwrap_or(0);
            candidate_playlists.push((bw, trimmed.to_string()));
            last_bw = None;
        }
    }

    candidate_playlists.sort_by_key(|(bw, _)| -*bw);
    let chosen_playlist_rel = candidate_playlists
        .first()
        .map(|(_, p)| p.clone())
        .or_else(|| {
            master_text
                .lines()
                .find(|l| l.trim().contains(".m3u8") || l.trim().starts_with("http"))
                .map(|s| s.trim().to_string())
        });

    let chosen_playlist = if let Some(ref relative_or_abs) = chosen_playlist_rel {
        if relative_or_abs.starts_with("http") {
            Some(relative_or_abs.clone())
        } else {
            Url::parse(&master_url)
                .ok()
                .and_then(|base| base.join(relative_or_abs).ok().map(|u| u.to_string()))
        }
    } else {
        None
    };

    let (title, thumbnail_url, start_time, duration, views, username) = if let Some(g) = gql_video {
        let thumb = g
            .thumbnail_urls
            .and_then(|v| v.into_iter().find(|s| s.starts_with("http")));
        let name = if let Some(owner) = g.owner {
            owner.login.or(owner.displayName)
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
        vod_uuid: Some(video_id),
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
