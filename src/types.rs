use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

// --- Download specific types ---

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StreamResolution {
    pub width: u64,
    pub height: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StreamQuality {
    pub index: usize,
    pub uri: String,
    pub resolution: Option<StreamResolution>,
    pub bandwidth: Option<u64>,
}

#[derive(Clone, Serialize, Debug)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum ProgressPayload {
    Downloading { percent: i64, message: String },
    Merging,
    Done,
    Error { message: String },
}

pub type ProgressCallback = Arc<dyn Fn(ProgressPayload) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QualityPreference {
    #[default]
    Best,
    Worst,
    Height(u64),
    Index(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VideoFormat {
    #[default]
    Mp4,
    Mkv,
    Mov,
    Ts,
}

impl VideoFormat {
    pub fn extension(&self) -> &'static str {
        match self {
            VideoFormat::Mp4 => "mp4",
            VideoFormat::Mkv => "mkv",
            VideoFormat::Mov => "mov",
            VideoFormat::Ts => "ts",
        }
    }
}

#[derive(Clone)]
pub struct DownloadOptions {
    pub output_dir: Option<PathBuf>,
    pub output_name: Option<String>,
    pub threads: usize,
    pub quality: QualityPreference,
    pub format: VideoFormat,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub buffer_ms: Option<u64>,
    pub progress_hook: Option<ProgressCallback>,
    pub cancel_rx: Option<tokio::sync::watch::Receiver<bool>>,
}

impl std::fmt::Debug for DownloadOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadOptions")
            .field("output_dir", &self.output_dir)
            .field("output_name", &self.output_name)
            .field("threads", &self.threads)
            .field("quality", &self.quality)
            .field("format", &self.format)
            .field("start_ms", &self.start_ms)
            .field("end_ms", &self.end_ms)
            .field("buffer_ms", &self.buffer_ms)
            .field("progress_hook", &if self.progress_hook.is_some() { "Some(Callback)" } else { "None" })
            .field("cancel_rx", &if self.cancel_rx.is_some() { "Some(Receiver)" } else { "None" })
            .finish()
    }
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            output_dir: None,
            output_name: None,
            threads: 4,
            quality: QualityPreference::Best,
            format: VideoFormat::Mp4,
            start_ms: None,
            end_ms: None,
            buffer_ms: None,
            progress_hook: None,
            cancel_rx: None,
        }
    }
}

// --- Platform & Metadata specific types ---

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Twitch,
    #[default]
    Kick,
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Platform::Twitch => write!(f, "twitch"),
            Platform::Kick => write!(f, "kick"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StreamStatus {
    Live,
    Clip,
    Vod,
    Offline,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct StreamMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub viewer_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub views: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub followers: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub playback_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vod_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_status: Option<StreamStatus>,
    pub platform: Platform,
}

// --- Kick Internal Types ---

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub(crate) struct Chatroom {
    pub id: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub(crate) struct User {
    pub username: Option<String>,
    #[serde(alias = "profilepic", alias = "profile_pic", default)]
    pub profile_pic: Option<String>,
    #[serde(default)]
    pub bio: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub(crate) struct Channel {
    #[serde(rename = "id", alias = "channel_id")]
    pub id: Option<i64>,
    pub slug: Option<String>,
    #[serde(rename = "followersCount", alias = "followers_count", default)]
    pub followers_count: Option<i64>,
    #[serde(default)]
    pub user: Option<User>,
    #[serde(default)]
    pub chatroom: Option<Chatroom>,
    #[serde(default, alias = "playbackUrl")]
    pub playback_url: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub(crate) enum ChannelField {
    Id(i64),
    Obj(Channel),
}

impl Default for ChannelField {
    fn default() -> Self {
        ChannelField::Id(0)
    }
}

fn parse_srcset(s: &str) -> Option<String> {
    s.split(',')
        .filter_map(|part| {
            let mut pieces = part.trim().rsplitn(2, ' ');
            let width = pieces.next()?.trim_end_matches('w').parse::<u32>().ok()?;
            let url = pieces.next()?;
            Some((width, url.to_string()))
        })
        .max_by_key(|(w, _)| *w)
        .map(|(_, url)| url)
}

fn deserialize_thumbnail<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Value = Value::deserialize(deserializer)?;
    match v {
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                return Ok(None);
            }
            if s.contains(' ') && s.contains('w') {
                Ok(parse_srcset(s))
            } else {
                Ok(Some(s.to_string()))
            }
        }
        Value::Object(map) => {
            let best_link = map
                .get("responsive")
                .or_else(|| map.get("srcset"))
                .and_then(|v| v.as_str())
                .and_then(parse_srcset);
            if best_link.is_some() {
                return Ok(best_link);
            }
            let fallback = map
                .get("url")
                .or_else(|| map.get("src"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if fallback.is_some() {
                return Ok(fallback);
            }
            Ok(map
                .values()
                .filter_map(|v| v.as_str())
                .find(|s| s.starts_with("http"))
                .map(|s| s.to_string()))
        }
        Value::Array(arr) => Ok(arr.iter().find_map(|item| match item {
            Value::String(s) if s.starts_with("http") => Some(s.to_string()),
            Value::Object(_) => item
                .get("url")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            _ => None,
        })),
        _ => Ok(None),
    }
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct KickVideoResponse {
    pub uuid: Option<String>,
    pub views: Option<i64>,
    pub source: Option<String>,
    #[serde(alias = "playbackUrl", default)]
    pub playback_url: Option<String>,
    #[serde(default)]
    pub livestream: Option<Livestream>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub(crate) struct Livestream {
    pub id: Option<i64>,
    pub session_title: Option<String>,
    pub start_time: Option<String>,
    pub duration: Option<i64>,
    #[serde(deserialize_with = "deserialize_thumbnail", default)]
    pub thumbnail: Option<String>,
    #[serde(rename = "viewer_count", alias = "viewerCount", default)]
    pub viewer_count: Option<i64>,
    pub is_live: Option<bool>,
    #[serde(default)]
    pub channel: Option<ChannelField>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct KickChannelResponse {
    pub id: Option<i64>,
    pub user: Option<User>,
    pub chatroom: Option<Chatroom>,
    pub livestream: Option<Livestream>,
    #[serde(rename = "followersCount", alias = "followers_count")]
    pub followers_count: Option<i64>,
    pub playback_url: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct KickClipResponse {
    pub clip: Option<KickClipData>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct KickClipData {
    pub title: Option<String>,
    pub thumbnail_url: Option<String>,
    pub views: Option<i64>,
    pub duration: Option<f64>,
    #[serde(rename = "created_at")]
    pub created_at: Option<String>,
    pub video_url: Option<String>,
    pub channel: Option<KickClipChannel>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct KickClipChannel {
    pub id: Option<i64>,
    pub username: Option<String>,
}

// --- Twitch GraphQL Types ---

#[derive(Debug, Deserialize)]
pub(crate) struct TwitchGqlClipResponse {
    pub data: Option<TwitchGqlClipData>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TwitchGqlClipData {
    pub clip: Option<TwitchGqlClip>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlClip {
    pub video_offset_seconds: Option<f64>,
    pub duration_seconds: Option<f64>,
    pub video: Option<TwitchGqlVideoId>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TwitchGqlVideoId {
    pub id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TwitchGqlCommentsResponse {
    pub data: Option<TwitchGqlCommentsData>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TwitchGqlCommentsData {
    pub video: Option<TwitchGqlVideo>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TwitchGqlVideo {
    pub comments: Option<TwitchGqlCommentsConnection>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlCommentsConnection {
    pub edges: Option<Vec<TwitchGqlCommentEdge>>,
    pub page_info: Option<TwitchGqlPageInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlCommentEdge {
    pub cursor: Option<String>,
    pub node: Option<TwitchGqlCommentNode>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlCommentNode {
    pub id: Option<String>,
    pub content_offset_seconds: Option<f64>,
    pub message: Option<TwitchGqlCommentMessage>,
    pub commenter: Option<TwitchGqlCommenter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlCommentMessage {
    pub user_badges: Option<Vec<TwitchGqlUserBadge>>,
    pub user_color: Option<String>,
    pub fragments: Option<Vec<TwitchGqlMessageFragment>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlUserBadge {
    #[serde(rename = "setID")]
    pub set_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TwitchGqlMessageFragment {
    pub text: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlCommenter {
    pub id: Option<String>,
    pub login: Option<String>,
    pub display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlPageInfo {
    pub has_next_page: Option<bool>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlRequest<'a> {
    pub operation_name: &'static str,
    pub variables: TwitchGqlVariables<'a>,
    pub extensions: TwitchGqlExtensions,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlVariables<'a> {
    #[serde(rename = "videoID")]
    pub video_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_offset_seconds: Option<i64>,
}

// FIX: Added missing rename_all here to prevent sending "persisted_query"
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlExtensions {
    pub persisted_query: PersistedQuery,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PersistedQuery {
    pub version: u32,
    pub sha256_hash: &'static str,
}

#[derive(Clone)]
pub struct ChatOptions {
    pub output_dir: Option<PathBuf>,
    pub output_name: Option<String>,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub buffer_ms: Option<u64>,
    pub max_retries: usize,
    pub kick_concurrency: usize,
    pub empty_cycle_threshold: usize,
    pub progress_hook: Option<ProgressCallback>,
    pub cancel_rx: Option<tokio::sync::watch::Receiver<bool>>,
}

impl fmt::Debug for ChatOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChatOptions")
            .field("output_dir", &self.output_dir)
            .field("output_name", &self.output_name)
            .field("start_ms", &self.start_ms)
            .field("end_ms", &self.end_ms)
            .field("buffer_ms", &self.buffer_ms)
            .field("max_retries", &self.max_retries)
            .field("kick_concurrency", &self.kick_concurrency)
            .field("empty_cycle_threshold", &self.empty_cycle_threshold)
            .field(
                "progress_hook",
                &if self.progress_hook.is_some() { "Some(Callback)" } else { "None" },
            )
            .field(
                "cancel_rx",
                &if self.cancel_rx.is_some() { "Some(Receiver)" } else { "None" },
            )
            .finish()
    }
}

impl Default for ChatOptions {
    fn default() -> Self {
        Self {
            output_dir: None,
            output_name: None,
            start_ms: None,
            end_ms: None,
            buffer_ms: None,
            max_retries: 8,
            kick_concurrency: 10,
            empty_cycle_threshold: 6,
            progress_hook: None,
            cancel_rx: None,
        }
    }
}

// --- Chat Data Structures ---

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Badge {
    pub r#type: String,
    pub text: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Identity {
    pub color: String,
    #[serde(default)]
    pub badges: Vec<Badge>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Sender {
    pub id: i64,
    pub slug: String,
    pub username: String,
    pub identity: Identity,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Message {
    pub id: String,
    pub chat_id: i64,
    pub user_id: i64,
    pub content: String,
    pub r#type: String,
    pub metadata: String,
    pub sender: Sender,
    pub created_at: String,
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct MessageEnriched {
    pub id: String,
    pub chat_id: i64,
    pub user_id: i64,
    pub content: String,
    pub r#type: String,
    pub metadata: String,
    pub sender: Sender,
    pub created_at_raw: String,
    pub created_at_secs: i64,
    pub created_at_str: String,
}

impl MessageEnriched {
    pub fn from_message(msg: &Message, stream_start: DateTime<Utc>) -> Self {
        let created_at = DateTime::parse_from_rfc3339(&msg.created_at)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(stream_start);

        let delta = created_at - stream_start;
        let total_seconds = delta.num_seconds().max(0);

        let h = total_seconds / 3600;
        let m = (total_seconds % 3600) / 60;
        let s = total_seconds % 60;

        Self {
            id: msg.id.clone(),
            chat_id: msg.chat_id,
            user_id: msg.user_id,
            content: msg.content.clone(),
            r#type: msg.r#type.clone(),
            metadata: msg.metadata.clone(),
            sender: msg.sender.clone(),
            created_at_raw: msg.created_at.clone(),
            created_at_secs: total_seconds,
            created_at_str: format!("{:02}:{:02}:{:02}", h, m, s),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ChatData {
    pub messages: Vec<Message>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ChatResponse {
    pub data: ChatData,
    pub message: String,
}