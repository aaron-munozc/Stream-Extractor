use chrono::{DateTime, Utc};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "serde-types")]
use serde::{Deserialize, Deserializer, Serialize};
#[cfg(feature = "serde-types")]
use serde_json::Value;

// Internal serde is always available (GQL/chat layers need it).
// We alias the derives so internal-only structs don't have to cfg-gate.
use serde::{Deserialize as Deser, Serialize as Ser};

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Parses an optional RFC 3339 / space-separated datetime string into a
/// `DateTime<Utc>`. Returns `None` if the input is `None` or parsing fails.
pub(crate) fn parse_datetime(s: Option<String>) -> Option<DateTime<Utc>> {
    s.and_then(|raw| {
        DateTime::parse_from_rfc3339(&raw)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
            .or_else(|| {
                chrono::NaiveDateTime::parse_from_str(&raw, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .map(|ndt| ndt.and_utc())
            })
    })
}

// ---------------------------------------------------------------------------
// Progress / callback types
// ---------------------------------------------------------------------------

/// Progress update emitted during a download or merge operation.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde-types", derive(Serialize, Deserialize))]
#[cfg_attr(
    feature = "serde-types",
    serde(tag = "status", rename_all = "camelCase")
)]
pub enum ProgressPayload {
    /// `percent` is clamped to 0–100 by the caller.
    Downloading {
        percent: u8,
        message: String,
    },
    Merging,
    Done,
    Error {
        message: String,
    },
}

/// A cheaply-clonable progress callback. Pass one to any `*DownloadOptions`.
pub type ProgressCallback = Arc<dyn Fn(ProgressPayload) + Send + Sync>;

// ---------------------------------------------------------------------------
// VOD download types (feature = "vod")
// ---------------------------------------------------------------------------

#[cfg(feature = "vod")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde-types", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde-types", serde(rename_all = "camelCase"))]
pub struct StreamResolution {
    pub width: u64,
    pub height: u64,
}

#[cfg(feature = "vod")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde-types", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde-types", serde(rename_all = "camelCase"))]
pub struct StreamQuality {
    pub index: usize,
    pub uri: String,
    pub resolution: Option<StreamResolution>,
    pub bandwidth: Option<u64>,
}

#[cfg(feature = "vod")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde-types", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde-types", serde(rename_all = "camelCase"))]
pub enum QualityPreference {
    #[default]
    Best,
    Worst,
    Height(u64),
    Index(usize),
}

#[cfg(feature = "vod")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde-types", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde-types", serde(rename_all = "lowercase"))]
pub enum VideoFormat {
    #[default]
    Mp4,
    Mkv,
    Mov,
    Ts,
}

#[cfg(feature = "vod")]
impl VideoFormat {
    pub fn extension(self) -> &'static str {
        match self {
            VideoFormat::Mp4 => "mp4",
            VideoFormat::Mkv => "mkv",
            VideoFormat::Mov => "mov",
            VideoFormat::Ts => "ts",
        }
    }
}

/// Options controlling a VOD or clip video download.
///
/// Construct with `VodDownloadOptions::default()` and chain the `with_*`
/// setters for any fields you want to override.
///
/// ```rust
/// use stream_extractor::{VodDownloadOptions, QualityPreference, VideoFormat};
///
/// let opts = VodDownloadOptions::default()
///     .with_quality(QualityPreference::Best)
///     .with_format(VideoFormat::Mkv)
///     .with_threads(8);
/// ```
#[cfg(feature = "vod")]
#[derive(Clone, Default)]
pub struct VodDownloadOptions {
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

#[cfg(feature = "vod")]
impl VodDownloadOptions {
    #[must_use]
    pub fn with_output_dir<P: Into<PathBuf>>(mut self, dir: P) -> Self {
        self.output_dir = Some(dir.into());
        self
    }

    #[must_use]
    pub fn with_output_name<S: Into<String>>(mut self, name: S) -> Self {
        self.output_name = Some(name.into());
        self
    }

    /// Number of concurrent segment downloads (clamped to 1–16 internally).
    #[must_use]
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    #[must_use]
    pub fn with_quality(mut self, quality: QualityPreference) -> Self {
        self.quality = quality;
        self
    }

    #[must_use]
    pub fn with_format(mut self, format: VideoFormat) -> Self {
        self.format = format;
        self
    }

    #[must_use]
    pub fn with_start_ms(mut self, ms: u64) -> Self {
        self.start_ms = Some(ms);
        self
    }

    #[must_use]
    pub fn with_end_ms(mut self, ms: u64) -> Self {
        self.end_ms = Some(ms);
        self
    }

    #[must_use]
    pub fn with_buffer_ms(mut self, ms: u64) -> Self {
        self.buffer_ms = Some(ms);
        self
    }

    #[must_use]
    pub fn with_progress_hook(mut self, hook: ProgressCallback) -> Self {
        self.progress_hook = Some(hook);
        self
    }

    #[must_use]
    pub fn with_cancel_rx(mut self, rx: tokio::sync::watch::Receiver<bool>) -> Self {
        self.cancel_rx = Some(rx);
        self
    }
}

#[cfg(feature = "vod")]
impl fmt::Debug for VodDownloadOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VodDownloadOptions")
            .field("output_dir", &self.output_dir)
            .field("output_name", &self.output_name)
            .field("threads", &self.threads)
            .field("quality", &self.quality)
            .field("format", &self.format)
            .field("start_ms", &self.start_ms)
            .field("end_ms", &self.end_ms)
            .field("buffer_ms", &self.buffer_ms)
            .field(
                "progress_hook",
                &self.progress_hook.as_ref().map(|_| "<callback>"),
            )
            .field("cancel_rx", &self.cancel_rx.as_ref().map(|_| "<receiver>"))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Platform
// ---------------------------------------------------------------------------

/// The streaming platform a [`Stream`](crate::Stream) belongs to.
///
/// Marked `#[non_exhaustive]` so adding new platforms is not a breaking change.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde-types", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde-types", serde(rename_all = "lowercase"))]
#[non_exhaustive]
pub enum Platform {
    Twitch,
    #[default]
    Kick,
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Platform::Twitch => f.write_str("twitch"),
            Platform::Kick => f.write_str("kick"),
        }
    }
}

// ---------------------------------------------------------------------------
// Public stream-info structs
// ---------------------------------------------------------------------------

/// Metadata for a recorded VOD (Twitch VOD or Kick VOD).
///
/// All fields that may not be populated by a given platform are `Option`.
/// Use `playback_url` (or the fallback `source`) to drive video download;
/// use `chat_id` for Kick chat downloads.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-types", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde-types", serde(rename_all = "camelCase"))]
pub struct VodInfo {
    /// Unique VOD identifier (numeric string on Twitch, UUID on Kick).
    pub vod_id: String,
    pub platform: Platform,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub title: Option<String>,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub username: Option<String>,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub thumbnail_url: Option<String>,
    /// VOD start wall-clock time.
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub start_time: Option<DateTime<Utc>>,
    /// Duration in **seconds**.
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub duration: Option<i64>,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub views: Option<i64>,
    /// Kick chatroom ID — required for Kick chat downloads.
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub chat_id: Option<i64>,
    /// The highest-quality media playlist URL (chosen by the fetcher).
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub playback_url: Option<String>,
    /// The raw master playlist URL before quality selection (fallback).
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub source: Option<String>,
}

/// Metadata for a clip (Twitch clip or Kick clip).
///
/// Clips are short, finite recordings — they always have a direct MP4/M3U8
/// URL and a known duration, but no live viewer count.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-types", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde-types", serde(rename_all = "camelCase"))]
pub struct ClipInfo {
    /// Platform-specific clip identifier (slug on Twitch, ULID on Kick).
    pub clip_id: String,
    pub platform: Platform,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub title: Option<String>,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub username: Option<String>,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub thumbnail_url: Option<String>,
    /// Wall-clock time when the clip was created.
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub start_time: Option<DateTime<Utc>>,
    /// Clip duration in **seconds**.
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub duration: Option<i64>,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub views: Option<i64>,
    /// Kick chatroom ID — used when downloading clip chat.
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub chat_id: Option<i64>,
    /// Direct playback URL (MP4 for Twitch clips; M3U8 for Kick clips).
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub playback_url: Option<String>,
}

/// Metadata for a currently-live or recently-offline channel.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-types", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde-types", serde(rename_all = "camelCase"))]
pub struct LiveInfo {
    pub platform: Platform,

    // Universal channel/broadcaster ID (String for all platforms)
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub channel_id: Option<String>,

    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub username: Option<String>,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub title: Option<String>,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub thumbnail_url: Option<String>,
    /// When the current stream session started.
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub start_time: Option<DateTime<Utc>>,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub viewer_count: Option<i64>,
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub followers: Option<i64>,
    /// Live HLS playlist URL.
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub playback_url: Option<String>,

    // Universal chat routing ID (String for all platforms)
    #[cfg_attr(
        feature = "serde-types",
        serde(skip_serializing_if = "Option::is_none")
    )]
    pub chat_id: Option<String>,
    pub is_live: bool,
}

// ---------------------------------------------------------------------------
// Platform-specific chat options
// ---------------------------------------------------------------------------

/// Options that only affect Kick chat downloads.
#[derive(Debug, Clone, Copy)]
pub struct KickOptions {
    /// Number of concurrent chat-history requests per batch window.
    pub concurrency: usize,
    /// Consecutive empty response batches before the download is considered
    /// complete (only applies when no explicit end time is given).
    pub empty_cycle_threshold: usize,
}

impl Default for KickOptions {
    fn default() -> Self {
        Self {
            concurrency: 4,
            empty_cycle_threshold: 8,
        }
    }
}

impl KickOptions {
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    #[must_use]
    pub fn with_empty_cycle_threshold(mut self, threshold: usize) -> Self {
        self.empty_cycle_threshold = threshold;
        self
    }
}

/// Options that only affect Twitch chat downloads.
///
/// Reserved for future Twitch-specific knobs.
#[derive(Debug, Clone, Copy, Default)]
pub struct TwitchOptions {}

/// Per-platform settings for chat downloading.
///
/// Obtain one via the [`From`] impls rather than constructing directly:
///
/// ```rust
/// use stream_extractor::{ChatDownloadOptions, KickOptions};
///
/// let opts = ChatDownloadOptions::default()
///     .with_platform_options(KickOptions::default().with_concurrency(20));
/// ```
#[derive(Debug, Clone)]
pub enum PlatformChatOptions {
    Kick(KickOptions),
    Twitch(TwitchOptions),
}

impl From<KickOptions> for PlatformChatOptions {
    fn from(opts: KickOptions) -> Self {
        Self::Kick(opts)
    }
}

impl From<TwitchOptions> for PlatformChatOptions {
    fn from(opts: TwitchOptions) -> Self {
        Self::Twitch(opts)
    }
}

// ---------------------------------------------------------------------------
// Chat download options
// ---------------------------------------------------------------------------

/// Options controlling a VOD or clip chat download.
///
/// ```rust
/// use stream_extractor::ChatDownloadOptions;
///
/// let opts = ChatDownloadOptions::default()
///     .with_output_dir("/tmp")
///     .with_start_ms(60_000)
///     .with_end_ms(120_000);
/// ```
pub struct ChatDownloadOptions {
    pub output_dir: Option<PathBuf>,
    pub output_name: Option<String>,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub buffer_ms: Option<u64>,
    pub max_retries: usize,
    /// Platform-specific tuning. If `None`, per-platform defaults apply.
    pub platform_options: Option<PlatformChatOptions>,
    pub progress_hook: Option<ProgressCallback>,
    pub cancel_rx: Option<tokio::sync::watch::Receiver<bool>>,
}

impl ChatDownloadOptions {
    #[must_use]
    pub fn with_output_dir<P: Into<PathBuf>>(mut self, dir: P) -> Self {
        self.output_dir = Some(dir.into());
        self
    }

    #[must_use]
    pub fn with_output_name<S: Into<String>>(mut self, name: S) -> Self {
        self.output_name = Some(name.into());
        self
    }

    #[must_use]
    pub fn with_start_ms(mut self, ms: u64) -> Self {
        self.start_ms = Some(ms);
        self
    }

    #[must_use]
    pub fn with_end_ms(mut self, ms: u64) -> Self {
        self.end_ms = Some(ms);
        self
    }

    #[must_use]
    pub fn with_buffer_ms(mut self, ms: u64) -> Self {
        self.buffer_ms = Some(ms);
        self
    }

    #[must_use]
    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    #[must_use]
    pub fn with_platform_options<P: Into<PlatformChatOptions>>(mut self, opts: P) -> Self {
        self.platform_options = Some(opts.into());
        self
    }

    #[must_use]
    pub fn with_progress_hook(mut self, hook: ProgressCallback) -> Self {
        self.progress_hook = Some(hook);
        self
    }

    #[must_use]
    pub fn with_cancel_rx(mut self, rx: tokio::sync::watch::Receiver<bool>) -> Self {
        self.cancel_rx = Some(rx);
        self
    }

    // ------------------------------------------------------------------
    // Crate-internal helpers
    // ------------------------------------------------------------------

    pub(crate) fn kick_options(&self) -> KickOptions {
        match &self.platform_options {
            Some(PlatformChatOptions::Kick(opts)) => *opts,
            _ => KickOptions::default(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn twitch_options(&self) -> TwitchOptions {
        match &self.platform_options {
            Some(PlatformChatOptions::Twitch(opts)) => *opts,
            _ => TwitchOptions::default(),
        }
    }
}

impl Default for ChatDownloadOptions {
    fn default() -> Self {
        Self {
            output_dir: None,
            output_name: None,
            start_ms: None,
            end_ms: None,
            buffer_ms: None,
            max_retries: 8,
            platform_options: None,
            progress_hook: None,
            cancel_rx: None,
        }
    }
}

impl fmt::Debug for ChatDownloadOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChatDownloadOptions")
            .field("output_dir", &self.output_dir)
            .field("output_name", &self.output_name)
            .field("start_ms", &self.start_ms)
            .field("end_ms", &self.end_ms)
            .field("buffer_ms", &self.buffer_ms)
            .field("max_retries", &self.max_retries)
            .field("platform_options", &self.platform_options)
            .field(
                "progress_hook",
                &self.progress_hook.as_ref().map(|_| "<callback>"),
            )
            .field("cancel_rx", &self.cancel_rx.as_ref().map(|_| "<receiver>"))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Chat data structures (public)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deser, Ser)]
pub struct Badge {
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
}

#[derive(Debug, Clone, Deser, Ser)]
pub struct Identity {
    pub color: String,
    #[serde(default)]
    pub badges: Vec<Badge>,
}

#[derive(Debug, Clone, Deser, Ser)]
pub struct Sender {
    pub id: i64,
    pub slug: String,
    pub username: String,
    pub identity: Identity,
}

/// A saved chat message with precomputed timing fields relative to both the
/// stream start time and the downloaded range start time.
#[derive(Debug, Clone, Deser, Ser)]
pub struct MessageSaved {
    pub id: String,
    pub chat_id: i64,
    pub user_id: i64,
    pub content: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub metadata: String,
    pub sender: Sender,
    pub created_at_raw: String,
    /// Seconds since stream start.
    pub created_at_secs: i64,
    /// `"HH:MM:SS"` since stream start.
    pub created_at_str: String,
    /// Seconds since the requested range start.
    pub range_offset_secs: i64,
    /// `"HH:MM:SS"` since the requested range start.
    pub range_offset_str: String,
}

impl MessageSaved {
    pub(crate) fn from_message(
        msg: &Message,
        stream_start: DateTime<Utc>,
        range_start_ms: u64,
    ) -> Self {
        let created_at = DateTime::parse_from_rfc3339(&msg.created_at)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or(stream_start);

        let total_seconds = (created_at - stream_start).num_seconds().max(0);
        let range_seconds = (total_seconds - (range_start_ms as i64 / 1000)).max(0);

        fn fmt_hms(s: i64) -> String {
            format!("{:02}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
        }

        Self {
            id: msg.id.clone(),
            chat_id: msg.chat_id,
            user_id: msg.user_id,
            content: msg.content.clone(),
            kind: msg.kind.clone(),
            metadata: msg.metadata.clone(),
            sender: msg.sender.clone(),
            created_at_raw: msg.created_at.clone(),
            created_at_secs: total_seconds,
            created_at_str: fmt_hms(total_seconds),
            range_offset_secs: range_seconds,
            range_offset_str: fmt_hms(range_seconds),
        }
    }
}

// ---------------------------------------------------------------------------
// Chat data structures (crate-internal)
// ---------------------------------------------------------------------------

#[derive(Debug, Deser, Ser)]
pub(crate) struct Message {
    pub id: String,
    pub chat_id: i64,
    pub user_id: i64,
    pub content: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub metadata: String,
    pub sender: Sender,
    pub created_at: String,
}

#[derive(Debug, Deser, Ser)]
pub(crate) struct ChatData {
    pub messages: Vec<Message>,
}

#[derive(Debug, Deser, Ser)]
pub(crate) struct ChatResponse {
    pub data: ChatData,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Kick internal API types
// ---------------------------------------------------------------------------

#[derive(Debug, Deser, Ser, Clone, Default)]
pub(crate) struct Chatroom {
    pub id: Option<i64>,
}

#[derive(Debug, Deser, Ser, Clone, Default)]
pub(crate) struct User {
    pub username: Option<String>,
    #[serde(alias = "profilepic", alias = "profile_pic", default)]
    pub profile_pic: Option<String>,
    #[serde(default)]
    pub bio: Option<String>,
}

#[derive(Debug, Deser, Ser, Clone, Default)]
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

#[derive(Debug, Deser, Ser, Clone)]
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

// ---------------------------------------------------------------------------
// Kick thumbnail deserialisation helper
// ---------------------------------------------------------------------------

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

/// Handles `String`, `{responsive, url, src, …}`, or `[…]` thumbnail shapes.
pub(crate) fn deserialize_kick_thumbnail<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde_json::Value;
    let v = Value::deserialize(deserializer)?;
    Ok(match v {
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                None
            } else if s.contains(' ') && s.contains('w') {
                parse_srcset(s)
            } else {
                Some(s.to_string())
            }
        }
        Value::Object(map) => {
            let best = map
                .get("responsive")
                .or_else(|| map.get("srcset"))
                .and_then(|v| v.as_str())
                .and_then(parse_srcset);
            if best.is_some() {
                return Ok(best);
            }
            map.get("url")
                .or_else(|| map.get("src"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| {
                    map.values()
                        .filter_map(|v| v.as_str())
                        .find(|s| s.starts_with("http"))
                        .map(|s| s.to_string())
                })
        }
        Value::Array(arr) => arr.iter().find_map(|item| match item {
            Value::String(s) if s.starts_with("http") => Some(s.clone()),
            Value::Object(_) => item
                .get("url")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            _ => None,
        }),
        _ => None,
    })
}

// ---------------------------------------------------------------------------
// Kick API response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deser, Clone)]
pub(crate) struct KickVideoResponse {
    #[allow(dead_code)]
    pub uuid: Option<String>,
    pub views: Option<i64>,
    pub source: Option<String>,
    #[serde(alias = "playbackUrl", default)]
    pub playback_url: Option<String>,
    #[serde(default)]
    pub livestream: Option<Livestream>,
}

#[derive(Debug, Deser, Ser, Clone, Default)]
pub(crate) struct Livestream {
    pub id: Option<i64>,
    pub session_title: Option<String>,
    pub start_time: Option<String>,
    pub duration: Option<i64>,
    #[serde(deserialize_with = "deserialize_kick_thumbnail", default)]
    pub thumbnail: Option<String>,
    #[serde(rename = "viewer_count", alias = "viewerCount", default)]
    pub viewer_count: Option<i64>,
    pub is_live: Option<bool>,
    #[serde(default)]
    pub channel: Option<ChannelField>,
}

#[derive(Debug, Deser, Clone)]
pub(crate) struct KickChannelResponse {
    pub id: Option<i64>,
    pub user: Option<User>,
    pub chatroom: Option<Chatroom>,
    pub livestream: Option<Livestream>,
    #[serde(rename = "followersCount", alias = "followers_count")]
    pub followers_count: Option<i64>,
    pub playback_url: Option<String>,
}

#[derive(Debug, Deser, Clone)]
pub(crate) struct KickClipResponse {
    pub clip: Option<KickClipData>,
}

#[derive(Debug, Deser, Clone)]
pub(crate) struct KickClipData {
    pub title: Option<String>,
    pub thumbnail_url: Option<String>,
    pub views: Option<i64>,
    pub channel_id: Option<i64>,
    /// Duration in **seconds** as returned by the Kick API.
    pub duration: Option<f64>,
    pub started_at: Option<String>,
    pub created_at: Option<String>,
    pub video_url: Option<String>,
    pub channel: Option<KickClipChannel>,
}

#[derive(Debug, Deser, Clone)]
pub(crate) struct KickClipChannel {
    pub id: Option<i64>,
    pub username: Option<String>,
}

// ---------------------------------------------------------------------------
// Twitch GraphQL types
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Serialize)]
pub(crate) struct SimpleGqlQuery {
    pub(crate) query: String,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchGqlClipResponse {
    pub data: Option<TwitchGqlClipData>,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchGqlClipData {
    pub clip: Option<TwitchGqlClip>,
}

#[derive(Debug, Deser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlClip {
    pub video_offset_seconds: Option<f64>,
    pub duration_seconds: Option<f64>,
    pub video: Option<TwitchGqlVideoId>,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchGqlVideoId {
    pub id: Option<String>,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchGqlCommentsResponse {
    pub data: Option<TwitchGqlCommentsData>,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchGqlCommentsData {
    pub video: Option<TwitchGqlVideo>,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchGqlVideo {
    pub comments: Option<TwitchGqlCommentsConnection>,
}

#[derive(Debug, Deser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlCommentsConnection {
    pub edges: Option<Vec<TwitchGqlCommentEdge>>,
    pub page_info: Option<TwitchGqlPageInfo>,
}

#[derive(Debug, Deser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlCommentEdge {
    pub cursor: Option<String>,
    pub node: Option<TwitchGqlCommentNode>,
}

#[derive(Debug, Deser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlCommentNode {
    pub id: Option<String>,
    pub content_offset_seconds: Option<f64>,
    pub message: Option<TwitchGqlCommentMessage>,
    pub commenter: Option<TwitchGqlCommenter>,
}

#[derive(Debug, Deser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlCommentMessage {
    pub user_badges: Option<Vec<TwitchGqlUserBadge>>,
    pub user_color: Option<String>,
    pub fragments: Option<Vec<TwitchGqlMessageFragment>>,
}

#[derive(Debug, Deser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlUserBadge {
    #[serde(rename = "setID")]
    pub set_id: Option<String>,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchGqlMessageFragment {
    pub text: Option<String>,
}

#[derive(Debug, Deser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlCommenter {
    pub id: Option<String>,
    pub login: Option<String>,
    pub display_name: Option<String>,
}

#[derive(Debug, Deser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlPageInfo {
    pub has_next_page: Option<bool>,
}

#[derive(Ser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlRequest<'a> {
    pub operation_name: &'static str,
    pub variables: TwitchGqlVariables<'a>,
    pub extensions: TwitchGqlExtensions,
}

#[derive(Ser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlVariables<'a> {
    #[serde(rename = "videoID")]
    pub video_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_offset_seconds: Option<i64>,
}

#[derive(Ser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchGqlExtensions {
    pub persisted_query: PersistedQuery,
}

#[derive(Ser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PersistedQuery {
    pub version: u32,
    pub sha256_hash: &'static str,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchClipQueryResponse {
    pub data: TwitchClipQueryData,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchClipQueryData {
    pub clip: Option<TwitchClipDetails>,
}

#[derive(Debug, Deser)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TwitchClipDetails {
    pub title: Option<String>,
    pub duration_seconds: Option<i64>,
    pub view_count: Option<i64>,
    pub created_at: Option<String>,
    #[serde(rename = "thumbnailURL")]
    pub thumbnail_url: Option<String>,
    pub broadcaster: Option<TwitchBroadcaster>,
    pub video_qualities: Option<Vec<TwitchVideoQuality>>,
    pub playback_access_token: Option<TwitchPlaybackAccessToken>,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchBroadcaster {
    pub login: Option<String>,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchVideoQuality {
    #[serde(rename = "sourceURL")]
    pub source_url: Option<String>,
}

#[derive(Debug, Deser)]
pub(crate) struct TwitchPlaybackAccessToken {
    pub signature: Option<String>,
    pub value: Option<String>,
}
