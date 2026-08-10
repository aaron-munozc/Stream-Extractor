use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::future::join_all;
use rand::RngExt;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::fs as async_fs;
use tokio::io::{AsyncWriteExt, BufWriter as AsyncBufWriter};
use tokio::sync::mpsc;
use url::Url;

use crate::ProgressPayload;
use crate::client::StreamClient;
use crate::error::{Error, Result};
use crate::types::{
    ChatDownloadOptions, ChatResponse, ClipInfo, MessageSaved, PersistedQuery, Platform,
    TwitchGqlClipResponse, TwitchGqlCommentsResponse, TwitchGqlExtensions, TwitchGqlRequest,
    TwitchGqlVariables, VodInfo,
};

const TWITCH_GQL_CLIENT_ID:  &str = "kimne78kx3ncx6brgo4mv6wki5h1ko";
const TWITCH_CHAT_CLIENT_ID: &str = "kd1unb4b3q4t58fwlpcbzcbnm76a8fp";

const SAVE_CHANNEL_CAPACITY: usize = 4096;
const KICK_STEP_SECS:        i64   = 5;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn to_kick_timestamp(dt: DateTime<Utc>) -> String {
    format!(
        "{}.{:03}Z",
        dt.format("%Y-%m-%dT%H:%M:%S"),
        dt.timestamp_subsec_millis()
    )
}

/// Checks a cancel receiver and returns `Err(Cancelled)` if it has been set.
#[inline]
fn check_cancel(rx: Option<&tokio::sync::watch::Receiver<bool>>) -> Result<()> {
    if rx.is_some_and(|rx| *rx.borrow()) {
        Err(Error::Cancelled("User requested abort".into()))
    } else {
        Ok(())
    }
}

async fn fetch_json_with_retries(
    client: &StreamClient,
    url: &str,
    max_tries: usize,
    cancel_rx: Option<&tokio::sync::watch::Receiver<bool>>,
) -> Result<ChatResponse> {
    let mut attempt = 0usize;
    loop {
        check_cancel(cancel_rx)?;

        match client.inner.get(url).header("Accept", "application/json").send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.as_u16() == 429 {
                    attempt += 1;
                    if attempt > max_tries {
                        return Err(Error::RateLimited);
                    }
                    let wait = resp
                        .headers()
                        .get("retry-after")
                        .and_then(|h| h.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(2);
                    tokio::time::sleep(std::time::Duration::from_secs(wait + 1)).await;
                    continue;
                } else if status.is_client_error() {
                    return Err(Error::InvalidUrl(url.to_string()));
                } else {
                    let body = resp.text().await?;
                    return serde_json::from_str::<ChatResponse>(&body).map_err(Error::Json);
                }
            }
            Err(e) => {
                attempt += 1;
                if attempt > max_tries {
                    return Err(Error::Network(e));
                }
            }
        }

        let base_ms = 200u64;
        let exp = 2u64.saturating_pow(attempt.min(6) as u32);
        let backoff = base_ms.saturating_mul(exp);
        let jitter: u64 = rand::rng().random_range(0..=(backoff / 4));
        tokio::time::sleep(std::time::Duration::from_millis(
            (backoff + jitter).min(10_000),
        ))
            .await;
    }
}

/// Build the output path, creating parent directories on disk.
fn resolve_output_path(
    options: &ChatDownloadOptions,
    platform: &Platform,
    username: Option<&str>,
    id_marker: &str,
) -> Result<PathBuf> {
    let target_dir = options
        .output_dir
        .clone()
        .or_else(dirs::download_dir)
        .or_else(dirs::document_dir)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());

    let base_name = options.output_name.clone().unwrap_or_else(|| {
        let safe_user = username
            .unwrap_or("streamer")
            .replace(|c: char| !c.is_alphanumeric(), "_");
        format!("{platform}_{safe_user}_{id_marker}")
    });

    let name = if base_name.ends_with(".jsonl") {
        base_name
    } else {
        format!("{base_name}.jsonl")
    };

    Ok(target_dir.join(name))
}

// ---------------------------------------------------------------------------
// Async writer task — shared between VOD and clip chat downloads
// ---------------------------------------------------------------------------

/// Spawns a Tokio task that receives JSONL lines and writes them to `path`.
///
/// Returns `(sender, task_handle, io_error_receiver)`.
/// The caller drains the sender, awaits the handle, then checks the error
/// channel — the same pattern used in both VOD and clip flows.
fn spawn_writer_task(
    path: &Path,
) -> (
    mpsc::Sender<String>,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<std::io::Error>,
) {
    let (tx, mut rx) = mpsc::channel::<String>(SAVE_CHANNEL_CAPACITY);
    let (err_tx, err_rx) = tokio::sync::oneshot::channel::<std::io::Error>();
    let path = path.to_path_buf();

    let handle = tokio::spawn(async move {
        let file = match async_fs::File::create(&path).await {
            Ok(f) => f,
            Err(e) => {
                let _ = err_tx.send(e);
                return;
            }
        };
        let mut buf = AsyncBufWriter::new(file);
        while let Some(line) = rx.recv().await {
            if buf.write_all(line.as_bytes()).await.is_err()
                || buf.write_all(b"\n").await.is_err()
            {
                break;
            }
        }
        let _ = buf.flush().await;
    });

    (tx, handle, err_rx)
}

// ---------------------------------------------------------------------------
// Twitch chat
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn download_twitch_chat_inner(
    client: &StreamClient,
    video_id: &str,
    clip_offset_sec: f64,
    clip_duration_sec: f64,
    stream_start: DateTime<Utc>,
    start_offset_ms: u64,
    mut effective_end_ms: u64,
    buffer: u64,
    options: &ChatDownloadOptions,
    tx: mpsc::Sender<String>,
    seen_msg_ids: &mut HashSet<String>,
) -> Result<()> {
    if clip_duration_sec > 0.0 && options.end_ms.is_none() {
        effective_end_ms = (clip_duration_sec * 1000.0) as u64 + buffer;
    }

    let window_length_ms = effective_end_ms.saturating_sub(start_offset_ms);
    let mut offset_sec = clip_offset_sec + (start_offset_ms as f64 / 1000.0);
    let mut cursor: Option<String> = None;
    let mut consecutive_empty = 0usize;

    let absolute_end_ms = if effective_end_ms > 0 {
        (clip_offset_sec * 1000.0) as u64 + effective_end_ms
    } else {
        0
    };

    let report = |payload: ProgressPayload| {
        if let Some(ref hook) = options.progress_hook { hook(payload); }
    };

    loop {
        check_cancel(options.cancel_rx.as_ref())?;

        let body = TwitchGqlRequest {
            operation_name: "VideoCommentsByOffsetOrCursor",
            variables: TwitchGqlVariables {
                video_id,
                cursor: cursor.as_deref(),
                content_offset_seconds: cursor.is_none().then(|| offset_sec.floor() as i64),
            },
            extensions: TwitchGqlExtensions {
                persisted_query: PersistedQuery {
                    version:     1,
                    sha256_hash: "b70a3591ff0f4e0313d126c6a1502d79a1c02baebb288227c582044aa76adf6a",
                },
            },
        };

        let resp = client
            .inner
            .post("https://gql.twitch.tv/gql")
            .header("Client-ID", TWITCH_CHAT_CLIENT_ID)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            break;
        }

        let parsed: TwitchGqlCommentsResponse = resp.json().await?;

        let (edges, page_info) = parsed
            .data
            .and_then(|d| d.video)
            .and_then(|v| v.comments)
            .map(|c| (c.edges.unwrap_or_default(), c.page_info))
            .unwrap_or_default();

        if edges.is_empty() {
            consecutive_empty += 1;
            if consecutive_empty >= 30 {
                break;
            }
        } else {
            consecutive_empty = 0;
            let mut max_page_offset = offset_sec;

            for edge in &edges {
                let node = match &edge.node {
                    Some(n) => n,
                    None => continue,
                };

                let offset = node.content_offset_seconds.unwrap_or(0.0);
                let absolute_msg_ms = offset * 1000.0;

                if offset > max_page_offset {
                    max_page_offset = offset;
                }

                if absolute_msg_ms < (clip_offset_sec * 1000.0 + start_offset_ms as f64) {
                    continue;
                }
                if absolute_end_ms > 0 && absolute_msg_ms > absolute_end_ms as f64 {
                    continue;
                }

                let msg_id = node.id.clone().unwrap_or_default();
                if msg_id.is_empty() || !seen_msg_ids.insert(msg_id.clone()) {
                    continue;
                }

                let mut badges = Vec::new();
                let mut content = String::new();
                let mut user_color = String::new();

                if let Some(msg_data) = &node.message {
                    user_color = msg_data.user_color.clone().unwrap_or_default();

                    if let Some(arr) = &msg_data.user_badges {
                        for b in arr {
                            let set_id = b.set_id.clone().unwrap_or_default();
                            let text = match set_id.as_str() {
                                "broadcaster" => "👑",
                                "moderator"   => "⚔",
                                "subscriber"  => "★",
                                "staff"       => "⛨",
                                _             => "",
                            }
                                .to_string();
                            badges.push(crate::types::Badge { kind: set_id, text });
                        }
                    }

                    if let Some(frags) = &msg_data.fragments {
                        content = frags.iter().filter_map(|f| f.text.as_deref()).collect();
                    }
                }

                let commenter_id: i64 = node
                    .commenter
                    .as_ref()
                    .and_then(|c| c.id.as_deref())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| {
                        log::warn!("Failed to parse commenter ID, defaulting to 0");
                        0
                    });

                let commenter_login = node
                    .commenter
                    .as_ref()
                    .and_then(|c| c.login.clone())
                    .unwrap_or_default();

                let commenter_name = node
                    .commenter
                    .as_ref()
                    .and_then(|c| c.display_name.clone())
                    .unwrap_or_else(|| commenter_login.clone());

                let msg = crate::types::Message {
                    id:      msg_id,
                    chat_id: video_id.parse().unwrap_or_else(|_| {
                        log::warn!("Failed to parse chat ID from video ID, defaulting to 0");
                        0
                    }),
                    user_id:  commenter_id,
                    content,
                    kind:     "chat".into(),
                    metadata: String::new(),
                    sender: crate::types::Sender {
                        id:       commenter_id,
                        slug:     commenter_login,
                        username: commenter_name,
                        identity: crate::types::Identity {
                            color:  user_color,
                            badges,
                        },
                    },
                    created_at: (stream_start
                        + ChronoDuration::milliseconds(absolute_msg_ms as i64))
                        .to_rfc3339(),
                };

                let _ = tx
                    .send(serde_json::to_string(&MessageSaved::from_message(
                        &msg,
                        stream_start,
                        start_offset_ms,
                    ))?)
                    .await;
            }

            if window_length_ms > 0 {
                let current_ms = (max_page_offset * 1000.0) - (clip_offset_sec * 1000.0);
                let pct = ((current_ms - start_offset_ms as f64) / window_length_ms as f64 * 100.0)
                    .clamp(0.0, 100.0);
                report(ProgressPayload::Downloading {
                    percent: pct as u8,
                    message: "Paginating Twitch chat...".into(),
                });
            }

            if absolute_end_ms > 0 && (max_page_offset * 1000.0) > absolute_end_ms as f64 {
                break;
            }

            let has_next = page_info.and_then(|p| p.has_next_page).unwrap_or(false);
            if has_next {
                match edges.last().and_then(|e| e.cursor.as_ref()) {
                    Some(c) => cursor = Some(c.clone()),
                    None    => break,
                }
            } else {
                break;
            }

            offset_sec = max_page_offset;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Kick chat
// ---------------------------------------------------------------------------

async fn download_kick_chat_inner(
    client: &StreamClient,
    chat_id: i64,
    stream_start: DateTime<Utc>,
    start_offset_ms: u64,
    effective_end_ms: u64,
    options: &ChatDownloadOptions,
    tx: mpsc::Sender<String>,
    seen_msg_ids: &mut HashSet<String>,
) -> Result<()> {
    let window_length_ms = effective_end_ms.saturating_sub(start_offset_ms);
    let aligned_start = (start_offset_ms as i64 / KICK_STEP_SECS) * KICK_STEP_SECS;
    let mut next_start = stream_start + ChronoDuration::milliseconds(aligned_start);
    let mut empty_cycles = 0usize;

    let kick_opts = options.kick_options();

    let report = |payload: ProgressPayload| {
        if let Some(ref hook) = options.progress_hook { hook(payload); }
    };

    loop {
        check_cancel(options.cancel_rx.as_ref())?;

        // Build a batch of start timestamps.
        let mut starts = Vec::with_capacity(kick_opts.concurrency);
        let mut candidate = next_start;
        for _ in 0..kick_opts.concurrency {
            if effective_end_ms > 0
                && (candidate - stream_start).num_milliseconds() as u64 >= effective_end_ms
            {
                break;
            }
            starts.push(candidate);
            candidate += ChronoDuration::seconds(KICK_STEP_SECS);
        }
        if starts.is_empty() {
            break;
        }

        let futs: Vec<_> = starts
            .iter()
            .map(|st| {
                let mut url = Url::parse(&format!(
                    "https://web.kick.com/api/v1/chat/{chat_id}/history"
                ))
                    .expect("static URL is valid");
                url.query_pairs_mut()
                   .append_pair("start_time", &to_kick_timestamp(*st));
                let url_str = url.to_string();
                let cancel_rx = options.cancel_rx.clone();
                let cl = client.clone();
                let max_retries = options.max_retries;
                async move {
                    fetch_json_with_retries(&cl, &url_str, max_retries, cancel_rx.as_ref()).await
                }
            })
            .collect();

        let results = join_all(futs).await;
        let mut got_messages = false;

        for res in results {
            let resp = match res {
                Ok(r) if r.message == "OK" && !r.data.messages.is_empty() => r,
                _ => continue,
            };
            got_messages = true;
            for m in &resp.data.messages {
                if seen_msg_ids.insert(m.id.clone()) {
                    let _ = tx
                        .send(serde_json::to_string(&MessageSaved::from_message(
                            m,
                            stream_start,
                            start_offset_ms,
                        ))?)
                        .await;
                }
            }
        }

        if got_messages {
            empty_cycles = 0;
        } else {
            empty_cycles += 1;
            if effective_end_ms == 0 && empty_cycles >= kick_opts.empty_cycle_threshold {
                break;
            }
        }

        next_start = candidate;

        if window_length_ms > 0 {
            let elapsed = (next_start - stream_start).num_milliseconds() as f64
                - start_offset_ms as f64;
            let pct = (elapsed / window_length_ms as f64 * 100.0).clamp(0.0, 100.0);
            report(ProgressPayload::Downloading {
                percent: pct as u8,
                message: "Fetching Kick chat buckets...".into(),
            });
        }

        if !got_messages {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Shared finalisation helper
// ---------------------------------------------------------------------------

/// Await the writer task, check for I/O errors, then atomically rename the
/// temp file to the final path.
async fn finalise_chat_file(
    writer_task: tokio::task::JoinHandle<()>,
    mut err_rx: tokio::sync::oneshot::Receiver<std::io::Error>,
    tmp_path: &Path,
    final_path: &Path,
    report: impl Fn(ProgressPayload),
) -> Result<PathBuf> {
    writer_task.await.ok();
    if let Ok(e) = err_rx.try_recv() {
        return Err(Error::Io(e));
    }
    async_fs::rename(tmp_path, final_path).await?;
    report(ProgressPayload::Done);
    Ok(final_path.to_path_buf())
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Download chat for a VOD (Twitch or Kick).
pub(crate) async fn download_vod_chat(
    client: &StreamClient,
    vod: &VodInfo,
    options: ChatDownloadOptions,
) -> Result<PathBuf> {
    let report = |payload: ProgressPayload| {
        if let Some(ref hook) = options.progress_hook { hook(payload); }
    };

    report(ProgressPayload::Downloading {
        percent: 0,
        message: "Initializing chat download...".into(),
    });

    let stream_start      = vod.start_time.unwrap_or_else(Utc::now);
    let duration_ms       = vod.duration.unwrap_or(0) as u64 * 1000;
    let start_offset_ms   = options.start_ms.unwrap_or(0);
    let buffer            = options.buffer_ms.unwrap_or(0);
    let effective_end_ms  = options.end_ms.map(|e| e + buffer).unwrap_or_else(|| {
        if duration_ms > 0 { duration_ms + buffer } else { 0 }
    });

    let final_path = resolve_output_path(&options, &vod.platform, vod.username.as_deref(), &vod.vod_id)?;
    if let Some(parent) = final_path.parent() {
        async_fs::create_dir_all(parent).await?;
    }

    let tmp_path = final_path.with_extension("jsonl.tmp");
    let (tx, writer_task, err_rx) = spawn_writer_task(&tmp_path);
    let mut seen_msg_ids = HashSet::new();

    match vod.platform {
        Platform::Twitch => {
            download_twitch_chat_inner(
                client, &vod.vod_id, 0.0, 0.0,
                stream_start, start_offset_ms, effective_end_ms, buffer,
                &options, tx, &mut seen_msg_ids,
            ).await?;
        }
        Platform::Kick => {
            let chat_id = vod.chat_id.ok_or(Error::MissingId)?;
            download_kick_chat_inner(
                client, chat_id,
                stream_start, start_offset_ms, effective_end_ms,
                &options, tx, &mut seen_msg_ids,
            ).await?;
        }
    }

    finalise_chat_file(writer_task, err_rx, &tmp_path, &final_path, report).await
}

/// Download chat for a clip.
///
/// For Twitch clips the slug is resolved to its parent VOD ID + offset via
/// GQL. For Kick clips, the clip window is fetched as a time range of the
/// parent chatroom.
pub(crate) async fn download_clip_chat(
    client: &StreamClient,
    clip: &ClipInfo,
    options: ChatDownloadOptions,
) -> Result<PathBuf> {
    let report = |payload: ProgressPayload| {
        if let Some(ref hook) = options.progress_hook { hook(payload); }
    };

    report(ProgressPayload::Downloading {
        percent: 0,
        message: "Initializing clip chat download...".into(),
    });

    let stream_start    = clip.start_time.unwrap_or_else(Utc::now);
    let start_offset_ms = options.start_ms.unwrap_or(0);
    let buffer          = options.buffer_ms.unwrap_or(0);

    let final_path = resolve_output_path(&options, &clip.platform, clip.username.as_deref(), &clip.clip_id)?;
    if let Some(parent) = final_path.parent() {
        async_fs::create_dir_all(parent).await?;
    }

    let tmp_path = final_path.with_extension("jsonl.tmp");
    let (tx, writer_task, err_rx) = spawn_writer_task(&tmp_path);
    let mut seen_msg_ids = HashSet::new();

    match clip.platform {
        Platform::Twitch => {
            report(ProgressPayload::Downloading {
                percent: 0,
                message: "Resolving Twitch clip to parent VOD...".into(),
            });

            let clip_query = serde_json::json!({
                "query": format!(
                    "query{{clip(slug:\"{}\"){{videoOffsetSeconds,durationSeconds,video{{id}}}}}}",
                    clip.clip_id
                )
            });

            let parsed: TwitchGqlClipResponse = client
                .inner
                .post("https://gql.twitch.tv/gql")
                .header("Client-ID", TWITCH_GQL_CLIENT_ID)
                .json(&clip_query)
                .send()
                .await?
                .json()
                .await?;

            let clip_node = parsed
                .data
                .and_then(|d| d.clip)
                .ok_or_else(|| Error::InvalidUrl("Invalid Twitch clip slug or API error.".into()))?;

            let video_id = clip_node
                .video
                .and_then(|v| v.id)
                .ok_or_else(|| Error::InvalidUrl("Clip has no associated VOD.".into()))?;

            let clip_offset_sec   = clip_node.video_offset_seconds.unwrap_or(0.0);
            let clip_duration_sec = clip_node.duration_seconds.unwrap_or(0.0);
            let effective_end_ms  = options
                .end_ms
                .map(|e| e + buffer)
                .unwrap_or_else(|| (clip_duration_sec * 1000.0) as u64 + buffer);

            download_twitch_chat_inner(
                client, &video_id,
                clip_offset_sec, clip_duration_sec,
                stream_start, start_offset_ms, effective_end_ms, buffer,
                &options, tx, &mut seen_msg_ids,
            ).await?;
        }

        Platform::Kick => {
            let chat_id        = clip.chat_id.ok_or(Error::MissingId)?;
            let clip_dur_ms    = clip.duration.unwrap_or(0) as u64 * 1000;
            let effective_end_ms = options
                .end_ms
                .map(|e| e + buffer)
                .unwrap_or(clip_dur_ms + buffer);

            download_kick_chat_inner(
                client, chat_id,
                stream_start, start_offset_ms, effective_end_ms,
                &options, tx, &mut seen_msg_ids,
            ).await?;
        }
    }

    finalise_chat_file(writer_task, err_rx, &tmp_path, &final_path, report).await
}