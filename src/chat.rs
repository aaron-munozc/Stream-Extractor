use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::future::join_all;
use rand::RngExt;
use std::collections::HashSet;
use tokio::fs as async_fs;
use tokio::io::{AsyncWriteExt, BufWriter as AsyncBufWriter};
use tokio::sync::mpsc;
use url::Url;

use crate::ProgressPayload;
use crate::client::StreamClient;
use crate::error::{Error, Result};
use crate::types::{ChatOptions, ChatResponse, MessageEnriched, Platform, StreamMetadata};

const SAVE_CHANNEL_CAPACITY: usize = 4096;
const KICK_STEP_SECS: i64 = 5;

fn to_kick_timestamp(dt: DateTime<Utc>) -> String {
    let secs = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let ms = dt.timestamp_subsec_millis();
    format!("{}.{:03}Z", secs, ms)
}

async fn fetch_json_with_retries(
    client: &StreamClient,
    url: &str,
    max_tries: usize,
    cancel_rx: Option<&tokio::sync::watch::Receiver<bool>>,
) -> Result<ChatResponse> {
    let mut attempt = 0;
    loop {
        if let Some(rx) = cancel_rx
            && *rx.borrow()
        {
            return Err(Error::Cancelled("User requested abort".into()));
        }

        match client
            .inner
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                if status.as_u16() == 429 {
                    attempt += 1;
                    if attempt > max_tries {
                        return Err(Error::RateLimited);
                    }

                    if let Some(ra) = resp
                        .headers()
                        .get("retry-after")
                        .and_then(|h| h.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        tokio::time::sleep(std::time::Duration::from_secs(ra + 1)).await;
                        continue;
                    }
                } else if status.is_client_error() {
                    return Err(Error::InvalidUrl(url.to_string()));
                } else {
                    let body = resp.text().await?;
                    return match serde_json::from_str::<ChatResponse>(&body) {
                        Ok(parsed) => Ok(parsed),
                        Err(e) => Err(Error::Json(e)),
                    };
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
        let backoff_ms = base_ms.saturating_mul(exp);
        let jitter: u64 = rand::rng().random_range(0..=(backoff_ms / 4));
        tokio::time::sleep(std::time::Duration::from_millis(
            (backoff_ms + jitter).min(10_000),
        ))
        .await;
    }
}

pub(crate) async fn download_chat_internal(
    client: &StreamClient,
    meta: &StreamMetadata,
    options: ChatOptions,
) -> Result<()> {
    let report = |payload: ProgressPayload| {
        if let Some(ref hook) = options.progress_hook {
            hook(payload);
        }
    };

    report(ProgressPayload::Downloading {
        percent: 0,
        message: "Initializing chat targets...".into(),
    });

    let stream_start = meta
        .start_time
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);

    let duration_ms = meta.duration.unwrap_or(0) as u64 * 1000;
    let start_offset_ms = options.start_ms.unwrap_or(0);
    let buffer = options.buffer_ms.unwrap_or(0);

    let mut effective_end_ms = options.end_ms.map(|e| e + buffer).unwrap_or_else(|| {
        if duration_ms > 0 {
            (duration_ms) + buffer
        } else {
            0
        }
    });

    let target_dir = options
        .output_dir
        .clone()
        .or_else(dirs::download_dir)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());

    let base_name = options.output_name.clone().unwrap_or_else(|| {
        let safe_username = meta
            .username
            .as_deref()
            .unwrap_or("streamer")
            .replace(|c: char| !c.is_alphanumeric(), "_");
        let id_marker = meta.vod_uuid.as_deref().unwrap_or("chat");
        format!("{}_{}_{}", meta.platform, safe_username, id_marker)
    });

    let target_name = if base_name.ends_with(".jsonl") {
        base_name
    } else {
        format!("{}.jsonl", base_name)
    };

    let final_output_path = target_dir.join(target_name);

    if let Some(parent) = final_output_path.parent() {
        async_fs::create_dir_all(parent).await?;
    }

    let (tx, mut rx) = mpsc::channel::<String>(SAVE_CHANNEL_CAPACITY);
    let tmp_path = final_output_path.with_extension("jsonl.tmp");
    let writer_tmp = tmp_path.clone();

    let writer_task = tokio::spawn(async move {
        let file = async_fs::File::create(&writer_tmp).await.unwrap();
        let mut buf_writer = AsyncBufWriter::new(file);
        while let Some(line) = rx.recv().await {
            let _ = buf_writer.write_all(line.as_bytes()).await;
            let _ = buf_writer.write_all(b"\n").await;
        }
        let _ = buf_writer.flush().await;
    });

    let mut seen_msg_ids = HashSet::new();

    if meta.platform == Platform::Twitch {
        let mut video_id = meta.vod_uuid.clone().ok_or(Error::MissingId)?;
        let is_clip = !video_id.chars().all(char::is_numeric);

        let twitch_client = reqwest::Client::new();

        let (clip_offset_sec, clip_duration_sec) = if is_clip {
            report(ProgressPayload::Downloading {
                percent: 0,
                message: "Resolving Twitch Clip...".into(),
            });

            let clip_query = serde_json::json!({
                "query": format!("query{{clip(slug:\"{}\"){{videoOffsetSeconds,durationSeconds,video{{id}}}}}}", video_id)
            });

            let resp = twitch_client
                .post("https://gql.twitch.tv/gql")
                .header("Client-ID", "kimne78kx3ncx6brgo4mv6wki5h1ko")
                .json(&clip_query)
                .send()
                .await?;

            let val: serde_json::Value = resp.json().await?;

            let clip_node = val
                .get("data")
                .and_then(|d| d.get("clip"))
                .and_then(|c| c.as_object())
                .ok_or_else(|| {
                    Error::InvalidUrl("Invalid Twitch clip slug or API error.".into())
                })?;

            let v_id = clip_node
                .get("video")
                .and_then(|v| v.get("id"))
                .and_then(|i| i.as_str())
                .ok_or_else(|| Error::InvalidUrl("Clip has no associated VOD.".into()))?;

            video_id = v_id.to_string();

            let offset = clip_node
                .get("videoOffsetSeconds")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let duration = clip_node
                .get("durationSeconds")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            (offset, duration)
        } else {
            (0.0, 0.0)
        };

        if is_clip && options.end_ms.is_none() {
            effective_end_ms = (clip_duration_sec * 1000.0) as u64 + buffer;
        }

        let window_length_ms = effective_end_ms.saturating_sub(start_offset_ms);

        let mut offset_sec = clip_offset_sec + (start_offset_ms as f64) / 1000.0;
        let mut cursor: Option<String> = None;
        let mut consecutive_empty = 0;

        let absolute_end_ms = if effective_end_ms > 0 {
            (clip_offset_sec * 1000.0) as u64 + effective_end_ms
        } else {
            0
        };

        loop {
            if let Some(ref rx_cancel) = options.cancel_rx
                && *rx_cancel.borrow()
            {
                return Err(Error::Cancelled("User requested abort".into()));
            }

            let body = if let Some(ref cur) = cursor {
                serde_json::json!({
                    "operationName": "VideoCommentsByOffsetOrCursor",
                    "variables": { "videoID": video_id, "cursor": cur },
                    "extensions": { "persistedQuery": { "version": 1, "sha256Hash": "b70a3591ff0f4e0313d126c6a1502d79a1c02baebb288227c582044aa76adf6a" } }
                })
            } else {
                serde_json::json!({
                    "operationName": "VideoCommentsByOffsetOrCursor",
                    "variables": { "videoID": video_id, "contentOffsetSeconds": offset_sec.floor() as i64 },
                    "extensions": { "persistedQuery": { "version": 1, "sha256Hash": "b70a3591ff0f4e0313d126c6a1502d79a1c02baebb288227c582044aa76adf6a" } }
                })
            };

            let resp = twitch_client
                .post("https://gql.twitch.tv/gql")
                .header("Client-ID", "kd1unb4b3q4t58fwlpcbzcbnm76a8fp")
                .json(&body)
                .send()
                .await?;

            if !resp.status().is_success() {
                break;
            }
            let val: serde_json::Value = resp.json().await?;

            let edges = val
                .get("data")
                .and_then(|d| d.get("video"))
                .and_then(|v| v.get("comments"))
                .and_then(|c| c.get("edges"))
                .and_then(|e| e.as_array())
                .cloned()
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
                    let node = &edge["node"];
                    let offset = node["contentOffsetSeconds"].as_f64().unwrap_or(0.0);
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

                    let msg_id = node["id"].as_str().unwrap_or("").to_string();
                    if msg_id.is_empty() || !seen_msg_ids.insert(msg_id.clone()) {
                        continue;
                    }

                    let mut badges = Vec::new();
                    if let Some(arr) = node["message"]["userBadges"].as_array() {
                        for b in arr {
                            let text = match b["setID"].as_str().unwrap_or("") {
                                "broadcaster" => "👑",
                                "moderator" => "⚔",
                                "subscriber" => "★",
                                "staff" => "⛨",
                                _ => "",
                            }
                            .to_string();
                            badges.push(crate::types::Badge {
                                r#type: b["setID"].as_str().unwrap_or("").into(),
                                text,
                            });
                        }
                    }

                    let content = node["message"]["fragments"]
                        .as_array()
                        .map(|f| {
                            f.iter()
                                .filter_map(|x| x["text"].as_str())
                                .collect::<String>()
                        })
                        .unwrap_or_default();
                    let commenter = &node["commenter"];

                    let msg = crate::types::Message {
                        id: msg_id,
                        chat_id: video_id.parse().unwrap_or(0),
                        user_id: commenter["id"].as_str().unwrap_or("0").parse().unwrap_or(0),
                        content,
                        r#type: "chat".into(),
                        metadata: "".into(),
                        sender: crate::types::Sender {
                            id: commenter["id"].as_str().unwrap_or("0").parse().unwrap_or(0),
                            slug: commenter["login"].as_str().unwrap_or("").into(),
                            username: commenter["displayName"]
                                .as_str()
                                .unwrap_or(commenter["login"].as_str().unwrap_or(""))
                                .into(),
                            identity: crate::types::Identity {
                                color: node["message"]["userColor"].as_str().unwrap_or("").into(),
                                badges,
                            },
                        },
                        created_at: (stream_start
                            + ChronoDuration::milliseconds(absolute_msg_ms as i64))
                        .to_rfc3339(),
                    };

                    let _ = tx
                        .send(serde_json::to_string(&MessageEnriched::from_message(
                            &msg,
                            stream_start,
                        ))?)
                        .await;
                }

                if window_length_ms > 0 {
                    let current_ms = (max_page_offset * 1000.0) - (clip_offset_sec * 1000.0);
                    let pct = (((current_ms - start_offset_ms as f64) / window_length_ms as f64)
                        * 100.0)
                        .clamp(0.0, 100.0);
                    report(ProgressPayload::Downloading {
                        percent: pct as i64,
                        message: "Paginating Twitch chat...".into(),
                    });
                }

                if absolute_end_ms > 0 && (max_page_offset * 1000.0) > absolute_end_ms as f64 {
                    break;
                }

                let page_info = val
                    .get("data")
                    .and_then(|d| d.get("video"))
                    .and_then(|v| v.get("comments"))
                    .and_then(|c| c.get("pageInfo"));

                let has_next = page_info
                    .and_then(|p| p.get("hasNextPage"))
                    .and_then(|h| h.as_bool())
                    .unwrap_or(false);

                if has_next {
                    if let Some(last_edge) = edges.last() {
                        if let Some(c) = last_edge.get("cursor").and_then(|cur| cur.as_str()) {
                            cursor = Some(c.to_string());
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }

                offset_sec = max_page_offset;
            }
        }
    } else {
        let window_length_ms = effective_end_ms.saturating_sub(start_offset_ms);
        let chat_id = meta.chat_id.ok_or(Error::MissingId)?;
        let aligned_start = (start_offset_ms as i64 / KICK_STEP_SECS) * KICK_STEP_SECS;
        let mut next_start = stream_start + ChronoDuration::milliseconds(aligned_start);
        let mut empty_cycles = 0;

        loop {
            if let Some(ref rx_cancel) = options.cancel_rx
                && *rx_cancel.borrow()
            {
                return Err(Error::Cancelled("User requested abort".into()));
            }

            let mut starts = Vec::new();
            let mut candidate = next_start;
            for _ in 0..options.kick_concurrency {
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

            let mut futs = Vec::new();
            for st in &starts {
                let mut url = Url::parse(&format!(
                    "https://web.kick.com/api/v1/chat/{}/history",
                    chat_id
                ))?;
                url.query_pairs_mut()
                    .append_pair("start_time", &to_kick_timestamp(*st));
                let url_str = url.to_string();
                let cancel_ref = options.cancel_rx.clone();
                let cl = client.clone();
                futs.push(async move {
                    fetch_json_with_retries(&cl, &url_str, options.max_retries, cancel_ref.as_ref())
                        .await
                });
            }

            let results = join_all(futs).await;
            let mut got_messages = false;

            for res in results {
                if let Ok(resp) = res
                    && resp.message == "OK"
                    && !resp.data.messages.is_empty()
                {
                    got_messages = true;
                    for m in &resp.data.messages {
                        if seen_msg_ids.insert(m.id.clone()) {
                            let _ = tx
                                .send(serde_json::to_string(&MessageEnriched::from_message(
                                    m,
                                    stream_start,
                                ))?)
                                .await;
                        }
                    }
                }
            }

            if got_messages {
                empty_cycles = 0;
            } else {
                empty_cycles += 1;
                if effective_end_ms == 0 && empty_cycles >= options.empty_cycle_threshold {
                    break;
                }
            }

            next_start = candidate;

            if window_length_ms > 0 {
                let elapsed =
                    (next_start - stream_start).num_milliseconds() as f64 - start_offset_ms as f64;
                let pct = ((elapsed / window_length_ms as f64) * 100.0).clamp(0.0, 100.0);
                report(ProgressPayload::Downloading {
                    percent: pct as i64,
                    message: "Fetching Kick chat buckets...".into(),
                });
            }

            if !got_messages {
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            }
        }
    }

    drop(tx);
    let _ = writer_task.await;
    async_fs::rename(&tmp_path, &final_output_path).await?;

    report(ProgressPayload::Done);
    Ok(())
}
