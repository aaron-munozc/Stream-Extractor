use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::future::join_all;
use rand::RngExt;
use std::collections::HashSet;
use tokio::fs as async_fs;
use tokio::io::{AsyncWriteExt, BufWriter as AsyncBufWriter};
use tokio::sync::mpsc;
use url::Url;

use crate::client::StreamClient;
use crate::error::{Error, Result};
use crate::types::{ChatOptions, ChatResponse, MessageEnriched, Platform, ProgressPayload, StreamMetadata};

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
        if let Some(rx) = cancel_rx {
            if *rx.borrow() { return Err(Error::Cancelled("User requested abort".into())); }
        }

        match client.inner.get(url).header("Accept", "application/json").send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.as_u16() == 429 {
                    attempt += 1; // Increment attempt here before checking max_tries
                    if attempt > max_tries { return Err(Error::RateLimited); }

                    if let Some(ra) = resp.headers().get("retry-after").and_then(|h| h.to_str().ok()).and_then(|s| s.parse::<u64>().ok()) {
                        tokio::time::sleep(std::time::Duration::from_secs(ra + 1)).await;
                        continue;
                    }
                    // If no Retry-After header exists, drop through to use the fallback exponential backoff below!
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
                if attempt > max_tries { return Err(Error::Network(e)); }
            }
        }

        // --- Cast to u32 here fixes the E0308 error ---
        let base_ms = 200u64;
        let exp = 2u64.saturating_pow(attempt.min(6) as u32);

        let backoff_ms = base_ms.saturating_mul(exp);
        let jitter: u64 = rand::rng().random_range(0..=(backoff_ms / 4));
        tokio::time::sleep(std::time::Duration::from_millis((backoff_ms + jitter).min(10_000))).await;
    }
}

pub async fn download_chat_internal(
    client: &StreamClient,
    meta: &StreamMetadata,
    options: ChatOptions,
) -> Result<()> {
    let report = |payload: ProgressPayload| {
        if let Some(ref hook) = options.progress_hook { hook(payload); }
    };

    report(ProgressPayload::Downloading { percent: 0, message: "Initializing chat targets...".into() });

    let start_time_str = meta.start_time.as_deref().ok_or(Error::TimeParse("Missing stream start_time".into()))?;
    let stream_start = DateTime::parse_from_rfc3339(start_time_str)
        .map_err(|e| Error::TimeParse(e.to_string()))?.with_timezone(&Utc);

    let duration_ms = meta.duration.unwrap_or(0);
    let start_offset_ms = options.start_ms.unwrap_or(0);
    let buffer = options.buffer_ms.unwrap_or(0);

    let mut effective_end_ms = options.end_ms.map(|e| e + buffer).unwrap_or_else(|| if duration_ms > 0 { (duration_ms as u64) + buffer } else { 0 });
    let window_length_ms = if effective_end_ms > start_offset_ms { effective_end_ms - start_offset_ms } else { 0 };

    let target_dir = options.output_dir.clone().unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
    let target_name = options.output_name.clone().unwrap_or_else(|| {
        let safe_username = meta.username.as_deref().unwrap_or("streamer").replace(|c: char| !c.is_alphanumeric(), "_");
        let id_marker = meta.vod_uuid.as_deref().unwrap_or("chat");
        format!("{}_{}_{}.jsonl", meta.platform, safe_username, id_marker)
    });

    let final_output_path = target_dir.join(target_name);
    if let Some(parent) = final_output_path.parent() { async_fs::create_dir_all(parent).await?; }

    // 1. Setup JSONL Async Disk Writer Worker
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

    // 2. Dispatch Engine Based on Platform
    if meta.platform == Platform::Twitch {
        let video_id = meta.vod_uuid.clone().ok_or(Error::MissingId)?;
        let mut offset_sec = (start_offset_ms as f64) / 1000.0;
        let mut cursor: Option<String> = None;
        let mut consecutive_empty = 0;

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Client-ID", reqwest::header::HeaderValue::from_static("kd1unb4b3q4t58fwlpcbzcbnm76a8fp"));
        let twitch_client = reqwest::Client::builder().default_headers(headers).build()?;

        loop {
            if let Some(ref rx_cancel) = options.cancel_rx {
                if *rx_cancel.borrow() { return Err(Error::Cancelled("User requested abort".into())); }
            }

            let body = if let Some(ref cur) = cursor {
                serde_json::json!([{ "operationName": "VideoCommentsByOffsetOrCursor", "variables": { "videoID": video_id, "cursor": cur }, "extensions": { "persistedQuery": { "version": 1, "sha256Hash": "b70a3591ff0f4e0313d126c6a1502d79a1c02baebb288227c582044aa76adf6a" } } }])
            } else {
                serde_json::json!([{ "operationName": "VideoCommentsByOffsetOrCursor", "variables": { "videoID": video_id, "contentOffsetSeconds": offset_sec.floor() as i64 }, "extensions": { "persistedQuery": { "version": 1, "sha256Hash": "b70a3591ff0f4e0313d126c6a1502d79a1c02baebb288227c582044aa76adf6a" } } }])
            };

            let resp = twitch_client.post("https://gql.twitch.tv/gql").json(&body).send().await?;
            if !resp.status().is_success() { break; }
            let val: serde_json::Value = resp.json().await?;

            let edges = val[0]["data"]["video"]["comments"]["edges"].as_array().cloned().unwrap_or_default();
            if edges.is_empty() {
                consecutive_empty += 1;
                if consecutive_empty >= 30 { break; }
            } else {
                consecutive_empty = 0;
                let mut max_page_offset = offset_sec;

                for edge in &edges {
                    let node = &edge["node"];
                    let offset = node["contentOffsetSeconds"].as_f64().unwrap_or(0.0);
                    if offset < offset_sec { continue; }
                    if effective_end_ms > 0 && offset * 1000.0 > effective_end_ms as f64 { continue; }
                    if offset > max_page_offset { max_page_offset = offset; }

                    let msg_id = edge["cursor"].as_str().unwrap_or("").to_string();
                    if msg_id.is_empty() || !seen_msg_ids.insert(msg_id.clone()) { continue; }

                    let mut badges = Vec::new();
                    if let Some(arr) = node["message"]["userBadges"].as_array() {
                        for b in arr {
                            let text = match b["setID"].as_str().unwrap_or("") {
                                "broadcaster" => "👑", "moderator" => "⚔", "subscriber" => "★", "staff" => "⛨", _ => ""
                            }.to_string();
                            badges.push(crate::types::Badge { r#type: b["setID"].as_str().unwrap_or("").into(), text });
                        }
                    }

                    let content = node["message"]["fragments"].as_array().map(|f| f.iter().filter_map(|x| x["text"].as_str()).collect::<String>()).unwrap_or_default();
                    let commenter = &node["commenter"];

                    let msg = crate::types::Message {
                        id: msg_id, chat_id: video_id.parse().unwrap_or(0),
                        user_id: commenter["id"].as_str().unwrap_or("0").parse().unwrap_or(0),
                        content, r#type: "chat".into(), metadata: "".into(),
                        sender: crate::types::Sender {
                            id: commenter["id"].as_str().unwrap_or("0").parse().unwrap_or(0),
                            slug: commenter["login"].as_str().unwrap_or("").into(),
                            username: commenter["displayName"].as_str().unwrap_or(commenter["login"].as_str().unwrap_or("")).into(),
                            identity: crate::types::Identity { color: node["message"]["userColor"].as_str().unwrap_or("").into(), badges },
                        },
                        created_at: (stream_start + ChronoDuration::milliseconds((offset * 1000.0) as i64)).to_rfc3339(),
                    };

                    let _ = tx.send(serde_json::to_string(&MessageEnriched::from_message(&msg, stream_start)).unwrap()).await;
                }

                if window_length_ms > 0 {
                    let pct = (((max_page_offset * 1000.0 - start_offset_ms as f64) / window_length_ms as f64) * 100.0).clamp(0.0, 100.0);
                    report(ProgressPayload::Downloading { percent: pct as i64, message: "Paginating Twitch chat...".into() });
                }

                let page_info = &val[0]["data"]["video"]["comments"]["pageInfo"];
                if page_info["hasNextPage"].as_bool().unwrap_or(false) {
                    if let Some(c) = page_info["endCursor"].as_str() { cursor = Some(c.to_string()); }
                } else { break; }

                offset_sec = max_page_offset;
            }
            if effective_end_ms > 0 && offset_sec * 1000.0 >= effective_end_ms as f64 { break; }
        }
    } else {
        // Kick Concurrent Pipeline
        let chat_id = meta.chat_id.ok_or(Error::MissingId)?;
        let aligned_start = (start_offset_ms as i64 / KICK_STEP_SECS) * KICK_STEP_SECS;
        let mut next_start = stream_start + ChronoDuration::milliseconds(aligned_start);
        let mut empty_cycles = 0;

        loop {
            if let Some(ref rx_cancel) = options.cancel_rx {
                if *rx_cancel.borrow() { return Err(Error::Cancelled("User requested abort".into())); }
            }

            let mut starts = Vec::new();
            let mut candidate = next_start;
            for _ in 0..options.kick_concurrency {
                if effective_end_ms > 0 && (candidate - stream_start).num_milliseconds() as u64 >= effective_end_ms { break; }
                starts.push(candidate);
                candidate += ChronoDuration::seconds(KICK_STEP_SECS);
            }
            if starts.is_empty() { break; }

            let mut futs = Vec::new();
            for st in &starts {
                let mut url = Url::parse(&format!("https://web.kick.com/api/v1/chat/{}/history", chat_id)).unwrap();
                url.query_pairs_mut().append_pair("start_time", &to_kick_timestamp(*st));
                let url_str = url.to_string();
                let cancel_ref = options.cancel_rx.clone();
                let cl = client.clone();
                futs.push(async move { fetch_json_with_retries(&cl, &url_str, options.max_retries , cancel_ref.as_ref()).await });
            }

            let results = join_all(futs).await;
            let mut got_messages = false;

            for res in results {
                if let Ok(resp) = res {
                    if resp.message == "OK" && !resp.data.messages.is_empty() {
                        got_messages = true;
                        for m in &resp.data.messages {
                            if seen_msg_ids.insert(m.id.clone()) {
                                let _ = tx.send(serde_json::to_string(&MessageEnriched::from_message(m, stream_start)).unwrap()).await;
                            }
                        }
                    }
                }
            }

            if got_messages {
                empty_cycles = 0;
            } else {
                empty_cycles += 1;
                if effective_end_ms == 0 && empty_cycles >= options.empty_cycle_threshold { break; }
            }

            next_start = candidate;

            if window_length_ms > 0 {
                let elapsed = (next_start - stream_start).num_milliseconds() as f64 - start_offset_ms as f64;
                let pct = ((elapsed / window_length_ms as f64) * 100.0).clamp(0.0, 100.0);
                report(ProgressPayload::Downloading { percent: pct as i64, message: "Fetching Kick chat buckets...".into() });
            }

            if !got_messages { tokio::time::sleep(std::time::Duration::from_millis(150)).await; }
        }
    }

    // 3. Graceful Teardown
    drop(tx);
    let _ = writer_task.await;
    async_fs::rename(&tmp_path, &final_output_path).await?;

    report(ProgressPayload::Done);
    Ok(())
}