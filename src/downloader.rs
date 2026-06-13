use futures::stream::{self, StreamExt};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs as async_fs;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::Duration;
use url::Url;

use crate::client::StreamClient;
use crate::error::{Error, Result};
use crate::types::{DownloadOptions, ProgressPayload, QualityPreference, StreamMetadata, StreamQuality, StreamResolution};

const RETRIES: usize = 3;
const MAX_CONCURRENCY: usize = 16;

pub(crate) async fn run_ffmpeg(
    args: &[&str],
    cancel_rx: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<()> {
    #[cfg(target_family = "unix")]
    let mut cmd = {
        let mut c = Command::new("nice");
        c.arg("-n").arg("19").arg("ffmpeg");
        c
    };
    #[cfg(not(target_family = "unix"))]
    let mut cmd = Command::new("ffmpeg");

    cmd.kill_on_drop(true);

    for arg in args {
        cmd.arg(arg);
    }

    cmd.stdout(Stdio::null()).stderr(Stdio::piped());

    let output = if let Some(mut rx) = cancel_rx {
        tokio::select! {
            res = cmd.output() => {
                res.map_err(|e| Error::Ffmpeg(format!("Failed to execute ffmpeg: {}", e)))?
            }
            _ = async {
                // Keep checking the channel until it flips to true
                while rx.changed().await.is_ok() {
                    if *rx.borrow() { break; }
                }
            } => {
                return Err(Error::Cancelled("FFmpeg merging aborted by user".into()));
            }
        }
    } else {
        cmd.output()
            .await
            .map_err(|e| Error::Ffmpeg(format!("Failed to execute ffmpeg: {}", e)))?
    };

    if !output.status.success() {
        let err_msg = String::from_utf8_lossy(&output.stderr).into_owned();
        return Err(Error::Ffmpeg(err_msg));
    }

    Ok(())
}

pub(crate) async fn get_qualities_internal(
    client: &StreamClient,
    m3u8_url: &str,
) -> Result<Vec<StreamQuality>> {
    let resp = client.inner.get(m3u8_url).send().await?.bytes().await?;

    match m3u8_rs::parse_playlist(&resp) {
        Ok((_, m3u8_rs::Playlist::MasterPlaylist(master))) => {
            let base = Url::parse(m3u8_url)?;
            Ok(master
                .variants
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    let uri = if v.uri.starts_with("http") {
                        v.uri
                    } else {
                        let mut u = base.join(&v.uri).ok()?;
                        if u.query().is_none() && base.query().is_some() {
                            u.set_query(base.query());
                        }
                        u.to_string()
                    };
                    Some(StreamQuality {
                        index: i,
                        uri,
                        resolution: v.resolution.map(|r| StreamResolution {
                            width: r.width,
                            height: r.height,
                        }),
                        bandwidth: Some(v.bandwidth),
                    })
                })
                .collect())
        }
        Ok((_, m3u8_rs::Playlist::MediaPlaylist(_))) => Ok(vec![StreamQuality {
            index: 0,
            uri: m3u8_url.to_string(),
            resolution: None,
            bandwidth: None,
        }]),
        Err(e) => Err(Error::PlaylistParse(format!(
            "Manifest Parsing Failed: {:?}",
            e
        ))),
    }
}

pub(crate) async fn download_vod_internal(
    client: &StreamClient,
    meta: &StreamMetadata,
    options: DownloadOptions,
) -> Result<PathBuf> {
    let m3u8_url = meta
        .playback_url
        .as_ref()
        .or(meta.source.as_ref())
        .ok_or(Error::NotFound)?;
    let report = |payload: ProgressPayload| {
        if let Some(ref hook) = options.progress_hook {
            hook(payload);
        }
    };

    let target_dir = options
        .output_dir
        .clone()
        .or_else(dirs::download_dir)
        .or_else(dirs::video_dir)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());

    let base_name = options.output_name.clone().unwrap_or_else(|| {
        let safe_username = meta
            .username
            .as_deref()
            .unwrap_or("streamer")
            .replace(|c: char| !c.is_alphanumeric(), "_");
        let id_marker = meta.vod_uuid.as_deref().unwrap_or("media");
        format!("{}_{}_{}", meta.platform, safe_username, id_marker)
    });

    let ext = options.format.extension();

    let target_name = if base_name.ends_with(&format!(".{}", ext)) {
        base_name
    } else {
        let clean_base = std::path::Path::new(&base_name)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(&base_name);

        format!("{}.{}", clean_base, ext)
    };

    let final_output_path = target_dir.join(target_name);

    if m3u8_url.contains(".mp4") {
        report(ProgressPayload::Downloading {
            percent: 0,
            message: "Initializing direct MP4 download...".into(),
        });
        let mut resp = client.inner.get(m3u8_url).send().await?;
        if !resp.status().is_success() {
            return Err(Error::Network(resp.error_for_status().unwrap_err()));
        }

        let total_size = resp.content_length().unwrap_or(0) as f64;
        let mut file = async_fs::File::create(&final_output_path).await?;
        let mut downloaded: u64 = 0;

        while let Some(chunk) = resp.chunk().await? {
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            if total_size > 0.0 {
                let pct = ((downloaded as f64 / total_size) * 100.0) as i64;
                report(ProgressPayload::Downloading {
                    percent: pct,
                    message: "Streaming MP4 to disk...".into(),
                });
            }
        }
        file.flush().await?;
        report(ProgressPayload::Done);

        // Fix: Return the path for the MP4 branch
        return Ok(final_output_path);
    }

    report(ProgressPayload::Downloading {
        percent: 0,
        message: "Initializing M3U8 target...".into(),
    });

    let manifest_bytes = client.inner.get(m3u8_url).send().await?.bytes().await?;
    let playlist_url = match m3u8_rs::parse_playlist(&manifest_bytes) {
        Ok((_, m3u8_rs::Playlist::MasterPlaylist(master))) => {
            let base = Url::parse(m3u8_url)?;

            let variant = match options.quality {
                QualityPreference::Best => master
                    .variants
                    .iter()
                    .max_by_key(|v| v.bandwidth),

                QualityPreference::Worst => master
                    .variants
                    .iter()
                    .min_by_key(|v| v.bandwidth),

                QualityPreference::Height(target_height) => master
                    .variants
                    .iter()
                    .filter(|v| v.resolution.map_or(false, |r| r.height == target_height))
                    .max_by_key(|v| v.bandwidth)
                    .or_else(|| master.variants.iter().max_by_key(|v| v.bandwidth)),

                QualityPreference::Index(idx) => master
                    .variants
                    .get(idx),
            }
                .or_else(|| master.variants.first())
                .ok_or(Error::PlaylistParse("No variants found in master playlist".into()))?;

            let mut joined = base.join(&variant.uri)?;
            if joined.query().is_none() && base.query().is_some() {
                joined.set_query(base.query());
            }
            joined
        }
        Ok((_, m3u8_rs::Playlist::MediaPlaylist(_))) => Url::parse(m3u8_url)?,
        Err(e) => return Err(Error::PlaylistParse(format!("Manifest Error: {:?}", e))),
    };

    log::info!("Fetching Media Playlist: {}", playlist_url);
    let media_bytes = client
        .inner
        .get(playlist_url.clone())
        .send()
        .await?
        .bytes()
        .await?;

    let playlist = match m3u8_rs::parse_playlist(&media_bytes) {
        Ok((_, m3u8_rs::Playlist::MediaPlaylist(p))) => p,
        Ok((_, m3u8_rs::Playlist::MasterPlaylist(_))) => {
            return Err(Error::PlaylistParse(
                "Expected Media Playlist but received Master.".into(),
            ));
        }
        Err(e) => {
            let text = String::from_utf8_lossy(&media_bytes);
            let safe_head: String = text.chars().take(150).collect();
            return Err(Error::PlaylistParse(format!(
                "Manifest Error: {:?} | URL: {} | Head: {}",
                e, playlist_url, safe_head
            )));
        }
    };
    let buffer = options.buffer_ms.unwrap_or(0) as f64;
    let start_target = (options.start_ms.unwrap_or(0) as f64 - buffer).max(0.0);
    let end_target = options
        .end_ms
        .map(|e| e as f64 + buffer)
        .or_else(|| meta.duration.map(|d| start_target + (d as f64 * 1000.0)));

    let mut selected = Vec::new();
    let mut current_ms = 0.0;
    let mut first_seg_start = -1.0;

    for (idx, seg) in playlist.segments.iter().enumerate() {
        let dur_ms = seg.duration as f64 * 1000.0;
        let seg_end = current_ms + dur_ms;
        if seg_end > start_target && end_target.is_none_or(|e| current_ms < e) {
            if first_seg_start < 0.0 {
                first_seg_start = current_ms;
            }
            selected.push((idx, seg.uri.clone()));
        }
        current_ms += dur_ms;
    }

    if selected.is_empty() {
        return Err(Error::PlaylistParse(
            "No segments matched specified timeframe parameters".into(),
        ));
    }

    let tmp = tempfile::Builder::new()
        .prefix("vod_")
        .tempdir_in(&target_dir)?;
    let tmp_path = tmp.path().to_path_buf();
    let downloaded_count = Arc::new(AtomicU64::new(0));
    let total_count = selected.len() as f64;

    let mut paths_result = stream::iter(selected).map(|(idx, uri)| {
        let client = client.inner.clone();
        let mut url = playlist_url.join(&uri).unwrap();
        if url.query().is_none() && playlist_url.query().is_some() {
            url.set_query(playlist_url.query());
        }

        let path = tmp_path.join(format!("{:08}.ts", idx));
        let counter = downloaded_count.clone();
        let cancel_rx = options.cancel_rx.clone();
        let report_hook = options.progress_hook.clone();

        async move {
            if let Some(ref rx) = cancel_rx && *rx.borrow() { return Err(Error::Cancelled("User requested abort".into())); }
            let task = async {
                let mut attempts = 0;
                loop {
                    match client.get(url.clone()).send().await {
                        Ok(resp) => {
                            let mut file = async_fs::File::create(&path).await?;
                            let mut byte_stream = resp.bytes_stream();
                            let mut failed = false;
                            while let Some(chunk_res) = byte_stream.next().await {
                                match chunk_res {
                                    Ok(chunk) => { file.write_all(&chunk).await?; }
                                    Err(e) => {
                                        failed = true;
                                        if attempts < RETRIES { attempts += 1; tokio::time::sleep(Duration::from_millis(400 * attempts as u64)).await; break; }
                                        return Err(Error::Network(e));
                                    }
                                }
                            }
                            if !failed { file.flush().await?; break Ok(()); }
                        }
                        Err(_e) if attempts < RETRIES => { attempts += 1; tokio::time::sleep(Duration::from_millis(400 * attempts as u64)).await; }
                        Err(e) => return Err(Error::Network(e)),
                    }
                }
            };

            if let Some(mut rx) = cancel_rx { tokio::select! { res = task => { res?; } _ = rx.changed() => { if *rx.borrow() { return Err(Error::Cancelled("Abort".into())); } } } } else { task.await?; }

            let completed = counter.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(ref hook) = report_hook { hook(ProgressPayload::Downloading { percent: ((completed as f64 / total_count) * 100.0) as i64, message: format!("Downloading {}/{}", completed, total_count) }); }
            Ok::<(usize, PathBuf), Error>((idx, path))
        }
    }).buffer_unordered(options.threads.clamp(1, MAX_CONCURRENCY)).collect::<Vec<_>>().await.into_iter().collect::<Result<Vec<_>>>()?;

    paths_result.sort_by_key(|(idx, _)| *idx);
    let list_path = tmp_path.join("list.txt");
    async_fs::write(
        &list_path,
        paths_result
            .iter()
            .map(|(_, p)| format!("file '{}'", p.file_name().unwrap().to_str().unwrap()))
            .collect::<Vec<_>>()
            .join("\n"),
    )
        .await?;

    report(ProgressPayload::Merging);
    let mut args = vec![
        "-y".into(),
        "-f".into(),
        "concat".into(),
        "-safe".into(),
        "0".into(),
    ];
    if start_target > 0.0 {
        args.extend([
            "-ss".into(),
            format!("{:.3}", (start_target - first_seg_start).max(0.0) / 1000.0),
        ]);
    }
    args.extend(["-i".into(), list_path.to_string_lossy().into_owned()]);
    if let Some(d) = end_target {
        args.extend(["-t".into(), format!("{:.3}", (d - start_target) / 1000.0)]);
    }
    args.extend([
        "-c".into(),
        "copy".into(),
        "-movflags".into(),
        "+faststart".into(),
        final_output_path.to_string_lossy().into_owned(),
    ]);

    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    if let Err(e) = run_ffmpeg(&arg_refs, options.cancel_rx.clone()).await {
        report(ProgressPayload::Error {
            message: e.to_string(),
        });
        return Err(e);
    }

    report(ProgressPayload::Done);

    Ok(final_output_path)
}