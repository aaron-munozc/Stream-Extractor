#![cfg(feature = "vod")]

use futures::stream::{self, StreamExt};
use std::path::{Path, PathBuf};
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
use crate::types::{
    ClipInfo, ProgressPayload, QualityPreference, StreamQuality, StreamResolution,
    VodDownloadOptions, VodInfo,
};

const RETRIES: usize = 3;
const MAX_CONCURRENCY: usize = 16;

// ---------------------------------------------------------------------------
// FFmpeg runner
// ---------------------------------------------------------------------------

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
    cmd.args(args);
    cmd.stdout(Stdio::null()).stderr(Stdio::piped());

    let output = match cancel_rx {
        Some(mut rx) => tokio::select! {
            res = cmd.output() => {
                res.map_err(|e| Error::Ffmpeg(format!("Failed to execute ffmpeg: {e}")))?
            }
            _ = async {
                while rx.changed().await.is_ok() {
                    if *rx.borrow() { break; }
                }
            } => {
                return Err(Error::Cancelled("FFmpeg merging aborted by user".into()));
            }
        },
        None => cmd
            .output()
            .await
            .map_err(|e| Error::Ffmpeg(format!("Failed to execute ffmpeg: {e}")))?,
    };

    if !output.status.success() {
        return Err(Error::Ffmpeg(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Quality enumeration
// ---------------------------------------------------------------------------

pub(crate) async fn get_qualities_internal(
    client: &StreamClient,
    m3u8_url: &str,
) -> Result<Vec<StreamQuality>> {
    // Direct MP4 — single synthetic quality entry.
    if m3u8_url.contains(".mp4") {
        return Ok(vec![StreamQuality {
            index: 0,
            uri: m3u8_url.to_string(),
            resolution: None,
            bandwidth: None,
        }]);
    }

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
        // Already a media playlist — treat as a single quality.
        Ok((_, m3u8_rs::Playlist::MediaPlaylist(_))) => Ok(vec![StreamQuality {
            index: 0,
            uri: m3u8_url.to_string(),
            resolution: None,
            bandwidth: None,
        }]),
        Err(e) => Err(Error::PlaylistParse(format!(
            "Manifest Parsing Failed: {e:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Internal download machinery
// ---------------------------------------------------------------------------

async fn resolve_media_playlist(
    client: &StreamClient,
    m3u8_url: &str,
    quality: QualityPreference,
) -> Result<Url> {
    let manifest_bytes = client.inner.get(m3u8_url).send().await?.bytes().await?;
    match m3u8_rs::parse_playlist(&manifest_bytes) {
        Ok((_, m3u8_rs::Playlist::MasterPlaylist(master))) => {
            let base = Url::parse(m3u8_url)?;

            let variant = match quality {
                QualityPreference::Best => master.variants.iter().max_by_key(|v| v.bandwidth),
                QualityPreference::Worst => master.variants.iter().min_by_key(|v| v.bandwidth),
                QualityPreference::Height(h) => master
                    .variants
                    .iter()
                    .filter(|v| v.resolution.is_some_and(|r| r.height == h))
                    .max_by_key(|v| v.bandwidth)
                    .or_else(|| master.variants.iter().max_by_key(|v| v.bandwidth)),
                QualityPreference::Index(idx) => master.variants.get(idx),
            }
            .or_else(|| master.variants.first())
            .ok_or_else(|| Error::PlaylistParse("No variants found in master playlist".into()))?;

            let mut joined = base.join(&variant.uri)?;
            if joined.query().is_none() && base.query().is_some() {
                joined.set_query(base.query());
            }
            Ok(joined)
        }
        Ok((_, m3u8_rs::Playlist::MediaPlaylist(_))) => Ok(Url::parse(m3u8_url)?),
        Err(e) => Err(Error::PlaylistParse(format!("Manifest Error: {e:?}"))),
    }
}

/// Download a single segment with up to `RETRIES` retries, honouring cancellation.
async fn download_segment(
    client: crate::http::Client,
    url: Url,
    path: PathBuf,
    cancel_rx: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<()> {
    // Bail immediately if already cancelled.
    if cancel_rx.as_ref().is_some_and(|rx| *rx.borrow()) {
        return Err(Error::Cancelled("User requested abort".into()));
    }

    let task = async {
        let mut attempts = 0usize;
        loop {
            match client.get(url.as_str()).send().await {
                Ok(resp) => {
                    let mut file = async_fs::File::create(&path).await?;
                    let mut stream = resp.bytes_stream();
                    let mut chunk_err = false;

                    while let Some(chunk_res) = stream.next().await {
                        match chunk_res {
                            Ok(chunk) => file.write_all(&chunk).await?,
                            Err(e) => {
                                chunk_err = true;
                                if attempts < RETRIES {
                                    attempts += 1;
                                    tokio::time::sleep(Duration::from_millis(
                                        400 * attempts as u64,
                                    ))
                                    .await;
                                    break;
                                }
                                return Err(Error::Network(e));
                            }
                        }
                    }

                    if !chunk_err {
                        file.flush().await?;
                        return Ok(());
                    }
                }
                Err(_) if attempts < RETRIES => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(400 * attempts as u64)).await;
                }
                Err(e) => return Err(Error::Network(e)),
            }
        }
    };

    match cancel_rx {
        Some(mut rx) => tokio::select! {
            res = task => res,
            _ = async {
                while rx.changed().await.is_ok() {
                    if *rx.borrow() { break; }
                }
            } => Err(Error::Cancelled("Abort".into())),
        },
        None => task.await,
    }
}

async fn download_segments(
    client: &StreamClient,
    playlist_url: &Url,
    selected: Vec<(usize, String)>,
    options: &VodDownloadOptions,
    tmp_path: &Path,
) -> Result<Vec<(usize, PathBuf)>> {
    let total = selected.len() as f64;
    let done = Arc::new(AtomicU64::new(0));

    let mut results: Vec<_> = stream::iter(selected)
        .map(|(idx, uri)| {
            let inner_client = client.inner.clone();
            let done = done.clone();
            let cancel_rx = options.cancel_rx.clone();
            let hook = options.progress_hook.clone();

            let mut url = playlist_url.join(&uri).unwrap();
            if url.query().is_none() && playlist_url.query().is_some() {
                url.set_query(playlist_url.query());
            }
            let path = tmp_path.join(format!("{idx:08}.ts"));

            async move {
                download_segment(inner_client, url, path.clone(), cancel_rx).await?;
                let completed = done.fetch_add(1, Ordering::Relaxed) + 1;
                if let Some(ref h) = hook {
                    h(ProgressPayload::Downloading {
                        percent: ((completed as f64 / total) * 100.0) as u8,
                        message: format!("Downloading {completed}/{}", total as u64),
                    });
                }
                Ok::<_, Error>((idx, path))
            }
        })
        .buffer_unordered(options.threads.clamp(1, MAX_CONCURRENCY))
        .collect::<Vec<Result<_>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    results.sort_by_key(|(idx, _)| *idx);
    Ok(results)
}

// ---------------------------------------------------------------------------
// download_m3u8 — shared core logic
// ---------------------------------------------------------------------------

/// Parameters for `download_m3u8`, split out to avoid a 8-argument function.
struct DownloadRequest<'a> {
    m3u8_url: &'a str,
    duration_secs: Option<i64>,
    platform_str: &'a str,
    username: Option<&'a str>,
    id_marker: &'a str,
    options: &'a VodDownloadOptions,
    target_dir: &'a Path,
}

async fn download_m3u8(req: DownloadRequest<'_>, client: &StreamClient) -> Result<PathBuf> {
    let DownloadRequest {
        m3u8_url,
        duration_secs,
        platform_str,
        username,
        id_marker,
        options,
        target_dir,
    } = req;

    let report = |payload: ProgressPayload| {
        if let Some(ref hook) = options.progress_hook {
            hook(payload);
        }
    };

    let ext = options.format.extension();
    let base_name = options.output_name.clone().unwrap_or_else(|| {
        let safe_user = username
            .unwrap_or("streamer")
            .replace(|c: char| !c.is_alphanumeric(), "_");
        format!("{platform_str}_{safe_user}_{id_marker}")
    });

    let target_name = if base_name.ends_with(&format!(".{ext}")) {
        base_name
    } else {
        let stem = Path::new(&base_name)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(&base_name);
        format!("{stem}.{ext}")
    };

    let final_output = target_dir.join(target_name);

    // -----------------------------------------------------------------------
    // Fast path: direct MP4 download (Twitch clips, Kick clips)
    // -----------------------------------------------------------------------
    if m3u8_url.contains(".mp4") {
        report(ProgressPayload::Downloading {
            percent: 0,
            message: "Initializing direct MP4 download...".into(),
        });

        let resp = client.inner.get(m3u8_url).send().await?;
        if !resp.status().is_success() {
            return Err(Error::Network(resp.error_for_status().unwrap_err()));
        }

        let total_size = resp.content_length().unwrap_or(0) as f64;
        let mut file = async_fs::File::create(&final_output).await?;
        let mut downloaded = 0u64;

        let mut stream = resp.bytes_stream();
        while let Some(chunk_res) = stream.next().await {
            let chunk = chunk_res?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            if total_size > 0.0 {
                let pct = ((downloaded as f64 / total_size) * 100.0).clamp(0.0, 100.0);
                report(ProgressPayload::Downloading {
                    percent: pct as u8,
                    message: "Streaming MP4 to disk...".into(),
                });
            }
        }
        file.flush().await?;
        report(ProgressPayload::Done);
        return Ok(final_output);
    }

    // -----------------------------------------------------------------------
    // HLS path
    // -----------------------------------------------------------------------
    report(ProgressPayload::Downloading {
        percent: 0,
        message: "Initializing M3U8 target...".into(),
    });

    let playlist_url = resolve_media_playlist(client, m3u8_url, options.quality).await?;
    log::info!("Fetching Media Playlist: {playlist_url}");

    let media_bytes = client
        .inner
        .get(playlist_url.as_str())
        .send()
        .await?
        .bytes()
        .await?;

    let playlist = match m3u8_rs::parse_playlist(&media_bytes) {
        Ok((_, m3u8_rs::Playlist::MediaPlaylist(p))) => p,
        Ok((_, m3u8_rs::Playlist::MasterPlaylist(_))) => {
            return Err(Error::PlaylistParse(
                "Expected Media Playlist but received Master".into(),
            ));
        }
        Err(e) => {
            let head: String = String::from_utf8_lossy(&media_bytes)
                .chars()
                .take(150)
                .collect();
            return Err(Error::PlaylistParse(format!(
                "Manifest Error: {e:?} | URL: {playlist_url} | Head: {head}"
            )));
        }
    };

    let buffer_f = options.buffer_ms.unwrap_or(0) as f64;
    let start_target = (options.start_ms.unwrap_or(0) as f64 - buffer_f).max(0.0);
    let end_target = options
        .end_ms
        .map(|e| e as f64 + buffer_f)
        .or_else(|| duration_secs.map(|d| start_target + (d as f64 * 1000.0)));

    let mut selected = Vec::new();
    let mut current_ms = 0.0f64;
    let mut first_seg_start = -1.0f64;

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
            "No segments matched the specified timeframe".into(),
        ));
    }

    let tmp = tempfile::Builder::new()
        .prefix("vod_")
        .tempdir_in(target_dir)?;
    let tmp_path = tmp.path().to_path_buf();

    let segment_paths =
        download_segments(client, &playlist_url, selected, options, &tmp_path).await?;

    let list_path = tmp_path.join("list.txt");
    async_fs::write(
        &list_path,
        segment_paths
            .iter()
            .map(|(_, p)| format!("file '{}'", p.file_name().unwrap().to_str().unwrap()))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await?;

    report(ProgressPayload::Merging);

    let mut args: Vec<String> = vec![
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
        final_output.to_string_lossy().into_owned(),
    ]);

    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    if let Err(e) = run_ffmpeg(&arg_refs, options.cancel_rx.clone()).await {
        log::error!(
            "FFmpeg failed. Segments preserved in: {}",
            tmp_path.display()
        );
        let _ = tmp.keep();
        report(ProgressPayload::Error {
            message: e.to_string(),
        });
        return Err(e);
    }

    report(ProgressPayload::Done);
    Ok(final_output)
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Download a VOD's video track.
pub(crate) async fn download_vod_video(
    client: &StreamClient,
    vod: &VodInfo,
    options: VodDownloadOptions,
) -> Result<PathBuf> {
    let m3u8_url = vod
        .playback_url
        .as_deref()
        .or(vod.source.as_deref())
        .ok_or(Error::NotFound)?;

    let target_dir = resolve_target_dir(options.output_dir.as_deref());

    download_m3u8(
        DownloadRequest {
            m3u8_url,
            duration_secs: vod.duration,
            platform_str: &vod.platform.to_string(),
            username: vod.username.as_deref(),
            id_marker: &vod.vod_id,
            options: &options,
            target_dir: &target_dir,
        },
        client,
    )
    .await
}

/// Download a clip's video track.
pub(crate) async fn download_clip_video(
    client: &StreamClient,
    clip: &ClipInfo,
    options: VodDownloadOptions,
) -> Result<PathBuf> {
    let url = clip.playback_url.as_deref().ok_or(Error::NotFound)?;
    let target_dir = resolve_target_dir(options.output_dir.as_deref());

    download_m3u8(
        DownloadRequest {
            m3u8_url: url,
            duration_secs: clip.duration,
            platform_str: &clip.platform.to_string(),
            username: clip.username.as_deref(),
            id_marker: &clip.clip_id,
            options: &options,
            target_dir: &target_dir,
        },
        client,
    )
    .await
}

/// Get available quality variants for a VOD.
pub(crate) async fn get_vod_qualities(
    client: &StreamClient,
    vod: &VodInfo,
) -> Result<Vec<StreamQuality>> {
    let url = vod
        .playback_url
        .as_deref()
        .or(vod.source.as_deref())
        .ok_or(Error::NotFound)?;
    get_qualities_internal(client, url).await
}

/// Get available quality variants for a clip.
pub(crate) async fn get_clip_qualities(
    client: &StreamClient,
    clip: &ClipInfo,
) -> Result<Vec<StreamQuality>> {
    let url = clip.playback_url.as_deref().ok_or(Error::NotFound)?;
    get_qualities_internal(client, url).await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn resolve_target_dir(explicit: Option<&Path>) -> PathBuf {
    explicit
        .map(PathBuf::from)
        .or_else(dirs::download_dir)
        .or_else(dirs::video_dir)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default())
}
