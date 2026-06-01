use futures::stream::{self, StreamExt};
use std::fs as stdfs;
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
    DownloadOptions, ProgressPayload, StreamMetadata, StreamQuality, StreamResolution,
};

const RETRIES: usize = 3;
const MAX_CONCURRENCY: usize = 16;

pub async fn run_ffmpeg(args: &[&str], tmp_dir: &Path) -> Result<()> {
    let stderr_path = tmp_dir.join("merge_stderr.log");
    let stdout_path = tmp_dir.join("merge_stdout.log");

    let stderr_file = stdfs::File::create(&stderr_path)?;
    let stdout_file = stdfs::File::create(&stdout_path)?;

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
    cmd.stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));

    let mut child = cmd
        .spawn()
        .map_err(|e| Error::Ffmpeg(format!("Failed to spawn ffmpeg: {}", e)))?;
    let status = child
        .wait()
        .await
        .map_err(|e| Error::Ffmpeg(format!("Error waiting for ffmpeg: {}", e)))?;

    if status.code().unwrap_or(-1) != 0 {
        return Err(Error::Ffmpeg(
            async_fs::read_to_string(stderr_path)
                .await
                .unwrap_or_default(),
        ));
    }
    Ok(())
}

pub async fn get_qualities_internal(
    client: &StreamClient,
    m3u8_url: &str,
) -> Result<Vec<StreamQuality>> {
    let resp = client.inner.get(m3u8_url).send().await?.bytes().await?;
    let (_, master) = m3u8_rs::parse_master_playlist(&resp)
        .map_err(|e| Error::PlaylistParse(format!("{:?}", e)))?;
    let base = Url::parse(m3u8_url)?;

    Ok(master
        .variants
        .into_iter()
        .enumerate()
        .filter_map(|(i, v)| {
            let uri = if v.uri.starts_with("http") {
                v.uri
            } else {
                base.join(&v.uri).ok()?.to_string()
            };
            Some(StreamQuality {
                index: i,
                uri,
                resolution: v.resolution.map(|r| StreamResolution {
                    width: r.width as u64,
                    height: r.height as u64,
                }),
                bandwidth: Some(v.bandwidth as u64),
            })
        })
        .collect())
}

pub async fn download_vod_internal(
    client: &StreamClient,
    meta: &StreamMetadata,
    options: DownloadOptions,
) -> Result<()> {
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
    report(ProgressPayload::Downloading {
        percent: 0,
        message: "Initializing target...".into(),
    });

    let target_dir = options
        .output_dir
        .clone()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
    let target_name = options.output_name.clone().unwrap_or_else(|| {
        let safe_username = meta
            .username
            .as_deref()
            .unwrap_or("streamer")
            .replace(|c: char| !c.is_alphanumeric(), "_");
        let id_marker = meta.vod_uuid.as_deref().unwrap_or("media");
        format!("{}_{}_{}.mp4", meta.platform, safe_username, id_marker)
    });
    let final_output_path = target_dir.join(target_name);

    let manifest_bytes = client.inner.get(m3u8_url).send().await?.bytes().await?;
    let playlist_url = match m3u8_rs::parse_master_playlist(&manifest_bytes) {
        Ok((_, master)) => {
            let idx = options.quality_index.unwrap_or(0);
            let variant = master
                .variants
                .get(idx)
                .or(master.variants.first())
                .ok_or(Error::InvalidQualityIndex(idx))?;
            Url::parse(m3u8_url)?.join(&variant.uri)?
        }
        Err(_) => Url::parse(m3u8_url)?,
    };

    let media_bytes = client
        .inner
        .get(playlist_url.as_str())
        .send()
        .await?
        .bytes()
        .await?;
    let (_, playlist) = m3u8_rs::parse_media_playlist(&media_bytes)
        .map_err(|_| Error::PlaylistParse("Invalid Media Manifest".into()))?;

    let buffer = options.buffer_ms.unwrap_or(0) as f64;
    let mut start_target = (options.start_ms.unwrap_or(0) as f64 - buffer).max(0.0);
    let end_target = options
        .end_ms
        .map(|e| e as f64 + buffer)
        .or_else(|| meta.duration.map(|d| (start_target + (d as f64 * 1000.0))));

    let mut selected = Vec::new();
    let mut current_ms = 0.0;
    let mut first_seg_start = -1.0;

    for (idx, seg) in playlist.segments.iter().enumerate() {
        let dur_ms = seg.duration as f64 * 1000.0;
        let seg_end = current_ms + dur_ms;
        if seg_end > start_target && end_target.map_or(true, |e| current_ms < e) {
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
        let url = playlist_url.join(&uri).unwrap();
        let path = tmp_path.join(format!("{:08}.ts", idx));
        let counter = downloaded_count.clone();
        let cancel_rx = options.cancel_rx.clone();
        let report_hook = options.progress_hook.clone();

        async move {
            if let Some(ref rx) = cancel_rx { if *rx.borrow() { return Err(Error::Cancelled("User requested abort".into())); } }
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
                        Err(e) if attempts < RETRIES => { attempts += 1; tokio::time::sleep(Duration::from_millis(400 * attempts as u64)).await; }
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
    if let Err(e) = run_ffmpeg(&arg_refs, &tmp_path).await {
        report(ProgressPayload::Error {
            message: e.to_string(),
        });
        return Err(e);
    }

    report(ProgressPayload::Done);
    Ok(())
}
