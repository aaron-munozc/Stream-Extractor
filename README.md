# stream_extractor

A fast, asynchronous Rust library for fetching metadata and downloading VODs, clips, and chat logs from Twitch and Kick.

## Features

- **Unified API**: Automatically detects Twitch or Kick URLs and works with VODs, clips, and live channels.
- **Time-range support**: Download full streams or specific portions by setting `start_ms` and `end_ms`.
- **Video downloads** (requires the `vod` feature):
  - Handles HLS playlists (most VODs) and direct MP4 files (clips).
  - Parallel chunk downloads with configurable concurrency.
  - Quality selection (best/worst, by height, or index).
  - Output formats: MP4 (default), MKV, MOV, TS.
- **Chat downloads**:
  - Saved as `.jsonl` with timestamps relative to stream start.
  - Rich metadata: badges, colors, sender info.
  - Platform-tuned behavior (Twitch uses GraphQL comments; Kick uses efficient batch polling).
- **Progress & cancellation**: Callbacks for real-time updates and `tokio::watch` cancellation.
- **Flexible HTTP backends**: Choose `reqwest` (default) or `wreq`.
- **FFmpeg integration**: Merges segments (low priority) for HLS content.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
stream_extractor = { version = "0.1.0", features = ["reqwest-backend"] }  # or "wreq-backend"
tokio = { version = "1", features = ["full"] }
```

### Optional Features

- **`vod`** (for VOD/clip downloads): Enables HLS parsing, quality selection, and FFmpeg merging. Requires `tempfile` and `m3u8-rs`.
- **`reqwest-backend`** (default): Uses the popular `reqwest` HTTP client.
- **`wreq-backend`**: Alternative lightweight backend (mutually exclusive with `reqwest-backend`).

**Note:** FFmpeg must be installed and in your PATH for merging HLS VODs. Clips (single-file MP4s) work without it.

## Quick Start

```rust
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use stream_extractor::{
    fetch_stream, ChatDownloadOptions, VodDownloadOptions, ProgressCallback, ProgressPayload, StreamClient,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = StreamClient::new()?;
    let url = "https://kick.com/taodota/videos/428ae744-41be-4339-a01c-44e65181be85";
    let output_dir = PathBuf::from("downloads");

    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)?;
    }

    let stream = fetch_stream(&client, url).await?;
    println!("Platform: {}", stream.platform);
    println!("Title: {:?}", stream.title);

    // Progress callback
    let progress_hook: ProgressCallback = Arc::new(|payload| match payload {
        ProgressPayload::Downloading { percent, message } => {
            print!("\r[{}] {}% ", message, percent);
            let _ = std::io::stdout().flush();
        }
        ProgressPayload::Merging => print!("\r[FFmpeg] Merging segments... "),
        ProgressPayload::Done => println!("\nDownload complete!"),
        ProgressPayload::Error { message } => println!("\nError: {}", message),
    });

    // Video download (optional: 60s–90s segment)
    let video_opts = VodDownloadOptions::default()
        .with_output_dir(output_dir.clone())
        .with_output_name("highlight")
        .with_threads(4)
        .with_start_ms(60_000)
        .with_end_ms(90_000)
        .with_progress_hook(progress_hook.clone());

    if let Ok(path) = stream.download_video(video_opts).await {
        println!("Video saved to: {:?}", path);
    }

    // Chat download for the same range
    let chat_opts = ChatDownloadOptions::default()
        .with_output_dir(output_dir)
        .with_output_name("highlight_chat")
        .with_start_ms(60_000)
        .with_end_ms(90_000)
        .with_progress_hook(progress_hook);

    if let Ok(path) = stream.download_chat(chat_opts).await {
        println!("Chat saved to: {:?}", path);
    }

    Ok(())
}
```

## API Overview

- `StreamClient::new()` — Creates a reusable HTTP client.
- `fetch_stream(&client, url)` — Parses URL and fetches metadata (`StreamMetadata`).
- `stream.download_video(opts)` — (VOD feature) Downloads and merges video.
- `stream.download_chat(opts)` — Downloads chat log.
- `stream.get_qualities()` — (VOD feature) Lists available video qualities for HLS streams.

See the module docs and builder methods (`with_*`) on `VodDownloadOptions` / `ChatDownloadOptions` for full options, including platform-specific tuning for Kick chat.

## Requirements & Notes

- **FFmpeg**: Required for HLS VOD merging. Clips bypass this.
- **Async runtime**: Designed for Tokio.
- **Logging**: Uses the `log` crate — enable a logger like `env_logger` for debug output.
- **Error handling**: Rich `thiserror`-based errors for network issues, parsing failures, FFmpeg problems, etc.