# stream_extractor

A high-performance, fully asynchronous Rust engine designed to resolve stream metadata, extract multi-threaded video assets, and parse timeline-enriched chat logs from major streaming platforms like Twitch and Kick.

## Features

* **Unified Platform Router**: Automatically detects and normalization-routes URLs for Twitch and Kick into standard data layouts. Supports metadata and asset resolution for VODs, Clips, and Live Channels.
* **Granular Sub-Segment Trimming**: Download precisely targeted slices of streams down to the millisecond (`start_ms` and `end_ms`).
* **Direct MP4 & HLS Support**: Seamlessly handles both direct `.mp4` downloads (e.g., Twitch Clips) and chunked HLS `.m3u8` playlists (VODs).
* **Concurrent Chunk Harvesting**: Highly parallelized media asset collection using `buffer_unordered`, capped by user-configurable thread limits to maximize throughput without overwhelming network resources.
* **Decoupled Chat IO Pipeline**: Asynchronously ingests chat streams through a bounded MPSC channel (4096 capacity) running on an isolated file-writer task. This completely prevents disk IO backpressure from choking the network thread. Outputs in `.jsonl` format.
* **Platform-Specific Chat Heuristics**: 
  * **Twitch**: Utilizes native GraphQL endpoints with embedded web-player Client-IDs for highly accurate pagination and badge resolution.
  * **Kick**: Configurable concurrent batch-polling with built-in empty-cycle threshold detection.
* **OS-Polite Resource Management**: Spawns post-download FFmpeg joining processes using standard process constraints (`nice -n 19` on Unix targets) to ensure system stability during CPU-bound stitching operations.
* **Graceful Abort Architecture**: Complete integration with `tokio::sync::watch` notification vectors for clean, instantaneous, cascade cancellation across all ongoing network chunks and disk writes.

## Installation

Add the following to your project's `Cargo.toml`. 

**Important:** You must specify an HTTP backend by enabling either the `reqwest-backend` or `wreq-backend` feature. `reqwest-backend` is enabled by default.

```toml
[dependencies]
stream_extractor = { version = "0.1.0", features = ["reqwest-backend"] }
tokio = { version = "1.0", features = ["full"] }

```

## Quick Start

This example illustrates the end-to-end flow: creating a client, resolving stream schemas, fetching a high-bitrate video range, and downloading the corresponding chat slice using the shared `Stream` reference.

```rust
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use stream_extractor::{
    fetch_stream, ChatOptions, DownloadOptions, ProgressCallback, ProgressPayload, StreamClient,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize the HTTP client engine (configured by your feature flags)
    let client = StreamClient::new()?;
    let target_url = "[https://kick.com/taodota/videos/428ae744-41be-4339-a01c-44e65181be85](https://kick.com/taodota/videos/428ae744-41be-4339-a01c-44e65181be85)";
    let output_dir = PathBuf::from("target/media_vault");

    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)?;
    }

    // 2. Resolve platform-agnostic stream handles
    let stream = fetch_stream(&client, target_url).await?;
    println!("Platform Identity Verified: {}", stream.platform);
    println!("Stream Title: {:?}", stream.title);

    // 3. Architect a thread-safe telemetry pipeline
    let progress_hook: ProgressCallback = Arc::new(|payload| match payload {
        ProgressPayload::Downloading { percent, message } => {
            print!("\r[{}] Progress: {}% ", message, percent);
            use std::io::Write;
            std::io::stdout().flush().unwrap();
        }
        ProgressPayload::Merging => {
            print!("\r[FFmpeg] Commencing zero-copy manifest stitching... ");
            use std::io::Write;
            std::io::stdout().flush().unwrap();
        }
        ProgressPayload::Done => println!("\nTarget acquisition complete."),
        ProgressPayload::Error { message } => println!("\nPipeline failure: {}", message),
    });

    // 4. Extract target video timeline
    let video_opts = DownloadOptions::default()
        .with_output_dir(output_dir.clone())
        .with_output_name("target_capture")
        .with_threads(4)
        .with_start_ms(60_000)
        .with_end_ms(90_000)
        .with_progress_hook(progress_hook.clone());

    // `.download_video()` borrows `stream`, allowing reuse
    match stream.download_video(video_opts).await {
        Ok(path) => println!("Video saved to: {:?}", path),
        Err(e) => eprintln!("Video download failed: {}", e),
    }

    // 5. Extract synchronized chat history using the same stream reference
    let chat_opts = ChatOptions::default()
        .with_output_dir(output_dir)
        .with_output_name("target_chat")
        .with_start_ms(60_000)
        .with_end_ms(90_000)
        .with_progress_hook(progress_hook);

    match stream.download_chat(chat_opts).await {
        Ok(path) => println!("Chat saved to: {:?}", path),
        Err(e) => eprintln!("Chat download failed: {}", e),
    }

    Ok(())
}

```

## System Prerequisites

**FFmpeg is strictly required for VOD extraction.** While single-file clips (like `.mp4` Twitch Clips) can be downloaded natively via standard chunked streams, full VODs utilize HLS/M3U8 segmented playlists. `stream_extractor` natively parses the manifest and downloads the `.ts` chunks concurrently, but relies on `ffmpeg` to stitch these segments back together flawlessly without re-encoding.

Ensure `ffmpeg` is present and discoverable within your host operating system's PATH.

## Error Handling

Errors are tracked systematically via explicit, exhaustive variants powered by the `thiserror` crate. Variants completely map structural conditions including `PlaylistParse`, `Network` connection states, `Ffmpeg` executable failures, and explicit upstream server `RateLimited` throttling events, allowing your application to gracefully handle and retry specific failure modes.