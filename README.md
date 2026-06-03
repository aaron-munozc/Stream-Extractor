# stream_extractor

A high-performance, fully asynchronous Rust engine designed to resolve stream metadata, extract multi-threaded video assets, and parse timeline-enriched chat logs from major streaming platforms like Twitch and Kick.

## Features

* **Unified Platform Router**: Abstracted interface automatically detects and normalization-routes assets for Twitch and Kick streams (Live Streams, VODs, and Clips) into standard data layouts.
* **Granular Sub-Segment Trimming**: Download precisely targeted slices of streams down to the millisecond (`start_ms` and `end_ms`), optimizing network bandwidth and disk consumption.
* **Concurrent Chunk Harvesting**: Highly parallelized media asset collection engine utilizing bounded workers with user-configurable threads to maximize download throughput.
* **Asynchronous Progress Lifecycle**: Emits non-blocking lifecycle updates via thread-safe closures (`Arc<dyn Fn(ProgressPayload) + Send + Sync>`), ready for seamless pairing with terminal layout UIs (e.g., `ratatui`, `indicatif`) or web interfaces.
* **Decoupled Chat IO Pipeline**: Asynchronously ingests chat streams through a bounded MPSC channel (4096 capacity) running on an isolated file-writer task to completely prevent database or IO backpressure from choking the network thread.
* **Built-in De-duplication**: Memory-efficient lookbehind tracking guards against duplicate message delivery over boundary frames.
* **OS-Polite Resource Management**: Spawns post-download FFmpeg joining processes using standard process constraints (`nice -n 19` on Unix targets) to ensure system stability during CPU-bound stitching operations.
* **Graceful Abort Architecture**: Features complete integration with `tokio::sync::watch` notification vectors for clean, instantaneous, cascade cancellation across all ongoing network chunks and disk writes.

## Installation

Add the following to your project's `Cargo.toml`:

```toml
[dependencies]
stream_extractor = "0.1.0"
tokio = { version = "1.0", features = ["full"] }

```

## Quick Start

This example illustrates the end-to-end flow: creating a client with anti-fingerprint defaults, resolving stream schemas, scanning available playback streams, and fetching a high-bitrate video range with a live progress hook.

```rust
use std::path::PathBuf;
use std::sync::Arc;
use stream_extractor::{DownloadOptions, ChatOptions, ProgressPayload, StreamClient, fetch_stream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize the browser-mimicking client engine
    let client = StreamClient::new()?;
    let target_url = "https://kick.com/taodota/videos/428ae744-41be-4339-a01c-44e65181be85";
    let output_dir = PathBuf::from("target/media_vault");

    // 2. Resolve platform-agnostic stream handles
    let stream = fetch_stream(&client, target_url).await?;
    println!("Platform Identity Verified: {}", stream.metadata.platform);
    println!("Stream Title: {:?}", stream.metadata.title);

    // 3. Optional: Inspect and enumerate adaptive manifest qualities
    let qualities = stream.get_qualities().await?;
    if let Some(best_quality) = qualities.first() {
        if let Some(res) = &best_quality.resolution {
            println!("Highest Available Quality Layer: {}x{} [Index: {}]", res.width, res.height, best_quality.index);
        }
    }

    // 4. Architect a thread-safe telemetry pipeline
    let progress_hook = Arc::new(|payload| match payload {
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
        ProgressPayload::Done => {
            println!("\nTarget acquisition complete.");
        }
        ProgressPayload::Error { message } => {
            println!("\nPipeline execution failure: {}", message);
        }
    });

    // 5. Build an isolated sub-segment video extraction configuration
    let video_opts = DownloadOptions {
        output_dir: Some(output_dir.clone()),
        output_name: Some("target_capture.mp4".into()),
        threads: 4,                        // Maximum parallel worker loops
        quality_index: Some(0),            // Select specific quality index manifest
        start_ms: Some(60_000),            // Start clip exactly at 1 minute
        end_ms: Some(90_000),              // Terminate clip exactly at 1 minute 30 seconds
        progress_hook: Some(progress_hook.clone()),
        cancel_rx: None,                   // Pass a tokio::sync::watch::Receiver here for abort signals
        ..Default::default()
    };

    // Execute multi-threaded segmentation and stitch pipeline
    stream.download_video(video_opts).await?;

    // 6. Pull timeline synchronized JSON Lines chat assets
    let chat_opts = ChatOptions {
        output_dir: Some(output_dir),
        output_name: Some("target_chat.jsonl".into()),
        start_ms: Some(60_000),
        end_ms: Some(90_000),
        progress_hook: Some(progress_hook),
        ..Default::default()
    };

    stream.download_chat(chat_opts).await?;

    Ok(())
}

```

## Deep-Dive Architecture

### Network Initialization Strategy (`StreamClient`)

The underlying connection engine sets up native HTTP/2 adaptive window resizing configurations alongside default request headers. These headers configure standard Accept layers, Language preferences, and User-Agents mirroring standard production browsers to minimize platform rate-limiting and connection rejections.

### The Chat IO Topography

Rather than performing inline blocking disk operations during platform API polling intervals, the architecture uses a background file consumption architecture:

```
[Network Loop Engine] 
         │
         ▼ (Enriched Serialized Packets)
┌──────────────────────────────────────┐
│ mpsc::channel (Capacity: 4096)      │
└──────────────────────────────────────┘
         │
         ▼ (De-queued Asynchronously)
[Async BufWriter Disk Task] ──> Target (.jsonl) Output

```

This structural separation insulates critical downstream frame network requests from variance in platform disk storage latencies.

### Resilience and Error Profiles

Errors are tracked systematically via explicit, exhaustive variants powered by the `thiserror` compilation subsystem. Variants completely map structural conditions including `PlaylistParse`, `Network` connection states, `Ffmpeg` executable paths, and explicit upstream server `RateLimited` throttling events.

## System Prerequisites

To perform target video compilation, `ffmpeg` must be present and discoverable within your host operating system environment execution path. The engine references system bindings to execute high-speed, stream-copy manipulations without altering base multi-stream encoding attributes.
