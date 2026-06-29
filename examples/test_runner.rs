use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use stream_extractor::{
    fetch_stream, ChatDownloadOptions, VodDownloadOptions, ProgressCallback, ProgressPayload, StreamClient,
};

struct TestCase {
    name: &'static str,
    url: &'static str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize the client using our updated StreamClient.
    // If compiled with --features wreq-backend, this automatically configures
    // wreq with the Chrome 126 fingerprint and cookie store under the hood.
    let client = StreamClient::new()?;
    let output_directory = PathBuf::from("target/test_downloads");

    if output_directory.exists() {
        fs::remove_dir_all(&output_directory)?;
    }
    fs::create_dir_all(&output_directory)?;

    let matrix = vec![
        TestCase {
            name: "Twitch VOD (First 30 Seconds Range)",
            url: "https://www.twitch.tv/videos/2785909269",
            start_ms: Some(20_000),
            end_ms: Some(50_000),
        },
        TestCase {
            name: "Twitch Clip (Full Duration)",
            url: "https://www.twitch.tv/topson/clip/TemperedRealPorcupineJKanStyle-tGFLIVpg_goO7YXA",
            start_ms: None,
            end_ms: None,
        },
        TestCase {
            name: "Kick VOD (Mid-Stream 30 Sec Range)",
            url: "https://kick.com/taodota/videos/a748634d-2975-4ce7-8ed2-c5c29c293672",
            start_ms: Some(60_000),
            end_ms: Some(90_000),
        },
        TestCase {
            name: "Kick Clip (Full Duration)",
            url: "https://kick.com/taodota/clips/clip_01HW0WG4QEXE410JTMZD1DHAGE",
            start_ms: None,
            end_ms: None,
        },
    ];

    println!("==================================================");
    println!("     STREAM EXTRACTOR PIPELINE TEST MATRIX        ");

    // Nice diagnostic printout to see which backend engine is currently under test
    #[cfg(feature = "wreq-backend")]
    println!("               [ ENGINE: WREQ ]                   ");
    #[cfg(feature = "reqwest-backend")]
    println!("              [ ENGINE: REQWEST ]                 ");

    println!("==================================================\n");

    let mut failed_tests = 0;

    for test in matrix {
        println!(" Running Test Case: {}", test.name);

        match fetch_stream(&client, test.url).await {
            Ok(stream) => {
                // Accessing the platform metadata field directly via Deref trait
                println!("     Metadata resolved! [{}]", stream.platform);

                // Explicitly typing the Arc to ProgressCallback is strictly required here
                // so the Rust compiler successfully coerces the closure into a dynamic trait object.
                let progress_hook: ProgressCallback = Arc::new(|payload| match payload {
                    ProgressPayload::Downloading { percent, .. } => {
                        print!("\r     [Downloading] {}% ", percent);
                        let _ = std::io::stdout().flush();
                    }
                    ProgressPayload::Merging => {
                        print!("\r     [Ffmpeg] Stitching...                    ");
                        let _ = std::io::stdout().flush();
                    }
                    ProgressPayload::Done => println!("\n     Task Complete!"),
                    ProgressPayload::Error { message } => {
                        println!("\n     ❌ Hook Error: {}", message)
                    }
                });

                let safe_name = test
                    .name
                    .replace(|c: char| !c.is_alphanumeric(), "_")
                    .to_lowercase();

                // --- CHAT DOWNLOAD ---
                let chat_file = format!("{}_chat", safe_name);
                let chat_opts = ChatDownloadOptions {
                    output_dir: Some(output_directory.clone()),
                    output_name: Some(chat_file),
                    start_ms: test.start_ms,
                    end_ms: test.end_ms,
                    progress_hook: Some(progress_hook.clone()),
                    ..Default::default()
                };

                // Because `download_chat` takes `&self`, we don't consume the stream anymore!
                let actual_chat_path = match stream.download_chat(chat_opts).await {
                    Ok(path) => path,
                    Err(e) => {
                        println!("      Chat extraction failed: {:?}", e);
                        failed_tests += 1;
                        continue;
                    }
                };

                assert!(
                    actual_chat_path.exists(),
                    "Chat file was reported successful but does not exist at {:?}",
                    actual_chat_path
                );
                let chat_size = fs::metadata(&actual_chat_path)?.len();
                println!("      Chat Written: {} bytes", chat_size);

                // --- VIDEO DOWNLOAD ---
                let video_file = format!("{}_video", safe_name);
                let video_opts = VodDownloadOptions {
                    output_dir: Some(output_directory.clone()),
                    output_name: Some(video_file),
                    start_ms: test.start_ms,
                    end_ms: test.end_ms,
                    threads: 4,
                    progress_hook: Some(progress_hook.clone()),
                    ..Default::default()
                };

                // Reusing the exact same stream reference for the video
                let actual_video_path = match stream.download_video(video_opts).await {
                    Ok(path) => path,
                    Err(e) => {
                        println!("      Video extraction failed: {:?}", e);
                        failed_tests += 1;
                        continue;
                    }
                };

                assert!(
                    actual_video_path.exists(),
                    "Video file was reported successful but does not exist at {:?}",
                    actual_video_path
                );
                let video_size = fs::metadata(&actual_video_path)?.len();
                println!("      Video Written: {} bytes", video_size);
            }
            Err(e) => {
                println!("   Metadata resolution failed: {:?}", e);
                failed_tests += 1;
            }
        }
        println!("\n--------------------------------------------------\n");
    }

    if failed_tests > 0 {
        eprintln!(" Test matrix finished with {} failures.", failed_tests);
        std::process::exit(1);
    } else {
        println!(" All pipeline integration tests passed successfully!");
        Ok(())
    }
}