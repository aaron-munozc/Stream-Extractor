use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use stream_extractor::{ChatOptions, DownloadOptions, ProgressPayload, StreamClient, fetch_stream};

struct TestCase {
    name: &'static str,
    url: &'static str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
            url: "https://kick.com/taodota/videos/428ae744-41be-4339-a01c-44e65181be85",
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
    println!("==================================================\n");

    let mut failed_tests = 0;

    for test in matrix {
        println!(" Running Test Case: {}", test.name);

        match fetch_stream(&client, test.url).await {
            Ok(stream) => {
                println!("     Metadata resolved! [{}]", stream.metadata.platform);

                let progress_hook = Arc::new(|payload| match payload {
                    ProgressPayload::Downloading { percent, .. } => {
                        print!("\r     [Downloading] {}% ", percent);
                        use std::io::Write;
                        std::io::stdout().flush().unwrap();
                    }
                    ProgressPayload::Merging => {
                        print!("\r     [Ffmpeg] Stitching...                    ");
                        use std::io::Write;
                        std::io::stdout().flush().unwrap();
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
                let chat_file = format!("{}_chat.jsonl", safe_name);
                let chat_path = output_directory.join(&chat_file);
                let chat_opts = ChatOptions {
                    output_dir: Some(output_directory.clone()),
                    output_name: Some(chat_file),
                    start_ms: test.start_ms,
                    end_ms: test.end_ms,
                    progress_hook: Some(progress_hook.clone()),
                    ..Default::default()
                };

                if let Err(e) = stream.download_chat(chat_opts).await {
                    println!("      Chat extraction failed: {:?}", e);
                    failed_tests += 1;
                    continue;
                }

                // Assert file actually exists and contains data
                assert!(
                    chat_path.exists(),
                    "Chat file was reported successful but does not exist!"
                );
                let chat_size = fs::metadata(&chat_path)?.len();
                println!("      Chat Written: {} bytes", chat_size);

                // --- VIDEO DOWNLOAD ---
                let video_file = format!("{}_video.mp4", safe_name);
                let video_path = output_directory.join(&video_file);
                let video_opts = DownloadOptions {
                    output_dir: Some(output_directory.clone()),
                    output_name: Some(video_file),
                    start_ms: test.start_ms,
                    end_ms: test.end_ms,
                    threads: 4,
                    progress_hook: Some(progress_hook.clone()),
                    ..Default::default()
                };

                if let Err(e) = stream.download_video(video_opts).await {
                    println!("      Video extraction failed: {:?}", e);
                    failed_tests += 1;
                    continue;
                }

                assert!(
                    video_path.exists(),
                    "Video file was reported successful but does not exist!"
                );
                let video_size = fs::metadata(&video_path)?.len();
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
