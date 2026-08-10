use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use stream_extractor::{
    download_clip_chat, download_vod_chat, fetch_stream, ChatDownloadOptions, ProgressCallback,
    ProgressPayload, Stream, StreamClient,
};

#[cfg(feature = "vod")]
use stream_extractor::{download_clip_video, download_vod_video, VodDownloadOptions};

struct TestCase {
    name: &'static str,
    url: &'static str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
}

// Holds the isolated results of each test phase
struct TestReport {
    name: String,
    metadata_res: Result<String, String>,  // Ok(Platform) or Err(Reason)
    chat_res: Option<Result<u64, String>>, // Ok(Bytes) or Err(Reason)

    #[cfg(feature = "vod")]
    video_res: Option<Result<u64, String>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = StreamClient::new()?;
    let output_directory = PathBuf::from("target/test_downloads");

    // Clean slate for tests
    if output_directory.exists() {
        fs::remove_dir_all(&output_directory)?;
    }
    fs::create_dir_all(&output_directory)?;

    // Test matrix
    let matrix = vec![
        TestCase {
            name: "Twitch VOD (First 30s)",
            url: "https://www.twitch.tv/videos/2839722386",
            start_ms: Some(20_000),
            end_ms: Some(50_000),
        },
        TestCase {
            name: "Twitch Clip (Full)",
            url: "https://www.twitch.tv/vgbootcamp/clip/MotionlessCloudyBaconStrawBeary-upwzlCtk1_Pe0lbu",
            start_ms: None,
            end_ms: None,
        },
        TestCase {
            name: "Kick VOD (Mid-Stream 3s)",
            url: "https://kick.com/parkerdota/videos/019fdd43-c168-735e-8e95-bdb14200929b",
            start_ms: Some(100),
            end_ms: Some(900),
        },
        TestCase {
            name: "Kick Clip (Full)",
            url: "https://kick.com/parkerdota/clips/clip_01KYZ5HHGTPZKZRHXHT6ZHH2W4",
            start_ms: None,
            end_ms: None,
        },
    ];

    print_header();

    let mut reports = Vec::new();

    // Run the tests cleanly
    for test in matrix {
        println!("▶ Running: {}", test.name);
        reports.push(execute_test(&client, &output_directory, &test).await);
        println!("--------------------------------------------------");
    }

    // Print the final summary table
    print_summary(&reports);

    // Determine exit code based on the presence of ANY errors in the reports
    let has_failures = reports.iter().any(|r| {
        r.metadata_res.is_err()
            || r.chat_res.as_ref().map_or(false, |res| res.is_err())
            || {
            #[cfg(feature = "vod")]
            {
                r.video_res.as_ref().map_or(false, |res| res.is_err())
            }
            #[cfg(not(feature = "vod"))]
            {
                false
            }
        }
    });

    if has_failures {
        std::process::exit(1);
    }

    Ok(())
}

/// Executes a single test case, isolating errors for chat and video.
async fn execute_test(client: &StreamClient, out_dir: &Path, test: &TestCase) -> TestReport {
    let safe_name = test
        .name
        .replace(|c: char| !c.is_alphanumeric(), "_")
        .to_lowercase();

    // --- METADATA PHASE ---
    let stream = match fetch_stream(client, test.url).await {
        Ok(s) => {
            println!("  ✅ Metadata resolved [{}]", s.platform());
            s
        }
        Err(e) => {
            println!("  ❌ Metadata failed: {:?}", e);
            return TestReport {
                name: test.name.to_string(),
                metadata_res: Err(format!("{:?}", e)),
                chat_res: None,
                #[cfg(feature = "vod")]
                video_res: None,
            };
        }
    };

    let progress_hook = create_progress_hook();

    // --- CHAT PHASE ---
    let chat_res: Option<Result<u64, String>> = match &stream {
        Stream::Vod(v) => {
            let chat_opts = ChatDownloadOptions {
                output_dir: Some(out_dir.to_path_buf()),
                output_name: Some(format!("{}_chat", safe_name)),
                start_ms: test.start_ms,
                end_ms: test.end_ms,
                progress_hook: Some(progress_hook.clone()),
                ..Default::default()
            };

            let res = async {
                let path = download_vod_chat(client, v, chat_opts)
                    .await
                    .map_err(|e| format!("{:?}", e))?;
                if !path.exists() {
                    return Err("Reported success, but file is missing".to_string());
                }

                let size = fs::metadata(&path).map_err(|e| e.to_string())?.len();
                println!("\n  ✅ Chat Written: {}", format_bytes(size));
                Ok(size)
            }
                .await;

            if let Err(ref e) = res {
                println!("\n  ❌ Chat Error: {}", e);
            }
            Some(res)
        }
        Stream::Clip(c) => {
            let chat_opts = ChatDownloadOptions {
                output_dir: Some(out_dir.to_path_buf()),
                output_name: Some(format!("{}_chat", safe_name)),
                start_ms: test.start_ms,
                end_ms: test.end_ms,
                progress_hook: Some(progress_hook.clone()),
                ..Default::default()
            };

            let res = async {
                let path = download_clip_chat(client, c, chat_opts)
                    .await
                    .map_err(|e| format!("{:?}", e))?;
                if !path.exists() {
                    return Err("Reported success, but file is missing".to_string());
                }

                let size = fs::metadata(&path).map_err(|e| e.to_string())?.len();
                println!("\n  ✅ Chat Written: {}", format_bytes(size));
                Ok(size)
            }
                .await;

            if let Err(ref e) = res {
                println!("\n  ❌ Chat Error: {}", e);
            }
            Some(res)
        }
        Stream::Live(_) => {
            println!("  ℹ️ Chat skipped (Live stream)");
            None
        }
        _ => {
            println!("  ⚠️ Chat skipped (Unknown stream type)");
            None
        }
    };

    // --- VIDEO PHASE ---
    #[cfg(feature = "vod")]
    let video_res: Option<Result<u64, String>> = match &stream {
        Stream::Vod(v) => {
            let video_opts = VodDownloadOptions {
                output_dir: Some(out_dir.to_path_buf()),
                output_name: Some(format!("{}_video", safe_name)),
                start_ms: test.start_ms,
                end_ms: test.end_ms,
                threads: 4,
                progress_hook: Some(progress_hook.clone()),
                ..Default::default()
            };

            let res = async {
                let path = download_vod_video(client, v, video_opts)
                    .await
                    .map_err(|e| format!("{:?}", e))?;
                if !path.exists() {
                    return Err("Reported success, but file is missing".to_string());
                }

                let size = fs::metadata(&path).map_err(|e| e.to_string())?.len();
                println!("\n  ✅ Video Written: {}", format_bytes(size));
                Ok(size)
            }
                .await;

            if let Err(ref e) = res {
                println!("\n  ❌ Video Error: {}", e);
            }
            Some(res)
        }
        Stream::Clip(c) => {
            let video_opts = VodDownloadOptions {
                output_dir: Some(out_dir.to_path_buf()),
                output_name: Some(format!("{}_video", safe_name)),
                start_ms: test.start_ms,
                end_ms: test.end_ms,
                threads: 4,
                progress_hook: Some(progress_hook.clone()),
                ..Default::default()
            };

            let res = async {
                let path = download_clip_video(client, c, video_opts)
                    .await
                    .map_err(|e| format!("{:?}", e))?;
                if !path.exists() {
                    return Err("Reported success, but file is missing".to_string());
                }

                let size = fs::metadata(&path).map_err(|e| e.to_string())?.len();
                println!("\n  ✅ Video Written: {}", format_bytes(size));
                Ok(size)
            }
                .await;

            if let Err(ref e) = res {
                println!("\n  ❌ Video Error: {}", e);
            }
            Some(res)
        }
        Stream::Live(_) => {
            println!("  ℹ️ Video skipped (Live stream)");
            None
        }
        _ => {
            println!("  ⚠️ Video skipped (Unknown stream type)");
            None
        }
    };

    #[cfg(not(feature = "vod"))]
    println!("  ⚠️ Video Skipped (feature disabled)");

    TestReport {
        name: test.name.to_string(),
        metadata_res: Ok(format!("{:?}", stream.platform())),
        chat_res,
        #[cfg(feature = "vod")]
        video_res,
    }
}

// --- HELPER FUNCTIONS ---

fn create_progress_hook() -> ProgressCallback {
    Arc::new(|payload| match payload {
        ProgressPayload::Downloading { percent, .. } => {
            print!("\r     [Downloading] {}% ", percent);
            let _ = std::io::stdout().flush();
        }
        ProgressPayload::Merging => {
            print!("\r     [Ffmpeg] Stitching...                    ");
            let _ = std::io::stdout().flush();
        }
        ProgressPayload::Done => print!("\r     [Task Complete]                          "),
        ProgressPayload::Error { message } => {
            print!("\r     [Error] {}                               ", message)
        }
    })
}

fn print_header() {
    println!("==================================================");
    println!("     STREAM EXTRACTOR PIPELINE TEST MATRIX        ");
    #[cfg(feature = "wreq-backend")]
    println!("               [ ENGINE: WREQ ]                   ");
    #[cfg(feature = "reqwest-backend")]
    println!("              [ ENGINE: REQWEST ]                 ");
    println!("==================================================\n");
}

fn print_summary(reports: &[TestReport]) {
    println!(
        "\n========================================================================================="
    );
    println!(
        "{:<28} | {:<15} | {:<15} | {:<15}",
        "TEST NAME", "METADATA", "CHAT", "VIDEO"
    );
    println!(
        "-----------------------------------------------------------------------------------------"
    );

    for r in reports {
        let meta_str = match &r.metadata_res {
            Ok(p) => format!("✅ OK ({})", p),
            Err(_) => "❌ Failed".to_string(),
        };

        let chat_str = match &r.chat_res {
            Some(Ok(size)) => format!("✅ {}", format_bytes(*size)),
            Some(Err(_)) => "❌ Error".to_string(),
            None => "-".to_string(),
        };

        #[cfg(feature = "vod")]
        let video_str = match &r.video_res {
            Some(Ok(size)) => format!("✅ {}", format_bytes(*size)),
            Some(Err(_)) => "❌ Error".to_string(),
            None => "-".to_string(),
        };
        #[cfg(not(feature = "vod"))]
        let video_str = "⚠️ Disabled".to_string();

        println!(
            "{:<28} | {:<15} | {:<15} | {:<15}",
            r.name, meta_str, chat_str, video_str
        );
    }
    println!(
        "=========================================================================================\n"
    );
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.2} MB", bytes as f64 / 1024.0 / 1024.0)
    }
}