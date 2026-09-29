mod audio;
mod hardware;
mod video;

use hardware::HardwareProfile;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

fn main() -> anyhow::Result<()> {
    let profile = HardwareProfile::AVERMEDIA_LIVE_GAMER_MINI_GC311;
    let shutdown = Arc::new(AtomicBool::new(false));

    let (rx, stats, capture_handle) = video::spawn_capture(&profile, Arc::clone(&shutdown))?;

    let shutdown_audio = Arc::clone(&shutdown);
    let audio_handle = thread::spawn(move || {
        if let Err(e) = audio::run(profile.audio_device_keyword, shutdown_audio) {
            eprintln!("Audio pipeline error: {e}");
        }
    });

    let options = eframe::NativeOptions {
        // プロファイルの解像度に追従させる(以前は1920x1080固定でプロファイル変更時にずれていた)
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([profile.video.width as f32, profile.video.height as f32]),
        vsync: false,
        ..Default::default()
    };
    let result = eframe::run_native(
        "Switch Capture",
        options,
        Box::new(move |_cc| Ok(Box::new(video::DisplayApp::new(rx, profile.video, stats)))),
    );

    shutdown.store(true, Ordering::Relaxed);

    if let Err(e) = capture_handle.join() {
        eprintln!("Capture thread panicked: {e:?}");
    }
    if let Err(e) = audio_handle.join() {
        eprintln!("Audio thread panicked: {e:?}");
    }

    result.map_err(|e| anyhow::anyhow!("eframe error: {e}"))
}
