mod audio;
mod hardware;
mod video;

use hardware::HardwareProfile;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

fn spawn_audio(device_keyword: &'static str, shutdown: Arc<AtomicBool>) -> JoinHandle<()> {
    thread::spawn(move || {
        if let Err(e) = audio::run(device_keyword, shutdown) {
            eprintln!("Audio pipeline error: {e}");
        }
    })
}

fn join_thread(name: &str, handle: JoinHandle<()>) {
    if let Err(e) = handle.join() {
        eprintln!("{name} thread panicked: {e:?}");
    }
}

fn main() -> anyhow::Result<()> {
    let profile = HardwareProfile::AVERMEDIA_LIVE_GAMER_MINI_GC311;
    let shutdown = Arc::new(AtomicBool::new(false));

    let (rx, stats, capture_handle) = video::spawn_capture(&profile, Arc::clone(&shutdown))?;
    let audio_handle = spawn_audio(profile.audio_device_keyword, Arc::clone(&shutdown));

    let [width, height] = profile.video.size();
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([width as f32, height as f32]),
        vsync: false,
        ..Default::default()
    };
    let result = eframe::run_native(
        "Switch Capture",
        options,
        Box::new(move |_cc| Ok(Box::new(video::DisplayApp::new(rx, profile.video, stats)))),
    );

    shutdown.store(true, Ordering::Relaxed);
    join_thread("Capture", capture_handle);
    join_thread("Audio", audio_handle);

    result.map_err(|e| anyhow::anyhow!("eframe error: {e}"))
}
