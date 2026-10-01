mod audio;
mod hardware;
mod shutdown;
mod video;

use hardware::HardwareProfile;
use shutdown::Shutdown;
use std::thread::JoinHandle;

fn join_thread(name: &str, handle: JoinHandle<()>) {
    if let Err(e) = handle.join() {
        log::error!("{name} thread panicked: {e:?}");
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let profile = HardwareProfile::AVERMEDIA_LIVE_GAMER_MINI_GC311;
    let shutdown = Shutdown::new();

    let (rx, stats, capture_handle) = video::spawn_capture(&profile, shutdown.clone())?;
    let (audio_status, audio_handle) = audio::spawn(profile.audio_device_keyword, shutdown.clone());

    let [width, height] = profile.video.size();
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([width as f32, height as f32]),
        vsync: false,
        ..Default::default()
    };
    let result = eframe::run_native(
        "Switch Capture",
        options,
        Box::new(move |_cc| {
            Ok(Box::new(video::DisplayApp::new(
                rx,
                profile.video,
                stats,
                audio_status,
            )))
        }),
    );

    shutdown.trigger();
    join_thread("Capture", capture_handle);
    join_thread("Audio", audio_handle);

    result.map_err(|e| anyhow::anyhow!("eframe error: {e}"))
}
