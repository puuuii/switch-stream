use anyhow::Result;
use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use egui::{Color32, ColorImage, RichText, TextureHandle, TextureOptions, Vec2};
use nokhwa::{
    Camera,
    pixel_format::RgbFormat,
    query,
    utils::{
        ApiBackend, CameraFormat, CameraIndex, RequestedFormat, RequestedFormatType, Resolution,
    },
};
use rayon::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::audio::AudioStatus;
use crate::hardware::{HardwareProfile, VideoSpec};
use crate::shutdown::Shutdown;

pub type Frame = Vec<Color32>;

const METRICS_WINDOW: Duration = Duration::from_millis(500);
const MINIMIZED_POLL_INTERVAL: Duration = Duration::from_millis(250);
const FRAME_ERROR_BACKOFF: Duration = Duration::from_millis(10);
const TEXTURE_OPTIONS: TextureOptions = TextureOptions::LINEAR;

/// キャプチャスレッドとUIスレッドで共有する統計と、表示の有効/無効フラグ。
pub struct CaptureStats {
    captured: AtomicU64,
    dropped: AtomicU64,
    display_active: AtomicBool,
}

impl CaptureStats {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            captured: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            display_active: AtomicBool::new(true),
        })
    }
}

/// 1スロットのチャネルで「常に最新フレームだけ」を配信する送信側。溢れて捨てられたバッファは呼び出し元へ返す。
struct LatestFrameSender {
    tx: Sender<Frame>,
    drain: Receiver<Frame>,
}

impl LatestFrameSender {
    fn send(&self, frame: Frame) -> Option<Frame> {
        match self.tx.try_send(frame) {
            Ok(()) => None,
            Err(TrySendError::Full(frame)) => {
                let reclaimed = self.drain.try_recv().ok();
                let _ = self.tx.try_send(frame);
                reclaimed
            }
            Err(TrySendError::Disconnected(_)) => None,
        }
    }
}

/// キャプチャスレッドを起動し、最新フレームだけを流す受信側と共有統計を返す。
pub fn spawn_capture(
    profile: &HardwareProfile,
    shutdown: Shutdown,
) -> Result<(Receiver<Frame>, Arc<CaptureStats>, JoinHandle<()>)> {
    let camera = open_camera(profile)?;
    let spec = profile.video;

    let (tx, rx) = bounded::<Frame>(1);
    let publisher = LatestFrameSender {
        tx,
        drain: rx.clone(),
    };
    let stats = CaptureStats::new();
    let capture_stats = Arc::clone(&stats);

    let handle =
        thread::spawn(move || capture_loop(camera, spec, publisher, capture_stats, shutdown));
    Ok((rx, stats, handle))
}

fn open_camera(profile: &HardwareProfile) -> Result<Camera> {
    let spec = profile.video;
    let desired = CameraFormat::new(
        Resolution::new(spec.width, spec.height),
        spec.frame_format,
        spec.fps,
    );
    let requested = RequestedFormat::new::<RgbFormat>(RequestedFormatType::Exact(desired));

    let index = find_device_index(profile.video_device_keyword)?;
    let mut camera = Camera::new(index, requested)
        .map_err(|e| anyhow::anyhow!("Failed to open video device ({desired:?}): {e}"))?;
    camera.open_stream()?;

    let actual = camera.camera_format();
    anyhow::ensure!(
        actual.format() == spec.frame_format
            && actual.resolution() == Resolution::new(spec.width, spec.height),
        "Unexpected camera format: {actual:?}"
    );
    Ok(camera)
}

fn capture_loop(
    mut camera: Camera,
    spec: VideoSpec,
    publisher: LatestFrameSender,
    stats: Arc<CaptureStats>,
    shutdown: Shutdown,
) {
    let [width, height] = spec.size();
    // UIが遅れて押し出されたバッファだけ再利用できる。通常はテクスチャ更新で消費され毎フレーム確保になる。
    let mut spare: Option<Frame> = None;

    while !shutdown.is_set() {
        let raw = match camera.frame_raw() {
            Ok(raw) => raw,
            Err(e) => {
                log::warn!("Capture frame error: {e}");
                thread::sleep(FRAME_ERROR_BACKOFF);
                continue;
            }
        };
        stats.captured.fetch_add(1, Ordering::Relaxed);

        // 非表示中はデバイスの読み出しだけ続け、変換と送信を省く。
        if !stats.display_active.load(Ordering::Relaxed) {
            continue;
        }

        let mut pixels = spare
            .take()
            .unwrap_or_else(|| vec![Color32::BLACK; width * height]);
        decode_yuyv(&raw, &mut pixels, width);
        spare = publisher.send(pixels);
        if spare.is_some() {
            stats.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// デバイス名の部分一致で映像デバイスを特定する(index変動に強い)。
fn find_device_index(keyword: &str) -> Result<CameraIndex> {
    let devices = query(ApiBackend::Auto)
        .map_err(|e| anyhow::anyhow!("Failed to enumerate video devices: {e}"))?;
    let keyword = keyword.to_lowercase();

    match devices
        .iter()
        .find(|d| d.human_name().to_lowercase().contains(&keyword))
    {
        Some(device) => Ok(device.index().clone()),
        None => {
            let available: Vec<String> = devices.iter().map(|d| d.human_name()).collect();
            anyhow::bail!("No video device matching \"{keyword}\". Available: {available:?}")
        }
    }
}

fn yuv_to_color(luma: i32, u: i32, v: i32) -> Color32 {
    let (y, u, v) = (luma - 16, u - 128, v - 128);
    let r = ((298 * y + 409 * v + 128) >> 8).clamp(0, 255) as u8;
    let g = ((298 * y - 100 * u - 208 * v + 128) >> 8).clamp(0, 255) as u8;
    let b = ((298 * y + 516 * u + 128) >> 8).clamp(0, 255) as u8;
    Color32::from_rgb(r, g, b)
}

fn decode_yuyv(yuyv: &[u8], out: &mut [Color32], width: usize) {
    out.par_chunks_exact_mut(width)
        .zip(yuyv.par_chunks_exact(width * 2))
        .for_each(|(out_row, in_row)| {
            for (pair, quad) in out_row.chunks_exact_mut(2).zip(in_row.chunks_exact(4)) {
                let (y0, u, y1, v) = (
                    quad[0] as i32,
                    quad[1] as i32,
                    quad[2] as i32,
                    quad[3] as i32,
                );
                pair[0] = yuv_to_color(y0, u, v);
                pair[1] = yuv_to_color(y1, u, v);
            }
        });
}

/// FPS計測。一定時間ごとに窓を区切って平均を出す。
struct Metrics {
    window_start: Instant,
    captured_base: u64,
    displayed: u32,
    capture_fps: f32,
    display_fps: f32,
}

impl Metrics {
    fn new(captured: u64) -> Self {
        Self {
            window_start: Instant::now(),
            captured_base: captured,
            displayed: 0,
            capture_fps: 0.0,
            display_fps: 0.0,
        }
    }

    fn start_window(&mut self, captured: u64) {
        self.window_start = Instant::now();
        self.captured_base = captured;
        self.displayed = 0;
    }

    fn tick(&mut self, captured: u64) {
        let elapsed = self.window_start.elapsed();
        if elapsed < METRICS_WINDOW {
            return;
        }
        let secs = elapsed.as_secs_f32();
        self.capture_fps = (captured - self.captured_base) as f32 / secs;
        self.display_fps = self.displayed as f32 / secs;
        self.start_window(captured);
    }
}

/// 受信したフレームをテクスチャへ反映して描画する。
struct VideoView {
    rx: Receiver<Frame>,
    size: [usize; 2],
    texture: Option<TextureHandle>,
}

impl VideoView {
    fn new(rx: Receiver<Frame>, spec: VideoSpec) -> Self {
        Self {
            rx,
            size: spec.size(),
            texture: None,
        }
    }

    /// 溜まっているフレームは最新だけ使う。更新があればtrueを返す。
    fn upload_latest(&mut self, ctx: &egui::Context) -> bool {
        let Some(pixels) = self.rx.try_iter().last() else {
            return false;
        };
        let image = ColorImage {
            size: self.size,
            source_size: Vec2::new(self.size[0] as f32, self.size[1] as f32),
            pixels,
        };
        match &mut self.texture {
            Some(tex) => tex.set(image, TEXTURE_OPTIONS),
            None => self.texture = Some(ctx.load_texture("video_frame", image, TEXTURE_OPTIONS)),
        }
        true
    }

    fn draw(&self, ctx: &egui::Context) {
        egui::CentralPanel::default()
            .frame(egui::Frame::NONE)
            .show(ctx, |ui| match &self.texture {
                Some(tex) => {
                    ui.add(egui::Image::new(tex).fit_to_exact_size(ui.available_size()));
                }
                None => {
                    ui.label("Waiting for video...");
                }
            });
    }
}

/// F2で切り替える統計オーバーレイ。
struct Overlay {
    visible: bool,
    metrics: Metrics,
}

impl Overlay {
    fn new() -> Self {
        Self {
            visible: false,
            metrics: Metrics::new(0),
        }
    }

    fn toggle(&mut self) {
        self.visible = !self.visible;
    }

    fn frame_displayed(&mut self) {
        self.metrics.displayed += 1;
    }

    fn tick(&mut self, captured: u64) {
        self.metrics.tick(captured);
    }

    fn restart(&mut self, captured: u64) {
        self.metrics = Metrics::new(captured);
    }

    fn draw(
        &self,
        ctx: &egui::Context,
        stats: &CaptureStats,
        audio: &AudioStatus,
        spec: VideoSpec,
    ) {
        if !self.visible {
            return;
        }
        let mut text = format!(
            "Display: {:.1} fps\nCapture: {:.1} fps\nDropped: {}\nSource:  {}x{} @{}\nAudio:   in {:.1} ms / out {:.1} ms",
            self.metrics.display_fps,
            self.metrics.capture_fps,
            stats.dropped.load(Ordering::Relaxed),
            spec.width,
            spec.height,
            spec.fps,
            audio.input_latency_ms(),
            audio.output_latency_ms(),
        );
        if let Some(err) = audio.error() {
            text = format!("{text}\n{err}");
        }
        let color = if audio.error().is_some() {
            Color32::LIGHT_RED
        } else {
            Color32::WHITE
        };

        egui::Area::new(egui::Id::new("stats_overlay"))
            .order(egui::Order::Foreground)
            .anchor(egui::Align2::LEFT_TOP, [8.0, 8.0])
            .interactable(false)
            .show(ctx, |ui| {
                egui::Frame::NONE
                    .fill(Color32::from_black_alpha(160))
                    .corner_radius(4.0)
                    .inner_margin(8.0)
                    .show(ui, |ui| {
                        ui.label(RichText::new(text).monospace().color(color));
                    });
            });
    }
}

pub struct DisplayApp {
    view: VideoView,
    overlay: Overlay,
    spec: VideoSpec,
    stats: Arc<CaptureStats>,
    audio: Arc<AudioStatus>,
    paused: bool,
}

impl DisplayApp {
    pub fn new(
        rx: Receiver<Frame>,
        spec: VideoSpec,
        stats: Arc<CaptureStats>,
        audio: Arc<AudioStatus>,
    ) -> Self {
        Self {
            view: VideoView::new(rx, spec),
            overlay: Overlay::new(),
            spec,
            stats,
            audio,
            paused: false,
        }
    }

    /// 最小化中はデコードを止め、復帰時に計測をリセットする。
    fn set_minimized(&mut self, ctx: &egui::Context, minimized: bool, captured: u64) {
        self.stats
            .display_active
            .store(!minimized, Ordering::Relaxed);
        if minimized {
            self.paused = true;
            ctx.request_repaint_after(MINIMIZED_POLL_INTERVAL);
        } else if self.paused {
            self.paused = false;
            self.overlay.restart(captured);
        }
    }
}

impl eframe::App for DisplayApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let minimized = ctx.input(|i| i.viewport().minimized.unwrap_or(false));
        let captured = self.stats.captured.load(Ordering::Relaxed);
        self.set_minimized(ctx, minimized, captured);
        if minimized {
            return;
        }
        ctx.request_repaint();

        if ctx.input(|i| i.key_pressed(egui::Key::F2)) {
            self.overlay.toggle();
        }

        if self.view.upload_latest(ctx) {
            self.overlay.frame_displayed();
        }
        self.overlay.tick(captured);
        self.view.draw(ctx);
        self.overlay.draw(ctx, &self.stats, &self.audio, self.spec);
    }
}
