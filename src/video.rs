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

use crate::hardware::{HardwareProfile, VideoSpec};

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

/// 1スロットのチャネルで「常に最新フレームだけ」を配信する送信側。溢れて捨てられたバッファは呼び出し元へ返し再利用させる。
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
    shutdown: Arc<AtomicBool>,
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
    shutdown: Arc<AtomicBool>,
) {
    let [width, height] = spec.size();
    // 押し出されて戻ってきたバッファを次のフレームに使い回す(定常状態でのアロケーションを避ける)
    let mut spare: Option<Frame> = None;

    while !shutdown.load(Ordering::Relaxed) {
        let raw = match camera.frame() {
            Ok(raw) => raw,
            Err(e) => {
                eprintln!("Capture frame error: {e}");
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
        decode_yuyv(raw.buffer(), &mut pixels, width);
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

/// オーバーレイ用のFPS計測。一定時間ごとに窓を区切って平均を出す。
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

    fn tick(&mut self, captured: u64) {
        let elapsed = self.window_start.elapsed();
        if elapsed < METRICS_WINDOW {
            return;
        }
        let secs = elapsed.as_secs_f32();
        self.capture_fps = (captured - self.captured_base) as f32 / secs;
        self.display_fps = self.displayed as f32 / secs;
        *self = Self {
            capture_fps: self.capture_fps,
            display_fps: self.display_fps,
            ..Self::new(captured)
        };
    }
}

pub struct DisplayApp {
    rx: Receiver<Frame>,
    spec: VideoSpec,
    stats: Arc<CaptureStats>,
    texture: Option<TextureHandle>,
    show_overlay: bool,
    paused: bool,
    metrics: Metrics,
}

impl DisplayApp {
    pub fn new(rx: Receiver<Frame>, spec: VideoSpec, stats: Arc<CaptureStats>) -> Self {
        Self {
            rx,
            spec,
            stats,
            texture: None,
            show_overlay: false,
            paused: false,
            metrics: Metrics::new(0),
        }
    }

    /// 最小化中は描画とデコードを止め、復帰時に計測をリセットする。
    fn handle_minimized(&mut self, ctx: &egui::Context, minimized: bool, captured: u64) -> bool {
        self.stats
            .display_active
            .store(!minimized, Ordering::Relaxed);
        if minimized {
            self.paused = true;
            ctx.request_repaint_after(MINIMIZED_POLL_INTERVAL);
        } else if self.paused {
            self.paused = false;
            self.metrics = Metrics::new(captured);
        }
        minimized
    }

    fn upload_latest_frame(&mut self, ctx: &egui::Context) {
        // 溜まっているフレームは最新だけ使う。
        let Some(pixels) = self.rx.try_iter().last() else {
            return;
        };
        self.metrics.displayed += 1;

        let size = self.spec.size();
        let image = ColorImage {
            size,
            source_size: Vec2::new(size[0] as f32, size[1] as f32),
            pixels,
        };
        match &mut self.texture {
            Some(tex) => tex.set(image, TEXTURE_OPTIONS),
            None => self.texture = Some(ctx.load_texture("video_frame", image, TEXTURE_OPTIONS)),
        }
    }

    fn draw_video(&self, ctx: &egui::Context) {
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

    fn draw_overlay(&self, ctx: &egui::Context) {
        let text = format!(
            "Display: {:.1} fps\nCapture: {:.1} fps\nDropped: {}\nSource:  {}x{} @{}",
            self.metrics.display_fps,
            self.metrics.capture_fps,
            self.stats.dropped.load(Ordering::Relaxed),
            self.spec.width,
            self.spec.height,
            self.spec.fps,
        );
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
                        ui.label(RichText::new(text).monospace().color(Color32::WHITE));
                    });
            });
    }
}

impl eframe::App for DisplayApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let minimized = ctx.input(|i| i.viewport().minimized.unwrap_or(false));
        let captured = self.stats.captured.load(Ordering::Relaxed);
        if self.handle_minimized(ctx, minimized, captured) {
            return;
        }
        ctx.request_repaint();

        if ctx.input(|i| i.key_pressed(egui::Key::F2)) {
            self.show_overlay = !self.show_overlay;
        }

        self.upload_latest_frame(ctx);
        self.metrics.tick(captured);
        self.draw_video(ctx);
        if self.show_overlay {
            self.draw_overlay(ctx);
        }
    }
}
