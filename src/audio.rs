use crate::shutdown::Shutdown;
use anyhow::{Context, Result, ensure};
use cpal::Stream;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::traits::{Consumer, Observer, Producer, Split};
use ringbuf::{HeapCons, HeapRb};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const BUFFER_FRAMES: u32 = 128;
const MARGIN_FRAMES: usize = 64;
const RING_MS: usize = 200;
const MAX_FILL_FACTOR: usize = 4;
const DRIFT_GAIN: f64 = 0.01;
const MAX_DRIFT: f64 = 0.005;
const FILL_SMOOTHING: f64 = 0.05;
const POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// UIへ公開する音声側の状態。デバイス遅延と致命的エラー。
#[derive(Default)]
pub struct AudioStatus {
    input_us: AtomicU64,
    output_us: AtomicU64,
    in_frames: AtomicUsize,
    error: Mutex<Option<String>>,
}

impl AudioStatus {
    fn record_input(&self, info: &cpal::InputCallbackInfo) {
        let ts = info.timestamp();
        if let Some(d) = ts.callback.duration_since(&ts.capture) {
            self.input_us.store(d.as_micros() as u64, Ordering::Relaxed);
        }
    }

    fn record_output(&self, info: &cpal::OutputCallbackInfo) {
        let ts = info.timestamp();
        if let Some(d) = ts.playback.duration_since(&ts.callback) {
            self.output_us
                .store(d.as_micros() as u64, Ordering::Relaxed);
        }
    }

    fn report_error(&self, message: String) {
        log::error!("{message}");
        *self.error.lock().unwrap_or_else(PoisonError::into_inner) = Some(message);
    }

    pub fn input_latency_ms(&self) -> f32 {
        self.input_us.load(Ordering::Relaxed) as f32 / 1000.0
    }

    pub fn output_latency_ms(&self) -> f32 {
        self.output_us.load(Ordering::Relaxed) as f32 / 1000.0
    }

    pub fn error(&self) -> Option<String> {
        self.error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// 出力コールバックの中で、入力リングから取り出しつつ線形補間で速度を調整する。
struct Renderer {
    cons: HeapCons<f32>,
    status: Arc<AudioStatus>,
    in_ch: usize,
    out_ch: usize,
    base_ratio: f64,
    prev: Vec<f32>,
    next: Vec<f32>,
    frac: f64,
    fill_avg: f64,
    primed: bool,
}

impl Renderer {
    fn new(
        cons: HeapCons<f32>,
        status: Arc<AudioStatus>,
        input: &cpal::SupportedStreamConfig,
        output: &cpal::SupportedStreamConfig,
    ) -> Self {
        let in_ch = input.channels() as usize;
        Self {
            cons,
            status,
            in_ch,
            out_ch: output.channels() as usize,
            base_ratio: input.sample_rate().0 as f64 / output.sample_rate().0 as f64,
            prev: vec![0.0; in_ch],
            next: vec![0.0; in_ch],
            frac: 1.0,
            fill_avg: 0.0,
            primed: false,
        }
    }

    fn unprime(&mut self) {
        self.primed = false;
        self.prev.fill(0.0);
        self.next.fill(0.0);
        self.frac = 1.0;
    }

    fn render(&mut self, out: &mut [f32]) {
        let out_frames = out.len() / self.out_ch;
        let in_frames = self.status.in_frames.load(Ordering::Relaxed);
        let target = in_frames + out_frames + MARGIN_FRAMES;
        let mut fill = self.cons.occupied_len() / self.in_ch;

        // 目標量が溜まるまでは無音を流す。
        if !self.primed {
            if fill < target {
                out.fill(0.0);
                return;
            }
            self.primed = true;
            self.fill_avg = target as f64;
        }
        if fill > target * MAX_FILL_FACTOR {
            self.cons.skip((fill - target) * self.in_ch);
            fill = target;
        }

        // リングが目標より多ければ速く消費し、少なければ遅く消費する。
        self.fill_avg += FILL_SMOOTHING * (fill as f64 - self.fill_avg);
        let t = target as f64;
        let correction = (DRIFT_GAIN * (self.fill_avg - t) / t).clamp(-MAX_DRIFT, MAX_DRIFT);
        let ratio = self.base_ratio * (1.0 + correction);

        for i in 0..out_frames {
            while self.frac >= 1.0 {
                // 入力が尽きたら無音にして、溜まり直すまで待つ。
                if self.cons.occupied_len() < self.in_ch {
                    self.unprime();
                    out[i * self.out_ch..].fill(0.0);
                    return;
                }
                std::mem::swap(&mut self.prev, &mut self.next);
                self.cons.pop_slice(&mut self.next);
                self.frac -= 1.0;
            }

            let t = self.frac as f32;
            let frame = &mut out[i * self.out_ch..(i + 1) * self.out_ch];
            for (ch, sample) in frame.iter_mut().enumerate() {
                let c = ch.min(self.in_ch - 1);
                *sample = self.prev[c] + (self.next[c] - self.prev[c]) * t;
            }
            self.frac += ratio;
        }
    }
}

/// コールバックが`&[f32]`/`&mut [f32]`固定なので、f32以外のデバイスは明示的に弾く。
fn require_f32(config: &cpal::SupportedStreamConfig) -> Result<()> {
    ensure!(
        config.sample_format() == cpal::SampleFormat::F32,
        "Unsupported sample format {:?} (f32 required)",
        config.sample_format()
    );
    Ok(())
}

fn find_input_device(host: &cpal::Host, keyword: &str) -> Result<cpal::Device> {
    let keyword = keyword.to_lowercase();
    host.input_devices()?
        .find(|d| {
            d.name()
                .map(|n| n.to_lowercase().contains(&keyword))
                .unwrap_or(false)
        })
        .context("Capture audio device not found")
}

fn stream_config(
    config: &cpal::SupportedStreamConfig,
    buffer_size: cpal::BufferSize,
) -> cpal::StreamConfig {
    let mut stream_config: cpal::StreamConfig = config.clone().into();
    stream_config.buffer_size = buffer_size;
    stream_config
}

/// 小さいバッファを要求し、拒否されたらデフォルトで作り直す。
fn with_buffer_fallback<T>(
    label: &str,
    attempt: impl Fn(cpal::BufferSize) -> Result<T>,
) -> Result<T> {
    attempt(cpal::BufferSize::Fixed(BUFFER_FRAMES)).or_else(|e| {
        log::warn!("Fixed {label} buffer unavailable ({e}); using default");
        attempt(cpal::BufferSize::Default)
    })
}

fn default_output_name(host: &cpal::Host) -> Option<String> {
    host.default_output_device().and_then(|d| d.name().ok())
}

/// フィールドの宣言順がDrop順になる(出力→入力)。
struct Pipeline {
    _output: Stream,
    _input: Stream,
    output_name: Option<String>,
}

fn start_pipeline(
    host: &cpal::Host,
    device_keyword: &str,
    status: &Arc<AudioStatus>,
) -> Result<Pipeline> {
    let input_device = find_input_device(host, device_keyword)?;
    let input_config = input_device.default_input_config()?;
    require_f32(&input_config)?;
    let output_device = host
        .default_output_device()
        .context("Default output device not found")?;
    let output_config = output_device.default_output_config()?;
    require_f32(&output_config)?;

    let in_ch = input_config.channels() as usize;
    let ring_samples = input_config.sample_rate().0 as usize * in_ch * RING_MS / 1000;

    let (input_stream, cons) = with_buffer_fallback("input", |size| {
        let (mut prod, cons) = HeapRb::<f32>::new(ring_samples).split();
        let status = Arc::clone(status);
        let stream = input_device.build_input_stream(
            &stream_config(&input_config, size),
            move |data: &[f32], info: &cpal::InputCallbackInfo| {
                status.record_input(info);
                status
                    .in_frames
                    .store(data.len() / in_ch, Ordering::Relaxed);
                // 入りきらないときは丸ごと捨てて、チャンネルの並びを保つ。
                if prod.vacant_len() >= data.len() {
                    prod.push_slice(data);
                }
            },
            |err| log::error!("Audio input stream error: {err}"),
            None,
        )?;
        Ok((stream, cons))
    })?;

    let renderer = Arc::new(Mutex::new(Renderer::new(
        cons,
        Arc::clone(status),
        &input_config,
        &output_config,
    )));
    let output_stream = with_buffer_fallback("output", |size| {
        let renderer = Arc::clone(&renderer);
        let status = Arc::clone(status);
        let stream = output_device.build_output_stream(
            &stream_config(&output_config, size),
            move |out: &mut [f32], info: &cpal::OutputCallbackInfo| {
                status.record_output(info);
                match renderer.try_lock() {
                    Ok(mut r) => r.render(out),
                    Err(_) => out.fill(0.0),
                }
            },
            |err| log::error!("Audio output stream error: {err}"),
            None,
        )?;
        Ok(stream)
    })?;

    input_stream.play()?;
    output_stream.play()?;
    Ok(Pipeline {
        _output: output_stream,
        _input: input_stream,
        output_name: output_device.name().ok(),
    })
}

/// 音声パススルーのスレッドを起動し、UI向けの状態を返す。
pub fn spawn(
    device_keyword: &'static str,
    shutdown: Shutdown,
) -> (Arc<AudioStatus>, JoinHandle<()>) {
    let status = Arc::new(AudioStatus::default());
    let thread_status = Arc::clone(&status);
    let handle = thread::spawn(move || {
        if let Err(e) = run(device_keyword, &shutdown, &thread_status) {
            thread_status.report_error(format!("Audio pipeline error: {e:#}"));
        }
    });
    (status, handle)
}

/// shutdownまでブロックし、出力デバイスが変わったらパイプラインを作り直す。
fn run(device_keyword: &str, shutdown: &Shutdown, status: &Arc<AudioStatus>) -> Result<()> {
    let host = cpal::default_host();
    let mut pipeline = Some(start_pipeline(&host, device_keyword, status)?);

    while !shutdown.wait(POLL_INTERVAL) {
        let name = default_output_name(&host);
        if pipeline.as_ref().is_some_and(|p| p.output_name == name) {
            continue;
        }

        // 入力デバイスを開き直すので、先に旧パイプラインを閉じる。
        pipeline = None;
        match start_pipeline(&host, device_keyword, status) {
            Ok(p) => {
                log::info!("Output device switched to {name:?}");
                pipeline = Some(p);
            }
            Err(e) => log::warn!("Failed to restart audio pipeline: {e:#}"),
        }
    }
    Ok(())
}
