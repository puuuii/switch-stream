mod resampler;
mod router;
mod worker;

use crate::shutdown::Shutdown;
use anyhow::{Context, Result, ensure};
use cpal::Stream;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::traits::{Producer, Split};
use ringbuf::{HeapCons, HeapRb};
use router::OutputRouter;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use worker::Worker;

const INPUT_RING_MS: f64 = 200.0;
// 192kHz x 8ch で100ms分。出力フォーマットが変わっても再確保しない。
const OUTPUT_RING_CAPACITY: usize = 192_000 * 8 / 10;
const LOW_LATENCY_FRAMES: u32 = 128;
const OUTPUT_DEVICE_POLL_INTERVAL: Duration = Duration::from_millis(1000);

// ---- 共有状態 ----

/// インターリーブ済みPCMのレートとチャンネル数。入力・出力の双方で使う。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AudioSpec {
    sample_rate: u32,
    channels: usize,
}

impl AudioSpec {
    fn of(config: &cpal::SupportedStreamConfig) -> Self {
        Self {
            sample_rate: config.sample_rate().0,
            channels: config.channels() as usize,
        }
    }

    fn samples_for_ms(&self, ms: f64) -> usize {
        let frames = (self.sample_rate as f64 * ms / 1000.0).round() as usize;
        frames * self.channels
    }

    fn ms_for_samples(&self, samples: usize) -> f64 {
        samples as f64 * 1000.0 / (self.sample_rate as f64 * self.channels as f64)
    }
}

/// レートとチャンネル数を1つのatomicにまとめ、読み出し時の整合を保つ。
struct SharedSpec(AtomicU64);

impl SharedSpec {
    fn new(spec: AudioSpec) -> Self {
        Self(AtomicU64::new(Self::pack(spec)))
    }

    fn pack(spec: AudioSpec) -> u64 {
        ((spec.sample_rate as u64) << 32) | spec.channels as u64
    }

    fn set(&self, spec: AudioSpec) {
        self.0.store(Self::pack(spec), Ordering::Relaxed);
    }

    fn get(&self) -> AudioSpec {
        let v = self.0.load(Ordering::Relaxed);
        AudioSpec {
            sample_rate: (v >> 32) as u32,
            channels: (v & 0xffff_ffff) as usize,
        }
    }
}

/// コールバック・リサンプルスレッド・メインスレッドで共有する状態。
struct Shared {
    output_spec: SharedSpec,
    output_active: AtomicBool,
    in_callback_samples: AtomicUsize,
    out_callback_samples: AtomicUsize,
    status: Arc<AudioStatus>,
}

impl Shared {
    fn new(output: AudioSpec, status: Arc<AudioStatus>) -> Self {
        Self {
            output_spec: SharedSpec::new(output),
            output_active: AtomicBool::new(false),
            in_callback_samples: AtomicUsize::new(0),
            out_callback_samples: AtomicUsize::new(0),
            status,
        }
    }

    fn in_period_ms(&self, input: AudioSpec) -> f64 {
        input.ms_for_samples(self.in_callback_samples.load(Ordering::Relaxed))
    }

    fn out_period_ms(&self, output: AudioSpec) -> f64 {
        output.ms_for_samples(self.out_callback_samples.load(Ordering::Relaxed))
    }
}

/// UIへ公開する音声側の状態。デバイス遅延(cpalのタイムスタンプ由来)と致命的エラー。
#[derive(Default)]
pub struct AudioStatus {
    input_us: AtomicU64,
    output_us: AtomicU64,
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

// ---- デバイス ----

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

fn default_output(host: &cpal::Host) -> Result<(cpal::Device, cpal::SupportedStreamConfig)> {
    let device = host
        .default_output_device()
        .context("Default output device not found")?;
    let config = device.default_output_config()?;
    require_f32(&config)?;
    Ok((device, config))
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
    attempt(cpal::BufferSize::Fixed(LOW_LATENCY_FRAMES)).or_else(|e| {
        log::warn!("Fixed {label} buffer unavailable ({e}); using default");
        attempt(cpal::BufferSize::Default)
    })
}

fn build_input_stream(
    device: &cpal::Device,
    config: &cpal::SupportedStreamConfig,
    ring_samples: usize,
    shared: Arc<Shared>,
    waker: Arc<OnceLock<thread::Thread>>,
) -> Result<(Stream, HeapCons<f32>)> {
    with_buffer_fallback("input", |buffer_size| {
        let (mut prod, cons) = HeapRb::<f32>::new(ring_samples).split();
        let shared = Arc::clone(&shared);
        let waker = Arc::clone(&waker);
        let stream = device.build_input_stream(
            &stream_config(config, buffer_size),
            move |data: &[f32], info: &cpal::InputCallbackInfo| {
                shared.status.record_input(info);
                shared
                    .in_callback_samples
                    .store(data.len(), Ordering::Relaxed);
                prod.push_slice(data);
                if let Some(worker) = waker.get() {
                    worker.unpark();
                }
            },
            |err| log::error!("Audio input stream error: {err}"),
            None,
        )?;
        Ok((stream, cons))
    })
}

// ---- パイプライン ----

/// フィールドの宣言順がDrop順になる(出力→入力→リサンプルスレッド)。
struct AudioPipeline {
    router: OutputRouter,
    _input_stream: Stream,
    _worker: Worker,
}

impl AudioPipeline {
    fn start(device_keyword: &str, status: Arc<AudioStatus>) -> Result<Self> {
        let host = cpal::default_host();
        let input_device = find_input_device(&host, device_keyword)?;
        let input_config = input_device.default_input_config()?;
        require_f32(&input_config)?;
        let input = AudioSpec::of(&input_config);

        let (output_device, output_config) = default_output(&host)?;
        let shared = Arc::new(Shared::new(AudioSpec::of(&output_config), status));

        let (out_prod, out_cons) = HeapRb::<f32>::new(OUTPUT_RING_CAPACITY).split();
        let consumer = Arc::new(Mutex::new(out_cons));

        let waker: Arc<OnceLock<thread::Thread>> = Arc::new(OnceLock::new());
        let (input_stream, raw_cons) = build_input_stream(
            &input_device,
            &input_config,
            input.samples_for_ms(INPUT_RING_MS),
            Arc::clone(&shared),
            Arc::clone(&waker),
        )?;

        let worker = Worker::spawn(raw_cons, out_prod, Arc::clone(&shared), input);
        let _ = waker.set(worker.waker());
        input_stream.play()?;

        let router = OutputRouter::start(host, consumer, shared, &output_device, &output_config)?;
        Ok(Self {
            router,
            _input_stream: input_stream,
            _worker: worker,
        })
    }
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

/// shutdownまでブロックし、出力デバイスを`OUTPUT_DEVICE_POLL_INTERVAL`ごとに確認する。
fn run(device_keyword: &str, shutdown: &Shutdown, status: &Arc<AudioStatus>) -> Result<()> {
    let mut pipeline = AudioPipeline::start(device_keyword, Arc::clone(status))?;
    while !shutdown.wait(OUTPUT_DEVICE_POLL_INTERVAL) {
        pipeline.router.follow_default_device();
    }
    Ok(())
}
