use anyhow::{Context, Result};
use cpal::Stream;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::traits::{Consumer, Observer, Producer, Split};
use ringbuf::{HeapCons, HeapProd, HeapRb};
use rubato::audioadapter_buffers::direct::InterleavedSlice;
use rubato::{
    Adjustable, Async, FixedAsync, Resampler, SincInterpolationParameters, SincInterpolationType,
    WindowFunction,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

const INPUT_RING_MS: usize = 200;
// 192kHz x 8ch で100ms分。出力フォーマットが変わっても再確保しない。
const OUTPUT_RING_CAPACITY: usize = 192_000 * 8 / 10;
const LOW_LATENCY_FRAMES: u32 = 128;
const DEFAULT_TARGET_MS: f64 = 8.0;
const FILL_MARGIN_MS: f64 = 3.0;
const MAX_FILL_FACTOR: usize = 3;
const RESAMPLE_CHUNK_FRAMES: usize = 128;
const SINC_LEN: usize = 128;
const MAX_RATIO_RELATIVE: f64 = 1.1;
const DRIFT_GAIN: f64 = 0.01;
const MAX_DRIFT: f64 = 0.005;
const FILL_SMOOTHING: f64 = 0.05;
const WORKER_WAKE_TIMEOUT: Duration = Duration::from_millis(10);
const FILL_LOG_INTERVAL: Duration = Duration::from_secs(1);
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const OUTPUT_DEVICE_POLL_INTERVAL: Duration = Duration::from_millis(1000);

fn debug_err(e: impl std::fmt::Debug) -> anyhow::Error {
    anyhow::anyhow!("{e:?}")
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct OutputSpec {
    sample_rate: u32,
    channels: usize,
}

impl OutputSpec {
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

/// 出力フォーマットをリサンプルスレッドと共有する。レートとチャンネル数を1つのatomicにまとめて整合を保つ。
struct OutputFormat(AtomicU64);

impl OutputFormat {
    fn new(spec: OutputSpec) -> Self {
        Self(AtomicU64::new(Self::pack(spec)))
    }

    fn pack(spec: OutputSpec) -> u64 {
        ((spec.sample_rate as u64) << 32) | spec.channels as u64
    }

    fn set(&self, spec: OutputSpec) {
        self.0.store(Self::pack(spec), Ordering::Relaxed);
    }

    fn get(&self) -> OutputSpec {
        let v = self.0.load(Ordering::Relaxed);
        OutputSpec {
            sample_rate: (v >> 32) as u32,
            channels: (v & 0xffff_ffff) as usize,
        }
    }
}

/// cpalのタイムスタンプから得たデバイス側の遅延(マイクロ秒)。診断用。
#[derive(Default)]
struct LatencyProbe {
    input_us: AtomicU64,
    output_us: AtomicU64,
}

/// コールバック・リサンプルスレッド・メインスレッドで共有する状態。
struct Shared {
    format: OutputFormat,
    output_active: AtomicBool,
    in_callback_samples: AtomicUsize,
    out_callback_samples: AtomicUsize,
    probe: LatencyProbe,
}

impl Shared {
    fn new(spec: OutputSpec) -> Self {
        Self {
            format: OutputFormat::new(spec),
            output_active: AtomicBool::new(false),
            in_callback_samples: AtomicUsize::new(0),
            out_callback_samples: AtomicUsize::new(0),
            probe: LatencyProbe::default(),
        }
    }

    fn in_period_ms(&self, channels: usize, rate: u32) -> f64 {
        self.in_callback_samples.load(Ordering::Relaxed) as f64 * 1000.0
            / (rate as f64 * channels as f64)
    }

    fn out_period_ms(&self, spec: OutputSpec) -> f64 {
        spec.ms_for_samples(self.out_callback_samples.load(Ordering::Relaxed))
    }
}

/// 入力チャンネルを出力チャンネル数に合わせて1フレーム分pushする(不足分はch0で埋める)。溢れたサンプルは捨てる。
fn push_mapped(
    producer: &mut HeapProd<f32>,
    in_channels: usize,
    out_channels: usize,
    sample: impl Fn(usize) -> f32,
) {
    for out_ch in 0..out_channels {
        let ch = if out_ch < in_channels { out_ch } else { 0 };
        let _ = producer.try_push(sample(ch));
    }
}

/// rubatoのsincリサンプラと、出力リングの充填量に基づくドリフト補正。
struct StreamResampler {
    inner: Async<f32>,
    in_channels: usize,
    spec: OutputSpec,
    base_ratio: f64,
    in_buf: Vec<f32>,
    out_buf: Vec<f32>,
    target_fill: usize,
    fill_avg: f64,
}

impl StreamResampler {
    fn new(in_channels: usize, in_rate: u32, spec: OutputSpec) -> Result<Self> {
        let base_ratio = spec.sample_rate as f64 / in_rate as f64;
        let params = SincInterpolationParameters::new(SINC_LEN, WindowFunction::BlackmanHarris2)
            .interpolation(SincInterpolationType::Cubic);
        let inner = Async::<f32>::new_sinc(
            base_ratio,
            MAX_RATIO_RELATIVE,
            &params,
            RESAMPLE_CHUNK_FRAMES,
            in_channels,
            FixedAsync::Input,
        )
        .map_err(debug_err)
        .context("Failed to build resampler")?;

        let target_fill = spec.samples_for_ms(DEFAULT_TARGET_MS);
        Ok(Self {
            in_buf: vec![0.0; inner.input_frames_next() * in_channels],
            out_buf: vec![0.0; inner.output_frames_max() * in_channels],
            fill_avg: target_fill as f64,
            target_fill,
            inner,
            in_channels,
            spec,
            base_ratio,
        })
    }

    fn set_target_fill(&mut self, samples: usize) {
        self.target_fill = samples.max(self.spec.channels);
    }

    fn has_chunk(&self, raw: &HeapCons<f32>) -> bool {
        raw.occupied_len() >= self.in_buf.len()
    }

    fn process_chunk(&mut self, raw: &mut HeapCons<f32>, out: &mut HeapProd<f32>) -> Result<()> {
        raw.pop_slice(&mut self.in_buf);
        self.track_drift(out.occupied_len())?;
        let produced = self.resample()?;

        if out.occupied_len() > self.target_fill * MAX_FILL_FACTOR {
            return Ok(());
        }
        let ch = self.in_channels;
        for frame in self.out_buf[..produced * ch].chunks_exact(ch) {
            push_mapped(out, ch, self.spec.channels, |c| frame[c]);
        }
        Ok(())
    }

    /// 充填量が目標より多ければ比率を下げ、少なければ上げる。
    fn track_drift(&mut self, fill: usize) -> Result<()> {
        let target = self.target_fill as f64;
        self.fill_avg += FILL_SMOOTHING * (fill as f64 - self.fill_avg);
        let correction =
            (-DRIFT_GAIN * (self.fill_avg - target) / target).clamp(-MAX_DRIFT, MAX_DRIFT);
        self.inner
            .set_resample_ratio(self.base_ratio * (1.0 + correction), true)
            .map_err(debug_err)
    }

    fn resample(&mut self) -> Result<usize> {
        let ch = self.in_channels;
        let input =
            InterleavedSlice::new(&self.in_buf, ch, self.in_buf.len() / ch).map_err(debug_err)?;
        let out_frames = self.out_buf.len() / ch;
        let mut output =
            InterleavedSlice::new_mut(&mut self.out_buf, ch, out_frames).map_err(debug_err)?;
        let (_, produced) = self
            .inner
            .process_into_buffer(&input, &mut output, None)
            .map_err(debug_err)?;
        Ok(produced)
    }
}

fn prime(out: &mut HeapProd<f32>, samples: usize) {
    for _ in 0..samples {
        let _ = out.try_push(0.0);
    }
}

fn log_latency(
    shared: &Shared,
    spec: OutputSpec,
    in_channels: usize,
    in_rate: u32,
    target_fill: usize,
    raw: &HeapCons<f32>,
    out: &HeapProd<f32>,
) {
    let in_ring_ms = raw.occupied_len() as f64 * 1000.0 / (in_rate as f64 * in_channels as f64);
    eprintln!(
        "out ring: {:.1} ms (target {:.1}) | in ring: {:.1} ms | dev in: {:.1} ms | dev out: {:.1} ms | cb in/out: {:.1}/{:.1} ms",
        spec.ms_for_samples(out.occupied_len()),
        spec.ms_for_samples(target_fill),
        in_ring_ms,
        shared.probe.input_us.load(Ordering::Relaxed) as f64 / 1000.0,
        shared.probe.output_us.load(Ordering::Relaxed) as f64 / 1000.0,
        shared.in_period_ms(in_channels, in_rate),
        shared.out_period_ms(spec),
    );
}

fn resampler_loop(
    raw: &mut HeapCons<f32>,
    out: &mut HeapProd<f32>,
    shared: &Shared,
    in_channels: usize,
    in_rate: u32,
    stop: &AtomicBool,
) -> Result<()> {
    let mut spec = shared.format.get();
    let mut resampler = StreamResampler::new(in_channels, in_rate, spec)?;
    let mut primed = false;
    let mut last_log = Instant::now();

    while !stop.load(Ordering::Relaxed) {
        // 出力が消費を始めるまでは入力を捨て、リングに遅延を溜め込まない。
        if !shared.output_active.load(Ordering::Relaxed) {
            raw.clear();
            thread::park_timeout(WORKER_WAKE_TIMEOUT);
            continue;
        }

        let latest = shared.format.get();
        if latest != spec {
            spec = latest;
            match StreamResampler::new(in_channels, in_rate, spec) {
                Ok(new) => resampler = new,
                Err(e) => eprintln!("{e:#}"),
            }
        }

        // 充填の目標はコールバック周期の半分+余裕。周期が短いほど遅延も短くなる。
        let period_ms = shared
            .in_period_ms(in_channels, in_rate)
            .max(shared.out_period_ms(spec));
        resampler.set_target_fill(spec.samples_for_ms(period_ms / 2.0 + FILL_MARGIN_MS));

        if !primed {
            prime(out, resampler.target_fill);
            primed = true;
        }

        while resampler.has_chunk(raw) {
            if let Err(e) = resampler.process_chunk(raw, out) {
                eprintln!("Resample error: {e}");
                break;
            }
        }

        if cfg!(debug_assertions) && last_log.elapsed() >= FILL_LOG_INTERVAL {
            last_log = Instant::now();
            log_latency(
                shared,
                spec,
                in_channels,
                in_rate,
                resampler.target_fill,
                raw,
                out,
            );
        }
        thread::park_timeout(WORKER_WAKE_TIMEOUT);
    }
    Ok(())
}

/// リサンプルスレッドの所有者。Drop時に停止してjoinする。
struct Worker {
    stop: Arc<AtomicBool>,
    thread: thread::Thread,
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn spawn(
        mut raw: HeapCons<f32>,
        mut out: HeapProd<f32>,
        shared: Arc<Shared>,
        in_channels: usize,
        in_rate: u32,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            if let Err(e) = resampler_loop(
                &mut raw,
                &mut out,
                &shared,
                in_channels,
                in_rate,
                &stop_flag,
            ) {
                eprintln!("Resampler thread error: {e}");
            }
        });
        Self {
            stop,
            thread: handle.thread().clone(),
            handle: Some(handle),
        }
    }

    fn waker(&self) -> thread::Thread {
        self.thread.clone()
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.unpark();
        if let Some(handle) = self.handle.take() {
            if handle.join().is_err() {
                eprintln!("Resampler thread panicked");
            }
        }
    }
}

/// キャプチャデバイスの音声をデフォルト出力へパススルーする。shutdownまでブロックする。
/// 出力デバイスは`OUTPUT_DEVICE_POLL_INTERVAL`ごとにポーリングし、変化していれば再構築する。
pub fn run(device_keyword: &str, shutdown: Arc<AtomicBool>) -> Result<()> {
    let host = cpal::default_host();
    let keyword = device_keyword.to_lowercase();

    let input_device = host
        .input_devices()?
        .find(|d| {
            d.name()
                .map(|n| n.to_lowercase().contains(&keyword))
                .unwrap_or(false)
        })
        .context("Capture audio device not found")?;
    let input_config = input_device.default_input_config()?;
    let in_channels = input_config.channels() as usize;
    let in_rate = input_config.sample_rate().0;

    let (initial_device, initial_config) = default_output(&host)?;
    let shared = Arc::new(Shared::new(OutputSpec::of(&initial_config)));

    let waker: Arc<OnceLock<thread::Thread>> = Arc::new(OnceLock::new());
    let input_ring_samples = in_rate as usize * in_channels * INPUT_RING_MS / 1000;
    let (input_stream, raw_cons) = build_input_stream(
        &input_device,
        &input_config,
        input_ring_samples,
        Arc::clone(&shared),
        Arc::clone(&waker),
    )?;

    let (out_prod, out_cons) = HeapRb::<f32>::new(OUTPUT_RING_CAPACITY).split();
    let out_cons = Arc::new(Mutex::new(out_cons));

    let worker = Worker::spawn(
        raw_cons,
        out_prod,
        Arc::clone(&shared),
        in_channels,
        in_rate,
    );
    let _ = waker.set(worker.waker());
    input_stream.play()?;

    let mut current_name = initial_device.name().ok();
    let mut output_stream = build_output_stream(
        &initial_device,
        &initial_config,
        Arc::clone(&out_cons),
        Arc::clone(&shared),
    )?;
    output_stream.play()?;

    let mut last_poll = Instant::now();
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(SHUTDOWN_POLL_INTERVAL);
        if last_poll.elapsed() < OUTPUT_DEVICE_POLL_INTERVAL {
            continue;
        }
        last_poll = Instant::now();

        let (device, config) = match default_output(&host) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Failed to query default output device: {e}");
                continue;
            }
        };
        let name = device.name().ok();
        if name == current_name {
            continue;
        }

        match build_output_stream(&device, &config, Arc::clone(&out_cons), Arc::clone(&shared)) {
            Ok(new_stream) => {
                if let Err(e) = new_stream.play() {
                    eprintln!("Failed to start new output stream: {e}");
                    continue;
                }
                shared.format.set(OutputSpec::of(&config));
                output_stream = new_stream;
                current_name = name;
                eprintln!("Output device switched to {current_name:?}");
            }
            Err(e) => eprintln!("Failed to switch output device: {e}"),
        }
    }

    drop(output_stream);
    drop(input_stream);
    Ok(())
}

fn default_output(host: &cpal::Host) -> Result<(cpal::Device, cpal::SupportedStreamConfig)> {
    let device = host
        .default_output_device()
        .context("Default output device not found")?;
    let config = device.default_output_config()?;
    Ok((device, config))
}

/// 小さいバッファを要求し、拒否されたらデフォルトで作り直す。
fn build_input_stream(
    device: &cpal::Device,
    config: &cpal::SupportedStreamConfig,
    ring_samples: usize,
    shared: Arc<Shared>,
    waker: Arc<OnceLock<thread::Thread>>,
) -> Result<(Stream, HeapCons<f32>)> {
    let attempt = |buffer_size: cpal::BufferSize| -> Result<(Stream, HeapCons<f32>)> {
        let (mut prod, cons) = HeapRb::<f32>::new(ring_samples).split();
        let shared = Arc::clone(&shared);
        let waker = Arc::clone(&waker);
        let mut stream_config: cpal::StreamConfig = config.clone().into();
        stream_config.buffer_size = buffer_size;
        let stream = device.build_input_stream(
            &stream_config,
            move |data: &[f32], info: &cpal::InputCallbackInfo| {
                let ts = info.timestamp();
                if let Some(d) = ts.callback.duration_since(&ts.capture) {
                    shared
                        .probe
                        .input_us
                        .store(d.as_micros() as u64, Ordering::Relaxed);
                }
                shared
                    .in_callback_samples
                    .store(data.len(), Ordering::Relaxed);
                prod.push_slice(data);
                if let Some(worker) = waker.get() {
                    worker.unpark();
                }
            },
            |err| eprintln!("Audio input stream error: {err}"),
            None,
        )?;
        Ok((stream, cons))
    };

    attempt(cpal::BufferSize::Fixed(LOW_LATENCY_FRAMES)).or_else(|e| {
        eprintln!("Fixed input buffer unavailable ({e}); using default");
        attempt(cpal::BufferSize::Default)
    })
}

/// 小さいバッファを要求し、拒否されたらデフォルトで作り直す。
fn build_output_stream(
    device: &cpal::Device,
    config: &cpal::SupportedStreamConfig,
    consumer: Arc<Mutex<HeapCons<f32>>>,
    shared: Arc<Shared>,
) -> Result<Stream> {
    let attempt = |buffer_size: cpal::BufferSize| -> Result<Stream> {
        let consumer = Arc::clone(&consumer);
        let shared = Arc::clone(&shared);
        let mut stream_config: cpal::StreamConfig = config.clone().into();
        stream_config.buffer_size = buffer_size;
        let stream = device.build_output_stream(
            &stream_config,
            move |out: &mut [f32], info: &cpal::OutputCallbackInfo| {
                let ts = info.timestamp();
                if let Some(d) = ts.playback.duration_since(&ts.callback) {
                    shared
                        .probe
                        .output_us
                        .store(d.as_micros() as u64, Ordering::Relaxed);
                }
                shared
                    .out_callback_samples
                    .store(out.len(), Ordering::Relaxed);
                shared.output_active.store(true, Ordering::Relaxed);
                let mut consumer = consumer.lock().unwrap();
                for sample in out.iter_mut() {
                    *sample = consumer.try_pop().unwrap_or(0.0);
                }
            },
            |err| eprintln!("Audio output stream error: {err}"),
            None,
        )?;
        Ok(stream)
    };

    attempt(cpal::BufferSize::Fixed(LOW_LATENCY_FRAMES)).or_else(|e| {
        eprintln!("Fixed output buffer unavailable ({e}); using default");
        attempt(cpal::BufferSize::Default)
    })
}
