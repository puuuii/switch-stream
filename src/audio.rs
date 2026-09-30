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

const INPUT_RING_MS: f64 = 200.0;
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
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const OUTPUT_DEVICE_POLL_INTERVAL: Duration = Duration::from_millis(1000);

fn debug_err(e: impl std::fmt::Debug) -> anyhow::Error {
    anyhow::anyhow!("{e:?}")
}

/// インターリーブ済みPCMのレートとチャンネル数。入力・出力の双方で使う。
#[derive(Clone, Copy, PartialEq, Eq)]
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

/// 出力フォーマットをリサンプルスレッドと共有する。レートとチャンネル数を1つのatomicにまとめて整合を保つ。
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

/// cpalのタイムスタンプから得たデバイス側の遅延(マイクロ秒)。診断用。
#[derive(Default)]
struct LatencyProbe {
    input_us: AtomicU64,
    output_us: AtomicU64,
}

impl LatencyProbe {
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
}

/// コールバック・リサンプルスレッド・メインスレッドで共有する状態。
struct Shared {
    output_spec: SharedSpec,
    output_active: AtomicBool,
    in_callback_samples: AtomicUsize,
    out_callback_samples: AtomicUsize,
    probe: LatencyProbe,
}

impl Shared {
    fn new(output: AudioSpec) -> Self {
        Self {
            output_spec: SharedSpec::new(output),
            output_active: AtomicBool::new(false),
            in_callback_samples: AtomicUsize::new(0),
            out_callback_samples: AtomicUsize::new(0),
            probe: LatencyProbe::default(),
        }
    }

    fn in_period_ms(&self, input: AudioSpec) -> f64 {
        input.ms_for_samples(self.in_callback_samples.load(Ordering::Relaxed))
    }

    fn out_period_ms(&self, output: AudioSpec) -> f64 {
        output.ms_for_samples(self.out_callback_samples.load(Ordering::Relaxed))
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
    input: AudioSpec,
    output: AudioSpec,
    base_ratio: f64,
    in_buf: Vec<f32>,
    out_buf: Vec<f32>,
    target_fill: usize,
    fill_avg: f64,
}

impl StreamResampler {
    fn new(input: AudioSpec, output: AudioSpec) -> Result<Self> {
        let base_ratio = output.sample_rate as f64 / input.sample_rate as f64;
        let params = SincInterpolationParameters::new(SINC_LEN, WindowFunction::BlackmanHarris2)
            .interpolation(SincInterpolationType::Cubic);
        let inner = Async::<f32>::new_sinc(
            base_ratio,
            MAX_RATIO_RELATIVE,
            &params,
            RESAMPLE_CHUNK_FRAMES,
            input.channels,
            FixedAsync::Input,
        )
        .map_err(debug_err)
        .context("Failed to build resampler")?;

        let target_fill = output.samples_for_ms(DEFAULT_TARGET_MS);
        Ok(Self {
            in_buf: vec![0.0; inner.input_frames_next() * input.channels],
            out_buf: vec![0.0; inner.output_frames_max() * input.channels],
            fill_avg: target_fill as f64,
            target_fill,
            inner,
            input,
            output,
            base_ratio,
        })
    }

    fn set_target_fill(&mut self, samples: usize) {
        self.target_fill = samples.max(self.output.channels);
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
        let ch = self.input.channels;
        for frame in self.out_buf[..produced * ch].chunks_exact(ch) {
            push_mapped(out, ch, self.output.channels, |c| frame[c]);
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
        let ch = self.input.channels;
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

/// リサンプルスレッド上で動く処理本体。入力リング→リサンプル→出力リングを回す。
struct ResampleTask {
    raw: HeapCons<f32>,
    out: HeapProd<f32>,
    shared: Arc<Shared>,
    input: AudioSpec,
    output: AudioSpec,
    resampler: StreamResampler,
    primed: bool,
}

impl ResampleTask {
    fn new(
        raw: HeapCons<f32>,
        out: HeapProd<f32>,
        shared: Arc<Shared>,
        input: AudioSpec,
    ) -> Result<Self> {
        let output = shared.output_spec.get();
        Ok(Self {
            resampler: StreamResampler::new(input, output)?,
            raw,
            out,
            shared,
            input,
            output,
            primed: false,
        })
    }

    fn run(&mut self, stop: &AtomicBool) {
        while !stop.load(Ordering::Relaxed) {
            if self.shared.output_active.load(Ordering::Relaxed) {
                self.step();
            } else {
                // 出力が消費を始めるまでは入力を捨て、リングに遅延を溜め込まない。
                self.raw.clear();
            }
            thread::park_timeout(WORKER_WAKE_TIMEOUT);
        }
    }

    fn step(&mut self) {
        self.follow_output_spec();
        self.update_target_fill();

        if !self.primed {
            prime(&mut self.out, self.resampler.target_fill);
            self.primed = true;
        }

        self.drain_input();
    }

    fn follow_output_spec(&mut self) {
        let latest = self.shared.output_spec.get();
        if latest == self.output {
            return;
        }
        self.output = latest;
        match StreamResampler::new(self.input, latest) {
            Ok(new) => self.resampler = new,
            Err(e) => eprintln!("{e:#}"),
        }
    }

    /// 充填の目標はコールバック周期の半分+余裕。周期が短いほど遅延も短くなる。
    fn update_target_fill(&mut self) {
        let period_ms = self
            .shared
            .in_period_ms(self.input)
            .max(self.shared.out_period_ms(self.output));
        let target = self.output.samples_for_ms(period_ms / 2.0 + FILL_MARGIN_MS);
        self.resampler.set_target_fill(target);
    }

    fn drain_input(&mut self) {
        while self.resampler.has_chunk(&self.raw) {
            if let Err(e) = self.resampler.process_chunk(&mut self.raw, &mut self.out) {
                eprintln!("Resample error: {e}");
                break;
            }
        }
    }
}

/// リサンプルスレッドの所有者。Drop時に停止してjoinする。
struct Worker {
    stop: Arc<AtomicBool>,
    thread: thread::Thread,
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn spawn(
        raw: HeapCons<f32>,
        out: HeapProd<f32>,
        shared: Arc<Shared>,
        input: AudioSpec,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let result =
                ResampleTask::new(raw, out, shared, input).map(|mut task| task.run(&stop_flag));
            if let Err(e) = result {
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

type SharedConsumer = Arc<Mutex<HeapCons<f32>>>;

/// デフォルト出力デバイスのストリームを保持し、変化を検知したら作り直す。
struct OutputRouter {
    host: cpal::Host,
    consumer: SharedConsumer,
    shared: Arc<Shared>,
    device_name: Option<String>,
    _stream: Stream,
}

impl OutputRouter {
    fn start(
        host: cpal::Host,
        consumer: SharedConsumer,
        shared: Arc<Shared>,
        device: &cpal::Device,
        config: &cpal::SupportedStreamConfig,
    ) -> Result<Self> {
        let stream = open_output(device, config, &consumer, &shared)?;
        Ok(Self {
            host,
            consumer,
            shared,
            device_name: device.name().ok(),
            _stream: stream,
        })
    }

    fn follow_default_device(&mut self) {
        let (device, config) = match default_output(&self.host) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Failed to query default output device: {e}");
                return;
            }
        };
        let name = device.name().ok();
        if name == self.device_name {
            return;
        }

        match open_output(&device, &config, &self.consumer, &self.shared) {
            Ok(stream) => {
                self.shared.output_spec.set(AudioSpec::of(&config));
                self._stream = stream;
                self.device_name = name;
                eprintln!("Output device switched to {:?}", self.device_name);
            }
            Err(e) => eprintln!("Failed to switch output device: {e:#}"),
        }
    }
}

/// キャプチャデバイスの音声をデフォルト出力へパススルーする。shutdownまでブロックする。
/// 出力デバイスは`OUTPUT_DEVICE_POLL_INTERVAL`ごとにポーリングし、変化していれば再構築する。
pub fn run(device_keyword: &str, shutdown: Arc<AtomicBool>) -> Result<()> {
    let host = cpal::default_host();
    let input_device = find_input_device(&host, device_keyword)?;
    let input_config = input_device.default_input_config()?;
    let input = AudioSpec::of(&input_config);

    let (output_device, output_config) = default_output(&host)?;
    let shared = Arc::new(Shared::new(AudioSpec::of(&output_config)));

    let waker: Arc<OnceLock<thread::Thread>> = Arc::new(OnceLock::new());
    let (input_stream, raw_cons) = build_input_stream(
        &input_device,
        &input_config,
        input.samples_for_ms(INPUT_RING_MS),
        Arc::clone(&shared),
        Arc::clone(&waker),
    )?;

    let (out_prod, out_cons) = HeapRb::<f32>::new(OUTPUT_RING_CAPACITY).split();
    let out_cons = Arc::new(Mutex::new(out_cons));

    let worker = Worker::spawn(raw_cons, out_prod, Arc::clone(&shared), input);
    let _ = waker.set(worker.waker());
    input_stream.play()?;

    let mut router = OutputRouter::start(host, out_cons, shared, &output_device, &output_config)?;

    let mut last_poll = Instant::now();
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(SHUTDOWN_POLL_INTERVAL);
        if last_poll.elapsed() >= OUTPUT_DEVICE_POLL_INTERVAL {
            last_poll = Instant::now();
            router.follow_default_device();
        }
    }

    drop(router);
    drop(input_stream);
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
        eprintln!("Fixed {label} buffer unavailable ({e}); using default");
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
                shared.probe.record_input(info);
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
    })
}

fn build_output_stream(
    device: &cpal::Device,
    config: &cpal::SupportedStreamConfig,
    consumer: &SharedConsumer,
    shared: &Arc<Shared>,
) -> Result<Stream> {
    with_buffer_fallback("output", |buffer_size| {
        let consumer = Arc::clone(consumer);
        let shared = Arc::clone(shared);
        let stream = device.build_output_stream(
            &stream_config(config, buffer_size),
            move |out: &mut [f32], info: &cpal::OutputCallbackInfo| {
                shared.probe.record_output(info);
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
    })
}

fn open_output(
    device: &cpal::Device,
    config: &cpal::SupportedStreamConfig,
    consumer: &SharedConsumer,
    shared: &Arc<Shared>,
) -> Result<Stream> {
    let stream = build_output_stream(device, config, consumer, shared)?;
    stream.play().context("Failed to start output stream")?;
    Ok(stream)
}
