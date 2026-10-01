use super::resampler::StreamResampler;
use super::{AudioSpec, Shared};
use anyhow::Result;
use ringbuf::traits::{Consumer, Producer};
use ringbuf::{HeapCons, HeapProd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

const FILL_MARGIN_MS: f64 = 3.0;
const WORKER_WAKE_TIMEOUT: Duration = Duration::from_millis(10);

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
    resampler: StreamResampler,
    primed: bool,
    failed_spec: Option<AudioSpec>,
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
            primed: false,
            failed_spec: None,
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
            prime(&mut self.out, self.resampler.target_fill());
            self.primed = true;
        }

        self.drain_input();
    }

    /// 現在のリサンプラの出力specを正とし、変化していれば作り直す。失敗したspecは再試行しない。
    fn follow_output_spec(&mut self) {
        let latest = self.shared.output_spec.get();
        if latest == self.resampler.output() || self.failed_spec == Some(latest) {
            return;
        }
        match StreamResampler::new(self.input, latest) {
            Ok(new) => {
                self.resampler = new;
                self.failed_spec = None;
            }
            Err(e) => {
                log::warn!("Failed to rebuild resampler for {latest:?}: {e:#}");
                self.failed_spec = Some(latest);
            }
        }
    }

    /// 充填の目標はコールバック周期の半分+余裕。周期が短いほど遅延も短くなる。
    fn update_target_fill(&mut self) {
        let output = self.resampler.output();
        let period_ms = self
            .shared
            .in_period_ms(self.input)
            .max(self.shared.out_period_ms(output));
        let target = output.samples_for_ms(period_ms / 2.0 + FILL_MARGIN_MS);
        self.resampler.set_target_fill(target);
    }

    fn drain_input(&mut self) {
        while self.resampler.has_chunk(&self.raw) {
            if let Err(e) = self.resampler.process_chunk(&mut self.raw, &mut self.out) {
                log::warn!("Resample error: {e:#}");
                break;
            }
        }
    }
}

/// リサンプルスレッドの所有者。Drop時に停止してjoinする。
pub(super) struct Worker {
    stop: Arc<AtomicBool>,
    thread: thread::Thread,
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub(super) fn spawn(
        raw: HeapCons<f32>,
        out: HeapProd<f32>,
        shared: Arc<Shared>,
        input: AudioSpec,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let status = Arc::clone(&shared.status);
        let handle = thread::spawn(move || {
            let result =
                ResampleTask::new(raw, out, shared, input).map(|mut task| task.run(&stop_flag));
            if let Err(e) = result {
                status.report_error(format!("Resampler thread error: {e:#}"));
            }
        });
        Self {
            stop,
            thread: handle.thread().clone(),
            handle: Some(handle),
        }
    }

    pub(super) fn waker(&self) -> thread::Thread {
        self.thread.clone()
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.unpark();
        if let Some(handle) = self.handle.take() {
            if handle.join().is_err() {
                log::error!("Resampler thread panicked");
            }
        }
    }
}
