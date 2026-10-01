use super::AudioSpec;
use anyhow::{Context, Result};
use ringbuf::traits::{Consumer, Observer, Producer};
use ringbuf::{HeapCons, HeapProd};
use rubato::audioadapter_buffers::direct::InterleavedSlice;
use rubato::{
    Adjustable, Async, FixedAsync, Resampler, SincInterpolationParameters, SincInterpolationType,
    WindowFunction,
};

const DEFAULT_TARGET_MS: f64 = 8.0;
const MAX_FILL_FACTOR: usize = 3;
const RESAMPLE_CHUNK_FRAMES: usize = 128;
const SINC_LEN: usize = 128;
const MAX_RATIO_RELATIVE: f64 = 1.1;
const DRIFT_GAIN: f64 = 0.01;
const MAX_DRIFT: f64 = 0.005;
const FILL_SMOOTHING: f64 = 0.05;

fn debug_err(e: impl std::fmt::Debug) -> anyhow::Error {
    anyhow::anyhow!("{e:?}")
}

/// 1フレーム分を出力チャンネル数に合わせてpushする(不足分はch0で埋める)。溢れたサンプルは捨てる。
fn map_channels(frame: &[f32], out_channels: usize, out: &mut HeapProd<f32>) {
    for ch in 0..out_channels {
        let _ = out.try_push(*frame.get(ch).unwrap_or(&frame[0]));
    }
}

/// rubatoのsincリサンプラと、出力リングの充填量に基づくドリフト補正。
pub(super) struct StreamResampler {
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
    pub(super) fn new(input: AudioSpec, output: AudioSpec) -> Result<Self> {
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

    pub(super) fn output(&self) -> AudioSpec {
        self.output
    }

    pub(super) fn target_fill(&self) -> usize {
        self.target_fill
    }

    pub(super) fn set_target_fill(&mut self, samples: usize) {
        self.target_fill = samples.max(self.output.channels);
    }

    pub(super) fn has_chunk(&self, raw: &HeapCons<f32>) -> bool {
        raw.occupied_len() >= self.in_buf.len()
    }

    pub(super) fn process_chunk(
        &mut self,
        raw: &mut HeapCons<f32>,
        out: &mut HeapProd<f32>,
    ) -> Result<()> {
        raw.pop_slice(&mut self.in_buf);
        self.track_drift(out.occupied_len())?;
        let produced = self.resample()?;

        if out.occupied_len() > self.target_fill * MAX_FILL_FACTOR {
            return Ok(());
        }
        let ch = self.input.channels;
        for frame in self.out_buf[..produced * ch].chunks_exact(ch) {
            map_channels(frame, self.output.channels, out);
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
