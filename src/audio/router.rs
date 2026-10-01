use super::{AudioSpec, Shared, default_output, stream_config, with_buffer_fallback};
use anyhow::{Context, Result};
use cpal::Stream;
use cpal::traits::{DeviceTrait, StreamTrait};
use ringbuf::HeapCons;
use ringbuf::traits::Consumer;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

pub(super) type SharedConsumer = Arc<Mutex<HeapCons<f32>>>;

/// デフォルト出力デバイスのストリームを保持し、変化を検知したら作り直す。
pub(super) struct OutputRouter {
    host: cpal::Host,
    consumer: SharedConsumer,
    shared: Arc<Shared>,
    device_name: Option<String>,
    _stream: Stream,
}

impl OutputRouter {
    pub(super) fn start(
        host: cpal::Host,
        consumer: SharedConsumer,
        shared: Arc<Shared>,
        device: &cpal::Device,
        config: &cpal::SupportedStreamConfig,
    ) -> Result<Self> {
        let stream = build_output_stream(device, config, &consumer, &shared)?;
        stream.play().context("Failed to start output stream")?;
        Ok(Self {
            host,
            consumer,
            shared,
            device_name: device.name().ok(),
            _stream: stream,
        })
    }

    pub(super) fn follow_default_device(&mut self) {
        let (device, config) = match default_output(&self.host) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("Failed to query default output device: {e:#}");
                return;
            }
        };
        let name = device.name().ok();
        if name == self.device_name {
            return;
        }

        let stream = match build_output_stream(&device, &config, &self.consumer, &self.shared) {
            Ok(stream) => stream,
            Err(e) => {
                log::warn!("Failed to switch output device: {e:#}");
                return;
            }
        };

        // 再生開始前に新しいspecを公開し、リサンプラが先に追従できるようにする。
        let previous = self.shared.output_spec.get();
        self.shared.output_spec.set(AudioSpec::of(&config));
        if let Err(e) = stream.play() {
            self.shared.output_spec.set(previous);
            log::warn!("Failed to start switched output stream: {e:#}");
            return;
        }

        self._stream = stream;
        self.device_name = name;
        log::info!("Output device switched to {:?}", self.device_name);
    }
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
                shared.status.record_output(info);
                shared
                    .out_callback_samples
                    .store(out.len(), Ordering::Relaxed);
                shared.output_active.store(true, Ordering::Relaxed);

                let Ok(mut consumer) = consumer.try_lock() else {
                    out.fill(0.0);
                    return;
                };
                let popped = consumer.pop_slice(out);
                out[popped..].fill(0.0);
            },
            |err| log::error!("Audio output stream error: {err}"),
            None,
        )?;
        Ok(stream)
    })
}
