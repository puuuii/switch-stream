use nokhwa::utils::FrameFormat;

#[derive(Debug, Clone, Copy)]
pub struct VideoSpec {
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    pub frame_format: FrameFormat,
}

impl VideoSpec {
    pub fn size(&self) -> [usize; 2] {
        [self.width as usize, self.height as usize]
    }
}

/// キャプチャデバイス識別用のキーワード(大文字小文字無視の部分一致)と映像形式。
#[derive(Debug, Clone, Copy)]
pub struct HardwareProfile {
    pub audio_device_keyword: &'static str,
    pub video_device_keyword: &'static str,
    pub video: VideoSpec,
}

impl HardwareProfile {
    pub const AVERMEDIA_LIVE_GAMER_MINI_GC311: Self = Self {
        audio_device_keyword: "gc311",
        video_device_keyword: "streamline",
        video: VideoSpec {
            width: 1920,
            height: 1080,
            fps: 60,
            frame_format: FrameFormat::YUYV,
        },
    };
}
