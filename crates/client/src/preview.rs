use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use parking_lot::RwLock;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::mpsc,
    task::JoinHandle,
};

#[derive(Clone)]
pub struct PreviewState {
    tx: mpsc::Sender<Vec<u8>>,
    latest_jpeg: Arc<RwLock<Vec<u8>>>,
    enabled: Arc<AtomicBool>,
}

impl PreviewState {
    pub fn submit_packet(&self, payload: &[u8]) {
        if !self.enabled() {
            return;
        }

        let _ = self.tx.try_send(payload.to_vec());
    }

    pub fn latest_jpeg_len(&self) -> usize {
        self.latest_jpeg.read().len()
    }

    pub fn latest_jpeg(&self) -> Vec<u8> {
        self.latest_jpeg.read().clone()
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        if !enabled {
            self.latest_jpeg.write().clear();
        }
    }
}

pub fn spawn_preview(max_jpeg_bytes: usize, decode_interval_secs: u64) -> PreviewState {
    let (tx, rx) = mpsc::channel(256);
    let latest_jpeg = Arc::new(RwLock::new(Vec::new()));
    let enabled = Arc::new(AtomicBool::new(true));
    let task_jpeg = latest_jpeg.clone();
    let task_enabled = enabled.clone();
    tokio::spawn(async move {
        let _ = preview_loop(
            rx,
            task_jpeg,
            task_enabled,
            max_jpeg_bytes,
            decode_interval_secs,
        )
        .await;
    });

    PreviewState {
        tx,
        latest_jpeg,
        enabled,
    }
}

async fn preview_loop(
    mut rx: mpsc::Receiver<Vec<u8>>,
    latest_jpeg: Arc<RwLock<Vec<u8>>>,
    enabled: Arc<AtomicBool>,
    max_jpeg_bytes: usize,
    decode_interval_secs: u64,
) -> Result<()> {
    let fps_filter = format!("fps=1/{},scale=420:-1", decode_interval_secs.max(1));
    let mut decoder = None::<PreviewDecoder>;
    let mut interval = tokio::time::interval(Duration::from_millis(250));

    loop {
        tokio::select! {
            packet = rx.recv() => {
                let Some(packet) = packet else {
                    break;
                };
                if !enabled.load(Ordering::Relaxed) {
                    continue;
                }
                if decoder.is_none() {
                    decoder = Some(spawn_decoder(&fps_filter, latest_jpeg.clone(), max_jpeg_bytes).await?);
                }
                let write_failed = if let Some(active_decoder) = decoder.as_mut() {
                    active_decoder.stdin.write_all(&packet).await.is_err()
                } else {
                    false
                };
                if write_failed {
                    if let Some(active_decoder) = decoder.take() {
                        active_decoder.stop().await;
                    }
                }
            }
            _ = interval.tick() => {
                if !enabled.load(Ordering::Relaxed) {
                    if let Some(decoder) = decoder.take() {
                        decoder.stop().await;
                    }
                }
            }
        }
    }

    if let Some(decoder) = decoder {
        decoder.stop().await;
    }
    Ok(())
}

struct PreviewDecoder {
    child: Child,
    stdin: ChildStdin,
    reader: JoinHandle<()>,
}

impl PreviewDecoder {
    async fn stop(mut self) {
        let _ = self.child.start_kill();
        self.reader.abort();
        let _ = self.child.wait().await;
    }
}

async fn spawn_decoder(
    fps_filter: &str,
    latest_jpeg: Arc<RwLock<Vec<u8>>>,
    max_jpeg_bytes: usize,
) -> Result<PreviewDecoder> {
    let mut child = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "mpegts",
            "-i",
            "pipe:0",
            "-vf",
            &fps_filter,
            "-q:v",
            "8",
            "-f",
            "image2pipe",
            "-vcodec",
            "mjpeg",
            "pipe:1",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("failed to spawn ffmpeg preview decoder")?;

    let stdin = child.stdin.take().context("ffmpeg stdin unavailable")?;
    let stdout = child.stdout.take().context("ffmpeg stdout unavailable")?;
    let reader = tokio::spawn(read_frames(stdout, latest_jpeg, max_jpeg_bytes));

    Ok(PreviewDecoder {
        child,
        stdin,
        reader,
    })
}

async fn read_frames(
    mut stdout: ChildStdout,
    latest_jpeg: Arc<RwLock<Vec<u8>>>,
    max_jpeg_bytes: usize,
) {
    let mut buf = [0_u8; 8192];
    let mut stream = Vec::new();
    loop {
        let Ok(read) = stdout.read(&mut buf).await else {
            break;
        };
        if read == 0 {
            break;
        }

        stream.extend_from_slice(&buf[..read]);
        while let Some((frame, consumed)) = take_jpeg_frame(&stream) {
            if frame.len() <= max_jpeg_bytes {
                *latest_jpeg.write() = frame;
            }
            stream.drain(..consumed);
        }
        if stream.len() > max_jpeg_bytes.saturating_mul(2) {
            stream.clear();
        }
    }
}

fn take_jpeg_frame(data: &[u8]) -> Option<(Vec<u8>, usize)> {
    let start = data.windows(2).position(|bytes| bytes == [0xff, 0xd8])?;
    let end = data[start + 2..]
        .windows(2)
        .position(|bytes| bytes == [0xff, 0xd9])?
        + start
        + 4;
    Some((data[start..end].to_vec(), end))
}

#[cfg(test)]
mod tests {
    use super::take_jpeg_frame;

    #[test]
    fn extracts_jpeg_frame() {
        let data = [0, 1, 0xff, 0xd8, 4, 5, 0xff, 0xd9, 9];
        let (frame, consumed) = take_jpeg_frame(&data).expect("frame");

        assert_eq!(frame, vec![0xff, 0xd8, 4, 5, 0xff, 0xd9]);
        assert_eq!(consumed, 8);
    }
}
