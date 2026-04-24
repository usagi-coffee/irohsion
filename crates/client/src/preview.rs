use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::mpsc,
};

#[derive(Clone)]
pub struct PreviewState {
    tx: mpsc::Sender<Vec<u8>>,
    latest_jpeg: Arc<RwLock<Vec<u8>>>,
}

impl PreviewState {
    pub fn submit_packet(&self, payload: &[u8]) {
        let _ = self.tx.try_send(payload.to_vec());
    }

    pub fn latest_jpeg_len(&self) -> usize {
        self.latest_jpeg.read().len()
    }

    pub fn latest_jpeg(&self) -> Vec<u8> {
        self.latest_jpeg.read().clone()
    }
}

pub fn spawn_preview(max_jpeg_bytes: usize, decode_interval_secs: u64) -> PreviewState {
    let (tx, rx) = mpsc::channel(256);
    let latest_jpeg = Arc::new(RwLock::new(Vec::new()));
    let task_jpeg = latest_jpeg.clone();
    tokio::spawn(async move {
        let _ = preview_loop(rx, task_jpeg, max_jpeg_bytes, decode_interval_secs).await;
    });

    PreviewState { tx, latest_jpeg }
}

async fn preview_loop(
    mut rx: mpsc::Receiver<Vec<u8>>,
    latest_jpeg: Arc<RwLock<Vec<u8>>>,
    max_jpeg_bytes: usize,
    decode_interval_secs: u64,
) -> Result<()> {
    let fps_filter = format!("fps=1/{},scale=320:-1", decode_interval_secs.max(1));
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

    let mut stdin = child.stdin.take().context("ffmpeg stdin unavailable")?;
    let mut stdout = child.stdout.take().context("ffmpeg stdout unavailable")?;
    let writer = tokio::spawn(async move {
        while let Some(packet) = rx.recv().await {
            if stdin.write_all(&packet).await.is_err() {
                break;
            }
        }
    });

    let mut buf = [0_u8; 8192];
    let mut stream = Vec::new();
    loop {
        let read = stdout.read(&mut buf).await?;
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

    writer.abort();
    let _ = child.wait().await;
    Ok(())
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
