use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use obws::Client;
use obws::requests::profiles::SetParameter;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

const OBS_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const OBS_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const COMMON_RECORDING_BITRATE_PARAMS: &[(&str, &str)] =
    &[("AdvOut", "FFVBitrate"), ("SimpleOutput", "VBitrate")];

#[derive(Clone, Debug)]
pub struct ObsConfig {
    pub host: String,
    pub port: u16,
    pub password: Option<String>,
    pub recording_bitrate_category: String,
    pub recording_bitrate_name: String,
}

#[derive(Clone)]
pub struct ObsRemote {
    config: ObsConfig,
    inner: Arc<RwLock<ObsState>>,
}

struct ObsState {
    client: Option<Arc<Client>>,
    status: ObsStatus,
}

#[derive(Clone, Debug, Serialize)]
pub struct ObsStatus {
    pub enabled: bool,
    pub connected: bool,
    pub host: String,
    pub port: u16,
    pub last_error: Option<String>,
    pub current_scene: Option<String>,
    pub scenes: Vec<String>,
    pub streaming: Option<bool>,
    pub recording: Option<bool>,
    pub recording_bitrate_kbps: Option<u32>,
    pub recording_bitrate_category: String,
    pub recording_bitrate_name: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ObsCommand {
    Connect,
    Disconnect,
    Refresh,
    StartRecord,
    StopRecord,
    SetRecordingBitrate { kbps: u32 },
}

impl ObsRemote {
    pub fn new(config: ObsConfig) -> Self {
        let status = ObsStatus {
            enabled: true,
            connected: false,
            host: config.host.clone(),
            port: config.port,
            last_error: None,
            current_scene: None,
            scenes: Vec::new(),
            streaming: None,
            recording: None,
            recording_bitrate_kbps: None,
            recording_bitrate_category: config.recording_bitrate_category.clone(),
            recording_bitrate_name: config.recording_bitrate_name.clone(),
        };
        Self {
            config,
            inner: Arc::new(RwLock::new(ObsState {
                client: None,
                status,
            })),
        }
    }

    pub async fn status(&self) -> ObsStatus {
        if let Err(err) = self.refresh().await {
            self.drop_client();
            self.set_error(err.to_string());
        }
        self.cached_status()
    }

    pub async fn apply(&self, command: ObsCommand) -> Result<ObsStatus> {
        let result = self.apply_inner(command.clone()).await;
        let result = if result.is_err() && command.should_retry_after_reconnect() {
            self.drop_client();
            self.apply_inner(command).await
        } else {
            result
        };
        if let Err(err) = &result {
            self.set_error(err.to_string());
        }
        result.map(|_| self.cached_status())
    }

    async fn apply_inner(&self, command: ObsCommand) -> Result<()> {
        match command {
            ObsCommand::Connect => {
                self.connect().await?;
                self.refresh().await?;
            }
            ObsCommand::Disconnect => self.disconnect().await,
            ObsCommand::Refresh => self.refresh().await?,
            ObsCommand::StartRecord => {
                let client = self.ensure_client().await?;
                let status = with_obs_timeout(client.recording().status()).await?;
                if !status.active {
                    with_obs_timeout(client.recording().start()).await?;
                }
                self.refresh().await?;
            }
            ObsCommand::StopRecord => {
                let client = self.ensure_client().await?;
                let status = with_obs_timeout(client.recording().status()).await?;
                if status.active {
                    with_obs_timeout(client.recording().stop()).await?;
                }
                self.refresh().await?;
            }
            ObsCommand::SetRecordingBitrate { kbps } => {
                if kbps == 0 {
                    return Err(anyhow!("recording bitrate must be greater than zero"));
                }
                let client = self.ensure_client().await?;
                self.set_recording_bitrate(&client, kbps).await?;
                self.refresh().await?;
            }
        }
        Ok(())
    }

    async fn ensure_client(&self) -> Result<Arc<Client>> {
        if let Some(client) = self.inner.read().client.clone() {
            return Ok(client);
        }
        self.connect().await
    }

    async fn connect(&self) -> Result<Arc<Client>> {
        let password = self.config.password.as_deref();
        let client = tokio::time::timeout(
            OBS_CONNECT_TIMEOUT,
            Client::connect(&self.config.host, self.config.port, password),
        )
        .await
        .context("OBS websocket connect timed out")?
        .context("OBS websocket connect failed")?;
        let client = Arc::new(client);
        {
            let mut state = self.inner.write();
            state.client = Some(client.clone());
            state.status.connected = true;
            state.status.last_error = None;
        }
        Ok(client)
    }

    async fn disconnect(&self) {
        let client = self.inner.write().client.take();
        if let Some(client) = client.and_then(Arc::into_inner) {
            let mut client = client;
            client.disconnect().await;
        }
        let mut state = self.inner.write();
        state.status.connected = false;
        state.status.recording = None;
    }

    async fn refresh(&self) -> Result<()> {
        let client = self.ensure_client().await?;
        let recording = with_obs_timeout(client.recording().status()).await?;
        let bitrate = self.read_recording_bitrate(&client).await;

        let mut state = self.inner.write();
        state.status.connected = true;
        state.status.last_error = None;
        state.status.recording = Some(recording.active);
        state.status.recording_bitrate_kbps = bitrate;
        Ok(())
    }

    fn set_error(&self, error: String) {
        let mut state = self.inner.write();
        state.status.connected = state.client.is_some();
        state.status.last_error = Some(error);
    }

    fn cached_status(&self) -> ObsStatus {
        self.inner.read().status.clone()
    }

    fn drop_client(&self) {
        let mut state = self.inner.write();
        state.client = None;
        state.status.connected = false;
    }

    async fn set_recording_bitrate(&self, client: &Client, kbps: u32) -> Result<()> {
        let value = kbps.to_string();
        let mut failures = Vec::new();
        let mut applied = false;
        for (category, name) in self.recording_bitrate_params() {
            match with_obs_timeout(client.profiles().set_parameter(SetParameter {
                category: &category,
                name: &name,
                value: Some(&value),
            }))
            .await
            {
                Ok(()) => applied = true,
                Err(err) => failures.push(format!("{category}/{name}: {err}")),
            }
        }

        if applied {
            Ok(())
        } else {
            Err(anyhow!(
                "failed to set OBS recording bitrate; tried {}",
                failures.join(", ")
            ))
        }
    }

    async fn read_recording_bitrate(&self, client: &Client) -> Option<u32> {
        for (category, name) in self.recording_bitrate_params() {
            let bitrate = with_obs_timeout(client.profiles().parameter(&category, &name))
                .await
                .ok()
                .and_then(|parameter| parameter.value)
                .and_then(|value| value.parse::<u32>().ok());
            if bitrate.is_some() {
                return bitrate;
            }
        }
        None
    }

    fn recording_bitrate_params(&self) -> Vec<(String, String)> {
        let mut params = vec![(
            self.config.recording_bitrate_category.clone(),
            self.config.recording_bitrate_name.clone(),
        )];
        for (category, name) in COMMON_RECORDING_BITRATE_PARAMS {
            let candidate = ((*category).to_string(), (*name).to_string());
            if !params.contains(&candidate) {
                params.push(candidate);
            }
        }
        params
    }
}

impl ObsCommand {
    fn should_retry_after_reconnect(&self) -> bool {
        !matches!(self, Self::Disconnect)
    }
}

async fn with_obs_timeout<T>(
    future: impl std::future::Future<
        Output = std::result::Result<T, impl std::error::Error + Send + Sync + 'static>,
    >,
) -> Result<T> {
    tokio::time::timeout(OBS_COMMAND_TIMEOUT, future)
        .await
        .context("OBS websocket command timed out")?
        .context("OBS websocket command failed")
}

impl Default for ObsStatus {
    fn default() -> Self {
        Self {
            enabled: false,
            connected: false,
            host: String::new(),
            port: 0,
            last_error: None,
            current_scene: None,
            scenes: Vec::new(),
            streaming: None,
            recording: None,
            recording_bitrate_kbps: None,
            recording_bitrate_category: String::new(),
            recording_bitrate_name: String::new(),
        }
    }
}

pub fn parse_command(bytes: &[u8]) -> Result<ObsCommand> {
    serde_json::from_slice(bytes).map_err(|err| anyhow!("invalid OBS command JSON: {err}"))
}
