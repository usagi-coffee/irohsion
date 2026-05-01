use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::{Context, Result};
use bluer::{
    adv::Advertisement,
    gatt::local::{
        Application, Characteristic, CharacteristicRead, CharacteristicWrite,
        CharacteristicWriteMethod, ReqError, Service,
    },
};
use iroh::{EndpointId, RelayUrl};
use obs_remote::{ObsRemote, parse_command as parse_obs_command};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::{
    context::ClientCtx,
    path_strategy::{ControlPatch, StrategyState},
    preview::PreviewState,
};

pub const SERVICE_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10001);
pub const STATUS_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10002);
pub const CONTROL_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10003);
pub const PREVIEW_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10004);
pub const PREVIEW_OFFSET_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10005);
pub const STATUS_OFFSET_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10006);
pub const OBS_SERVICE_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10011);
pub const OBS_STATUS_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10012);
pub const OBS_CONTROL_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10013);

const PREVIEW_CHUNK_BYTES: usize = 500;
const STATUS_CHUNK_BYTES: usize = 500;

type ReadFuture = Pin<Box<dyn Future<Output = bluer::gatt::local::ReqResult<Vec<u8>>> + Send>>;
type WriteFuture = Pin<Box<dyn Future<Output = bluer::gatt::local::ReqResult<()>> + Send>>;

#[derive(Debug, Clone)]
pub struct RemoteConfig {
    pub name: String,
    pub endpoint: EndpointId,
    pub addrs: Vec<SocketAddr>,
    pub relays: Vec<RelayUrl>,
}

pub async fn spawn_remote_server(
    config: RemoteConfig,
    strategy: StrategyState,
    preview: Option<PreviewState>,
    obs: Option<ObsRemote>,
    ctx: ClientCtx,
) -> Result<()> {
    let session = bluer::Session::new()
        .await
        .context("failed to open BlueZ D-Bus session")?;
    let adapter = session
        .default_adapter()
        .await
        .context("failed to find default Bluetooth adapter")?;
    adapter
        .set_powered(true)
        .await
        .context("failed to power Bluetooth adapter")?;

    let advertisement = Advertisement {
        // Keep advertising small and stable. The OBS service is still exposed in GATT and requested
        // by the web app as an optional service after connecting to the primary remote.
        service_uuids: [SERVICE_UUID].into_iter().collect(),
        discoverable: Some(true),
        local_name: Some(config.name.clone()),
        ..Default::default()
    };
    let adv_handle = adapter
        .advertise(advertisement)
        .await
        .context("failed to register BLE advertisement")?;

    let read_config = config.clone();
    let read_strategy = strategy.clone();
    let read_preview = preview.clone();
    let status_offset = Arc::new(AtomicUsize::new(0));
    let status_offset_read = status_offset.clone();
    let status_offset_write = status_offset.clone();
    let status_snapshot = Arc::new(RwLock::new(Vec::new()));
    let status_snapshot_read = status_snapshot.clone();
    let status_snapshot_write = status_snapshot.clone();
    let status_write_config = config.clone();
    let status_write_strategy = strategy.clone();
    let status_write_preview = preview.clone();
    let write_strategy = strategy.clone();
    let write_ctx = ctx.clone();
    let write_preview_control = preview.clone();
    let preview_read = preview.clone();
    let preview_offset = Arc::new(AtomicUsize::new(0));
    let preview_offset_read = preview_offset.clone();
    let preview_offset_write = preview_offset.clone();
    let preview_snapshot = Arc::new(RwLock::new(Vec::new()));
    let preview_snapshot_read = preview_snapshot.clone();
    let preview_snapshot_write = preview_snapshot.clone();
    let preview_write = preview.clone();
    let obs_status = obs.clone();
    let obs_control = obs.clone();
    let mut services = vec![Service {
        uuid: SERVICE_UUID,
        primary: true,
        characteristics: vec![
            Characteristic {
                uuid: STATUS_UUID,
                read: Some(CharacteristicRead {
                    read: true,
                    fun: Box::new(move |request| {
                        let config = read_config.clone();
                        let strategy = read_strategy.clone();
                        let preview = read_preview.clone();
                        let status_offset = status_offset_read.clone();
                        let status_snapshot = status_snapshot_read.clone();
                        Box::pin(async move {
                            let mut snapshot = status_snapshot.read().clone();
                            if snapshot.is_empty() {
                                snapshot =
                                    build_status_payload(&config, &strategy, preview.as_ref())?;
                                *status_snapshot.write() = snapshot.clone();
                            }
                            let max_len = (request.mtu.saturating_sub(1).max(1) as usize)
                                .min(STATUS_CHUNK_BYTES);
                            Ok(chunk_bytes(
                                &snapshot,
                                status_offset.load(Ordering::Relaxed),
                                max_len,
                            ))
                        }) as ReadFuture
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Characteristic {
                uuid: STATUS_OFFSET_UUID,
                write: Some(CharacteristicWrite {
                    write: true,
                    method: CharacteristicWriteMethod::Fun(Box::new(move |value, _| {
                        let status_offset = status_offset_write.clone();
                        let status_snapshot = status_snapshot_write.clone();
                        let config = status_write_config.clone();
                        let strategy = status_write_strategy.clone();
                        let preview = status_write_preview.clone();
                        Box::pin(async move {
                            let offset = parse_u32_offset(&value)?;
                            if offset == 0 {
                                *status_snapshot.write() =
                                    build_status_payload(&config, &strategy, preview.as_ref())?;
                            }
                            status_offset.store(offset, Ordering::Relaxed);
                            Ok(())
                        }) as WriteFuture
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Characteristic {
                uuid: CONTROL_UUID,
                write: Some(CharacteristicWrite {
                    write: true,
                    write_without_response: true,
                    method: CharacteristicWriteMethod::Fun(Box::new(move |value, _| {
                        let strategy = write_strategy.clone();
                        let ctx = write_ctx.clone();
                        let preview = write_preview_control.clone();
                        Box::pin(async move {
                            let patch = serde_json::from_slice::<ControlPatch>(&value)
                                .map_err(|_| ReqError::InvalidValueLength)?;
                            if let Some(enabled) = patch.preview_enabled {
                                if let Some(preview) = &preview {
                                    preview.set_enabled(enabled);
                                }
                            }
                            strategy.apply_patch(patch, &ctx);
                            Ok(())
                        }) as WriteFuture
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Characteristic {
                uuid: PREVIEW_UUID,
                read: Some(CharacteristicRead {
                    read: true,
                    fun: Box::new(move |request| {
                        let preview = preview_read.clone();
                        let preview_offset = preview_offset_read.clone();
                        let preview_snapshot = preview_snapshot_read.clone();
                        Box::pin(async move {
                            let Some(preview) = preview else {
                                return Ok(Vec::new());
                            };
                            let mut snapshot = preview_snapshot.read().clone();
                            if snapshot.is_empty() {
                                snapshot = preview.latest_jpeg();
                                *preview_snapshot.write() = snapshot.clone();
                            }
                            let max_len = (request.mtu.saturating_sub(1).max(1) as usize)
                                .min(PREVIEW_CHUNK_BYTES);
                            Ok(chunk_bytes(
                                &snapshot,
                                preview_offset.load(Ordering::Relaxed),
                                max_len,
                            ))
                        }) as ReadFuture
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Characteristic {
                uuid: PREVIEW_OFFSET_UUID,
                write: Some(CharacteristicWrite {
                    write: true,
                    method: CharacteristicWriteMethod::Fun(Box::new(move |value, _| {
                        let preview_offset = preview_offset_write.clone();
                        let preview_snapshot = preview_snapshot_write.clone();
                        let preview = preview_write.clone();
                        Box::pin(async move {
                            let offset = parse_u32_offset(&value)?;
                            if offset == 0 {
                                *preview_snapshot.write() = preview
                                    .as_ref()
                                    .map_or_else(Vec::new, PreviewState::latest_jpeg);
                            }
                            preview_offset.store(offset, Ordering::Relaxed);
                            Ok(())
                        }) as WriteFuture
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
        ..Default::default()
    }];
    if obs.is_some() {
        services.push(Service {
            uuid: OBS_SERVICE_UUID,
            primary: true,
            characteristics: vec![
                Characteristic {
                    uuid: OBS_STATUS_UUID,
                    read: Some(CharacteristicRead {
                        read: true,
                        fun: Box::new(move |_| {
                            let obs = obs_status.clone();
                            Box::pin(async move {
                                let Some(obs) = obs else {
                                    return Ok(Vec::new());
                                };
                                serde_json::to_vec(&obs.status().await)
                                    .map_err(|_| ReqError::Failed)
                            }) as ReadFuture
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Characteristic {
                    uuid: OBS_CONTROL_UUID,
                    write: Some(CharacteristicWrite {
                        write: true,
                        write_without_response: true,
                        method: CharacteristicWriteMethod::Fun(Box::new(move |value, _| {
                            let obs = obs_control.clone();
                            Box::pin(async move {
                                let Some(obs) = obs else {
                                    return Err(ReqError::NotSupported);
                                };
                                let command = parse_obs_command(&value)
                                    .map_err(|_| ReqError::InvalidValueLength)?;
                                obs.apply(command).await.map_err(|_| ReqError::Failed)?;
                                Ok(())
                            }) as WriteFuture
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
    }
    let app = Application {
        services,
        ..Default::default()
    };
    let app_handle = adapter
        .serve_gatt_application(app)
        .await
        .context("failed to register BLE GATT service")?;

    ctx.record_remote_ready(
        &adapter.name(),
        &config.name,
        &SERVICE_UUID.to_string(),
        &STATUS_UUID.to_string(),
        &CONTROL_UUID.to_string(),
    );

    tokio::spawn(async move {
        let _session = session;
        let _adv_handle = adv_handle;
        let _app_handle = app_handle;
        std::future::pending::<()>().await;
    });

    Ok(())
}

fn build_status_payload(
    config: &RemoteConfig,
    strategy: &StrategyState,
    preview: Option<&PreviewState>,
) -> bluer::gatt::local::ReqResult<Vec<u8>> {
    serde_json::to_vec(&status_payload(config, strategy, preview)).map_err(|_| ReqError::Failed)
}

#[derive(serde::Serialize)]
struct StatusPayload {
    kind: &'static str,
    endpoint: String,
    addrs: Vec<String>,
    relays: Vec<String>,
    control: crate::path_strategy::ControlStatus,
    preview: PreviewStatus,
}

#[derive(serde::Serialize)]
struct PreviewStatus {
    enabled: bool,
    decoding: bool,
    jpeg_bytes: usize,
    characteristic: String,
    offset_characteristic: String,
    chunk_bytes: usize,
}

fn status_payload(
    config: &RemoteConfig,
    strategy: &StrategyState,
    preview: Option<&PreviewState>,
) -> StatusPayload {
    StatusPayload {
        kind: "irohsion-client-remote",
        endpoint: config.endpoint.to_string(),
        addrs: config.addrs.iter().map(ToString::to_string).collect(),
        relays: config.relays.iter().map(ToString::to_string).collect(),
        control: strategy.status(),
        preview: PreviewStatus {
            enabled: preview.is_some(),
            decoding: preview.is_some_and(PreviewState::enabled),
            jpeg_bytes: preview.map_or(0, PreviewState::latest_jpeg_len),
            characteristic: PREVIEW_UUID.to_string(),
            offset_characteristic: PREVIEW_OFFSET_UUID.to_string(),
            chunk_bytes: PREVIEW_CHUNK_BYTES,
        },
    }
}

fn parse_u32_offset(value: &[u8]) -> bluer::gatt::local::ReqResult<usize> {
    let bytes: [u8; 4] = value.try_into().map_err(|_| ReqError::InvalidValueLength)?;
    Ok(u32::from_le_bytes(bytes) as usize)
}

fn chunk_bytes(bytes: &[u8], offset: usize, max_len: usize) -> Vec<u8> {
    if offset >= bytes.len() {
        return Vec::new();
    }

    let end = offset.saturating_add(max_len).min(bytes.len());
    bytes[offset..end].to_vec()
}
