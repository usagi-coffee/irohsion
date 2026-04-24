use std::{future::Future, net::SocketAddr, pin::Pin};

use anyhow::{Context, Result};
use bluer::{
    adv::Advertisement,
    gatt::local::{
        Application, Characteristic, CharacteristicRead, CharacteristicWrite,
        CharacteristicWriteMethod, ReqError, Service,
    },
};
use iroh::{EndpointId, RelayUrl};
use uuid::Uuid;

use crate::{
    context::ClientCtx,
    path_strategy::{ControlPatch, StrategyState},
};

pub const SERVICE_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10001);
pub const STATUS_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10002);
pub const CONTROL_UUID: Uuid = Uuid::from_u128(0x8b4f_82c8_4f5a_4e26_8f29_d1f0c0d10003);

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
        service_uuids: vec![SERVICE_UUID].into_iter().collect(),
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
    let write_strategy = strategy.clone();
    let write_ctx = ctx.clone();
    let app = Application {
        services: vec![Service {
            uuid: SERVICE_UUID,
            primary: true,
            characteristics: vec![
                Characteristic {
                    uuid: STATUS_UUID,
                    read: Some(CharacteristicRead {
                        read: true,
                        fun: Box::new(move |_| {
                            let config = read_config.clone();
                            let strategy = read_strategy.clone();
                            Box::pin(async move {
                                serde_json::to_vec(&status_payload(&config, &strategy))
                                    .map_err(|_| ReqError::Failed)
                            }) as ReadFuture
                        }),
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
                            Box::pin(async move {
                                let patch = serde_json::from_slice::<ControlPatch>(&value)
                                    .map_err(|_| ReqError::InvalidValueLength)?;
                                strategy.apply_patch(patch, &ctx);
                                Ok(())
                            }) as WriteFuture
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
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

#[derive(serde::Serialize)]
struct StatusPayload {
    kind: &'static str,
    endpoint: String,
    addrs: Vec<String>,
    relays: Vec<String>,
    control: crate::path_strategy::ControlStatus,
}

fn status_payload(config: &RemoteConfig, strategy: &StrategyState) -> StatusPayload {
    StatusPayload {
        kind: "irohsion-client-remote",
        endpoint: config.endpoint.to_string(),
        addrs: config.addrs.iter().map(ToString::to_string).collect(),
        relays: config.relays.iter().map(ToString::to_string).collect(),
        control: strategy.status(),
    }
}
