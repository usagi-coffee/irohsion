use crate::tui;
use anyhow::{Context, Result};
use cli::SecretArg;
use iroh::{Endpoint, RelayMode, endpoint::presets};
use tokio::task::JoinHandle;
use transport::HEALTH_ALPN;

pub struct HealthReceiver {
    pub endpoint_id: String,
    _endpoint: Endpoint,
    _task: JoinHandle<()>,
}

pub async fn spawn_health_receiver(
    secret: &SecretArg,
    ui_state: Option<tui::ClientUiState>,
) -> Result<HealthReceiver> {
    let secret_key = secret.resolve();
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![HEALTH_ALPN.to_vec()])
        .relay_mode(RelayMode::Default)
        .bind()
        .await
        .context("failed to bind client health endpoint")?;
    endpoint.online().await;

    let endpoint_id = endpoint.id().to_string();
    let accept_endpoint = endpoint.clone();
    let task = tokio::spawn(async move {
        loop {
            let Some(incoming) = accept_endpoint.accept().await else {
                break;
            };

            let Ok(accepting) = incoming.accept() else {
                continue;
            };
            let Ok(connection) = accepting.await else {
                continue;
            };

            let ui_state = ui_state.clone();
            tokio::spawn(async move {
                loop {
                    match connection.read_datagram().await {
                        Ok(_) => {
                            if let Some(ui_state) = &ui_state {
                                ui_state.record_health_received();
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    });

    Ok(HealthReceiver {
        endpoint_id,
        _endpoint: endpoint,
        _task: task,
    })
}
