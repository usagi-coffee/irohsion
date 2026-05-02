use crate::context::ClientCtx;
use crate::path_strategy::StrategyState;
use std::time::Duration;
use tokio::task::JoinHandle;
use transport::{PathConnection, decode_health_report};

pub struct HealthReceiverSet {
    _tasks: Vec<JoinHandle<()>>,
}

pub fn spawn_health_receivers(
    paths: &[PathConnection],
    ctx: ClientCtx,
    strategy: StrategyState,
) -> HealthReceiverSet {
    let mut tasks = Vec::with_capacity(paths.len());

    for path in paths {
        let path = path.clone();
        let task = tokio::spawn({
            let ctx = ctx.clone();
            let strategy = strategy.clone();
            async move {
                loop {
                    let Some(endpoint) = path.endpoint() else {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    };

                    let Some(incoming) = endpoint.accept().await else {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    };

                    let Ok(accepting) = incoming.accept() else {
                        continue;
                    };
                    let Ok(connection) = accepting.await else {
                        continue;
                    };

                    loop {
                        match connection.read_datagram().await {
                            Ok(payload) => match decode_health_report(&payload) {
                                Ok(report) => {
                                    strategy.record_health_report(&report);
                                    ctx.record_health_report(&report);
                                }
                                Err(err) => ctx.invalid_health_report(&err.to_string()),
                            },
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        tasks.push(task);
    }

    HealthReceiverSet { _tasks: tasks }
}
