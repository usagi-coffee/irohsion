use crate::context::ClientCtx;
use tokio::task::JoinHandle;
use transport::{PathConnection, decode_health_report};

pub struct HealthReceiverSet {
    _tasks: Vec<JoinHandle<()>>,
}

pub fn spawn_health_receivers(paths: &[PathConnection], ctx: ClientCtx) -> HealthReceiverSet {
    let mut tasks = Vec::with_capacity(paths.len());

    for path in paths {
        let endpoint = path.endpoint();
        let task = tokio::spawn({
            let ctx = ctx.clone();
            async move {
                loop {
                    let Some(incoming) = endpoint.accept().await else {
                        break;
                    };

                    let Ok(accepting) = incoming.accept() else {
                        continue;
                    };
                    let Ok(connection) = accepting.await else {
                        continue;
                    };

                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        loop {
                            match connection.read_datagram().await {
                                Ok(payload) => match decode_health_report(&payload) {
                                    Ok(report) => ctx.record_health_report(&report),
                                    Err(err) => ctx.invalid_health_report(&err.to_string()),
                                },
                                Err(_) => break,
                            }
                        }
                    });
                }
            }
        });

        tasks.push(task);
    }

    HealthReceiverSet { _tasks: tasks }
}
