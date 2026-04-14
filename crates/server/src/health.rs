use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use iroh::Endpoint;
use parking_lot::RwLock;
use transport::{EndpointHealth, HEALTH_ALPN, HealthReport, encode_health_report};

pub type HealthConnection = Arc<RwLock<Option<iroh::endpoint::Connection>>>;
pub type HealthStats = Arc<RwLock<BTreeMap<String, u64>>>;

pub async fn health_loop(
    health_connection: HealthConnection,
    health_stats: HealthStats,
    endpoint_targets: Arc<BTreeMap<String, f32>>,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    let mut seq = 0_u64;
    loop {
        ticker.tick().await;
        let connection = health_connection.read().clone();
        let Some(connection) = connection else {
            continue;
        };

        let report = build_health_report(seq, &health_stats, &endpoint_targets, interval)?;
        seq = seq.wrapping_add(1);
        if connection
            .send_datagram(encode_health_report(&report))
            .is_err()
        {
            *health_connection.write() = None;
        }
    }
}

pub async fn maintain_health_connection(
    endpoint: Endpoint,
    health_addr: iroh::EndpointAddr,
    health_connection: HealthConnection,
) -> Result<()> {
    loop {
        if health_connection.read().is_some() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        match endpoint.connect(health_addr.clone(), HEALTH_ALPN).await {
            Ok(connection) => {
                *health_connection.write() = Some(connection);
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

pub fn record_health_bytes(health_stats: &HealthStats, remote: &str, bytes: u64) {
    *health_stats.write().entry(remote.to_string()).or_default() += bytes;
}

fn build_health_report(
    seq: u64,
    health_stats: &HealthStats,
    endpoint_targets: &BTreeMap<String, f32>,
    interval: Duration,
) -> Result<HealthReport> {
    let interval_secs = interval.as_secs_f32().max(0.001);
    let mut drained = health_stats.write();
    let endpoint_ids = endpoint_targets
        .keys()
        .cloned()
        .chain(drained.keys().cloned())
        .collect::<BTreeSet<_>>();
    let mut endpoints = Vec::with_capacity(endpoint_ids.len());

    for endpoint_id in endpoint_ids {
        let bytes = drained.remove(&endpoint_id).unwrap_or(0);
        let target_mbps = endpoint_targets.get(&endpoint_id).copied().unwrap_or(0.0);
        let achieved_mbps = bytes as f32 * 8.0 / interval_secs / 1_000_000.0;
        endpoints.push(EndpointHealth {
            endpoint_id: endpoint_id.clone(),
            target_mbps,
            achieved_mbps,
        });
    }
    drained.clear();

    Ok(HealthReport {
        seq,
        unix_ms: unix_ms_now()?,
        endpoints,
    })
}

fn unix_ms_now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_millis()
        .try_into()
        .context("current unix timestamp does not fit in u64")?)
}
