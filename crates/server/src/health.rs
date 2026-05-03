use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use kick_remote::KickChat;
use parking_lot::RwLock;
use transport::{ChatMessage, EndpointHealth, HEALTH_ALPN, HealthReport, encode_health_report};
use twitch_remote::TwitchChat;

#[derive(Clone)]
pub struct HealthConnection {
    pub connection: iroh::endpoint::Connection,
    pub stable_id: usize,
}

pub type HealthConnections = Arc<RwLock<BTreeMap<String, HealthConnection>>>;
pub type HealthTargets = Arc<RwLock<HashSet<String>>>;
pub type HealthStats = Arc<RwLock<BTreeMap<String, EndpointSample>>>;

const MAX_CHAT_MESSAGES_PER_HEALTH_REPORT: usize = 3;

#[derive(Clone, Default)]
pub struct ChatSources {
    pub twitch: Option<TwitchChat>,
    pub kick: Option<KickChat>,
}

#[derive(Clone, Copy, Default)]
pub struct EndpointSample {
    pub bytes: u64,
    pub last_seq: Option<u64>,
    pub max_seq: Option<u64>,
}

pub async fn health_loop(
    health_connections: HealthConnections,
    health_stats: HealthStats,
    chat_sources: ChatSources,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    let mut seq = 0_u64;
    loop {
        ticker.tick().await;
        let connections = health_connections.read().clone();
        if connections.is_empty() {
            continue;
        }

        let report =
            build_health_report(seq, &health_stats, &connections, &chat_sources, interval)?;
        seq = seq.wrapping_add(1);
        let payload = encode_health_report(&report);
        let mut failed = Vec::new();
        for (endpoint_id, connection) in &connections {
            if connection
                .connection
                .send_datagram(payload.clone())
                .is_err()
            {
                failed.push((endpoint_id.clone(), connection.stable_id));
            }
        }
        if !failed.is_empty() {
            let mut live = health_connections.write();
            for (endpoint_id, stable_id) in failed {
                if live
                    .get(&endpoint_id)
                    .is_some_and(|connection| connection.stable_id == stable_id)
                {
                    live.remove(&endpoint_id);
                }
            }
        }
    }
}

pub async fn maintain_health_connection(
    endpoint: Endpoint,
    endpoint_id: EndpointId,
    endpoint_key: String,
    health_targets: HealthTargets,
    health_connections: HealthConnections,
) -> Result<()> {
    loop {
        if !health_targets.read().contains(&endpoint_key) {
            return Ok(());
        }

        if health_connections.read().contains_key(&endpoint_key) {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        let Some(remote_info) = endpoint.remote_info(endpoint_id).await else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };

        let health_addr = EndpointAddr::from_parts(
            remote_info.id(),
            remote_info.into_addrs().map(|addr| addr.into_addr()),
        );

        match endpoint.connect(health_addr, HEALTH_ALPN).await {
            Ok(connection) => {
                let stable_id = connection.stable_id();
                health_connections.write().insert(
                    endpoint_key.clone(),
                    HealthConnection {
                        connection,
                        stable_id,
                    },
                );
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

pub fn record_health_sample(health_stats: &HealthStats, remote: &str, bytes: u64, seq: u64) {
    let mut stats = health_stats.write();
    let sample = stats.entry(remote.to_string()).or_default();
    sample.bytes = sample.bytes.saturating_add(bytes);
    sample.last_seq = Some(seq);
    sample.max_seq = Some(sample.max_seq.map_or(seq, |current| current.max(seq)));
}

fn build_health_report(
    seq: u64,
    health_stats: &HealthStats,
    health_connections: &BTreeMap<String, HealthConnection>,
    chat_sources: &ChatSources,
    interval: Duration,
) -> Result<HealthReport> {
    let interval_secs = interval.as_secs_f32().max(0.001);
    let mut drained = health_stats.write();
    let endpoint_ids = health_connections.keys().cloned().collect::<Vec<_>>();
    let mut endpoints = Vec::with_capacity(endpoint_ids.len());

    for endpoint_id in endpoint_ids {
        let sample = drained.remove(&endpoint_id).unwrap_or_default();
        let achieved_mbps = sample.bytes as f32 * 8.0 / interval_secs / 1_000_000.0;
        endpoints.push(EndpointHealth {
            endpoint_id: endpoint_id.clone(),
            achieved_mbps,
            last_seq: sample.last_seq,
            max_seq: sample.max_seq,
        });
    }
    drained.clear();

    Ok(HealthReport {
        seq,
        unix_ms: unix_ms_now()?,
        endpoints,
        chat: recent_chat_messages(chat_sources),
    })
}

fn recent_chat_messages(chat_sources: &ChatSources) -> Vec<ChatMessage> {
    let mut messages = Vec::new();
    if let Some(chat) = &chat_sources.twitch {
        messages.extend(
            chat.recent_messages()
                .into_iter()
                .map(|message| ChatMessage {
                    id: message.id << 1,
                    unix_ms: message.unix_ms,
                    user: message.user,
                    text: message.text,
                }),
        );
    }
    if let Some(chat) = &chat_sources.kick {
        messages.extend(
            chat.recent_messages()
                .into_iter()
                .map(|message| ChatMessage {
                    id: (message.id << 1) | 1,
                    unix_ms: message.unix_ms,
                    user: message.user,
                    text: message.text,
                }),
        );
    }
    messages.sort_by_key(|message| (message.unix_ms, message.id));
    if messages.len() > MAX_CHAT_MESSAGES_PER_HEALTH_REPORT {
        messages.drain(0..messages.len() - MAX_CHAT_MESSAGES_PER_HEALTH_REPORT);
    }
    messages
}

fn unix_ms_now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_millis()
        .try_into()
        .context("current unix timestamp does not fit in u64")?)
}
