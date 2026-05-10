use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::context::ClientCtx;
use transport::HealthReport;

const AUTO_SPLIT_HEALTH_STALE_MS: u64 = 2_500;
const AUTO_SPLIT_DEGRADE_LAG_PACKETS: u64 = 800;
const AUTO_WEIGHT_MIN_RAW_SHARE: f64 = 0.01;
const AUTO_WEIGHT_MIN_LAG_PENALTY: f64 = 0.35;
const AUTO_WEIGHT_THROUGHPUT_BLEND: f64 = 0.25;
const AUTO_WEIGHT_MIN_THROUGHPUT_RATIO: f64 = 0.25;
const AUTO_WEIGHT_MAX_THROUGHPUT_RATIO: f64 = 2.0;
const AUTO_SPLIT_MIN_AVERAGE_SERVER_MBPS: f32 = 0.25;
const AUTO_SPLIT_MIN_SERVER_MBPS_RATIO: f32 = 0.15;
const AUTO_SPLIT_MIN_SERVER_MBPS: f32 = 0.05;
const TC_BACKLOG_TIMEOUT: Duration = Duration::from_secs(1);
const TC_QDISC_REPLACE_TIMEOUT: Duration = Duration::from_secs(1);
const TC_QDISC_KIND: &str = "fq_codel";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PathStrategy {
    Redundant = 0,
    Split = 1,
    RoundRobin = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StrategyMode {
    Auto = 0,
    Redundant = 1,
    Split = 2,
    RoundRobin = 3,
}

impl StrategyMode {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Redundant,
            2 => Self::Split,
            3 => Self::RoundRobin,
            _ => Self::Auto,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct InterfaceControlStatus {
    pub name: String,
    pub status: InterfaceLinkStatus,
    pub split_percentage: f64,
    pub tx_packets: u64,
    pub tx_bytes: u64,
    pub tx_mbps: f64,
    pub server_mbps: Option<f32>,
    pub server_last_seq: Option<u64>,
    pub server_max_seq: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum InterfaceLinkStatus {
    Connected,
    Reconnecting,
    Dead,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlStatus {
    pub mode: StrategyMode,
    pub effective_strategy: PathStrategy,
    pub monitor_packets: bool,
    pub packets: u64,
    pub payload_bytes: u64,
    pub interfaces: Vec<InterfaceControlStatus>,
}

#[derive(Clone)]
pub struct StrategyState {
    mode: Arc<AtomicU8>,
    strategy: Arc<AtomicU8>,
    weighted_auto_split: Arc<AtomicBool>,
    monitor_packets: Arc<AtomicBool>,
    packets: Arc<AtomicU64>,
    payload_bytes: Arc<AtomicU64>,
    round_robin_cursor: Arc<AtomicU64>,
    endpoint_ids_by_interface: Arc<RwLock<BTreeMap<String, String>>>,
    split_percentages: Arc<RwLock<BTreeMap<String, Option<f64>>>>,
    health_by_interface: Arc<RwLock<BTreeMap<String, InterfaceHealth>>>,
    interface_failures: Arc<RwLock<BTreeMap<String, InterfaceFailure>>>,
    interface_status: Arc<RwLock<BTreeMap<String, InterfaceLinkStatus>>>,
    interface_traffic: Arc<RwLock<BTreeMap<String, InterfaceTraffic>>>,
}

#[derive(Debug, Clone, Copy)]
struct InterfaceHealth {
    last_seq: Option<u64>,
    max_seq: Option<u64>,
    server_mbps: f32,
    last_unix_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct InterfaceFailure;

#[derive(Debug, Clone)]
pub struct StrategyInterface {
    pub display_name: String,
    pub device_name: String,
    pub endpoint_id: String,
}

#[derive(Debug, Clone, Copy)]
pub struct QdiscResetConfig {
    pub backlog_bytes: u64,
    pub max_server_mbps: f32,
    pub cooldown: Duration,
}

impl StrategyState {
    fn sync_ui(&self, ctx: &ClientCtx) {
        let mode = if self.weighted_auto_split_enabled() && self.mode() == StrategyMode::Auto {
            "auto+weighted".to_string()
        } else {
            format!("{:?}", self.mode()).to_lowercase()
        };
        ctx.record_strategy_state(&mode, &format!("{:?}", self.current()).to_lowercase());
    }

    pub fn current(&self) -> PathStrategy {
        match self.mode() {
            StrategyMode::Redundant => return PathStrategy::Redundant,
            StrategyMode::Split => return PathStrategy::Split,
            StrategyMode::RoundRobin => return PathStrategy::RoundRobin,
            StrategyMode::Auto => {}
        }

        match self.strategy.load(Ordering::Relaxed) {
            1 => PathStrategy::Split,
            _ => PathStrategy::Redundant,
        }
    }

    pub fn mode(&self) -> StrategyMode {
        StrategyMode::from_u8(self.mode.load(Ordering::Relaxed))
    }

    pub fn status(&self) -> ControlStatus {
        let endpoint_ids = self.endpoint_ids_by_interface.read();
        let health = self.health_by_interface.read();
        let statuses = self.interface_status.read();
        let split_percentages = self.effective_split_percentages();
        let mut traffic = self.interface_traffic.write();
        for counters in traffic.values_mut() {
            counters.update_rate();
        }
        ControlStatus {
            mode: self.mode(),
            effective_strategy: self.current(),
            monitor_packets: self.monitor_packets.load(Ordering::Relaxed),
            packets: self.packets.load(Ordering::Relaxed),
            payload_bytes: self.payload_bytes.load(Ordering::Relaxed),
            interfaces: endpoint_ids
                .iter()
                .map(|(name, _endpoint_id)| {
                    let counters = traffic.get(name).copied().unwrap_or_default();
                    let health = health.get(name);
                    InterfaceControlStatus {
                        name: name.clone(),
                        status: statuses
                            .get(name)
                            .copied()
                            .unwrap_or(InterfaceLinkStatus::Dead),
                        split_percentage: split_percentages.get(name).copied().unwrap_or(0.0),
                        tx_packets: counters.tx_packets,
                        tx_bytes: counters.tx_bytes,
                        tx_mbps: counters.tx_mbps,
                        server_mbps: health.map(|health| health.server_mbps),
                        server_last_seq: health.and_then(|health| health.last_seq),
                        server_max_seq: health.and_then(|health| health.max_seq),
                    }
                })
                .collect(),
        }
    }

    pub fn weighted_auto_split_enabled(&self) -> bool {
        self.weighted_auto_split.load(Ordering::Relaxed)
    }

    pub fn toggle_weighted_auto_split(&self, ctx: &ClientCtx) {
        let enabled = !self.weighted_auto_split.fetch_xor(true, Ordering::Relaxed);
        ctx.record_strategy_change(
            if enabled {
                "auto+weighted"
            } else {
                "auto weighted off"
            },
            "tui hotkey",
        );
        self.sync_ui(ctx);
    }

    pub fn set_mode(&self, mode: StrategyMode, ctx: &ClientCtx, reason: &str) {
        self.mode.store(mode as u8, Ordering::Relaxed);
        ctx.record_strategy_change(&format!("{mode:?}").to_lowercase(), reason);
        self.sync_ui(ctx);
    }

    pub fn record_packet(&self, payload_bytes: u64) {
        if !self.monitor_packets.load(Ordering::Relaxed) {
            return;
        }

        self.packets.fetch_add(1, Ordering::Relaxed);
        self.payload_bytes
            .fetch_add(payload_bytes, Ordering::Relaxed);
    }

    pub fn record_interface_send(&self, interface: &str, bytes: u64) {
        if !self.monitor_packets.load(Ordering::Relaxed) {
            return;
        }

        if let Some(counters) = self.interface_traffic.write().get_mut(interface) {
            counters.tx_packets = counters.tx_packets.saturating_add(1);
            counters.tx_bytes = counters.tx_bytes.saturating_add(bytes);
        }
    }

    pub fn record_interface_success(&self, interface: &str) {
        self.interface_failures.write().remove(interface);
        self.interface_status
            .write()
            .insert(interface.to_string(), InterfaceLinkStatus::Connected);
    }

    pub fn record_interface_failure(&self, interface: &str) {
        self.interface_failures
            .write()
            .insert(interface.to_string(), InterfaceFailure);
        self.record_interface_reconnecting(interface);
    }

    pub fn record_interface_reconnecting(&self, interface: &str) {
        self.interface_status
            .write()
            .insert(interface.to_string(), InterfaceLinkStatus::Reconnecting);
    }

    pub fn record_interface_dead(&self, interface: &str) {
        self.interface_failures
            .write()
            .insert(interface.to_string(), InterfaceFailure);
        self.interface_status
            .write()
            .insert(interface.to_string(), InterfaceLinkStatus::Dead);
    }

    pub fn next_round_robin_index(&self, path_count: usize) -> usize {
        if path_count == 0 {
            return 0;
        }

        let cursor = self.round_robin_cursor.fetch_add(1, Ordering::Relaxed);
        (cursor as usize) % path_count
    }

    pub fn split_weights(&self, interfaces: &[String]) -> Vec<f64> {
        let percentages = self.split_percentages.read();
        let even_weight = if interfaces.is_empty() {
            0.0
        } else {
            1.0 / interfaces.len() as f64
        };
        let weights = interfaces
            .iter()
            .map(|interface| {
                percentages
                    .get(interface)
                    .copied()
                    .flatten()
                    .map(|percentage| percentage.clamp(0.0, 100.0) / 100.0)
                    .unwrap_or(even_weight)
            })
            .collect::<Vec<_>>();
        let total = weights.iter().sum::<f64>();
        normalize_weights_with_fallback(weights, even_weight, total)
    }

    pub fn active_split_weights(&self, interfaces: &[String]) -> Vec<f64> {
        let weights = if self.mode() == StrategyMode::Auto && self.weighted_auto_split_enabled() {
            self.auto_split_weights(interfaces)
        } else {
            self.split_weights(interfaces)
        };
        self.zero_failed_weights(interfaces, weights)
    }

    pub fn effective_split_percentages(&self) -> BTreeMap<String, f64> {
        let interfaces = self
            .endpoint_ids_by_interface
            .read()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        self.effective_split_percentages_for(&interfaces)
    }

    pub fn effective_split_percentages_for(&self, interfaces: &[String]) -> BTreeMap<String, f64> {
        let weights = self.active_split_weights(interfaces);
        interfaces
            .iter()
            .cloned()
            .zip(weights)
            .map(|(interface, weight)| (interface, weight * 100.0))
            .collect()
    }

    pub fn record_health_report(&self, report: &HealthReport) {
        let endpoint_ids = self.endpoint_ids_by_interface.read().clone();
        let endpoints = report
            .endpoints
            .iter()
            .map(|endpoint| {
                (
                    endpoint.endpoint_id.as_str(),
                    (endpoint.last_seq, endpoint.max_seq, endpoint.achieved_mbps),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut health = self.health_by_interface.write();
        for (interface, endpoint_id) in endpoint_ids {
            if let Some((last_seq, max_seq, server_mbps)) = endpoints.get(endpoint_id.as_str()) {
                health.insert(
                    interface,
                    InterfaceHealth {
                        last_seq: *last_seq,
                        max_seq: *max_seq,
                        server_mbps: *server_mbps,
                        last_unix_ms: report.unix_ms,
                    },
                );
            }
        }
    }

    pub fn auto_path_groups(&self, interfaces: &[String]) -> (Vec<String>, Vec<String>) {
        let failed = self.recently_failed_interfaces();
        let mut degraded = interfaces
            .iter()
            .filter(|interface| failed.contains(*interface))
            .cloned()
            .collect::<Vec<_>>();
        let candidates = interfaces
            .iter()
            .filter(|interface| !failed.contains(*interface))
            .cloned()
            .collect::<Vec<_>>();
        if candidates.len() < 2 {
            degraded.extend(candidates.iter().cloned());
            return (candidates, degraded);
        }

        let health = self.health_by_interface.read();
        let now = unix_ms_now();
        let fresh = candidates
            .iter()
            .filter_map(|interface| {
                let state = health.get(interface)?;
                let age_ms = now.saturating_sub(state.last_unix_ms);
                (age_ms <= AUTO_SPLIT_HEALTH_STALE_MS).then_some((interface.clone(), *state))
            })
            .collect::<Vec<_>>();
        if fresh.is_empty() {
            degraded.extend(candidates.iter().cloned());
            return (Vec::new(), degraded);
        }

        let best_last_seq = fresh.iter().filter_map(|(_, state)| state.last_seq).max();
        let Some(best_last_seq) = best_last_seq else {
            degraded.extend(candidates.iter().cloned());
            return (Vec::new(), degraded);
        };
        let average_server_mbps = fresh
            .iter()
            .map(|(_, state)| state.server_mbps)
            .sum::<f32>()
            / fresh.len() as f32;

        let healthy = fresh
            .into_iter()
            .filter(|(_, state)| {
                path_health_allows_split(*state, best_last_seq, average_server_mbps)
            })
            .map(|(interface, _)| interface)
            .collect::<Vec<_>>();
        let healthy_set = healthy.iter().cloned().collect::<BTreeSet<_>>();
        degraded.extend(
            candidates
                .iter()
                .filter(|interface| !healthy_set.contains(*interface))
                .cloned(),
        );
        (healthy, degraded)
    }

    fn auto_split_weights(&self, interfaces: &[String]) -> Vec<f64> {
        if interfaces.is_empty() {
            return Vec::new();
        }

        let health = self.health_by_interface.read();
        let failed = self.recently_failed_interfaces();
        let now = unix_ms_now();
        let best_last_seq = interfaces
            .iter()
            .filter_map(|interface| health.get(interface).and_then(|state| state.last_seq))
            .max();
        let fresh_server_mbps = interfaces
            .iter()
            .filter(|interface| !failed.contains(*interface))
            .filter_map(|interface| {
                let state = health.get(interface)?;
                let age_ms = now.saturating_sub(state.last_unix_ms);
                (age_ms <= AUTO_SPLIT_HEALTH_STALE_MS)
                    .then_some(f64::from(state.server_mbps.max(0.0)))
            })
            .collect::<Vec<_>>();
        let average_server_mbps = if fresh_server_mbps.is_empty() {
            0.0
        } else {
            fresh_server_mbps.iter().sum::<f64>() / fresh_server_mbps.len() as f64
        };
        let raw = interfaces
            .iter()
            .map(|interface| {
                if failed.contains(interface) {
                    return 0.0;
                }
                let Some(state) = health.get(interface) else {
                    return AUTO_WEIGHT_MIN_RAW_SHARE;
                };
                let age_ms = now.saturating_sub(state.last_unix_ms);
                if age_ms > AUTO_SPLIT_HEALTH_STALE_MS {
                    return AUTO_WEIGHT_MIN_RAW_SHARE;
                }

                let lag_penalty = best_last_seq
                    .zip(state.last_seq)
                    .map(|(best, seq)| {
                        let lag = best.saturating_sub(seq) as f64;
                        (1.0 - lag / AUTO_SPLIT_DEGRADE_LAG_PACKETS as f64)
                            .clamp(AUTO_WEIGHT_MIN_LAG_PENALTY, 1.0)
                    })
                    .unwrap_or(1.0);
                blended_auto_weight(state.server_mbps, average_server_mbps, lag_penalty)
            })
            .collect::<Vec<_>>();

        normalize_weights(raw)
    }

    fn zero_failed_weights(&self, interfaces: &[String], mut weights: Vec<f64>) -> Vec<f64> {
        let failed = self.recently_failed_interfaces();
        for (interface, weight) in interfaces.iter().zip(weights.iter_mut()) {
            if failed.contains(interface) {
                *weight = 0.0;
            }
        }
        let total = weights.iter().sum::<f64>();
        if total <= f64::EPSILON {
            weights
        } else {
            weights.iter_mut().for_each(|weight| *weight /= total);
            weights
        }
    }

    pub fn auto_split_ready(&self, interfaces: &[String]) -> bool {
        let (healthy, degraded) = self.auto_path_groups(interfaces);
        healthy.len() >= 2 && degraded.is_empty()
    }

    pub fn degrade_to_redundant(&self, ctx: &ClientCtx, reason: String) {
        if self.mode() != StrategyMode::Auto {
            return;
        }
        if self.current() == PathStrategy::Redundant {
            return;
        }

        self.set(PathStrategy::Redundant);
        ctx.record_strategy_change("redundant", &reason);
        self.sync_ui(ctx);
    }

    fn set(&self, strategy: PathStrategy) {
        self.strategy.store(strategy as u8, Ordering::Relaxed);
    }

    fn recently_failed_interfaces(&self) -> BTreeSet<String> {
        self.interface_failures.read().keys().cloned().collect()
    }

    fn fresh_health(&self, interface: &str) -> Option<InterfaceHealth> {
        let health = self.health_by_interface.read().get(interface).copied()?;
        let age_ms = unix_ms_now().saturating_sub(health.last_unix_ms);
        (age_ms <= AUTO_SPLIT_HEALTH_STALE_MS).then_some(health)
    }
}

pub fn spawn_strategy_loop(
    interfaces: Vec<StrategyInterface>,
    poll_interval: Duration,
    degrade_backlog_bytes: u64,
    recover_backlog_bytes: u64,
    qdisc_reset: Option<QdiscResetConfig>,
    ctx: ClientCtx,
) -> StrategyState {
    let endpoint_ids_by_interface = interfaces
        .iter()
        .map(|interface| {
            (
                interface.display_name.clone(),
                interface.endpoint_id.clone(),
            )
        })
        .collect();
    let split_percentages = interfaces
        .iter()
        .map(|interface| (interface.display_name.clone(), None))
        .collect();
    let interface_status = interfaces
        .iter()
        .map(|interface| {
            (
                interface.display_name.clone(),
                InterfaceLinkStatus::Connected,
            )
        })
        .collect();
    let interface_traffic = interfaces
        .iter()
        .map(|interface| (interface.display_name.clone(), InterfaceTraffic::default()))
        .collect();
    let state = StrategyState {
        mode: Arc::new(AtomicU8::new(StrategyMode::Auto as u8)),
        strategy: Arc::new(AtomicU8::new(PathStrategy::Split as u8)),
        weighted_auto_split: Arc::new(AtomicBool::new(true)),
        monitor_packets: Arc::new(AtomicBool::new(true)),
        packets: Arc::new(AtomicU64::new(0)),
        payload_bytes: Arc::new(AtomicU64::new(0)),
        round_robin_cursor: Arc::new(AtomicU64::new(0)),
        endpoint_ids_by_interface: Arc::new(RwLock::new(endpoint_ids_by_interface)),
        split_percentages: Arc::new(RwLock::new(split_percentages)),
        health_by_interface: Arc::new(RwLock::new(BTreeMap::new())),
        interface_failures: Arc::new(RwLock::new(BTreeMap::new())),
        interface_status: Arc::new(RwLock::new(interface_status)),
        interface_traffic: Arc::new(RwLock::new(interface_traffic)),
    };
    ctx.record_split_percentages(&state.effective_split_percentages());
    state.sync_ui(&ctx);
    let loop_state = state.clone();
    tokio::spawn(async move {
        strategy_loop(
            interfaces,
            poll_interval,
            degrade_backlog_bytes,
            recover_backlog_bytes,
            qdisc_reset,
            loop_state,
            ctx,
        )
        .await;
    });
    state
}

#[derive(Debug, Clone, Copy)]
struct InterfaceTraffic {
    tx_packets: u64,
    tx_bytes: u64,
    last_tx_bytes: u64,
    last_sample: Instant,
    tx_mbps: f64,
}

impl Default for InterfaceTraffic {
    fn default() -> Self {
        Self {
            tx_packets: 0,
            tx_bytes: 0,
            last_tx_bytes: 0,
            last_sample: Instant::now(),
            tx_mbps: 0.0,
        }
    }
}

impl InterfaceTraffic {
    fn update_rate(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_sample).as_secs_f64();
        if elapsed <= f64::EPSILON {
            return;
        }

        let byte_delta = self.tx_bytes.saturating_sub(self.last_tx_bytes);
        self.tx_mbps = byte_delta as f64 * 8.0 / elapsed / 1_000_000.0;
        self.last_tx_bytes = self.tx_bytes;
        self.last_sample = now;
    }
}

async fn strategy_loop(
    interfaces: Vec<StrategyInterface>,
    poll_interval: Duration,
    degrade_backlog_bytes: u64,
    recover_backlog_bytes: u64,
    qdisc_reset: Option<QdiscResetConfig>,
    state: StrategyState,
    ctx: ClientCtx,
) {
    let mut interval = tokio::time::interval(poll_interval);
    let mut last_qdisc_reset_by_interface = BTreeMap::<String, Instant>::new();
    loop {
        interval.tick().await;

        let interface_names = interfaces
            .iter()
            .map(|interface| interface.display_name.clone())
            .collect::<Vec<_>>();
        let mut backlogs = Vec::new();
        let mut worst = None::<(&str, u64)>;
        for interface in &interfaces {
            let Some(backlog) = tc_backlog(&interface.device_name).await else {
                continue;
            };
            backlogs.push((interface, backlog));
            if worst.is_none_or(|(_, existing)| backlog > existing) {
                worst = Some((interface.display_name.as_str(), backlog));
            }
        }

        if let Some(qdisc_reset) = qdisc_reset {
            for (interface, backlog) in &backlogs {
                let health = state.fresh_health(&interface.display_name);
                if !qdisc_reset_should_run(*backlog, health, qdisc_reset) {
                    continue;
                }
                if last_qdisc_reset_by_interface
                    .get(&interface.display_name)
                    .is_some_and(|last| last.elapsed() < qdisc_reset.cooldown)
                {
                    continue;
                }

                last_qdisc_reset_by_interface
                    .insert(interface.display_name.clone(), Instant::now());
                let server_mbps = health.map(|health| health.server_mbps).unwrap_or(0.0);
                match replace_root_qdisc_fq_codel(&interface.device_name).await {
                    Ok(()) => ctx.record_qdisc_reset(
                        &interface.display_name,
                        &interface.device_name,
                        *backlog,
                        server_mbps,
                        TC_QDISC_KIND,
                    ),
                    Err(error) => ctx.record_qdisc_reset_failed(
                        &interface.display_name,
                        &interface.device_name,
                        *backlog,
                        server_mbps,
                        &error,
                    ),
                }
            }
        }

        if state.mode() != StrategyMode::Auto {
            continue;
        }

        if state.current() == PathStrategy::Split && !state.auto_split_ready(&interface_names) {
            state.degrade_to_redundant(&ctx, "not enough healthy paths for split".to_string());
            continue;
        }

        match (state.current(), worst) {
            (PathStrategy::Split, Some((interface, backlog)))
                if backlog >= degrade_backlog_bytes =>
            {
                state.degrade_to_redundant(
                    &ctx,
                    format!(
                        "tc backlog interface={interface} backlog_bytes={backlog} threshold_bytes={degrade_backlog_bytes}"
                    ),
                );
            }
            (PathStrategy::Redundant, Some((interface, backlog)))
                if backlog <= recover_backlog_bytes && state.auto_split_ready(&interface_names) =>
            {
                state.set(PathStrategy::Split);
                ctx.record_strategy_change(
                    "split",
                    &format!(
                        "tc backlog recovered interface={interface} backlog_bytes={backlog} threshold_bytes={recover_backlog_bytes}"
                    ),
                );
                state.sync_ui(&ctx);
            }
            (PathStrategy::Redundant, None) if state.auto_split_ready(&interface_names) => {
                state.set(PathStrategy::Split);
                ctx.record_strategy_change("split", "tc backlog unavailable");
                state.sync_ui(&ctx);
            }
            _ => {}
        }
    }
}

fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn normalize_weights(weights: Vec<f64>) -> Vec<f64> {
    let fallback = if weights.is_empty() {
        0.0
    } else {
        1.0 / weights.len() as f64
    };
    let total = weights.iter().sum::<f64>();
    normalize_weights_with_fallback(weights, fallback, total)
}

fn normalize_weights_with_fallback(mut weights: Vec<f64>, fallback: f64, total: f64) -> Vec<f64> {
    if total <= f64::EPSILON {
        weights.fill(fallback);
    } else {
        weights.iter_mut().for_each(|weight| *weight /= total);
    }
    weights
}

fn blended_auto_weight(server_mbps: f32, average_server_mbps: f64, lag_penalty: f64) -> f64 {
    let throughput_ratio = if average_server_mbps <= f64::EPSILON {
        1.0
    } else {
        (f64::from(server_mbps.max(0.0)) / average_server_mbps).clamp(
            AUTO_WEIGHT_MIN_THROUGHPUT_RATIO,
            AUTO_WEIGHT_MAX_THROUGHPUT_RATIO,
        )
    };
    let throughput_score =
        (1.0 - AUTO_WEIGHT_THROUGHPUT_BLEND) + AUTO_WEIGHT_THROUGHPUT_BLEND * throughput_ratio;
    (throughput_score * lag_penalty).max(AUTO_WEIGHT_MIN_RAW_SHARE)
}

fn path_health_allows_split(
    health: InterfaceHealth,
    best_last_seq: u64,
    average_server_mbps: f32,
) -> bool {
    let Some(last_seq) = health.last_seq else {
        return false;
    };
    if best_last_seq.saturating_sub(last_seq) > AUTO_SPLIT_DEGRADE_LAG_PACKETS {
        return false;
    }
    if average_server_mbps < AUTO_SPLIT_MIN_AVERAGE_SERVER_MBPS {
        return true;
    }

    let min_server_mbps =
        (average_server_mbps * AUTO_SPLIT_MIN_SERVER_MBPS_RATIO).max(AUTO_SPLIT_MIN_SERVER_MBPS);
    health.server_mbps >= min_server_mbps
}

fn qdisc_reset_should_run(
    backlog: u64,
    health: Option<InterfaceHealth>,
    config: QdiscResetConfig,
) -> bool {
    backlog >= config.backlog_bytes
        && health.is_some_and(|health| health.server_mbps <= config.max_server_mbps)
}

async fn tc_backlog(interface: &str) -> Option<u64> {
    let output = tokio::time::timeout(
        TC_BACKLOG_TIMEOUT,
        Command::new("tc")
            .args(["-s", "qdisc", "show", "dev", interface])
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    if !output.status.success() {
        return None;
    }

    parse_tc_backlog(&String::from_utf8_lossy(&output.stdout))
}

async fn replace_root_qdisc_fq_codel(interface: &str) -> Result<(), String> {
    let output = tokio::time::timeout(
        TC_QDISC_REPLACE_TIMEOUT,
        Command::new("tc")
            .args(["qdisc", "replace", "dev", interface, "root", TC_QDISC_KIND])
            .output(),
    )
    .await
    .map_err(|_| "timed out running tc qdisc replace".to_string())?
    .map_err(|err| format!("failed running tc qdisc replace: {err}"))?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let details = if stderr.trim().is_empty() {
        stdout.trim()
    } else {
        stderr.trim()
    };
    Err(format!(
        "tc qdisc replace exited with status={} error={details}",
        output.status
    ))
}

fn parse_tc_backlog(stdout: &str) -> Option<u64> {
    for line in stdout.lines() {
        if !line.contains("backlog") {
            continue;
        }

        let parts = line.split_whitespace().collect::<Vec<_>>();
        for (index, part) in parts.iter().enumerate() {
            if *part == "backlog" {
                return parse_backlog_bytes(parts.get(index + 1)?);
            }
        }
    }
    None
}

fn parse_backlog_bytes(value: &str) -> Option<u64> {
    value.strip_suffix('b')?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::{
        InterfaceHealth, QdiscResetConfig, blended_auto_weight, normalize_weights,
        parse_tc_backlog, path_health_allows_split, qdisc_reset_should_run,
    };
    use std::time::Duration;

    #[test]
    fn parses_tc_backlog_bytes() {
        let output = "qdisc fq_codel 0: root\n Sent 1 bytes 1 pkt\n backlog 1234b 8p requeues 0\n";

        assert_eq!(parse_tc_backlog(output), Some(1234));
    }

    #[test]
    fn auto_weighting_leans_healthy_paths_toward_even() {
        let weights = normalize_weights(vec![
            blended_auto_weight(9.0, 5.0, 1.0),
            blended_auto_weight(1.0, 5.0, 1.0),
        ]);

        assert!(weights[0] < 0.65, "{weights:?}");
        assert!(weights[1] > 0.35, "{weights:?}");
    }

    #[test]
    fn auto_weighting_still_penalizes_lagging_paths() {
        let weights = normalize_weights(vec![
            blended_auto_weight(5.0, 5.0, 1.0),
            blended_auto_weight(5.0, 5.0, 0.35),
        ]);

        assert!(weights[1] < 0.30, "{weights:?}");
    }

    #[test]
    fn split_health_rejects_near_zero_server_mbps_when_peers_are_receiving() {
        let health = InterfaceHealth {
            last_seq: Some(100),
            max_seq: Some(100),
            server_mbps: 0.02,
            last_unix_ms: 1,
        };

        assert!(!path_health_allows_split(health, 100, 5.0));
    }

    #[test]
    fn split_health_allows_low_server_mbps_when_whole_stream_is_low() {
        let health = InterfaceHealth {
            last_seq: Some(100),
            max_seq: Some(100),
            server_mbps: 0.02,
            last_unix_ms: 1,
        };

        assert!(path_health_allows_split(health, 100, 0.1));
    }

    #[test]
    fn qdisc_reset_requires_extreme_backlog_and_low_server_bandwidth() {
        let config = QdiscResetConfig {
            backlog_bytes: 1_000,
            max_server_mbps: 0.10,
            cooldown: Duration::from_secs(1),
        };
        let low_health = InterfaceHealth {
            last_seq: Some(100),
            max_seq: Some(100),
            server_mbps: 0.05,
            last_unix_ms: 1,
        };
        let ok_health = InterfaceHealth {
            server_mbps: 1.0,
            ..low_health
        };

        assert!(qdisc_reset_should_run(2_000, Some(low_health), config));
        assert!(!qdisc_reset_should_run(500, Some(low_health), config));
        assert!(!qdisc_reset_should_run(2_000, Some(ok_health), config));
        assert!(!qdisc_reset_should_run(2_000, None, config));
    }
}
