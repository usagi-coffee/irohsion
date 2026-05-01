use std::{
    collections::BTreeMap,
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
const AUTO_SPLIT_DEGRADE_LAG_PACKETS: u64 = 32;

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
    pub target_mbps: Option<f64>,
    pub split_percentage: Option<f64>,
    pub tx_packets: u64,
    pub tx_bytes: u64,
    pub tx_mbps: f64,
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

#[derive(Debug, Deserialize)]
pub struct ControlPatch {
    pub mode: Option<StrategyMode>,
    pub monitor_packets: Option<bool>,
    pub targets_mbps: Option<BTreeMap<String, Option<f64>>>,
    pub split_percentages: Option<BTreeMap<String, Option<f64>>>,
    pub preview_enabled: Option<bool>,
}

#[derive(Clone)]
pub struct StrategyState {
    mode: Arc<AtomicU8>,
    strategy: Arc<AtomicU8>,
    monitor_packets: Arc<AtomicBool>,
    packets: Arc<AtomicU64>,
    payload_bytes: Arc<AtomicU64>,
    round_robin_cursor: Arc<AtomicU64>,
    targets_mbps: Arc<RwLock<BTreeMap<String, Option<f64>>>>,
    endpoint_ids_by_interface: Arc<RwLock<BTreeMap<String, String>>>,
    split_percentages: Arc<RwLock<BTreeMap<String, Option<f64>>>>,
    health_by_interface: Arc<RwLock<BTreeMap<String, InterfaceHealth>>>,
    interface_traffic: Arc<RwLock<BTreeMap<String, InterfaceTraffic>>>,
}

#[derive(Debug, Clone, Copy)]
struct InterfaceHealth {
    last_seq: Option<u64>,
    last_unix_ms: u64,
}

impl StrategyState {
    fn sync_ui(&self, ctx: &ClientCtx) {
        ctx.record_strategy_state(
            &format!("{:?}", self.mode()).to_lowercase(),
            &format!("{:?}", self.current()).to_lowercase(),
        );
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
        let targets = self.targets_mbps.read();
        let percentages = self.split_percentages.read();
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
            interfaces: targets
                .iter()
                .map(|(name, target_mbps)| {
                    let counters = traffic.get(name).copied().unwrap_or_default();
                    InterfaceControlStatus {
                        name: name.clone(),
                        target_mbps: *target_mbps,
                        split_percentage: percentages.get(name).copied().flatten(),
                        tx_packets: counters.tx_packets,
                        tx_bytes: counters.tx_bytes,
                        tx_mbps: counters.tx_mbps,
                    }
                })
                .collect(),
        }
    }

    pub fn apply_patch(&self, patch: ControlPatch, ctx: &ClientCtx) {
        if let Some(mode) = patch.mode {
            self.mode.store(mode as u8, Ordering::Relaxed);
            ctx.record_strategy_change(&format!("{mode:?}").to_lowercase(), "remote control");
            self.sync_ui(ctx);
        }
        if let Some(monitor_packets) = patch.monitor_packets {
            self.monitor_packets
                .store(monitor_packets, Ordering::Relaxed);
        }
        if let Some(targets_mbps) = patch.targets_mbps {
            let mut targets = self.targets_mbps.write();
            for (interface, target_mbps) in targets_mbps {
                if targets.contains_key(&interface) {
                    targets.insert(interface, target_mbps);
                }
            }
        }
        if let Some(split_percentages) = patch.split_percentages {
            let mut percentages = self.split_percentages.write();
            for (interface, split_percentage) in split_percentages {
                if percentages.contains_key(&interface) {
                    percentages.insert(
                        interface,
                        split_percentage.map(|value| value.clamp(0.0, 100.0)),
                    );
                }
            }
        }
        ctx.record_split_percentages(&self.effective_split_percentages());
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
        let mut weights = interfaces
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
        if total <= f64::EPSILON {
            weights.fill(even_weight);
        } else {
            weights.iter_mut().for_each(|weight| *weight /= total);
        }
        weights
    }

    pub fn effective_split_percentages(&self) -> BTreeMap<String, f64> {
        let interfaces = self
            .targets_mbps
            .read()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let weights = self.split_weights(&interfaces);
        interfaces
            .into_iter()
            .zip(weights)
            .map(|(interface, weight)| (interface, weight * 100.0))
            .collect()
    }

    pub fn record_health_report(&self, report: &HealthReport) {
        let endpoint_ids = self.endpoint_ids_by_interface.read().clone();
        let endpoints = report
            .endpoints
            .iter()
            .map(|endpoint| (endpoint.endpoint_id.as_str(), endpoint.last_seq))
            .collect::<BTreeMap<_, _>>();
        let mut health = self.health_by_interface.write();
        for (interface, endpoint_id) in endpoint_ids {
            if let Some(last_seq) = endpoints.get(endpoint_id.as_str()) {
                health.insert(
                    interface,
                    InterfaceHealth {
                        last_seq: *last_seq,
                        last_unix_ms: report.unix_ms,
                    },
                );
            }
        }
    }

    pub fn auto_split_interfaces(&self, interfaces: &[String]) -> Vec<String> {
        let health = self.health_by_interface.read();
        let fresh = interfaces
            .iter()
            .filter_map(|interface| {
                let state = health.get(interface)?;
                let age_ms = unix_ms_now().saturating_sub(state.last_unix_ms);
                (age_ms <= AUTO_SPLIT_HEALTH_STALE_MS).then_some((interface.clone(), state.last_seq))
            })
            .collect::<Vec<_>>();
        let best_last_seq = fresh.iter().filter_map(|(_, last_seq)| *last_seq).max();
        let Some(best_last_seq) = best_last_seq else {
            return interfaces.to_vec();
        };

        let healthy = fresh
            .into_iter()
            .filter(|(_, last_seq)| {
                last_seq.is_some_and(|seq| {
                    best_last_seq.saturating_sub(seq) <= AUTO_SPLIT_DEGRADE_LAG_PACKETS
                })
            })
            .map(|(interface, _)| interface)
            .collect::<Vec<_>>();
        if healthy.len() >= 2 {
            healthy
        } else {
            interfaces.to_vec()
        }
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
}

pub fn spawn_strategy_loop(
    interfaces: Vec<(String, String)>,
    poll_interval: Duration,
    degrade_backlog_bytes: u64,
    recover_backlog_bytes: u64,
    ctx: ClientCtx,
) -> StrategyState {
    let targets_mbps = interfaces
        .iter()
        .map(|(interface, _)| (interface.clone(), None))
        .collect();
    let endpoint_ids_by_interface = interfaces
        .iter()
        .map(|(interface, endpoint_id)| (interface.clone(), endpoint_id.clone()))
        .collect();
    let split_percentages = interfaces
        .iter()
        .map(|(interface, _)| (interface.clone(), None))
        .collect();
    let interface_traffic = interfaces
        .iter()
        .map(|(interface, _)| (interface.clone(), InterfaceTraffic::default()))
        .collect();
    let state = StrategyState {
        mode: Arc::new(AtomicU8::new(StrategyMode::Auto as u8)),
        strategy: Arc::new(AtomicU8::new(PathStrategy::Split as u8)),
        monitor_packets: Arc::new(AtomicBool::new(true)),
        packets: Arc::new(AtomicU64::new(0)),
        payload_bytes: Arc::new(AtomicU64::new(0)),
        round_robin_cursor: Arc::new(AtomicU64::new(0)),
        targets_mbps: Arc::new(RwLock::new(targets_mbps)),
        endpoint_ids_by_interface: Arc::new(RwLock::new(endpoint_ids_by_interface)),
        split_percentages: Arc::new(RwLock::new(split_percentages)),
        health_by_interface: Arc::new(RwLock::new(BTreeMap::new())),
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
    interfaces: Vec<(String, String)>,
    poll_interval: Duration,
    degrade_backlog_bytes: u64,
    recover_backlog_bytes: u64,
    state: StrategyState,
    ctx: ClientCtx,
) {
    let mut interval = tokio::time::interval(poll_interval);
    loop {
        interval.tick().await;
        if state.mode() != StrategyMode::Auto {
            continue;
        }

        let mut worst = None::<(&str, u64)>;
        for (interface, _) in &interfaces {
            let Some(backlog) = tc_backlog(interface).await else {
                continue;
            };
            if worst.is_none_or(|(_, existing)| backlog > existing) {
                worst = Some((interface, backlog));
            }
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
                if backlog <= recover_backlog_bytes =>
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
            (PathStrategy::Redundant, None) => {
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

async fn tc_backlog(interface: &str) -> Option<u64> {
    let output = Command::new("tc")
        .args(["-s", "qdisc", "show", "dev", interface])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }

    parse_tc_backlog(&String::from_utf8_lossy(&output.stdout))
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
    use super::parse_tc_backlog;

    #[test]
    fn parses_tc_backlog_bytes() {
        let output = "qdisc fq_codel 0: root\n Sent 1 bytes 1 pkt\n backlog 1234b 8p requeues 0\n";

        assert_eq!(parse_tc_backlog(output), Some(1234));
    }
}
