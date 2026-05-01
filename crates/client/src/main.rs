mod context;
mod health;
mod path_strategy;
mod preview;
mod remote;
mod runtime;
mod tui;

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::{ArgAction, Parser};
use cli::{InterfaceSpec, SecretArg, parse_interface_configs};
use context::ClientCtx;
use health::spawn_health_receivers;
use iroh::{EndpointId, RelayUrl};
use parking_lot::RwLock;
use path_strategy::{PathStrategy, StrategyMode, spawn_strategy_loop};
use preview::spawn_preview;
use protocol::{MAX_FRAGMENTS, MAX_SEQUENCE, PacketHeader, encode_packet};
use remote::{RemoteConfig, spawn_remote_server};
use runtime::wait_for_shutdown;
use tokio::{net::UdpSocket, sync::mpsc};
use transport::{build_server_addr, connect_path_with_secret};

const MAX_UDP_PACKET_SIZE: usize = 65_507;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long, default_value = "", hide_default_value = true)]
    secret: SecretArg,
    #[arg(long)]
    endpoint: EndpointId,
    #[arg(long = "addr")]
    addrs: Vec<SocketAddr>,
    #[arg(long = "relay")]
    relays: Vec<RelayUrl>,
    #[arg(long = "interfaces", required = true, num_args = 1..)]
    interfaces: Vec<InterfaceSpec>,
    #[arg(long)]
    tui: bool,
    #[arg(long)]
    split_threshold_bytes: Option<usize>,
    #[arg(long)]
    mtu: Option<usize>,
    #[arg(long, default_value_t = 0)]
    burst_max_delay_ms: u64,
    #[arg(long)]
    burst_max_bytes: Option<usize>,
    #[arg(long, default_value_t = 500)]
    tc_backlog_poll_ms: u64,
    #[arg(long, default_value_t = 65_536)]
    tc_backlog_degrade_bytes: u64,
    #[arg(long, default_value_t = 16_384)]
    tc_backlog_recover_bytes: u64,
    #[arg(long)]
    remote: bool,
    #[arg(long, default_value = "irohsion")]
    remote_name: String,
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    remote_preview: bool,
    #[arg(long, default_value_t = 500_000)]
    remote_preview_max_jpeg_bytes: usize,
    #[arg(long, default_value_t = 10)]
    remote_preview_decode_interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let interface_configs = parse_interface_configs(&cli.interfaces)?;

    let listen_udp = SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::LOCALHOST,
        cli.port.unwrap_or(0),
    ));
    let server_addr = build_server_addr(cli.endpoint, &cli.addrs, &cli.relays)?;
    let listen_socket = Arc::new(
        UdpSocket::bind(listen_udp)
            .await
            .with_context(|| format!("failed to bind local UDP ingest socket on {listen_udp}"))?,
    );
    let (ui_command_tx, mut ui_command_rx) = mpsc::unbounded_channel();
    let ui = cli.tui.then(|| {
        tui::ClientUi::spawn(tui::ClientUiState::new(
            listen_socket
                .local_addr()
                .expect("listen socket has local addr")
                .port(),
            cli.endpoint.to_string(),
            "-".to_string(),
            interface_configs
                .iter()
                .map(|config| config.binding.name.clone())
                .collect(),
            Some(ui_command_tx),
        ))
    });
    let ctx = ClientCtx::new(ui.as_ref().map(|ui| ui.state.clone()));
    let health_endpoint_ids = interface_configs
        .iter()
        .map(|config| config.endpoint_id.clone())
        .collect::<Vec<_>>();
    let listen_udp = listen_socket
        .local_addr()
        .context("failed to read local UDP ingest socket address")?;
    // Replies from the server are sent back to whichever local UDP peer most recently fed us data.
    let last_ingest_peer = Arc::new(RwLock::new(None::<SocketAddr>));

    // Each configured interface gets its own iroh connection/path to the server.
    let mut paths = Vec::with_capacity(interface_configs.len());
    for config in interface_configs {
        let cli::InterfaceConfig {
            binding,
            endpoint_id,
            secret_key,
        } = config;
        let path =
            connect_path_with_secret(binding, server_addr.clone(), secret_key, &cli.relays)
                .await?;
        ctx.record_connection_paths(path.interface_name.clone(), &endpoint_id, &path.connection);
        ctx.connected_path(
            &path.interface_name,
            &endpoint_id,
            SocketAddr::V4(path.bound_addr),
        );
        paths.push(path);
    }
    if paths.len() > MAX_FRAGMENTS {
        bail!("at most {MAX_FRAGMENTS} interfaces are supported by the packed packet header");
    }
    let health_endpoint_summary = health_endpoint_ids.join(", ");
    ctx.set_health_endpoint(health_endpoint_summary.clone());
    if cli.tc_backlog_poll_ms == 0 {
        bail!("--tc-backlog-poll-ms must be greater than zero");
    }
    if cli.burst_max_bytes == Some(0) {
        bail!("--burst-max-bytes must be greater than zero when set");
    }
    if cli.tc_backlog_recover_bytes > cli.tc_backlog_degrade_bytes {
        bail!("--tc-backlog-recover-bytes must be <= --tc-backlog-degrade-bytes");
    }
    let strategy = spawn_strategy_loop(
        paths
            .iter()
            .zip(health_endpoint_ids.iter())
            .map(|(path, endpoint_id)| (path.interface_name.clone(), endpoint_id.clone()))
            .collect(),
        Duration::from_millis(cli.tc_backlog_poll_ms),
        cli.tc_backlog_degrade_bytes,
        cli.tc_backlog_recover_bytes,
        ctx.clone(),
    );
    let health = spawn_health_receivers(&paths, ctx.clone(), strategy.clone());
    {
        let strategy = strategy.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            while let Some(command) = ui_command_rx.recv().await {
                match command {
                    tui::UiCommand::SetAuto => {
                        strategy.set_mode(path_strategy::StrategyMode::Auto, &ctx, "tui hotkey");
                    }
                    tui::UiCommand::SetRedundant => {
                        strategy.set_mode(
                            path_strategy::StrategyMode::Redundant,
                            &ctx,
                            "tui hotkey",
                        );
                    }
                    tui::UiCommand::SetSplit => {
                        strategy.set_mode(path_strategy::StrategyMode::Split, &ctx, "tui hotkey");
                    }
                    tui::UiCommand::SetRoundRobin => {
                        strategy.set_mode(
                            path_strategy::StrategyMode::RoundRobin,
                            &ctx,
                            "tui hotkey",
                        );
                    }
                }
            }
        });
    }
    let path_names = paths
        .iter()
        .map(|path| path.interface_name.clone())
        .collect::<Vec<_>>();
    let _health = health;
    let preview = (cli.remote && cli.remote_preview).then(|| {
        spawn_preview(
            cli.remote_preview_max_jpeg_bytes,
            cli.remote_preview_decode_interval_secs,
        )
    });
    if cli.remote {
        spawn_remote_server(
            RemoteConfig {
                name: cli.remote_name.clone(),
                endpoint: cli.endpoint,
                addrs: cli.addrs.clone(),
                relays: cli.relays.clone(),
            },
            strategy.clone(),
            preview.clone(),
            ctx.clone(),
        )
        .await?;
    }

    for path in &paths {
        let interface_name = path.interface_name.clone();
        let connection = path.connection.clone();
        let listen_socket = listen_socket.clone();
        let last_ingest_peer = last_ingest_peer.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            // Server-to-client replies arrive over iroh and are bridged back onto the local UDP socket.
            loop {
                match connection.read_datagram().await {
                    Ok(payload) => {
                        let Some(peer) = last_ingest_peer.read().as_ref().copied() else {
                            ctx.missing_return_peer(&interface_name, payload.len());
                            continue;
                        };

                        if let Err(err) = listen_socket.send_to(&payload, peer).await {
                            ctx.return_forward_error(&interface_name, peer, &err.to_string());
                        } else {
                            ctx.forwarded_return_packet(&interface_name, peer, payload.len());
                        }
                    }
                    Err(err) => {
                        ctx.record_send_error(
                            interface_name.clone(),
                            format!("return path closed: {err}"),
                        );
                        ctx.return_path_closed(&interface_name, &err.to_string());
                        break;
                    }
                }
            }
        });
    }

    ctx.client_ready(listen_udp, paths.len(), &health_endpoint_summary);

    let mut seq = 0_u64;
    let mut buf = vec![0_u8; MAX_UDP_PACKET_SIZE];
    let burst_delay = burst_delay(cli.burst_max_delay_ms);
    let mut pending = Vec::new();
    loop {
        pending.clear();
        let shutdown = wait_for_shutdown(ctx.ui_state());
        let (len, src) = tokio::select! {
            biased;
            res = listen_socket.recv_from(&mut buf) => {
                res.context("failed reading from local UDP ingest socket")?
            }
            _ = shutdown => {
                break;
            }
        };

        pending.push(PendingPacket::new(src, &buf[..len]));
        let mut pending_bytes = len;

        if let Some(delay) = burst_delay {
            loop {
                if matches!(cli.burst_max_bytes, Some(limit) if pending_bytes >= limit) {
                    break;
                }

                let Ok(Ok((len, src))) = tokio::time::timeout(delay, listen_socket.recv_from(&mut buf)).await else {
                    break;
                };
                pending.push(PendingPacket::new(src, &buf[..len]));
                pending_bytes = pending_bytes.saturating_add(len);
            }
        }

        for packet in &pending {
            // Remember the active local UDP peer so reverse traffic has somewhere to go.
            *last_ingest_peer.write() = Some(packet.src);
            ctx.record_ingest(packet.payload.len() as u64, packet.src.to_string());
            strategy.record_packet(packet.payload.len() as u64);
            if let Some(preview) = &preview {
                preview.submit_packet(&packet.payload);
            }

            ctx.ingested_packet(seq, packet.payload.len(), packet.src);
            send_packet(
                &packet.payload,
                seq,
                &cli,
                &paths,
                &path_names,
                &strategy,
                &ctx,
            );
            seq = next_sequence(seq);
        }
    }

    Ok(())
}

struct PendingPacket {
    src: SocketAddr,
    payload: Vec<u8>,
}

impl PendingPacket {
    fn new(src: SocketAddr, payload: &[u8]) -> Self {
        Self {
            src,
            payload: payload.to_vec(),
        }
    }
}

fn send_packet(
    payload: &[u8],
    seq: u64,
    cli: &Cli,
    paths: &[transport::PathConnection],
    path_names: &[String],
    strategy: &path_strategy::StrategyState,
    ctx: &ClientCtx,
) {
    let mode = strategy.mode();
    let effective = strategy.current();
    let (split_interface_names, rescue_interface_names) =
        if matches!(mode, StrategyMode::Auto) && matches!(effective, PathStrategy::Split) {
            strategy.auto_path_groups(path_names)
        } else {
            (path_names.to_vec(), Vec::new())
        };
    let split_paths = paths
        .iter()
        .filter(|path| {
            split_interface_names
                .iter()
                .any(|name| name == &path.interface_name)
        })
        .collect::<Vec<_>>();
    let rescue_paths = if rescue_interface_names.is_empty() {
        Vec::new()
    } else {
        paths.iter()
            .filter(|path| {
                rescue_interface_names
                    .iter()
                    .any(|name| name == &path.interface_name)
            })
            .collect::<Vec<_>>()
    };
    if should_split(
        payload.len(),
        cli.split_threshold_bytes,
        cli.mtu,
        split_paths.len(),
        mode,
        effective,
    ) {
        let split_names = split_paths
            .iter()
            .map(|path| path.interface_name.clone())
            .collect::<Vec<_>>();
        let fragments = u8::try_from(split_paths.len()).expect("path count fits in u8");
        let split_ranges = weighted_split_ranges(payload.len(), &strategy.split_weights(&split_names));
        for (fragment, path) in split_paths.iter().enumerate() {
            let (start, end) = split_ranges[fragment];
            let packet = Arc::new(encode_packet(
                PacketHeader {
                    sequence: seq,
                    fragment: u8::try_from(fragment).expect("fragment fits in u8"),
                    fragments,
                },
                &payload[start..end],
            ));
            send_on_path(
                path,
                packet,
                strategy,
                ctx,
                seq,
                (end - start) as u64,
            );
        }

        if !rescue_paths.is_empty() {
            let packet = Arc::new(encode_packet(
                PacketHeader {
                    sequence: seq,
                    fragment: 0,
                    fragments: 1,
                },
                payload,
            ));
            for path in rescue_paths {
                send_on_path(path, packet.clone(), strategy, ctx, seq, payload.len() as u64);
            }
        }
        return;
    }

    let packet = Arc::new(encode_packet(
        PacketHeader {
            sequence: seq,
            fragment: 0,
            fragments: 1,
        },
        payload,
    ));
    if matches!(effective, PathStrategy::RoundRobin) {
        let index = strategy.next_round_robin_index(paths.len());
        let path = &paths[index];
        send_on_path(path, packet, strategy, ctx, seq, payload.len() as u64);
        return;
    }

    // Duplicate each ingested packet over every active interface-bound iroh path.
    for path in paths {
        send_on_path(path, packet.clone(), strategy, ctx, seq, payload.len() as u64);
    }
}

fn send_on_path(
    path: &transport::PathConnection,
    packet: Arc<Bytes>,
    strategy: &path_strategy::StrategyState,
    ctx: &ClientCtx,
    seq: u64,
    payload_len: u64,
) {
    let packet_len = packet.len() as u64;
    match path.send(packet) {
        Ok(()) => {
            strategy.record_interface_send(&path.interface_name, packet_len);
            ctx.record_send(path.interface_name.clone(), payload_len);
        }
        Err(err) => {
            ctx.record_send_error(path.interface_name.clone(), err.to_string());
            ctx.send_failure(&path.interface_name, seq, &err.to_string());
            strategy.degrade_to_redundant(
                ctx,
                format!(
                    "send error interface={} sequence={} error={err}",
                    path.interface_name, seq
                ),
            );
        }
    }
}

fn burst_delay(delay_ms: u64) -> Option<Duration> {
    if delay_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(delay_ms))
    }
}

fn should_split(
    packet_len: usize,
    threshold: Option<usize>,
    mtu: Option<usize>,
    path_count: usize,
    mode: StrategyMode,
    strategy: PathStrategy,
) -> bool {
    if path_count <= 1 {
        return false;
    }

    match mode {
        StrategyMode::Split => return true,
        StrategyMode::Redundant | StrategyMode::RoundRobin => return false,
        StrategyMode::Auto => {}
    }

    if matches!(mtu, Some(mtu) if packet_len >= mtu) {
        return true;
    }

    matches!(strategy, PathStrategy::Split)
        && matches!(threshold, Some(threshold) if packet_len > threshold)
}

fn next_sequence(sequence: u64) -> u64 {
    if sequence == MAX_SEQUENCE {
        0
    } else {
        sequence + 1
    }
}

fn weighted_split_ranges(packet_len: usize, weights: &[f64]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::with_capacity(weights.len());
    let mut start = 0_usize;
    let mut accumulated = 0.0_f64;
    for (index, weight) in weights.iter().enumerate() {
        accumulated += weight;
        let end = if index == weights.len() - 1 {
            packet_len
        } else {
            ((packet_len as f64 * accumulated).round() as usize).clamp(start, packet_len)
        };
        ranges.push((start, end));
        start = end;
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::{burst_delay, should_split, weighted_split_ranges};
    use crate::path_strategy::{PathStrategy, StrategyMode};
    use std::time::Duration;

    #[test]
    fn weighted_ranges_cover_packet_once() {
        let ranges = weighted_split_ranges(1000, &[0.75, 0.25]);

        assert_eq!(ranges, vec![(0, 750), (750, 1000)]);
    }

    #[test]
    fn weighted_ranges_assign_remainder_to_last_fragment() {
        let ranges = weighted_split_ranges(1001, &[0.5, 0.5]);

        assert_eq!(ranges, vec![(0, 501), (501, 1001)]);
    }

    #[test]
    fn packets_above_threshold_split_when_strategy_is_split() {
        assert!(should_split(
            1200,
            Some(1000),
            None,
            2,
            StrategyMode::Auto,
            PathStrategy::Split,
        ));
    }

    #[test]
    fn packets_at_or_below_threshold_stay_redundant() {
        assert!(!should_split(
            1000,
            Some(1000),
            None,
            2,
            StrategyMode::Auto,
            PathStrategy::Split,
        ));
        assert!(!should_split(
            999,
            Some(1000),
            None,
            2,
            StrategyMode::Auto,
            PathStrategy::Split,
        ));
    }

    #[test]
    fn packets_do_not_split_without_multiple_paths() {
        assert!(!should_split(
            1200,
            Some(1000),
            None,
            1,
            StrategyMode::Split,
            PathStrategy::Split,
        ));
        assert!(!should_split(
            1200,
            None,
            Some(1000),
            1,
            StrategyMode::Auto,
            PathStrategy::Split,
        ));
    }

    #[test]
    fn packets_above_mtu_split_even_when_strategy_is_redundant() {
        assert!(should_split(
            1400,
            Some(10_000),
            Some(1200),
            2,
            StrategyMode::Auto,
            PathStrategy::Redundant,
        ));
    }

    #[test]
    fn packets_at_mtu_split_even_when_strategy_is_redundant() {
        assert!(should_split(
            1128,
            Some(10_000),
            Some(1128),
            2,
            StrategyMode::Auto,
            PathStrategy::Redundant,
        ));
    }

    #[test]
    fn explicit_split_mode_always_splits_with_multiple_paths() {
        assert!(should_split(
            1128,
            None,
            Some(9_999),
            2,
            StrategyMode::Split,
            PathStrategy::Split,
        ));
    }

    #[test]
    fn explicit_redundant_mode_ignores_mtu_and_threshold() {
        assert!(!should_split(
            1128,
            Some(100),
            Some(1128),
            2,
            StrategyMode::Redundant,
            PathStrategy::Redundant,
        ));
    }

    #[test]
    fn explicit_round_robin_mode_ignores_mtu_and_threshold() {
        assert!(!should_split(
            1128,
            Some(100),
            Some(1128),
            2,
            StrategyMode::RoundRobin,
            PathStrategy::RoundRobin,
        ));
    }

    #[test]
    fn zero_burst_delay_disables_batching() {
        assert_eq!(burst_delay(0), None);
    }

    #[test]
    fn burst_delay_uses_requested_milliseconds() {
        assert_eq!(burst_delay(4), Some(Duration::from_millis(4)));
    }
}
