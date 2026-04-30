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
use clap::{ArgAction, Parser};
use cli::{InterfaceSpec, SecretArg, parse_interface_configs};
use context::ClientCtx;
use health::spawn_health_receiver;
use iroh::{EndpointId, RelayUrl};
use parking_lot::RwLock;
use path_strategy::{PathStrategy, spawn_strategy_loop};
use preview::spawn_preview;
use protocol::{MAX_FRAGMENTS, MAX_SEQUENCE, PacketHeader, encode_bundle, encode_packet};
use remote::{RemoteConfig, spawn_remote_server};
use runtime::wait_for_shutdown;
use tokio::net::UdpSocket;
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
    mtu: Option<usize>,
    #[arg(long)]
    bundle_threshold: Option<usize>,
    #[arg(long)]
    split_threshold_bytes: Option<usize>,
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
        ))
    });
    let ctx = ClientCtx::new(ui.as_ref().map(|ui| ui.state.clone()));
    let health = spawn_health_receiver(&cli.secret, &cli.relays, ctx.clone()).await?;
    let health_endpoint_id = health.endpoint_id.clone();
    ctx.set_health_endpoint(health_endpoint_id.clone());
    let listen_udp = listen_socket
        .local_addr()
        .context("failed to read local UDP ingest socket address")?;
    let _health = health;
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
    if cli.tc_backlog_poll_ms == 0 {
        bail!("--tc-backlog-poll-ms must be greater than zero");
    }
    if matches!(cli.mtu, Some(0)) {
        bail!("--mtu must be greater than zero");
    }
    if matches!(cli.bundle_threshold, Some(0)) {
        bail!("--bundle-threshold must be greater than zero");
    }
    if cli.bundle_threshold.is_some() && cli.mtu.is_none() {
        bail!("--bundle-threshold requires --mtu");
    }
    if cli.tc_backlog_recover_bytes > cli.tc_backlog_degrade_bytes {
        bail!("--tc-backlog-recover-bytes must be <= --tc-backlog-degrade-bytes");
    }
    let strategy = spawn_strategy_loop(
        paths
            .iter()
            .map(|path| path.interface_name.clone())
            .collect(),
        Duration::from_millis(cli.tc_backlog_poll_ms),
        cli.tc_backlog_degrade_bytes,
        cli.tc_backlog_recover_bytes,
        ctx.clone(),
    );
    let path_names = paths
        .iter()
        .map(|path| path.interface_name.clone())
        .collect::<Vec<_>>();
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

    ctx.client_ready(listen_udp, paths.len(), &health_endpoint_id);

    let mut seq = 0_u64;
    let mut buf = vec![0_u8; MAX_UDP_PACKET_SIZE];
    let mut split_bundles = paths
        .iter()
        .map(|path| PendingBundle::new(path.interface_name.clone()))
        .collect::<Vec<_>>();
    loop {
        let shutdown = wait_for_shutdown(ctx.ui_state());
        let (len, src) = tokio::select! {
            biased;
            res = listen_socket.recv_from(&mut buf) => {
                res.context("failed reading from local UDP ingest socket")?
            }
            _ = shutdown => {
                flush_all_split_bundles(&paths, &strategy, &ctx, &mut split_bundles);
                break;
            }
        };

        // Remember the active local UDP peer so reverse traffic has somewhere to go.
        *last_ingest_peer.write() = Some(src);
        ctx.record_ingest(len as u64, src.to_string());
        strategy.record_packet(len as u64);
        if let Some(preview) = &preview {
            preview.submit_packet(&buf[..len]);
        }

        ctx.ingested_packet(seq, len, src);

        send_ingest_payload(
            &paths,
            &path_names,
            &strategy,
            &ctx,
            seq,
            &buf[..len],
            cli.mtu,
            cli.bundle_threshold,
            cli.split_threshold_bytes,
            &mut split_bundles,
        )?;
        seq = next_sequence(seq);
    }

    Ok(())
}

fn send_ingest_payload(
    paths: &[transport::PathConnection],
    path_names: &[String],
    strategy: &path_strategy::StrategyState,
    ctx: &ClientCtx,
    seq: u64,
    payload: &[u8],
    mtu: Option<usize>,
    bundle_threshold: Option<usize>,
    split_threshold_bytes: Option<usize>,
    split_bundles: &mut [PendingBundle],
) -> Result<()> {
    if should_split(
        payload.len(),
        split_threshold_bytes,
        paths.len(),
        strategy.current(),
    ) {
        let fragments = u8::try_from(paths.len()).expect("path count fits in u8");
        let split_ranges = weighted_split_ranges(payload.len(), &strategy.split_weights(path_names));
        for (fragment, path) in paths.iter().enumerate() {
            let (start, end) = split_ranges[fragment];
            let packet = encode_packet(
                PacketHeader {
                    sequence: seq,
                    fragment: u8::try_from(fragment).expect("fragment fits in u8"),
                    fragments,
                },
                &payload[start..end],
            );
            let bundle = &mut split_bundles[fragment];
            let can_bundle = mtu
                .zip(bundle_threshold)
                .is_some_and(|(mtu, threshold)| packet.len() <= threshold && bundle.can_fit(packet.len(), mtu));

            if can_bundle {
                bundle.push(packet.to_vec(), (end - start) as u64, seq);
                continue;
            }

            if !bundle.is_empty() {
                flush_split_bundle(path, strategy, ctx, bundle)?;
            }

            send_packet(
                path,
                strategy,
                ctx,
                seq,
                Arc::new(packet),
                (end - start) as u64,
            );
        }
    } else {
        flush_all_split_bundles(paths, strategy, ctx, split_bundles);
        let packet = Arc::new(encode_packet(
            PacketHeader {
                sequence: seq,
                fragment: 0,
                fragments: 1,
            },
            payload,
        ));

        for path in paths {
            send_packet(
                path,
                strategy,
                ctx,
                seq,
                packet.clone(),
                payload.len() as u64,
            );
        }
    }

    Ok(())
}

fn send_packet(
    path: &transport::PathConnection,
    strategy: &path_strategy::StrategyState,
    ctx: &ClientCtx,
    seq: u64,
    packet: Arc<bytes::Bytes>,
    payload_bytes: u64,
) {
    let packet_len = packet.len() as u64;
    match path.send(packet) {
        Ok(()) => {
            strategy.record_interface_send(&path.interface_name, packet_len);
            ctx.record_send(path.interface_name.clone(), payload_bytes);
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

fn flush_all_split_bundles(
    paths: &[transport::PathConnection],
    strategy: &path_strategy::StrategyState,
    ctx: &ClientCtx,
    split_bundles: &mut [PendingBundle],
) {
    for (path, bundle) in paths.iter().zip(split_bundles.iter_mut()) {
        if !bundle.is_empty() {
            let _ = flush_split_bundle(path, strategy, ctx, bundle);
        }
    }
}

fn flush_split_bundle(
    path: &transport::PathConnection,
    strategy: &path_strategy::StrategyState,
    ctx: &ClientCtx,
    bundle: &mut PendingBundle,
) -> Result<()> {
    let payload = encode_bundle(bundle.frames.iter().map(Vec::as_slice))?;
    let payload_bytes = bundle.payload_bytes;
    let seq = bundle.first_seq.unwrap_or(0);
    bundle.clear();
    send_packet(path, strategy, ctx, seq, Arc::new(payload), payload_bytes);
    Ok(())
}

struct PendingBundle {
    frames: Vec<Vec<u8>>,
    bytes: usize,
    payload_bytes: u64,
    first_seq: Option<u64>,
}

impl PendingBundle {
    fn new(_interface_name: String) -> Self {
        Self {
            frames: Vec::new(),
            bytes: 0,
            payload_bytes: 0,
            first_seq: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    fn can_fit(&self, next_len: usize, mtu: usize) -> bool {
        if self.frames.len() >= u8::MAX as usize {
            return false;
        }
        let frame_count = self.frames.len() + 1;
        let table_len = 5 + (frame_count * 2);
        table_len + self.bytes + next_len <= mtu
    }

    fn push(&mut self, frame: Vec<u8>, payload_bytes: u64, seq: u64) {
        if self.first_seq.is_none() {
            self.first_seq = Some(seq);
        }
        self.bytes += frame.len();
        self.payload_bytes += payload_bytes;
        self.frames.push(frame);
    }

    fn clear(&mut self) {
        self.frames.clear();
        self.bytes = 0;
        self.payload_bytes = 0;
        self.first_seq = None;
    }
}

fn should_split(
    packet_len: usize,
    threshold: Option<usize>,
    path_count: usize,
    strategy: PathStrategy,
) -> bool {
    strategy == PathStrategy::Split
        && matches!(threshold, Some(threshold) if packet_len > threshold && path_count > 1)
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
    use super::weighted_split_ranges;

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
}
