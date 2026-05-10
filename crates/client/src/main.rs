mod context;
mod health;
mod path_strategy;
mod preview;
mod runtime;
mod tui;

use std::{
    collections::{BTreeMap, VecDeque},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::{ArgAction, Parser};
use cli::{InterfaceSpec, SecretArg, parse_interface_configs};
use client_remote::{
    RemoteChatHooks, RemoteConfig, RemoteControlHooks, RemotePreview, RemotePreviewHooks,
    RemoteReady, RemoteServer, spawn_remote_server,
};
use context::ClientCtx;
use health::spawn_health_receivers;
use iroh::{EndpointId, RelayUrl};
use obs_remote::{ObsConfig, ObsRemote};
use parking_lot::{Mutex, RwLock};
use path_strategy::{
    PathStrategy, QdiscResetConfig, StrategyInterface, StrategyMode, spawn_strategy_loop,
};
use preview::spawn_preview;
use protocol::{
    FEC_SEQUENCE, FecFrame, MAX_FEC_GROUP_PACKETS, MAX_FRAGMENTS, MAX_MEDIA_SEQUENCE, PacketHeader,
    RepairRequest, decode_repair_request, encode_fec_frame, encode_packet,
};
use runtime::wait_for_shutdown;
use tokio::{net::UdpSocket, sync::mpsc};
use transport::{
    PathConnection, build_server_addr, connect_path_with_secret, decode_health_report,
    resolve_interface_ipv4,
};

const MAX_UDP_PACKET_SIZE: usize = 65_507;
const MPEG_TS_PACKET_SIZE: usize = 188;
const RECONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const INTERFACE_WATCH_INTERVAL: Duration = Duration::from_secs(1);
const REPAIR_REQUEST_STREAM_LIMIT: usize = protocol::REPAIR_REQUEST_LEN;
const REPAIR_REQUEST_DEDUP_WINDOW: Duration = Duration::from_millis(10);

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
    status_file: Option<PathBuf>,
    #[arg(long, default_value_t = 1000)]
    status_file_interval_ms: u64,
    #[arg(long)]
    split_threshold_bytes: Option<usize>,
    #[arg(long)]
    mtu: Option<usize>,
    #[arg(long)]
    mpeg_ts_chunk_bytes: Option<usize>,
    #[arg(long, default_value_t = 500)]
    tc_backlog_poll_ms: u64,
    #[arg(long, default_value_t = 65_536)]
    tc_backlog_degrade_bytes: u64,
    #[arg(long, default_value_t = 16_384)]
    tc_backlog_recover_bytes: u64,
    #[arg(long, action = ArgAction::SetTrue)]
    tc_qdisc_reset: bool,
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    tc_qdisc_reset_backlog_bytes: u64,
    #[arg(long, default_value_t = 0.10)]
    tc_qdisc_reset_max_server_mbps: f32,
    #[arg(long, default_value_t = 5000)]
    tc_qdisc_reset_cooldown_ms: u64,
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
    #[arg(long, default_value = "127.0.0.1")]
    obs_websocket_host: String,
    #[arg(long, default_value_t = 4455)]
    obs_websocket_port: u16,
    #[arg(long)]
    obs_websocket_password: Option<String>,
    #[arg(long, default_value = "AdvOut")]
    obs_recording_bitrate_category: String,
    #[arg(long, default_value = "FFVBitrate")]
    obs_recording_bitrate_name: String,
    #[arg(long, default_value_t = 500)]
    repair_cache_ms: u64,
    #[arg(long, default_value_t = 4096)]
    repair_cache_packets: usize,
    #[arg(long, default_value_t = 0)]
    fec_group_packets: usize,
}

#[derive(Clone)]
struct CachedFragment {
    packet: Arc<Bytes>,
    payload_len: u64,
}

struct CachedPacket {
    fragments: Vec<CachedFragment>,
    stored_at: Instant,
}

struct RepairCache {
    ttl: Duration,
    max_packets: usize,
    inner: Mutex<RepairCacheInner>,
}

#[derive(Default)]
struct RepairCacheInner {
    packets: BTreeMap<u64, CachedPacket>,
    order: VecDeque<u64>,
    recent_requests: BTreeMap<(u64, u8), Instant>,
    request_order: VecDeque<(u64, u8)>,
}

struct FecSourcePacket {
    sequence: u64,
    payload: Vec<u8>,
}

struct FecEncoder {
    group_size: Option<usize>,
    packets: Vec<FecSourcePacket>,
}

impl RepairCache {
    fn new(ttl: Duration, max_packets: usize) -> Self {
        Self {
            ttl,
            max_packets,
            inner: Mutex::new(RepairCacheInner::default()),
        }
    }

    fn insert(&self, sequence: u64, fragments: Vec<CachedFragment>) {
        if fragments.is_empty() {
            return;
        }

        let now = Instant::now();
        let mut inner = self.inner.lock();
        Self::expire_locked(&mut inner, now, self.ttl);
        if !inner.packets.contains_key(&sequence) {
            inner.order.push_back(sequence);
        }
        inner.packets.insert(
            sequence,
            CachedPacket {
                fragments,
                stored_at: now,
            },
        );
        Self::enforce_limit_locked(&mut inner, self.max_packets);
    }

    fn fragments_for(&self, request: RepairRequest) -> Vec<CachedFragment> {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        Self::expire_locked(&mut inner, now, self.ttl);
        Self::expire_recent_requests_locked(&mut inner, now);
        if !inner.packets.contains_key(&request.sequence) {
            return Vec::new();
        }
        let request_key = (request.sequence, request.missing_mask);
        if inner
            .recent_requests
            .get(&request_key)
            .is_some_and(|last| now.duration_since(*last) < REPAIR_REQUEST_DEDUP_WINDOW)
        {
            return Vec::new();
        }
        if !inner.recent_requests.contains_key(&request_key) {
            inner.request_order.push_back(request_key);
        }
        inner.recent_requests.insert(request_key, now);
        Self::enforce_recent_request_limit_locked(&mut inner, self.max_packets);

        let packet = inner
            .packets
            .get(&request.sequence)
            .expect("packet exists after contains_key check");
        if packet.fragments.len() == 1 {
            return packet.fragments.clone();
        }

        packet
            .fragments
            .iter()
            .enumerate()
            .filter(|(index, _)| request.missing_mask & (1_u8 << index) != 0)
            .map(|(_, fragment)| fragment.clone())
            .collect()
    }

    fn expire_locked(inner: &mut RepairCacheInner, now: Instant, ttl: Duration) {
        while let Some(sequence) = inner.order.front().copied() {
            let expired = inner
                .packets
                .get(&sequence)
                .is_none_or(|packet| now.duration_since(packet.stored_at) >= ttl);
            if !expired {
                break;
            }

            inner.order.pop_front();
            inner.packets.remove(&sequence);
        }
    }

    fn enforce_limit_locked(inner: &mut RepairCacheInner, max_packets: usize) {
        while inner.packets.len() > max_packets {
            let Some(sequence) = inner.order.pop_front() else {
                break;
            };
            inner.packets.remove(&sequence);
        }
    }

    fn expire_recent_requests_locked(inner: &mut RepairCacheInner, now: Instant) {
        while let Some(request) = inner.request_order.front().copied() {
            let expired = inner
                .recent_requests
                .get(&request)
                .is_none_or(|last| now.duration_since(*last) >= REPAIR_REQUEST_DEDUP_WINDOW);
            if !expired {
                break;
            }

            inner.request_order.pop_front();
            inner.recent_requests.remove(&request);
        }
    }

    fn enforce_recent_request_limit_locked(inner: &mut RepairCacheInner, max_packets: usize) {
        let max_requests = max_packets.saturating_mul(4).max(1);
        while inner.recent_requests.len() > max_requests {
            let Some(request) = inner.request_order.pop_front() else {
                break;
            };
            inner.recent_requests.remove(&request);
        }
    }
}

impl FecEncoder {
    fn new(group_size: usize) -> Result<Self> {
        let group_size = match group_size {
            0 => None,
            1 => bail!("--fec-group-packets must be 0 or at least 2"),
            count if count > MAX_FEC_GROUP_PACKETS => {
                bail!("--fec-group-packets must be <= {MAX_FEC_GROUP_PACKETS}")
            }
            count => Some(count),
        };

        Ok(Self {
            group_size,
            packets: Vec::new(),
        })
    }

    fn record(
        &mut self,
        sequence: u64,
        payload: &[u8],
        paths: &[transport::PathConnection],
        strategy: &path_strategy::StrategyState,
        ctx: &ClientCtx,
    ) {
        let Some(group_size) = self.group_size else {
            return;
        };
        if payload.len() > u16::MAX as usize {
            return;
        }

        if self.packets.is_empty() {
            self.packets.reserve(group_size);
        }
        self.packets.push(FecSourcePacket {
            sequence,
            payload: payload.to_vec(),
        });

        if self.packets.len() == group_size {
            self.flush(paths, strategy, ctx);
        }
    }

    fn flush(
        &mut self,
        paths: &[transport::PathConnection],
        strategy: &path_strategy::StrategyState,
        ctx: &ClientCtx,
    ) {
        if self.packets.is_empty() {
            return;
        }

        let max_len = self
            .packets
            .iter()
            .map(|packet| packet.payload.len())
            .max()
            .unwrap_or(0);
        if max_len == 0 {
            self.packets.clear();
            return;
        }

        let mut parity = vec![0_u8; max_len];
        let mut payload_lengths = Vec::with_capacity(self.packets.len());
        for packet in &self.packets {
            payload_lengths.push(packet.payload.len());
            for (index, byte) in packet.payload.iter().enumerate() {
                parity[index] ^= *byte;
            }
        }

        let frame = FecFrame {
            base_sequence: self.packets[0].sequence,
            payload_lengths,
            parity: Bytes::from(parity),
        };
        let packet = Arc::new(encode_fec_frame(&frame));
        send_fec_packet(packet, paths, strategy, ctx);
        self.packets.clear();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let interface_configs = parse_interface_configs(&cli.interfaces)?;
    if cli.status_file_interval_ms == 0 {
        bail!("--status-file-interval-ms must be greater than zero");
    }
    if let Some(chunk_bytes) = cli.mpeg_ts_chunk_bytes {
        if chunk_bytes == 0 || chunk_bytes % MPEG_TS_PACKET_SIZE != 0 {
            bail!("--mpeg-ts-chunk-bytes must be a non-zero multiple of {MPEG_TS_PACKET_SIZE}");
        }
    }
    if cli.mtu == Some(0) {
        bail!("--mtu must be greater than zero when set");
    }
    if cli.repair_cache_ms == 0 {
        bail!("--repair-cache-ms must be greater than zero");
    }
    if cli.repair_cache_packets == 0 {
        bail!("--repair-cache-packets must be greater than zero");
    }

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
    let ui_state = (cli.tui || cli.status_file.is_some()).then(|| {
        tui::ClientUiState::new(
            listen_socket
                .local_addr()
                .expect("listen socket has local addr")
                .port(),
            cli.endpoint.to_string(),
            "-".to_string(),
            interface_configs
                .iter()
                .map(|config| config.label.clone())
                .collect(),
            Some(ui_command_tx),
        )
    });
    let _ui = cli
        .tui
        .then(|| tui::ClientUi::spawn(ui_state.clone().expect("TUI state is enabled")));
    let _status_file = cli.status_file.clone().map(|path| {
        tui::spawn_status_file_writer(
            ui_state.clone().expect("status file state is enabled"),
            path,
            Duration::from_millis(cli.status_file_interval_ms),
        )
    });
    let ctx = ClientCtx::new(ui_state);
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
            label: _,
        } = config;
        let path = match connect_path_with_secret(
            binding.clone(),
            server_addr.clone(),
            secret_key.clone(),
            &cli.relays,
        )
        .await
        {
            Ok(path) => {
                let connection = path
                    .connection()
                    .expect("newly connected path has a live connection");
                ctx.record_connection_paths(
                    path.display_name.clone(),
                    &endpoint_id,
                    &connection,
                    true,
                );
                ctx.connected_path(
                    &path.display_name,
                    &endpoint_id,
                    SocketAddr::V4(
                        path.current_bound_addr()
                            .expect("newly connected path has a bound address"),
                    ),
                );
                path
            }
            Err(err) => {
                ctx.record_send_error(
                    binding.display_name.clone(),
                    format!("initial connect failed, retrying: {err}"),
                );
                ctx.reconnect_failed(&binding.display_name, &err.to_string());
                PathConnection::pending(binding, server_addr.clone(), secret_key, &cli.relays)
            }
        };
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
    if cli.tc_backlog_recover_bytes > cli.tc_backlog_degrade_bytes {
        bail!("--tc-backlog-recover-bytes must be <= --tc-backlog-degrade-bytes");
    }
    if cli.tc_qdisc_reset {
        if cli.tc_qdisc_reset_backlog_bytes == 0 {
            bail!("--tc-qdisc-reset-backlog-bytes must be greater than zero");
        }
        if !(cli.tc_qdisc_reset_max_server_mbps.is_finite()
            && cli.tc_qdisc_reset_max_server_mbps >= 0.0)
        {
            bail!("--tc-qdisc-reset-max-server-mbps must be a finite non-negative number");
        }
        if cli.tc_qdisc_reset_cooldown_ms == 0 {
            bail!("--tc-qdisc-reset-cooldown-ms must be greater than zero");
        }
    }
    let qdisc_reset = cli.tc_qdisc_reset.then(|| QdiscResetConfig {
        backlog_bytes: cli.tc_qdisc_reset_backlog_bytes,
        max_server_mbps: cli.tc_qdisc_reset_max_server_mbps,
        cooldown: Duration::from_millis(cli.tc_qdisc_reset_cooldown_ms),
    });
    let strategy = spawn_strategy_loop(
        paths
            .iter()
            .zip(health_endpoint_ids.iter())
            .map(|(path, endpoint_id)| StrategyInterface {
                display_name: path.display_name.clone(),
                device_name: path.interface_name.clone(),
                endpoint_id: endpoint_id.clone(),
            })
            .collect(),
        Duration::from_millis(cli.tc_backlog_poll_ms),
        cli.tc_backlog_degrade_bytes,
        cli.tc_backlog_recover_bytes,
        qdisc_reset,
        ctx.clone(),
    );
    let repair_cache = Arc::new(RepairCache::new(
        Duration::from_millis(cli.repair_cache_ms),
        cli.repair_cache_packets,
    ));
    let mut fec = FecEncoder::new(cli.fec_group_packets)?;
    spawn_repair_control(&paths, repair_cache.clone(), strategy.clone(), ctx.clone());
    let health = spawn_health_receivers(&paths, ctx.clone(), strategy.clone());
    for path in &paths {
        if path.reconnect_requested() {
            strategy.record_interface_reconnecting(&path.display_name);
        }
    }
    let path_names = paths
        .iter()
        .map(|path| path.display_name.clone())
        .collect::<Vec<_>>();
    spawn_interface_watchers(&paths, strategy.clone(), ctx.clone());
    spawn_reconnect_loops(&paths, &health_endpoint_ids, strategy.clone(), ctx.clone());
    spawn_connection_liveness(&paths, &health_endpoint_ids, strategy.clone(), ctx.clone());
    spawn_split_display_updates(&paths, &path_names, strategy.clone(), ctx.clone());
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
                    tui::UiCommand::ToggleWeightedAuto => {
                        strategy.toggle_weighted_auto_split(&ctx);
                    }
                }
            }
        });
    }
    let _health = health;
    let preview = (cli.remote && cli.remote_preview).then(|| {
        spawn_preview(
            cli.remote_preview_max_jpeg_bytes,
            cli.remote_preview_decode_interval_secs,
        )
    });
    if cli.remote {
        let obs = Some(ObsRemote::new(ObsConfig {
            host: cli.obs_websocket_host.clone(),
            port: cli.obs_websocket_port,
            password: cli.obs_websocket_password.clone(),
            recording_bitrate_category: cli.obs_recording_bitrate_category.clone(),
            recording_bitrate_name: cli.obs_recording_bitrate_name.clone(),
        }));
        let control_strategy = strategy.clone();
        let control = Arc::new(RemoteControlHooks::new(
            move || {
                serde_json::to_value(control_strategy.status()).unwrap_or(serde_json::Value::Null)
            },
            |_| Ok(()),
        ));
        let chat_ctx = ctx.clone();
        let chat = Arc::new(RemoteChatHooks::new(move || {
            serde_json::to_value(chat_ctx.chat_messages())
                .unwrap_or(serde_json::Value::Array(Vec::new()))
        }));
        let remote_preview = preview.clone().map(|preview| {
            let enabled_preview = preview.clone();
            let set_enabled_preview = preview.clone();
            let len_preview = preview.clone();
            Arc::new(RemotePreviewHooks::new(
                move || enabled_preview.enabled(),
                move |enabled| set_enabled_preview.set_enabled(enabled),
                move || len_preview.latest_jpeg_len(),
                move || preview.latest_jpeg(),
            )) as Arc<dyn RemotePreview>
        });
        let ready_ctx = ctx.clone();
        spawn_remote_server(RemoteServer {
            config: RemoteConfig {
                name: cli.remote_name.clone(),
                endpoint: cli.endpoint,
                addrs: cli.addrs.clone(),
                relays: cli.relays.clone(),
            },
            control,
            preview: remote_preview,
            chat: Some(chat),
            obs,
            on_ready: Arc::new(move |ready: RemoteReady| {
                ready_ctx.record_remote_ready(
                    &ready.adapter,
                    &ready.name,
                    &ready.service_uuid,
                    &ready.status_uuid,
                    &ready.control_uuid,
                );
            }),
        })
        .await?;
    }

    for path in &paths {
        let path = path.clone();
        let interface_name = path.display_name.clone();
        let listen_socket = listen_socket.clone();
        let last_ingest_peer = last_ingest_peer.clone();
        let ctx = ctx.clone();
        let strategy = strategy.clone();
        tokio::spawn(async move {
            // Server-to-client replies arrive over iroh and are bridged back onto the local UDP socket.
            loop {
                let Some(connection) = path.connection() else {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                };
                let connection_id = connection.stable_id();
                loop {
                    match connection.read_datagram().await {
                        Ok(payload) => {
                            if let Ok(report) = decode_health_report(&payload) {
                                strategy.record_health_report(&report);
                                ctx.record_health_report(&report);
                                continue;
                            }

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
                            strategy.record_interface_failure(&interface_name);
                            if let Some(endpoint) = path.mark_failed(Some(connection_id)) {
                                tokio::spawn(async move {
                                    endpoint.close().await;
                                });
                            }
                            strategy.degrade_to_redundant(
                                &ctx,
                                format!(
                                    "return path closed interface={interface_name} error={err}"
                                ),
                            );
                            break;
                        }
                    }
                }
            }
        });
    }

    ctx.client_ready(listen_udp, paths.len(), &health_endpoint_summary);

    let mut seq = 0_u64;
    let mut buf = vec![0_u8; MAX_UDP_PACKET_SIZE];
    loop {
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

        let payload = &buf[..len];
        // Remember the active local UDP peer so reverse traffic has somewhere to go.
        *last_ingest_peer.write() = Some(src);
        ctx.record_ingest(payload.len() as u64, src.to_string());
        strategy.record_packet(payload.len() as u64);
        if let Some(preview) = &preview {
            preview.submit_packet(payload);
        }

        seq = send_payload(
            payload,
            seq,
            src,
            &cli,
            &paths,
            &path_names,
            &strategy,
            &ctx,
            Some(repair_cache.as_ref()),
            &mut fec,
        );
    }

    Ok(())
}

fn spawn_repair_control(
    paths: &[transport::PathConnection],
    repair_cache: Arc<RepairCache>,
    strategy: path_strategy::StrategyState,
    ctx: ClientCtx,
) {
    let all_paths = Arc::new(paths.to_vec());
    for path in paths.iter().cloned() {
        let all_paths = all_paths.clone();
        let repair_cache = repair_cache.clone();
        let strategy = strategy.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            loop {
                let Some(connection) = path.connection() else {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                };
                let connection_id = connection.stable_id();

                loop {
                    let mut stream = match connection.accept_uni().await {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    let Ok(payload) = stream.read_to_end(REPAIR_REQUEST_STREAM_LIMIT).await else {
                        continue;
                    };
                    let Ok(request) = decode_repair_request(&payload) else {
                        continue;
                    };

                    let fragments = repair_cache.fragments_for(request);
                    if fragments.is_empty() {
                        continue;
                    }

                    for fragment in fragments {
                        for repair_path in all_paths.iter().filter(|path| path.is_connected()) {
                            send_on_path(
                                repair_path,
                                fragment.packet.clone(),
                                &strategy,
                                &ctx,
                                request.sequence,
                                fragment.payload_len,
                            );
                        }
                    }

                    if path.connection_id() != Some(connection_id) {
                        break;
                    }
                }
            }
        });
    }
}

fn spawn_interface_watchers(
    paths: &[transport::PathConnection],
    strategy: path_strategy::StrategyState,
    ctx: ClientCtx,
) {
    for path in paths.iter().cloned() {
        let strategy = strategy.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(INTERFACE_WATCH_INTERVAL).await;

                match resolve_interface_ipv4(&path.interface_name) {
                    Ok(mut binding) => {
                        binding.display_name.clone_from(&path.display_name);
                        if let Some(endpoint) = path.mark_interface_changed(binding.clone()) {
                            strategy.record_interface_reconnecting(&path.display_name);
                            ctx.record_send_error(
                                path.display_name.clone(),
                                format!(
                                    "interface address changed, reconnecting: {}",
                                    binding.bind_addr
                                ),
                            );
                            tokio::spawn(async move {
                                endpoint.close().await;
                            });
                        }
                    }
                    Err(_) => {
                        if let Some(endpoint) = path.mark_failed(None) {
                            tokio::spawn(async move {
                                endpoint.close().await;
                            });
                        } else {
                            path.request_reconnect();
                        }
                        strategy.record_interface_dead(&path.display_name);
                    }
                }
            }
        });
    }
}

fn spawn_connection_liveness(
    paths: &[transport::PathConnection],
    endpoint_ids: &[String],
    strategy: path_strategy::StrategyState,
    ctx: ClientCtx,
) {
    for (path, endpoint_id) in paths.iter().cloned().zip(endpoint_ids.iter().cloned()) {
        let strategy = strategy.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                let Some(connection) = path.connection() else {
                    continue;
                };

                ctx.record_connection_paths(
                    path.display_name.clone(),
                    &endpoint_id,
                    &connection,
                    false,
                );
                let Some(reason) = connection.close_reason() else {
                    continue;
                };

                let connection_id = connection.stable_id();
                strategy.record_interface_reconnecting(&path.display_name);
                ctx.record_send_error(
                    path.display_name.clone(),
                    format!("connection closed: {reason}"),
                );
                if let Some(endpoint) = path.mark_failed(Some(connection_id)) {
                    tokio::spawn(async move {
                        endpoint.close().await;
                    });
                }
            }
        });
    }
}

fn spawn_split_display_updates(
    paths: &[transport::PathConnection],
    path_names: &[String],
    strategy: path_strategy::StrategyState,
    ctx: ClientCtx,
) {
    let paths = paths.to_vec();
    let path_names = path_names.to_vec();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            let mode = strategy.mode();
            let effective = strategy.current();
            let (split_interface_names, _) =
                if matches!(mode, StrategyMode::Auto) && matches!(effective, PathStrategy::Split) {
                    strategy.auto_path_groups(&path_names)
                } else {
                    (path_names.clone(), Vec::new())
                };
            let displayed_split_names = paths
                .iter()
                .filter(|path| path.is_connected())
                .filter(|path| {
                    !matches!(effective, PathStrategy::Split)
                        || split_interface_names
                            .iter()
                            .any(|name| name == &path.display_name)
                })
                .map(|path| path.display_name.clone())
                .collect::<Vec<_>>();
            ctx.record_split_percentages(
                &strategy.effective_split_percentages_for(&displayed_split_names),
            );
        }
    });
}

fn spawn_reconnect_loops(
    paths: &[transport::PathConnection],
    endpoint_ids: &[String],
    strategy: path_strategy::StrategyState,
    ctx: ClientCtx,
) {
    for (path, endpoint_id) in paths.iter().cloned().zip(endpoint_ids.iter().cloned()) {
        let strategy = strategy.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut retry_delay = Duration::from_millis(250);
            loop {
                if !path.reconnect_requested() || path.is_connected() {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }

                strategy.record_interface_reconnecting(&path.display_name);
                match tokio::time::timeout(RECONNECT_ATTEMPT_TIMEOUT, path.reconnect()).await {
                    Ok(Ok(connection)) => {
                        retry_delay = Duration::from_millis(250);
                        strategy.record_interface_success(&path.display_name);
                        ctx.record_connection_paths(
                            path.display_name.clone(),
                            &endpoint_id,
                            &connection,
                            true,
                        );
                        if let Some(bound_addr) = path.current_bound_addr() {
                            ctx.connected_path(
                                &path.display_name,
                                &endpoint_id,
                                SocketAddr::V4(bound_addr),
                            );
                        }
                    }
                    Ok(Err(err)) => {
                        strategy.record_interface_dead(&path.display_name);
                        let error = err.to_string();
                        ctx.record_send_error(
                            path.display_name.clone(),
                            format!("reconnect failed: {error}"),
                        );
                        ctx.reconnect_failed(&path.display_name, &error);
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
                    }
                    Err(_) => {
                        strategy.record_interface_dead(&path.display_name);
                        let error = format!(
                            "reconnect timed out after {}s",
                            RECONNECT_ATTEMPT_TIMEOUT.as_secs()
                        );
                        ctx.record_send_error(
                            path.display_name.clone(),
                            format!("reconnect failed: {error}"),
                        );
                        ctx.reconnect_failed(&path.display_name, &error);
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
                    }
                }
            }
        });
    }
}

fn send_payload(
    payload: &[u8],
    seq: u64,
    src: SocketAddr,
    cli: &Cli,
    paths: &[transport::PathConnection],
    path_names: &[String],
    strategy: &path_strategy::StrategyState,
    ctx: &ClientCtx,
    repair_cache: Option<&RepairCache>,
    fec: &mut FecEncoder,
) -> u64 {
    if let Some(next_seq) = send_mpeg_ts_chunks(
        payload,
        seq,
        src,
        cli,
        paths,
        path_names,
        strategy,
        ctx,
        repair_cache,
        fec,
    ) {
        return next_seq;
    }

    ctx.ingested_packet(seq, payload.len(), src);
    send_packet(
        payload,
        seq,
        cli,
        paths,
        path_names,
        strategy,
        ctx,
        repair_cache,
        fec,
    );
    next_sequence(seq)
}

fn send_mpeg_ts_chunks(
    payload: &[u8],
    seq: u64,
    src: SocketAddr,
    cli: &Cli,
    paths: &[transport::PathConnection],
    path_names: &[String],
    strategy: &path_strategy::StrategyState,
    ctx: &ClientCtx,
    repair_cache: Option<&RepairCache>,
    fec: &mut FecEncoder,
) -> Option<u64> {
    let chunk_bytes = cli.mpeg_ts_chunk_bytes?;
    if payload.len() <= chunk_bytes || !is_mpeg_ts_payload(payload) {
        return None;
    }

    let chunks = payload.chunks(chunk_bytes).collect::<Vec<_>>();
    let active_paths = paths
        .iter()
        .filter(|path| path.is_connected())
        .collect::<Vec<_>>();
    if active_paths.is_empty() {
        return Some(seq);
    }

    let mode = strategy.mode();
    let effective = strategy.current();
    let mut next_seq = seq;
    if matches!(effective, PathStrategy::RoundRobin) {
        for chunk in chunks {
            ctx.ingested_packet(next_seq, chunk.len(), src);
            let packet = single_fragment_packet(next_seq, chunk);
            cache_repair_fragments(
                repair_cache,
                next_seq,
                vec![CachedFragment {
                    packet: packet.clone(),
                    payload_len: chunk.len() as u64,
                }],
            );
            let index = strategy.next_round_robin_index(active_paths.len());
            send_on_path(
                active_paths[index],
                packet,
                strategy,
                ctx,
                next_seq,
                chunk.len() as u64,
            );
            fec.record(next_seq, chunk, paths, strategy, ctx);
            next_seq = next_sequence(next_seq);
        }
        return Some(next_seq);
    }

    if matches!(effective, PathStrategy::Redundant) || matches!(mode, StrategyMode::Redundant) {
        for chunk in chunks {
            ctx.ingested_packet(next_seq, chunk.len(), src);
            let packet = single_fragment_packet(next_seq, chunk);
            cache_repair_fragments(
                repair_cache,
                next_seq,
                vec![CachedFragment {
                    packet: packet.clone(),
                    payload_len: chunk.len() as u64,
                }],
            );
            for path in &active_paths {
                send_on_path(
                    path,
                    packet.clone(),
                    strategy,
                    ctx,
                    next_seq,
                    chunk.len() as u64,
                );
            }
            fec.record(next_seq, chunk, paths, strategy, ctx);
            next_seq = next_sequence(next_seq);
        }
        return Some(next_seq);
    }

    let (split_interface_names, _) =
        if matches!(mode, StrategyMode::Auto) && matches!(effective, PathStrategy::Split) {
            strategy.auto_path_groups(path_names)
        } else {
            (path_names.to_vec(), Vec::new())
        };
    let split_paths = active_paths
        .iter()
        .copied()
        .filter(|path| {
            split_interface_names
                .iter()
                .any(|name| name == &path.display_name)
        })
        .collect::<Vec<_>>();
    if split_paths.is_empty() {
        return Some(next_seq);
    }

    let split_names = split_paths
        .iter()
        .map(|path| path.display_name.clone())
        .collect::<Vec<_>>();
    ctx.record_split_percentages(&strategy.effective_split_percentages_for(&split_names));
    let chunk_ranges =
        weighted_split_ranges(chunks.len(), &strategy.active_split_weights(&split_names));
    for (path_index, path) in split_paths.iter().enumerate() {
        let (start, end) = chunk_ranges[path_index];
        for chunk in &chunks[start..end] {
            ctx.ingested_packet(next_seq, chunk.len(), src);
            let packet = single_fragment_packet(next_seq, chunk);
            cache_repair_fragments(
                repair_cache,
                next_seq,
                vec![CachedFragment {
                    packet: packet.clone(),
                    payload_len: chunk.len() as u64,
                }],
            );
            send_on_path(path, packet, strategy, ctx, next_seq, chunk.len() as u64);
            fec.record(next_seq, chunk, paths, strategy, ctx);
            next_seq = next_sequence(next_seq);
        }
    }
    Some(next_seq)
}

fn single_fragment_packet(seq: u64, payload: &[u8]) -> Arc<Bytes> {
    Arc::new(encode_packet(
        PacketHeader {
            sequence: seq,
            fragment: 0,
            fragments: 1,
        },
        payload,
    ))
}

fn send_packet(
    payload: &[u8],
    seq: u64,
    cli: &Cli,
    paths: &[transport::PathConnection],
    path_names: &[String],
    strategy: &path_strategy::StrategyState,
    ctx: &ClientCtx,
    repair_cache: Option<&RepairCache>,
    fec: &mut FecEncoder,
) {
    let active_paths = paths
        .iter()
        .filter(|path| path.is_connected())
        .collect::<Vec<_>>();
    if active_paths.is_empty() {
        return;
    }

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
        .filter(|path| path.is_connected())
        .filter(|path| {
            split_interface_names
                .iter()
                .any(|name| name == &path.display_name)
        })
        .collect::<Vec<_>>();
    let displayed_split_names = if matches!(effective, PathStrategy::Split) {
        split_paths
            .iter()
            .map(|path| path.display_name.clone())
            .collect::<Vec<_>>()
    } else {
        active_paths
            .iter()
            .map(|path| path.display_name.clone())
            .collect::<Vec<_>>()
    };
    ctx.record_split_percentages(&strategy.effective_split_percentages_for(&displayed_split_names));
    let rescue_paths = if rescue_interface_names.is_empty() {
        Vec::new()
    } else {
        active_paths
            .iter()
            .copied()
            .filter(|path| {
                rescue_interface_names
                    .iter()
                    .any(|name| name == &path.display_name)
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
            .map(|path| path.display_name.clone())
            .collect::<Vec<_>>();
        let Some(split_ranges) = packet_split_ranges(
            payload.len(),
            &strategy.active_split_weights(&split_names),
            cli.mtu,
        ) else {
            ctx.record_send_error(
                "split".to_string(),
                format!(
                    "packet length {} requires more than {MAX_FRAGMENTS} fragments for mtu {:?}",
                    payload.len(),
                    cli.mtu
                ),
            );
            return;
        };
        let fragments = u8::try_from(split_ranges.len()).expect("fragment count fits in u8");
        let mut sends = Vec::with_capacity(split_ranges.len());
        let mut cached_fragments = Vec::with_capacity(split_ranges.len());
        for (fragment, (path_index, start, end)) in split_ranges.into_iter().enumerate() {
            let path = split_paths[path_index];
            let packet = Arc::new(encode_packet(
                PacketHeader {
                    sequence: seq,
                    fragment: u8::try_from(fragment).expect("fragment fits in u8"),
                    fragments,
                },
                &payload[start..end],
            ));
            let payload_len = (end - start) as u64;
            cached_fragments.push(CachedFragment {
                packet: packet.clone(),
                payload_len,
            });
            sends.push((path, packet, payload_len));
        }
        cache_repair_fragments(repair_cache, seq, cached_fragments);
        for (path, packet, payload_len) in sends {
            send_on_path(path, packet, strategy, ctx, seq, payload_len);
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
            cache_repair_fragments(
                repair_cache,
                seq,
                vec![CachedFragment {
                    packet: packet.clone(),
                    payload_len: payload.len() as u64,
                }],
            );
            for path in rescue_paths {
                send_on_path(
                    path,
                    packet.clone(),
                    strategy,
                    ctx,
                    seq,
                    payload.len() as u64,
                );
            }
        }
        fec.record(seq, payload, paths, strategy, ctx);
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
    cache_repair_fragments(
        repair_cache,
        seq,
        vec![CachedFragment {
            packet: packet.clone(),
            payload_len: payload.len() as u64,
        }],
    );
    if matches!(effective, PathStrategy::RoundRobin) {
        let index = strategy.next_round_robin_index(active_paths.len());
        let path = active_paths[index];
        send_on_path(path, packet, strategy, ctx, seq, payload.len() as u64);
        fec.record(seq, payload, paths, strategy, ctx);
        return;
    }

    // Duplicate each ingested packet over every active interface-bound iroh path.
    for path in active_paths {
        send_on_path(
            path,
            packet.clone(),
            strategy,
            ctx,
            seq,
            payload.len() as u64,
        );
    }
    fec.record(seq, payload, paths, strategy, ctx);
}

fn send_fec_packet(
    packet: Arc<Bytes>,
    paths: &[transport::PathConnection],
    strategy: &path_strategy::StrategyState,
    ctx: &ClientCtx,
) {
    let packet_len = packet.len() as u64;
    for path in paths.iter().filter(|path| path.is_connected()) {
        let connection_id = path.connection_id();
        match path.send(packet.clone()) {
            Ok(()) => {
                strategy.record_interface_success(&path.display_name);
                strategy.record_interface_send(&path.display_name, packet_len);
                ctx.record_send(path.display_name.clone(), packet_len);
            }
            Err(err) => {
                strategy.record_interface_failure(&path.display_name);
                if let Some(endpoint) = path.mark_failed(connection_id) {
                    tokio::spawn(async move {
                        endpoint.close().await;
                    });
                }
                ctx.record_send_error(path.display_name.clone(), format!("fec send failed: {err}"));
                ctx.send_failure(&path.display_name, FEC_SEQUENCE, &err.to_string());
                strategy.degrade_to_redundant(
                    ctx,
                    format!("fec send error interface={} error={err}", path.display_name),
                );
            }
        }
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
    let connection_id = path.connection_id();
    match path.send(packet) {
        Ok(()) => {
            strategy.record_interface_success(&path.display_name);
            strategy.record_interface_send(&path.display_name, packet_len);
            ctx.record_send(path.display_name.clone(), payload_len);
        }
        Err(err) => {
            strategy.record_interface_failure(&path.display_name);
            if let Some(endpoint) = path.mark_failed(connection_id) {
                tokio::spawn(async move {
                    endpoint.close().await;
                });
            }
            ctx.record_send_error(path.display_name.clone(), err.to_string());
            ctx.send_failure(&path.display_name, seq, &err.to_string());
            strategy.degrade_to_redundant(
                ctx,
                format!(
                    "send error interface={} sequence={} error={err}",
                    path.display_name, seq
                ),
            );
        }
    }
}

fn cache_repair_fragments(
    repair_cache: Option<&RepairCache>,
    sequence: u64,
    fragments: Vec<CachedFragment>,
) {
    if let Some(repair_cache) = repair_cache {
        repair_cache.insert(sequence, fragments);
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
    if matches!(mtu, Some(mtu) if packet_len >= mtu) {
        return true;
    }

    if path_count <= 1 {
        return false;
    }

    match mode {
        StrategyMode::Split => return true,
        StrategyMode::Redundant | StrategyMode::RoundRobin => return false,
        StrategyMode::Auto => {}
    }

    matches!(strategy, PathStrategy::Split)
        && matches!(threshold, Some(threshold) if packet_len > threshold)
}

fn next_sequence(sequence: u64) -> u64 {
    if sequence == MAX_MEDIA_SEQUENCE {
        0
    } else {
        sequence + 1
    }
}

fn is_mpeg_ts_payload(payload: &[u8]) -> bool {
    !payload.is_empty()
        && payload.len() % MPEG_TS_PACKET_SIZE == 0
        && payload
            .chunks_exact(MPEG_TS_PACKET_SIZE)
            .all(|packet| packet.first() == Some(&0x47))
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

fn packet_split_ranges(
    packet_len: usize,
    weights: &[f64],
    mtu: Option<usize>,
) -> Option<Vec<(usize, usize, usize)>> {
    if let Some(max_payload) = mtu.filter(|mtu| packet_len >= *mtu) {
        return mtu_chunked_split_ranges(packet_len, weights, max_payload);
    }

    let weighted_ranges = weighted_split_ranges(packet_len, weights);
    let mut ranges = Vec::new();
    for (path_index, (start, end)) in weighted_ranges.into_iter().enumerate() {
        if start == end {
            continue;
        }
        ranges.push((path_index, start, end));
    }
    (ranges.len() <= MAX_FRAGMENTS).then_some(ranges)
}

fn mtu_chunked_split_ranges(
    packet_len: usize,
    weights: &[f64],
    max_payload: usize,
) -> Option<Vec<(usize, usize, usize)>> {
    let chunks = (0..packet_len)
        .step_by(max_payload)
        .map(|start| (start, (start + max_payload).min(packet_len)))
        .collect::<Vec<_>>();
    if chunks.len() > MAX_FRAGMENTS {
        return None;
    }
    if chunks.is_empty() || weights.is_empty() {
        return Some(Vec::new());
    }

    let chunk_ranges = weighted_split_ranges(chunks.len(), weights);
    let mut ranges = Vec::with_capacity(chunks.len());
    for (path_index, (chunk_start, chunk_end)) in chunk_ranges.into_iter().enumerate() {
        for chunk in &chunks[chunk_start..chunk_end] {
            ranges.push((path_index, chunk.0, chunk.1));
        }
    }
    Some(ranges)
}

#[cfg(test)]
mod tests {
    use super::{
        CachedFragment, RepairCache, is_mpeg_ts_payload, packet_split_ranges, should_split,
        weighted_split_ranges,
    };
    use crate::path_strategy::{PathStrategy, StrategyMode};
    use bytes::Bytes;
    use protocol::RepairRequest;
    use std::{sync::Arc, time::Duration};

    #[test]
    fn repair_cache_returns_requested_split_fragments() {
        let cache = RepairCache::new(Duration::from_secs(1), 8);
        cache.insert(
            7,
            vec![
                CachedFragment {
                    packet: Arc::new(Bytes::from_static(b"first")),
                    payload_len: 5,
                },
                CachedFragment {
                    packet: Arc::new(Bytes::from_static(b"second")),
                    payload_len: 6,
                },
            ],
        );

        let fragments = cache.fragments_for(RepairRequest {
            sequence: 7,
            missing_mask: 0b0000_0010,
        });

        assert_eq!(fragments.len(), 1);
        assert_eq!(&fragments[0].packet[..], b"second");
        assert_eq!(fragments[0].payload_len, 6);
    }

    #[test]
    fn repair_cache_uses_full_packet_for_any_fragment_request() {
        let cache = RepairCache::new(Duration::from_secs(1), 8);
        cache.insert(
            9,
            vec![CachedFragment {
                packet: Arc::new(Bytes::from_static(b"full")),
                payload_len: 4,
            }],
        );

        let fragments = cache.fragments_for(RepairRequest {
            sequence: 9,
            missing_mask: 0b0000_0100,
        });

        assert_eq!(fragments.len(), 1);
        assert_eq!(&fragments[0].packet[..], b"full");
    }

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
    fn mtu_capped_ranges_split_oversized_weighted_fragments() {
        let ranges = packet_split_ranges(2256, &[0.9, 0.1], Some(1128)).unwrap();

        assert_eq!(ranges, vec![(0, 0, 1128), (0, 1128, 2256)]);
        assert!(ranges.iter().all(|(_, start, end)| end - start <= 1128));
    }

    #[test]
    fn mtu_capped_ranges_distribute_even_chunks_across_even_weights() {
        let ranges = packet_split_ranges(2256, &[0.5, 0.5], Some(1128)).unwrap();

        assert_eq!(ranges, vec![(0, 0, 1128), (1, 1128, 2256)]);
    }

    #[test]
    fn mtu_capped_ranges_can_split_over_one_path() {
        let ranges = packet_split_ranges(2256, &[1.0], Some(1128)).unwrap();

        assert_eq!(ranges, vec![(0, 0, 1128), (0, 1128, 2256)]);
    }

    #[test]
    fn detects_mpeg_ts_payloads() {
        let mut payload = vec![0_u8; 376];
        payload[0] = 0x47;
        payload[188] = 0x47;

        assert!(is_mpeg_ts_payload(&payload));
    }

    #[test]
    fn rejects_mpeg_ts_payloads_without_sync_bytes() {
        let payload = vec![0_u8; 376];

        assert!(!is_mpeg_ts_payload(&payload));
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
        assert!(should_split(
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
    fn explicit_redundant_mode_still_honors_mtu() {
        assert!(should_split(
            1128,
            Some(100),
            Some(1128),
            2,
            StrategyMode::Redundant,
            PathStrategy::Redundant,
        ));
    }

    #[test]
    fn explicit_round_robin_mode_still_honors_mtu() {
        assert!(should_split(
            1128,
            Some(100),
            Some(1128),
            2,
            StrategyMode::RoundRobin,
            PathStrategy::RoundRobin,
        ));
    }
}
