mod context;
mod health;
mod runtime;
mod tui;

use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::{ArgAction, Parser};
use cli::{SecretArg, local_udp_dest, relay_mode};
use context::ServerCtx;
use health::{
    ChatSources, HealthConnections, HealthStats, HealthTargets, health_loop,
    maintain_health_connection, record_health_sample,
};
use iroh::{Endpoint, RelayUrl, endpoint::presets};
use iroh_tickets::endpoint::EndpointTicket;
use kick_remote::spawn_kick_chat;
use parking_lot::RwLock;
use protocol::{
    DecodedPacket, FEC_SEQUENCE, FecFrame, MAX_MEDIA_SEQUENCE, PacketHeader,
    REPAIR_ALL_FRAGMENTS_MASK, RepairRequest, decode_fec_frame, decode_packet,
    encode_repair_request,
};
use runtime::wait_for_shutdown;
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle, time::timeout};
use transport::ALPN;
use twitch_remote::spawn_twitch_chat;

const MAX_UDP_PACKET_SIZE: usize = 65_507;
const RESTART_BACKWARD_GAP: u64 = 4_096;
const RESTART_CONFIRM_PACKETS: usize = protocol::MAX_FRAGMENTS + 1;
const SKIPPED_SEQUENCE_TRACK_LIMIT: usize = 4_096;
const REPAIR_STREAM_TIMEOUT: Duration = Duration::from_millis(25);
const MAX_REPAIR_REQUESTS_PER_TICK: usize = 64;
const FEC_FRAME_TRACK_LIMIT: usize = 256;
const FEC_PAYLOAD_TRACK_LIMIT: usize = 4_096;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    port: u16,
    #[arg(long, default_value_t = 250)]
    health_interval_ms: u64,
    #[arg(long = "relay")]
    relays: Vec<RelayUrl>,
    #[arg(long, action = ArgAction::SetTrue)]
    ticket: bool,
    #[arg(long, default_value = "", hide_default_value = true)]
    secret: SecretArg,
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    tui: bool,
    #[arg(long, default_value_t = 30)]
    flow_idle_reset_secs: u64,
    #[arg(long, default_value_t = 80)]
    max_reorder_delay_ms: u64,
    #[arg(long, action = ArgAction::SetTrue)]
    repair: bool,
    #[arg(long, default_value_t = 12)]
    repair_request_interval_ms: u64,
    #[arg(long)]
    twitch_channel: Option<String>,
    #[arg(long)]
    kick_channel: Option<String>,
    #[arg(long, default_value_t = 50)]
    chat_history: usize,
}

#[derive(Debug)]
struct ReceivedPacket {
    remote: String,
    connection_id: usize,
    header: PacketHeader,
    payload: Vec<u8>,
}

#[derive(Debug)]
enum ReceivedFrame {
    Packet(ReceivedPacket),
    Fec(ReceivedFecFrame),
}

#[derive(Debug)]
struct ReceivedFecFrame {
    remote: String,
    frame: FecFrame,
}

#[derive(Debug)]
struct FragmentAssembly {
    fragments: Vec<Option<Vec<u8>>>,
    received: usize,
    first_seen: tokio::time::Instant,
    last_repair_request: Option<tokio::time::Instant>,
}

impl FragmentAssembly {
    fn new(fragments: u8, first_seen: tokio::time::Instant) -> Self {
        Self {
            fragments: vec![None; fragments as usize],
            received: 0,
            first_seen,
            last_repair_request: None,
        }
    }

    fn fragments(&self) -> u8 {
        u8::try_from(self.fragments.len()).expect("fragment count fits in u8")
    }

    fn insert(&mut self, fragment: u8, payload: Vec<u8>) -> bool {
        let slot = &mut self.fragments[fragment as usize];
        if slot.is_some() {
            return false;
        }

        *slot = Some(payload);
        self.received += 1;
        true
    }

    fn is_complete(&self) -> bool {
        self.received == self.fragments.len()
    }

    fn missing_mask(&self) -> u8 {
        self.fragments
            .iter()
            .enumerate()
            .filter(|(_, fragment)| fragment.is_none())
            .fold(0_u8, |mask, (index, _)| mask | (1_u8 << index))
    }

    fn into_payload(self) -> Vec<u8> {
        let total_len = self
            .fragments
            .iter()
            .map(|fragment| fragment.as_ref().map_or(0, Vec::len))
            .sum();
        let mut payload = Vec::with_capacity(total_len);
        for fragment in self.fragments {
            payload.extend(fragment.expect("complete assembly has every fragment"));
        }
        payload
    }
}

#[derive(Debug)]
struct BufferedPacket {
    payload: Vec<u8>,
    received_at: tokio::time::Instant,
}

#[derive(Default)]
struct FecState {
    frames: BTreeMap<u64, FecFrame>,
    frame_order: VecDeque<u64>,
    payloads: BTreeMap<u64, Vec<u8>>,
    payload_order: VecDeque<u64>,
}

#[derive(Clone)]
struct RegisteredConnection {
    connection: iroh::endpoint::Connection,
}

#[derive(Default)]
struct RestartDetector {
    count: usize,
    highest_seq: u64,
}

#[derive(Clone, Copy, Debug)]
enum SkipReason {
    NeverReceived,
    FragmentIncomplete,
}

#[derive(Default)]
struct RecentSkippedSequences {
    order: VecDeque<u64>,
    lookup: HashSet<u64>,
}

#[derive(Clone)]
struct RepairControl {
    connections: ConnectionRegistry,
    reply_routes: ReplyRoutes,
}

struct RepairRuntime {
    control: RepairControl,
    request_interval: Duration,
}

type ConnectionRegistry = Arc<RwLock<BTreeMap<String, BTreeMap<usize, RegisteredConnection>>>>;
type ReplyRoutes = Arc<RwLock<Vec<String>>>;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.flow_idle_reset_secs == 0 {
        bail!("--flow-idle-reset-secs must be greater than zero");
    }
    if cli.max_reorder_delay_ms == 0 {
        bail!("--max-reorder-delay-ms must be greater than zero");
    }
    if cli.repair && cli.repair_request_interval_ms == 0 {
        bail!("--repair-request-interval-ms must be greater than zero");
    }
    if cli.repair && cli.repair_request_interval_ms >= cli.max_reorder_delay_ms {
        bail!("--repair-request-interval-ms must be less than --max-reorder-delay-ms");
    }

    let secret_key = cli.secret.resolve();
    let udp_dest = local_udp_dest(cli.port);
    let relays = cli.relays.clone();
    let ui = cli
        .tui
        .then(|| tui::ServerUi::spawn(tui::ServerUiState::new(udp_dest.to_string())));
    let ctx = ServerCtx::new(ui.as_ref().map(|ui| ui.state.clone()));

    // The server owns a single public iroh endpoint and accepts every client path on it.
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(relay_mode(relays.clone()))
        .bind()
        .await
        .context("failed to bind server iroh endpoint")?;

    endpoint.online().await;
    let server_addrs = tui::server_addrs(&endpoint);
    let server_ticket = cli
        .ticket
        .then(|| EndpointTicket::new(endpoint.addr()).to_string());
    ctx.set_endpoint(endpoint.id().to_string());
    ctx.set_health_endpoint(Some("auto".to_string()));
    ctx.set_ticket(server_ticket);
    ctx.set_server_addrs(server_addrs);

    let out_socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
            .await
            .context("failed to bind local UDP output socket")?,
    );
    let (tx, rx) = mpsc::channel::<ReceivedFrame>(1024);
    let connections: ConnectionRegistry = Arc::new(RwLock::new(BTreeMap::new()));
    let reply_routes: ReplyRoutes = Arc::new(RwLock::new(Vec::new()));
    let health_connections: HealthConnections = Arc::new(RwLock::new(BTreeMap::new()));
    let health_targets: HealthTargets = Arc::new(RwLock::new(std::collections::HashSet::new()));
    let health_stats: HealthStats = Arc::new(RwLock::new(BTreeMap::new()));
    let twitch_chat = cli
        .twitch_channel
        .clone()
        .map(|channel| spawn_twitch_chat(channel, cli.chat_history));
    let kick_chat = cli
        .kick_channel
        .clone()
        .map(|channel| spawn_kick_chat(channel, cli.chat_history));
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    {
        // Client-to-server packets are deduped/reordered centrally before hitting the UDP target.
        let out_socket = out_socket.clone();
        let reply_routes = reply_routes.clone();
        let repair = cli.repair.then(|| RepairRuntime {
            control: RepairControl {
                connections: connections.clone(),
                reply_routes: reply_routes.clone(),
            },
            request_interval: Duration::from_millis(cli.repair_request_interval_ms),
        });
        let ctx = ctx.clone();
        let flow_idle_reset = Duration::from_secs(cli.flow_idle_reset_secs);
        let max_reorder_delay = Duration::from_millis(cli.max_reorder_delay_ms);
        tasks.push(tokio::spawn(async move {
            let _ = reorder_loop(
                rx,
                out_socket,
                udp_dest,
                reply_routes,
                repair,
                ctx,
                flow_idle_reset,
                max_reorder_delay,
            )
            .await;
        }));
    }

    {
        // Responses from the UDP target are sent back over whichever client paths are active.
        let out_socket = out_socket.clone();
        let connections = connections.clone();
        let reply_routes = reply_routes.clone();
        let ctx = ctx.clone();
        tasks.push(tokio::spawn(async move {
            let _ = response_loop(out_socket, udp_dest, connections, reply_routes, ctx).await;
        }));
    }

    {
        let health_connections = health_connections.clone();
        let health_stats = health_stats.clone();
        let chat_sources = ChatSources {
            twitch: twitch_chat.clone(),
            kick: kick_chat.clone(),
        };
        let interval = Duration::from_millis(cli.health_interval_ms);
        tasks.push(tokio::spawn(async move {
            let _ = health_loop(health_connections, health_stats, chat_sources, interval).await;
        }));
    }

    loop {
        let shutdown_signal = wait_for_shutdown(ctx.ui_state());
        let incoming = tokio::select! {
            incoming = endpoint.accept() => incoming,
            _ = shutdown_signal => break,
        };
        let Some(incoming) = incoming else {
            break;
        };

        let tx = tx.clone();
        let connections = connections.clone();
        let health_targets = health_targets.clone();
        let health_connections = health_connections.clone();
        let health_stats = health_stats.clone();
        let ctx = ctx.clone();
        let endpoint = endpoint.clone();
        tasks.push(tokio::spawn(async move {
            // Each accepted iroh connection represents one client path; all paths feed the same
            // reorder loop, and we remember the live connection so UDP responses can travel back.
            let accepting = match incoming.accept() {
                Ok(accepting) => accepting,
                Err(_) => return,
            };

            let connection = match accepting.await {
                Ok(connection) => connection,
                Err(_) => return,
            };

            let remote = connection.remote_id();
            let remote_key = remote.to_string();
            let stable_id = connection.stable_id();
            connections
                .write()
                .entry(remote_key.clone())
                .or_default()
                .insert(
                    stable_id,
                    RegisteredConnection {
                        connection: connection.clone(),
                    },
                );
            health_connections.write().insert(
                remote_key.clone(),
                health::HealthConnection {
                    connection: connection.clone(),
                    stable_id,
                },
            );
            let should_spawn_health = {
                let mut targets = health_targets.write();
                targets.insert(remote_key.clone())
            };
            if should_spawn_health {
                let endpoint = endpoint.clone();
                let health_targets = health_targets.clone();
                let health_connections = health_connections.clone();
                let endpoint_key = remote_key.clone();
                tokio::spawn(async move {
                    let _ = maintain_health_connection(
                        endpoint,
                        remote,
                        endpoint_key,
                        health_targets,
                        health_connections,
                    )
                    .await;
                });
            }

            ctx.record_connection(remote_key.clone(), tui::describe_paths(&connection));

            loop {
                match connection.read_datagram().await {
                    Ok(data) => match parse_frame(&data, remote_key.clone(), stable_id) {
                        Ok(ReceivedFrame::Packet(packet)) => {
                            ctx.record_connection_receive(
                                &remote_key,
                                data.len() as u64,
                                packet.header.sequence,
                            );
                            record_health_sample(
                                &health_stats,
                                &remote_key,
                                packet.payload.len() as u64,
                                packet.header.sequence,
                            );
                            if tx.send(ReceivedFrame::Packet(packet)).await.is_err() {
                                break;
                            }
                        }
                        Ok(fec @ ReceivedFrame::Fec(_)) => {
                            if tx.send(fec).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => ctx.record_invalid(),
                    },
                    Err(err) => {
                        let error = err.to_string();
                        ctx.record_connection_reset(&remote_key, &error);
                        let remaining_connections =
                            remove_connection_if_current(&connections, &remote_key, stable_id);
                        if health_connections
                            .read()
                            .get(&remote_key)
                            .is_some_and(|connection| connection.stable_id == stable_id)
                        {
                            health_connections.write().remove(&remote_key);
                        }
                        if !remaining_connections {
                            ctx.record_disconnect(remote_key.clone(), error);
                        }
                        break;
                    }
                }
            }
        }));
    }

    for task in tasks {
        task.abort();
    }
    endpoint.close().await;
    Ok(())
}

fn parse_frame(data: &[u8], remote: String, connection_id: usize) -> Result<ReceivedFrame> {
    let DecodedPacket { header, payload } = decode_packet(data)?;
    if header.sequence == FEC_SEQUENCE {
        return Ok(ReceivedFrame::Fec(ReceivedFecFrame {
            remote,
            frame: decode_fec_frame(data)?,
        }));
    }

    Ok(ReceivedFrame::Packet(ReceivedPacket {
        remote,
        connection_id,
        header,
        payload: payload.to_vec(),
    }))
}

async fn reorder_loop(
    mut rx: mpsc::Receiver<ReceivedFrame>,
    out_socket: Arc<UdpSocket>,
    out_udp: SocketAddr,
    reply_routes: ReplyRoutes,
    repair: Option<RepairRuntime>,
    ctx: ServerCtx,
    flow_idle_reset: Duration,
    max_reorder_delay: Duration,
) -> Result<()> {
    let mut initialized = false;
    let mut next_seq = 0_u64;
    let mut buffered = BTreeMap::<u64, BufferedPacket>::new();
    let mut fragments = BTreeMap::<u64, FragmentAssembly>::new();
    let mut seen = HashSet::<u64>::new();
    let mut skipped_sequences = RecentSkippedSequences::default();
    let mut gap_repair_requests = BTreeMap::<u64, tokio::time::Instant>::new();
    let mut restart_detector = RestartDetector::default();
    let mut flow_connections = HashSet::<usize>::new();
    let mut fec_state = FecState::default();

    loop {
        let timeout_duration = if has_pending_state(&buffered, &fragments, &seen) {
            repair.as_ref().map_or(max_reorder_delay, |repair| {
                repair.request_interval.min(max_reorder_delay)
            })
        } else {
            flow_idle_reset
        };
        let frame = match timeout(timeout_duration, rx.recv()).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(_) => {
                if !initialized {
                    continue;
                }

                if has_pending_state(&buffered, &fragments, &seen) {
                    advance_reorder_window(
                        &mut next_seq,
                        &mut buffered,
                        &mut fragments,
                        &mut seen,
                        &mut skipped_sequences,
                        &mut gap_repair_requests,
                        &mut fec_state,
                        repair.as_ref(),
                        &out_socket,
                        out_udp,
                        max_reorder_delay,
                        &ctx,
                    )
                    .await?;
                } else {
                    initialized = false;
                    next_seq = 0;
                    restart_detector.clear();
                    skipped_sequences.clear();
                    gap_repair_requests.clear();
                    flow_connections.clear();
                    fec_state.clear();
                }
                continue;
            }
        };

        let packet = match frame {
            ReceivedFrame::Packet(packet) => packet,
            ReceivedFrame::Fec(fec) => {
                fec_state.insert_frame(fec.frame);
                if initialized {
                    set_reply_routes(&reply_routes, &fec.remote, false);
                    recover_fec_ready(
                        &mut fec_state,
                        &mut next_seq,
                        &mut buffered,
                        &mut fragments,
                        &mut seen,
                        &mut gap_repair_requests,
                        &out_socket,
                        out_udp,
                        &ctx,
                    )
                    .await?;
                    advance_reorder_window(
                        &mut next_seq,
                        &mut buffered,
                        &mut fragments,
                        &mut seen,
                        &mut skipped_sequences,
                        &mut gap_repair_requests,
                        &mut fec_state,
                        repair.as_ref(),
                        &out_socket,
                        out_udp,
                        max_reorder_delay,
                        &ctx,
                    )
                    .await?;
                }
                continue;
            }
        };

        ctx.record_received(packet.payload.len() as u64);

        if initialized {
            set_reply_routes(&reply_routes, &packet.remote, false);
        } else {
            initialized = true;
            next_seq = packet.header.sequence;
            set_reply_routes(&reply_routes, &packet.remote, true);
            ctx.set_flow_start(next_seq);
            flow_connections.insert(packet.connection_id);
        }

        if packet.header.sequence < next_seq {
            if !flow_connections.contains(&packet.connection_id)
                && restart_detector.observe(packet.header.sequence, next_seq)
            {
                buffered.clear();
                fragments.clear();
                seen.clear();
                skipped_sequences.clear();
                gap_repair_requests.clear();
                fec_state.clear();
                flow_connections.clear();
                flow_connections.insert(packet.connection_id);
                next_seq = packet.header.sequence;
                set_reply_routes(&reply_routes, &packet.remote, true);
                ctx.record_flow_reset(next_seq, "confirmed sequence restart");
            } else if skipped_sequences.contains(packet.header.sequence) {
                ctx.record_late_after_skip(
                    packet.header.sequence,
                    (buffered.len() + fragments.len()) as u64,
                    next_seq,
                );
                continue;
            } else {
                ctx.record_duplicate(buffered.len() as u64, next_seq);
                continue;
            }
        } else {
            restart_detector.clear();
            flow_connections.insert(packet.connection_id);
        }

        if packet.header.sequence < next_seq {
            ctx.record_duplicate(buffered.len() as u64, next_seq);
            continue;
        }

        if seen.contains(&packet.header.sequence) {
            ctx.record_duplicate(buffered.len() as u64, next_seq);
            continue;
        }
        if buffered.contains_key(&packet.header.sequence) {
            ctx.record_duplicate(buffered.len() as u64, next_seq);
            continue;
        }

        let payload = if packet.header.fragments == 1 {
            fragments.remove(&packet.header.sequence);
            seen.insert(packet.header.sequence);
            packet.payload
        } else {
            let now = tokio::time::Instant::now();
            let entry = fragments
                .entry(packet.header.sequence)
                .or_insert_with(|| FragmentAssembly::new(packet.header.fragments, now));
            if entry.fragments() != packet.header.fragments {
                ctx.record_duplicate(buffered.len() as u64, next_seq);
                continue;
            }

            if !entry.insert(packet.header.fragment, packet.payload) {
                ctx.record_duplicate(buffered.len() as u64, next_seq);
                continue;
            }

            if !entry.is_complete() {
                ctx.record_buffered((buffered.len() + fragments.len()) as u64, next_seq);
                advance_reorder_window(
                    &mut next_seq,
                    &mut buffered,
                    &mut fragments,
                    &mut seen,
                    &mut skipped_sequences,
                    &mut gap_repair_requests,
                    &mut fec_state,
                    repair.as_ref(),
                    &out_socket,
                    out_udp,
                    max_reorder_delay,
                    &ctx,
                )
                .await?;
                continue;
            }

            let assembly = fragments
                .remove(&packet.header.sequence)
                .expect("complete assembly exists");
            seen.insert(packet.header.sequence);
            assembly.into_payload()
        };

        accept_complete_payload(
            packet.header.sequence,
            payload,
            &mut next_seq,
            &mut buffered,
            &mut seen,
            &mut gap_repair_requests,
            &mut fec_state,
            &out_socket,
            out_udp,
            &ctx,
        )
        .await?;
        recover_fec_ready(
            &mut fec_state,
            &mut next_seq,
            &mut buffered,
            &mut fragments,
            &mut seen,
            &mut gap_repair_requests,
            &out_socket,
            out_udp,
            &ctx,
        )
        .await?;

        advance_reorder_window(
            &mut next_seq,
            &mut buffered,
            &mut fragments,
            &mut seen,
            &mut skipped_sequences,
            &mut gap_repair_requests,
            &mut fec_state,
            repair.as_ref(),
            &out_socket,
            out_udp,
            max_reorder_delay,
            &ctx,
        )
        .await?;
    }

    Ok(())
}

async fn accept_complete_payload(
    sequence: u64,
    payload: Vec<u8>,
    next_seq: &mut u64,
    buffered: &mut BTreeMap<u64, BufferedPacket>,
    seen: &mut HashSet<u64>,
    gap_repair_requests: &mut BTreeMap<u64, tokio::time::Instant>,
    fec_state: &mut FecState,
    out_socket: &UdpSocket,
    out_udp: SocketAddr,
    ctx: &ServerCtx,
) -> Result<()> {
    seen.insert(sequence);
    fec_state.insert_payload(sequence, payload.clone());
    if sequence == *next_seq {
        gap_repair_requests.remove(&sequence);
        forward_payload(
            out_socket,
            out_udp,
            &payload,
            PacketHeader {
                sequence,
                fragment: 0,
                fragments: 1,
            },
        )
        .await?;
        ctx.record_forwarded(
            payload.len() as u64,
            buffered.len() as u64,
            sequence,
            next_sequence(*next_seq),
        );
        seen.remove(&sequence);
        *next_seq = next_sequence(*next_seq);
        drain_ready(
            next_seq,
            buffered,
            seen,
            gap_repair_requests,
            fec_state,
            out_socket,
            out_udp,
            ctx,
        )
        .await?;
    } else {
        buffered.insert(
            sequence,
            BufferedPacket {
                payload,
                received_at: tokio::time::Instant::now(),
            },
        );
        ctx.record_buffered(buffered.len() as u64, *next_seq);
    }

    Ok(())
}

async fn recover_fec_ready(
    fec_state: &mut FecState,
    next_seq: &mut u64,
    buffered: &mut BTreeMap<u64, BufferedPacket>,
    fragments: &mut BTreeMap<u64, FragmentAssembly>,
    seen: &mut HashSet<u64>,
    gap_repair_requests: &mut BTreeMap<u64, tokio::time::Instant>,
    out_socket: &UdpSocket,
    out_udp: SocketAddr,
    ctx: &ServerCtx,
) -> Result<()> {
    loop {
        let recovered = fec_state.recover_ready(*next_seq, buffered);
        if recovered.is_empty() {
            break;
        }

        let mut accepted = false;
        for (sequence, payload) in recovered {
            if sequence < *next_seq || seen.contains(&sequence) || buffered.contains_key(&sequence)
            {
                continue;
            }

            fragments.remove(&sequence);
            ctx.record_fec_recovered(sequence, payload.len() as u64);
            accept_complete_payload(
                sequence,
                payload,
                next_seq,
                buffered,
                seen,
                gap_repair_requests,
                fec_state,
                out_socket,
                out_udp,
                ctx,
            )
            .await?;
            accepted = true;
        }

        if !accepted {
            break;
        }
    }

    Ok(())
}

async fn advance_reorder_window(
    next_seq: &mut u64,
    buffered: &mut BTreeMap<u64, BufferedPacket>,
    fragments: &mut BTreeMap<u64, FragmentAssembly>,
    seen: &mut HashSet<u64>,
    skipped_sequences: &mut RecentSkippedSequences,
    gap_repair_requests: &mut BTreeMap<u64, tokio::time::Instant>,
    fec_state: &mut FecState,
    repair: Option<&RepairRuntime>,
    out_socket: &UdpSocket,
    out_udp: SocketAddr,
    max_reorder_delay: Duration,
    ctx: &ServerCtx,
) -> Result<()> {
    recover_fec_ready(
        fec_state,
        next_seq,
        buffered,
        fragments,
        seen,
        gap_repair_requests,
        out_socket,
        out_udp,
        ctx,
    )
    .await?;
    if let Some(repair) = repair {
        request_due_repairs(
            *next_seq,
            buffered,
            fragments,
            gap_repair_requests,
            repair.request_interval,
            &repair.control,
            ctx,
        );
    }
    expire_reorder_gap(
        next_seq,
        buffered,
        fragments,
        seen,
        skipped_sequences,
        gap_repair_requests,
        max_reorder_delay,
        ctx,
    );
    drain_ready(
        next_seq,
        buffered,
        seen,
        gap_repair_requests,
        fec_state,
        out_socket,
        out_udp,
        ctx,
    )
    .await
}

fn has_pending_state(
    buffered: &BTreeMap<u64, BufferedPacket>,
    fragments: &BTreeMap<u64, FragmentAssembly>,
    seen: &HashSet<u64>,
) -> bool {
    !(buffered.is_empty() && fragments.is_empty() && seen.is_empty())
}

fn request_due_repairs(
    next_seq: u64,
    buffered: &BTreeMap<u64, BufferedPacket>,
    fragments: &mut BTreeMap<u64, FragmentAssembly>,
    gap_repair_requests: &mut BTreeMap<u64, tokio::time::Instant>,
    repair_request_interval: Duration,
    repair_control: &RepairControl,
    ctx: &ServerCtx,
) {
    let now = tokio::time::Instant::now();
    let mut requests = Vec::new();

    if !buffered.contains_key(&next_seq)
        && !fragments.contains_key(&next_seq)
        && (!buffered.is_empty() || !fragments.is_empty())
        && gap_repair_requests
            .get(&next_seq)
            .copied()
            .is_none_or(|last| now.duration_since(last) >= repair_request_interval)
    {
        gap_repair_requests.insert(next_seq, now);
        requests.push(RepairRequest {
            sequence: next_seq,
            missing_mask: REPAIR_ALL_FRAGMENTS_MASK,
        });
    }

    for (sequence, assembly) in fragments.iter_mut() {
        if requests.len() >= MAX_REPAIR_REQUESTS_PER_TICK {
            break;
        }
        if assembly
            .last_repair_request
            .is_some_and(|last| now.duration_since(last) < repair_request_interval)
        {
            continue;
        }

        let missing_mask = assembly.missing_mask();
        if missing_mask == 0 {
            continue;
        }

        assembly.last_repair_request = Some(now);
        requests.push(RepairRequest {
            sequence: *sequence,
            missing_mask,
        });
    }

    for request in requests {
        ctx.record_repair_request(request.sequence, request.missing_mask);
        repair_control.send(request);
    }
}

fn expire_reorder_gap(
    next_seq: &mut u64,
    buffered: &mut BTreeMap<u64, BufferedPacket>,
    fragments: &mut BTreeMap<u64, FragmentAssembly>,
    seen: &mut HashSet<u64>,
    skipped_sequences: &mut RecentSkippedSequences,
    gap_repair_requests: &mut BTreeMap<u64, tokio::time::Instant>,
    max_reorder_delay: Duration,
    ctx: &ServerCtx,
) {
    loop {
        if buffered.contains_key(next_seq) {
            break;
        }

        if let Some(assembly) = fragments.get(next_seq) {
            if assembly.first_seen.elapsed() < max_reorder_delay {
                break;
            }

            fragments.remove(next_seq);
            seen.remove(next_seq);
            skip_sequence(
                next_seq,
                buffered,
                fragments,
                skipped_sequences,
                gap_repair_requests,
                ctx,
                SkipReason::FragmentIncomplete,
            );
            continue;
        }

        let oldest_pending = buffered
            .values()
            .map(|packet| packet.received_at)
            .chain(fragments.values().map(|assembly| assembly.first_seen))
            .min();
        let Some(oldest_pending) = oldest_pending else {
            break;
        };
        if oldest_pending.elapsed() < max_reorder_delay {
            break;
        }

        skip_sequence(
            next_seq,
            buffered,
            fragments,
            skipped_sequences,
            gap_repair_requests,
            ctx,
            SkipReason::NeverReceived,
        );
    }
}

fn skip_sequence(
    next_seq: &mut u64,
    buffered: &BTreeMap<u64, BufferedPacket>,
    fragments: &BTreeMap<u64, FragmentAssembly>,
    skipped_sequences: &mut RecentSkippedSequences,
    gap_repair_requests: &mut BTreeMap<u64, tokio::time::Instant>,
    ctx: &ServerCtx,
    reason: SkipReason,
) {
    let skipped = *next_seq;
    *next_seq = next_sequence(*next_seq);
    gap_repair_requests.remove(&skipped);
    skipped_sequences.insert(skipped);
    let buffered_count = (buffered.len() + fragments.len()) as u64;
    match reason {
        SkipReason::NeverReceived => {
            ctx.record_never_received_skip(skipped, buffered_count, *next_seq);
        }
        SkipReason::FragmentIncomplete => {
            ctx.record_fragment_incomplete_skip(skipped, buffered_count, *next_seq);
        }
    }
}

async fn drain_ready(
    next_seq: &mut u64,
    buffered: &mut BTreeMap<u64, BufferedPacket>,
    seen: &mut HashSet<u64>,
    gap_repair_requests: &mut BTreeMap<u64, tokio::time::Instant>,
    fec_state: &mut FecState,
    out_socket: &UdpSocket,
    out_udp: SocketAddr,
    ctx: &ServerCtx,
) -> Result<()> {
    while let Some(packet) = buffered.remove(next_seq) {
        gap_repair_requests.remove(next_seq);
        fec_state.insert_payload(*next_seq, packet.payload.clone());
        forward_payload(
            out_socket,
            out_udp,
            &packet.payload,
            PacketHeader {
                sequence: *next_seq,
                fragment: 0,
                fragments: 1,
            },
        )
        .await?;
        ctx.record_forwarded(
            packet.payload.len() as u64,
            buffered.len() as u64,
            *next_seq,
            next_sequence(*next_seq),
        );
        seen.remove(next_seq);
        *next_seq = next_sequence(*next_seq);
    }

    Ok(())
}

fn next_sequence(sequence: u64) -> u64 {
    if sequence == MAX_MEDIA_SEQUENCE {
        0
    } else {
        sequence + 1
    }
}

fn sequence_after(mut sequence: u64, offset: usize) -> u64 {
    for _ in 0..offset {
        sequence = next_sequence(sequence);
    }
    sequence
}

impl FecState {
    fn clear(&mut self) {
        self.frames.clear();
        self.frame_order.clear();
        self.payloads.clear();
        self.payload_order.clear();
    }

    fn insert_frame(&mut self, frame: FecFrame) {
        let base_sequence = frame.base_sequence;
        if !self.frames.contains_key(&base_sequence) {
            self.frame_order.push_back(base_sequence);
        }
        self.frames.insert(base_sequence, frame);
        self.enforce_frame_limit();
    }

    fn insert_payload(&mut self, sequence: u64, payload: Vec<u8>) {
        if sequence > MAX_MEDIA_SEQUENCE || self.payloads.contains_key(&sequence) {
            return;
        }

        self.payload_order.push_back(sequence);
        self.payloads.insert(sequence, payload);
        self.enforce_payload_limit();
    }

    fn recover_ready(
        &mut self,
        next_seq: u64,
        buffered: &BTreeMap<u64, BufferedPacket>,
    ) -> Vec<(u64, Vec<u8>)> {
        let bases = self.frames.keys().copied().collect::<Vec<_>>();
        let mut recovered = Vec::new();

        for base in bases {
            let Some(frame) = self.frames.get(&base).cloned() else {
                continue;
            };

            let mut parity = frame.parity.to_vec();
            let mut missing = None::<(u64, usize)>;
            let mut multiple_missing = false;
            let mut unusable = false;

            for (index, expected_len) in frame.payload_lengths.iter().copied().enumerate() {
                let sequence = sequence_after(frame.base_sequence, index);
                if expected_len > parity.len() {
                    unusable = true;
                    break;
                }

                if let Some(payload) = self.payload_for(sequence, buffered) {
                    if payload.len() != expected_len || payload.len() > parity.len() {
                        unusable = true;
                        break;
                    }

                    for (byte_index, byte) in payload.iter().enumerate() {
                        parity[byte_index] ^= *byte;
                    }
                } else if missing.is_none() {
                    missing = Some((sequence, expected_len));
                } else {
                    multiple_missing = true;
                    break;
                }
            }

            if unusable {
                self.frames.remove(&base);
                continue;
            }
            if multiple_missing {
                continue;
            }

            self.frames.remove(&base);
            let Some((sequence, expected_len)) = missing else {
                continue;
            };
            if sequence < next_seq {
                continue;
            }

            parity.truncate(expected_len);
            recovered.push((sequence, parity));
        }

        recovered
    }

    fn payload_for<'a>(
        &'a self,
        sequence: u64,
        buffered: &'a BTreeMap<u64, BufferedPacket>,
    ) -> Option<&'a [u8]> {
        self.payloads.get(&sequence).map(Vec::as_slice).or_else(|| {
            buffered
                .get(&sequence)
                .map(|packet| packet.payload.as_slice())
        })
    }

    fn enforce_frame_limit(&mut self) {
        while self.frames.len() > FEC_FRAME_TRACK_LIMIT {
            let Some(sequence) = self.frame_order.pop_front() else {
                break;
            };
            self.frames.remove(&sequence);
        }
    }

    fn enforce_payload_limit(&mut self) {
        while self.payloads.len() > FEC_PAYLOAD_TRACK_LIMIT {
            let Some(sequence) = self.payload_order.pop_front() else {
                break;
            };
            self.payloads.remove(&sequence);
        }
    }
}

impl RestartDetector {
    fn observe(&mut self, sequence: u64, next_seq: u64) -> bool {
        if next_seq.saturating_sub(sequence) < RESTART_BACKWARD_GAP {
            self.clear();
            return false;
        }

        if self.count == 0 || sequence >= self.highest_seq {
            self.highest_seq = self.highest_seq.max(sequence);
            self.count = self.count.saturating_add(1);
        } else {
            self.count = 1;
            self.highest_seq = sequence;
        }

        self.count >= RESTART_CONFIRM_PACKETS
    }

    fn clear(&mut self) {
        self.count = 0;
        self.highest_seq = 0;
    }
}

impl RecentSkippedSequences {
    fn insert(&mut self, sequence: u64) {
        if self.lookup.insert(sequence) {
            self.order.push_back(sequence);
        }

        while self.order.len() > SKIPPED_SEQUENCE_TRACK_LIMIT {
            if let Some(expired) = self.order.pop_front() {
                self.lookup.remove(&expired);
            }
        }
    }

    fn contains(&self, sequence: u64) -> bool {
        self.lookup.contains(&sequence)
    }

    fn clear(&mut self) {
        self.order.clear();
        self.lookup.clear();
    }
}

impl RepairControl {
    fn send(&self, request: RepairRequest) {
        let payload = encode_repair_request(request);
        let connections = {
            let routes = self.reply_routes.read().clone();
            let registry = self.connections.read();
            routes
                .iter()
                .filter_map(|remote| registry.get(remote))
                .flat_map(|connections| {
                    connections
                        .values()
                        .map(|registered| registered.connection.clone())
                })
                .collect::<Vec<_>>()
        };

        for connection in connections {
            let payload = payload.clone();
            tokio::spawn(async move {
                let _ = send_repair_request_stream(connection, payload).await;
            });
        }
    }
}

async fn send_repair_request_stream(
    connection: iroh::endpoint::Connection,
    payload: Bytes,
) -> Result<()> {
    let mut stream = timeout(REPAIR_STREAM_TIMEOUT, connection.open_uni())
        .await
        .context("timed out opening repair request stream")?
        .context("failed opening repair request stream")?;
    stream
        .write_all(&payload)
        .await
        .context("failed writing repair request stream")?;
    stream
        .finish()
        .context("failed finishing repair request stream")?;
    Ok(())
}

async fn forward_payload(
    socket: &UdpSocket,
    out_udp: SocketAddr,
    payload: &[u8],
    header: PacketHeader,
) -> Result<()> {
    socket
        .send_to(payload, out_udp)
        .await
        .with_context(|| format!("failed forwarding seq {} to {}", header.sequence, out_udp))?;
    Ok(())
}

async fn response_loop(
    socket: Arc<UdpSocket>,
    udp_dest: SocketAddr,
    connections: ConnectionRegistry,
    reply_routes: ReplyRoutes,
    ctx: ServerCtx,
) -> Result<()> {
    let mut buf = vec![0_u8; MAX_UDP_PACKET_SIZE];
    loop {
        let (len, src) = socket
            .recv_from(&mut buf)
            .await
            .context("failed receiving response from UDP destination")?;

        if src != udp_dest {
            continue;
        }

        let payload = Bytes::copy_from_slice(&buf[..len]);
        let remotes = reply_routes.read().clone();
        if remotes.is_empty() {
            continue;
        }

        let mut sent = false;
        for remote in remotes {
            let remote_connections = connections
                .read()
                .get(&remote)
                .map(|connections| {
                    connections
                        .iter()
                        .rev()
                        .map(|(stable_id, registered)| (*stable_id, registered.connection.clone()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let mut failed = Vec::new();
            for (stable_id, connection) in remote_connections {
                match connection.send_datagram(payload.clone()) {
                    Ok(()) => {
                        sent = true;
                        break;
                    }
                    Err(err) => {
                        ctx.record_send_pressure_drop(&remote, &err.to_string());
                        failed.push(stable_id);
                    }
                }
            }
            if !failed.is_empty() {
                let mut connections = connections.write();
                if let Some(remote_connections) = connections.get_mut(&remote) {
                    for stable_id in failed {
                        remote_connections.remove(&stable_id);
                    }
                    if remote_connections.is_empty() {
                        connections.remove(&remote);
                    }
                }
            }
            if sent {
                break;
            }
        }
    }
}

fn remove_connection_if_current(
    connections: &ConnectionRegistry,
    remote: &str,
    stable_id: usize,
) -> bool {
    let mut connections = connections.write();
    if let Some(remote_connections) = connections.get_mut(remote) {
        remote_connections.remove(&stable_id);
        if remote_connections.is_empty() {
            connections.remove(remote);
            return false;
        }
        return true;
    }
    false
}

fn set_reply_routes(reply_routes: &ReplyRoutes, remote: &str, reset: bool) {
    let mut routes = reply_routes.write();
    if reset {
        routes.clear();
    }
    if !routes.iter().any(|existing| existing == remote) {
        routes.push(remote.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::{ReceivedFecFrame, ReceivedFrame, ReceivedPacket, reorder_loop};
    use crate::context::ServerCtx;
    use crate::tui::ServerUiState;
    use bytes::Bytes;
    use parking_lot::RwLock;
    use protocol::{FecFrame, PacketHeader};
    use std::{sync::Arc, time::Duration};
    use tokio::{net::UdpSocket, sync::mpsc, time::timeout};

    #[tokio::test]
    async fn skips_stale_gap_while_packets_keep_arriving() {
        let out_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let out_udp = recv_socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel(16);
        let reply_routes = Arc::new(RwLock::new(Vec::new()));
        let ctx = ServerCtx::default();

        let task = tokio::spawn(reorder_loop(
            rx,
            out_socket,
            out_udp,
            reply_routes,
            None,
            ctx,
            Duration::from_secs(30),
            Duration::from_millis(20),
        ));

        tx.send(packet(10, b"first")).await.unwrap();
        assert_eq!(recv_payload(&recv_socket).await, b"first");

        tx.send(packet(12, b"third")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        tx.send(packet(13, b"fourth")).await.unwrap();

        assert_eq!(recv_payload(&recv_socket).await, b"third");
        assert_eq!(recv_payload(&recv_socket).await, b"fourth");

        drop(tx);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn resets_after_confirmed_sequence_restart() {
        let out_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let out_udp = recv_socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel(32);
        let reply_routes = Arc::new(RwLock::new(Vec::new()));
        let ctx = ServerCtx::default();

        let task = tokio::spawn(reorder_loop(
            rx,
            out_socket,
            out_udp,
            reply_routes,
            None,
            ctx,
            Duration::from_secs(30),
            Duration::from_millis(20),
        ));

        tx.send(packet(5000, b"old")).await.unwrap();
        assert_eq!(recv_payload(&recv_socket).await, b"old");

        for seq in 0..8 {
            tx.send(packet_with_connection(
                2,
                seq,
                format!("new{seq}").as_bytes(),
            ))
            .await
            .unwrap();
        }
        tx.send(packet_with_connection(2, 8, b"new8"))
            .await
            .unwrap();

        assert_eq!(recv_payload(&recv_socket).await, b"new7");
        assert_eq!(recv_payload(&recv_socket).await, b"new8");

        drop(tx);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn classifies_never_received_gap_and_late_arrival() {
        let out_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let out_udp = recv_socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel(16);
        let reply_routes = Arc::new(RwLock::new(Vec::new()));
        let ui = ServerUiState::new("test".to_string());
        let ctx = ServerCtx::new(Some(ui.clone()));

        let task = tokio::spawn(reorder_loop(
            rx,
            out_socket,
            out_udp,
            reply_routes,
            None,
            ctx,
            Duration::from_secs(30),
            Duration::from_millis(20),
        ));

        tx.send(packet(10, b"first")).await.unwrap();
        assert_eq!(recv_payload(&recv_socket).await, b"first");

        tx.send(packet(12, b"third")).await.unwrap();
        assert_eq!(recv_payload(&recv_socket).await, b"third");

        tx.send(packet(11, b"late")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(ui.skipped_never_received_packets(), 1);
        assert_eq!(ui.late_after_skip_packets(), 1);

        drop(tx);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn classifies_incomplete_fragment_skip() {
        let out_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let out_udp = recv_socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel(16);
        let reply_routes = Arc::new(RwLock::new(Vec::new()));
        let ui = ServerUiState::new("test".to_string());
        let ctx = ServerCtx::new(Some(ui.clone()));

        let task = tokio::spawn(reorder_loop(
            rx,
            out_socket,
            out_udp,
            reply_routes,
            None,
            ctx,
            Duration::from_secs(30),
            Duration::from_millis(20),
        ));

        tx.send(fragment_packet(10, 0, 2, b"half")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;

        assert_eq!(ui.fragment_incomplete_packets(), 1);

        drop(tx);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn fec_recovers_one_missing_live_packet() {
        let out_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let out_udp = recv_socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel(16);
        let reply_routes = Arc::new(RwLock::new(Vec::new()));
        let ctx = ServerCtx::default();

        let task = tokio::spawn(reorder_loop(
            rx,
            out_socket,
            out_udp,
            reply_routes,
            None,
            ctx,
            Duration::from_secs(30),
            Duration::from_millis(100),
        ));

        tx.send(packet(10, b"one")).await.unwrap();
        assert_eq!(recv_payload(&recv_socket).await, b"one");

        tx.send(packet(12, b"tri")).await.unwrap();
        tx.send(fec_frame(
            10,
            &[b"one".as_slice(), b"two".as_slice(), b"tri".as_slice()],
        ))
        .await
        .unwrap();

        assert_eq!(recv_payload(&recv_socket).await, b"two");
        assert_eq!(recv_payload(&recv_socket).await, b"tri");

        drop(tx);
        task.await.unwrap().unwrap();
    }

    fn packet(sequence: u64, payload: &[u8]) -> ReceivedFrame {
        packet_with_connection(1, sequence, payload)
    }

    fn fragment_packet(
        sequence: u64,
        fragment: u8,
        fragments: u8,
        payload: &[u8],
    ) -> ReceivedFrame {
        ReceivedFrame::Packet(ReceivedPacket {
            remote: "test".to_string(),
            connection_id: 1,
            header: PacketHeader {
                sequence,
                fragment,
                fragments,
            },
            payload: payload.to_vec(),
        })
    }

    fn packet_with_connection(
        connection_id: usize,
        sequence: u64,
        payload: &[u8],
    ) -> ReceivedFrame {
        ReceivedFrame::Packet(ReceivedPacket {
            remote: "test".to_string(),
            connection_id,
            header: PacketHeader {
                sequence,
                fragment: 0,
                fragments: 1,
            },
            payload: payload.to_vec(),
        })
    }

    fn fec_frame(base_sequence: u64, payloads: &[&[u8]]) -> ReceivedFrame {
        let max_len = payloads.iter().map(|payload| payload.len()).max().unwrap();
        let mut parity = vec![0_u8; max_len];
        for payload in payloads {
            for (index, byte) in payload.iter().enumerate() {
                parity[index] ^= *byte;
            }
        }

        ReceivedFrame::Fec(ReceivedFecFrame {
            remote: "test".to_string(),
            frame: FecFrame {
                base_sequence,
                payload_lengths: payloads.iter().map(|payload| payload.len()).collect(),
                parity: Bytes::from(parity),
            },
        })
    }

    async fn recv_payload(socket: &UdpSocket) -> Vec<u8> {
        let mut buf = [0_u8; 64];
        let (len, _) = timeout(Duration::from_millis(200), socket.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        buf[..len].to_vec()
    }
}
