mod context;
mod health;
mod runtime;
mod tui;

use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::{ArgAction, Parser};
use cli::{EndpointTarget, SecretArg, endpoint_targets, local_udp_dest, relay_mode};
use context::ServerCtx;
use health::{
    HealthConnection, HealthStats, health_loop, maintain_health_connection, record_health_bytes,
};
use iroh::{Endpoint, EndpointId, RelayUrl, endpoint::presets};
use parking_lot::RwLock;
use protocol::{DecodedPacket, MAX_SEQUENCE, PacketHeader, decode_bundle, decode_packet};
use runtime::wait_for_shutdown;
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle, time::timeout};
use transport::{ALPN, build_server_addr};

const MAX_UDP_PACKET_SIZE: usize = 65_507;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    port: u16,
    #[arg(long)]
    health_endpoint: Option<EndpointId>,
    #[arg(long = "endpoint-target")]
    endpoint_targets: Vec<EndpointTarget>,
    #[arg(long, default_value_t = 1000)]
    health_interval_ms: u64,
    #[arg(long = "relay")]
    relays: Vec<RelayUrl>,
    #[arg(long, default_value = "", hide_default_value = true)]
    secret: SecretArg,
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    tui: bool,
    #[arg(long, default_value_t = 30)]
    flow_idle_reset_secs: u64,
    #[arg(long, default_value_t = 100)]
    max_reorder_delay_ms: u64,
}

#[derive(Debug)]
struct ReceivedPacket {
    remote: String,
    header: PacketHeader,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct FragmentAssembly {
    fragments: Vec<Option<Vec<u8>>>,
    received: usize,
    first_seen: tokio::time::Instant,
}

impl FragmentAssembly {
    fn new(fragments: u8, first_seen: tokio::time::Instant) -> Self {
        Self {
            fragments: vec![None; fragments as usize],
            received: 0,
            first_seen,
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

type ConnectionRegistry = Arc<RwLock<BTreeMap<String, iroh::endpoint::Connection>>>;
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

    let secret_key = cli.secret.resolve();
    let udp_dest = local_udp_dest(cli.port);
    let relays = cli.relays.clone();
    let endpoint_targets = Arc::new(endpoint_targets(&cli.endpoint_targets));

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
    ctx.set_endpoint(endpoint.id().to_string());
    ctx.set_health_endpoint(cli.health_endpoint.map(|id| id.to_string()));
    ctx.set_server_addrs(server_addrs);

    let out_socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
            .await
            .context("failed to bind local UDP output socket")?,
    );
    let (tx, rx) = mpsc::channel::<ReceivedPacket>(1024);
    let connections: ConnectionRegistry = Arc::new(RwLock::new(BTreeMap::new()));
    let reply_routes: ReplyRoutes = Arc::new(RwLock::new(Vec::new()));
    let health_connection: HealthConnection = Arc::new(RwLock::new(None));
    let health_stats: HealthStats = Arc::new(RwLock::new(BTreeMap::new()));
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    {
        // Client-to-server packets are deduped/reordered centrally before hitting the UDP target.
        let out_socket = out_socket.clone();
        let reply_routes = reply_routes.clone();
        let ctx = ctx.clone();
        let flow_idle_reset = Duration::from_secs(cli.flow_idle_reset_secs);
        let max_reorder_delay = Duration::from_millis(cli.max_reorder_delay_ms);
        tasks.push(tokio::spawn(async move {
            let _ = reorder_loop(
                rx,
                out_socket,
                udp_dest,
                reply_routes,
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
        tasks.push(tokio::spawn(async move {
            let _ = response_loop(out_socket, udp_dest, connections, reply_routes).await;
        }));
    }

    {
        let health_connection = health_connection.clone();
        let health_stats = health_stats.clone();
        let endpoint_targets = endpoint_targets.clone();
        let interval = Duration::from_millis(cli.health_interval_ms);
        tasks.push(tokio::spawn(async move {
            let _ = health_loop(health_connection, health_stats, endpoint_targets, interval).await;
        }));
    }

    if let Some(health_endpoint) = cli.health_endpoint {
        let endpoint = endpoint.clone();
        let health_connection = health_connection.clone();
        let health_addr = build_server_addr(health_endpoint, &[], &relays)?;
        tasks.push(tokio::spawn(async move {
            let _ = maintain_health_connection(endpoint, health_addr, health_connection).await;
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
        let health_stats = health_stats.clone();
        let ctx = ctx.clone();
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
            connections
                .write()
                .insert(remote_key.clone(), connection.clone());

            ctx.record_connection(remote_key.clone(), tui::describe_paths(&connection));

            loop {
                match connection.read_datagram().await {
                    Ok(data) => {
                        match parse_packets(&data, remote_key.clone()) {
                            Ok(packets) => {
                                for packet in packets {
                                    ctx.record_connection_receive(
                                        &remote_key,
                                        data.len() as u64,
                                        packet.header.sequence,
                                    );
                                    record_health_bytes(
                                        &health_stats,
                                        &remote_key,
                                        packet.payload.len() as u64,
                                    );
                                    if tx.send(packet).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(_) => ctx.record_invalid(),
                        }
                    }
                    Err(err) => {
                        connections.write().remove(&remote_key);
                        ctx.record_disconnect(remote_key.clone(), err.to_string());
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

fn parse_packets(data: &[u8], remote: String) -> Result<Vec<ReceivedPacket>> {
    let frames = decode_bundle(data)?
        .unwrap_or_else(|| vec![Bytes::copy_from_slice(data)]);
    frames
        .into_iter()
        .map(|frame| {
            let DecodedPacket { header, payload } = decode_packet(&frame)?;
            Ok(ReceivedPacket {
                remote: remote.clone(),
                header,
                payload: payload.to_vec(),
            })
        })
        .collect()
}

async fn reorder_loop(
    mut rx: mpsc::Receiver<ReceivedPacket>,
    out_socket: Arc<UdpSocket>,
    out_udp: SocketAddr,
    reply_routes: ReplyRoutes,
    ctx: ServerCtx,
    flow_idle_reset: Duration,
    max_reorder_delay: Duration,
) -> Result<()> {
    let mut initialized = false;
    let mut next_seq = 0_u64;
    let mut buffered = BTreeMap::<u64, BufferedPacket>::new();
    let mut fragments = BTreeMap::<u64, FragmentAssembly>::new();
    let mut seen = HashSet::<u64>::new();

    loop {
        let timeout_duration = if has_pending_state(&buffered, &fragments, &seen) {
            max_reorder_delay
        } else {
            flow_idle_reset
        };
        let packet = match timeout(timeout_duration, rx.recv()).await {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
            Err(_) => {
                if !initialized {
                    continue;
                }

                if has_pending_state(&buffered, &fragments, &seen) {
                    expire_reorder_gap(
                        &mut next_seq,
                        &mut buffered,
                        &mut fragments,
                        &mut seen,
                        max_reorder_delay,
                        &ctx,
                    );
                    drain_ready(
                        &mut next_seq,
                        &mut buffered,
                        &mut seen,
                        &out_socket,
                        out_udp,
                        &ctx,
                    )
                    .await?;
                } else {
                    initialized = false;
                    next_seq = 0;
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
        }

        if packet.header.sequence < next_seq {
            ctx.record_duplicate(buffered.len() as u64, next_seq);
            continue;
        }

        if seen.contains(&packet.header.sequence) {
            ctx.record_duplicate(buffered.len() as u64, next_seq);
            continue;
        }

        let payload = if packet.header.fragments == 1 {
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
                continue;
            }

            let assembly = fragments
                .remove(&packet.header.sequence)
                .expect("complete assembly exists");
            seen.insert(packet.header.sequence);
            assembly.into_payload()
        };

        if packet.header.sequence == next_seq {
            let forwarded_bytes = forward_payload(&out_socket, out_udp, &payload, packet.header).await?;
            ctx.record_forwarded(
                forwarded_bytes,
                buffered.len() as u64,
                next_sequence(next_seq),
            );
            next_seq = next_sequence(next_seq);
            seen.remove(&packet.header.sequence);
            drain_ready(
                &mut next_seq,
                &mut buffered,
                &mut seen,
                &out_socket,
                out_udp,
                &ctx,
            )
            .await?;
        } else {
            buffered.insert(
                packet.header.sequence,
                BufferedPacket {
                    payload,
                    received_at: tokio::time::Instant::now(),
                },
            );
            ctx.record_buffered(buffered.len() as u64, next_seq);
        }
    }

    Ok(())
}

fn has_pending_state(
    buffered: &BTreeMap<u64, BufferedPacket>,
    fragments: &BTreeMap<u64, FragmentAssembly>,
    seen: &HashSet<u64>,
) -> bool {
    !(buffered.is_empty() && fragments.is_empty() && seen.is_empty())
}

fn expire_reorder_gap(
    next_seq: &mut u64,
    buffered: &mut BTreeMap<u64, BufferedPacket>,
    fragments: &mut BTreeMap<u64, FragmentAssembly>,
    seen: &mut HashSet<u64>,
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
            skip_sequence(next_seq, buffered, fragments, ctx);
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

        skip_sequence(next_seq, buffered, fragments, ctx);
    }
}

fn skip_sequence(
    next_seq: &mut u64,
    buffered: &BTreeMap<u64, BufferedPacket>,
    fragments: &BTreeMap<u64, FragmentAssembly>,
    ctx: &ServerCtx,
) {
    let skipped = *next_seq;
    *next_seq = next_sequence(*next_seq);
    ctx.record_reorder_skip(
        skipped,
        (buffered.len() + fragments.len()) as u64,
        *next_seq,
    );
}

async fn drain_ready(
    next_seq: &mut u64,
    buffered: &mut BTreeMap<u64, BufferedPacket>,
    seen: &mut HashSet<u64>,
    out_socket: &UdpSocket,
    out_udp: SocketAddr,
    ctx: &ServerCtx,
) -> Result<()> {
    while let Some(packet) = buffered.remove(next_seq) {
        let forwarded_bytes = forward_payload(
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
            forwarded_bytes,
            buffered.len() as u64,
            next_sequence(*next_seq),
        );
        seen.remove(next_seq);
        *next_seq = next_sequence(*next_seq);
    }

    Ok(())
}

fn next_sequence(sequence: u64) -> u64 {
    if sequence == MAX_SEQUENCE {
        0
    } else {
        sequence + 1
    }
}

async fn forward_payload(
    socket: &UdpSocket,
    out_udp: SocketAddr,
    payload: &[u8],
    header: PacketHeader,
) -> Result<u64> {
    socket
        .send_to(payload, out_udp)
        .await
        .with_context(|| format!("failed forwarding seq {} to {}", header.sequence, out_udp))?;
    Ok(payload.len() as u64)
}

async fn response_loop(
    socket: Arc<UdpSocket>,
    udp_dest: SocketAddr,
    connections: ConnectionRegistry,
    reply_routes: ReplyRoutes,
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

        for remote in remotes {
            let connection = connections.read().get(&remote).cloned();
            let Some(connection) = connection else {
                continue;
            };

            if connection.send_datagram(payload.clone()).is_ok() {
                break;
            }
        }
    }
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
