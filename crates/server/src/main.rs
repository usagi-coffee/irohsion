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
use cli::{SecretArg, local_udp_dest, relay_mode};
use context::ServerCtx;
use health::{
    HealthConnections, HealthStats, HealthTargets, health_loop, maintain_health_connection,
    record_health_sample,
};
use iroh::{Endpoint, RelayUrl, endpoint::presets};
use parking_lot::RwLock;
use protocol::{DecodedPacket, MAX_SEQUENCE, PacketHeader, decode_packet};
use runtime::wait_for_shutdown;
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle, time::timeout};
use transport::ALPN;

const MAX_UDP_PACKET_SIZE: usize = 65_507;
const RESTART_BACKWARD_GAP: u64 = 4_096;
const RESTART_CONFIRM_PACKETS: usize = protocol::MAX_FRAGMENTS + 1;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    port: u16,
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
    connection_id: usize,
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

#[derive(Clone)]
struct RegisteredConnection {
    connection: iroh::endpoint::Connection,
}

#[derive(Default)]
struct RestartDetector {
    count: usize,
    highest_seq: u64,
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
    ctx.set_endpoint(endpoint.id().to_string());
    ctx.set_health_endpoint(Some("auto".to_string()));
    ctx.set_server_addrs(server_addrs);

    let out_socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
            .await
            .context("failed to bind local UDP output socket")?,
    );
    let (tx, rx) = mpsc::channel::<ReceivedPacket>(1024);
    let connections: ConnectionRegistry = Arc::new(RwLock::new(BTreeMap::new()));
    let reply_routes: ReplyRoutes = Arc::new(RwLock::new(Vec::new()));
    let health_connections: HealthConnections = Arc::new(RwLock::new(BTreeMap::new()));
    let health_targets: HealthTargets = Arc::new(RwLock::new(std::collections::HashSet::new()));
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
        let health_connections = health_connections.clone();
        let health_stats = health_stats.clone();
        let interval = Duration::from_millis(cli.health_interval_ms);
        tasks.push(tokio::spawn(async move {
            let _ = health_loop(health_connections, health_stats, interval).await;
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
                    Ok(data) => match parse_packet(&data, remote_key.clone(), stable_id) {
                        Ok(packet) => {
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
                            if tx.send(packet).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => ctx.record_invalid(),
                    },
                    Err(err) => {
                        remove_connection_if_current(&connections, &remote_key, stable_id);
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

fn parse_packet(data: &[u8], remote: String, connection_id: usize) -> Result<ReceivedPacket> {
    let DecodedPacket { header, payload } = decode_packet(data)?;
    Ok(ReceivedPacket {
        remote,
        connection_id,
        header,
        payload: payload.to_vec(),
    })
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
    let mut restart_detector = RestartDetector::default();
    let mut flow_connections = HashSet::<usize>::new();

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
                    advance_reorder_window(
                        &mut next_seq,
                        &mut buffered,
                        &mut fragments,
                        &mut seen,
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
                    flow_connections.clear();
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
                flow_connections.clear();
                flow_connections.insert(packet.connection_id);
                next_seq = packet.header.sequence;
                set_reply_routes(&reply_routes, &packet.remote, true);
                ctx.record_flow_reset(next_seq, "confirmed sequence restart");
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

        if packet.header.sequence == next_seq {
            forward_payload(&out_socket, out_udp, &payload, packet.header).await?;
            ctx.record_forwarded(
                payload.len() as u64,
                buffered.len() as u64,
                packet.header.sequence,
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

        advance_reorder_window(
            &mut next_seq,
            &mut buffered,
            &mut fragments,
            &mut seen,
            &out_socket,
            out_udp,
            max_reorder_delay,
            &ctx,
        )
        .await?;
    }

    Ok(())
}

async fn advance_reorder_window(
    next_seq: &mut u64,
    buffered: &mut BTreeMap<u64, BufferedPacket>,
    fragments: &mut BTreeMap<u64, FragmentAssembly>,
    seen: &mut HashSet<u64>,
    out_socket: &UdpSocket,
    out_udp: SocketAddr,
    max_reorder_delay: Duration,
    ctx: &ServerCtx,
) -> Result<()> {
    expire_reorder_gap(next_seq, buffered, fragments, seen, max_reorder_delay, ctx);
    drain_ready(next_seq, buffered, seen, out_socket, out_udp, ctx).await
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
    if sequence == MAX_SEQUENCE {
        0
    } else {
        sequence + 1
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
                    Err(_) => failed.push(stable_id),
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

fn remove_connection_if_current(connections: &ConnectionRegistry, remote: &str, stable_id: usize) {
    let mut connections = connections.write();
    if let Some(remote_connections) = connections.get_mut(remote) {
        remote_connections.remove(&stable_id);
        if remote_connections.is_empty() {
            connections.remove(remote);
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

#[cfg(test)]
mod tests {
    use super::{ReceivedPacket, reorder_loop};
    use crate::context::ServerCtx;
    use parking_lot::RwLock;
    use protocol::PacketHeader;
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

    fn packet(sequence: u64, payload: &[u8]) -> ReceivedPacket {
        packet_with_connection(1, sequence, payload)
    }

    fn packet_with_connection(
        connection_id: usize,
        sequence: u64,
        payload: &[u8],
    ) -> ReceivedPacket {
        ReceivedPacket {
            remote: "test".to_string(),
            connection_id,
            header: PacketHeader {
                sequence,
                fragment: 0,
                fragments: 1,
            },
            payload: payload.to_vec(),
        }
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
