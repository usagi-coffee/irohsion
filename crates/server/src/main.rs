mod context;
mod runtime;
mod tui;

use std::{
    collections::{BTreeMap, HashSet},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{ArgAction, Parser};
use cli::SecretArg;
use context::ServerCtx;
use iroh::{Endpoint, RelayMode, RelayUrl, endpoint::presets};
use parking_lot::RwLock;
use protocol::{DecodedPacket, PacketHeader, decode_packet};
use runtime::wait_for_shutdown;
use tokio::{net::UdpSocket, sync::mpsc};
use transport::ALPN;

const MAX_UDP_PACKET_SIZE: usize = 65_507;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    port: u16,
    #[arg(long = "relay")]
    relays: Vec<RelayUrl>,
    #[arg(long, default_value = "", hide_default_value = true)]
    secret: SecretArg,
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    tui: bool,
}

#[derive(Debug)]
struct ReceivedPacket {
    remote: String,
    header: PacketHeader,
    payload: Vec<u8>,
}

type ConnectionRegistry = Arc<RwLock<BTreeMap<String, iroh::endpoint::Connection>>>;
type ReplyRoutes = Arc<RwLock<Vec<String>>>;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let secret_key = cli.secret.resolve();
    let udp_dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, cli.port));

    let ui = cli
        .tui
        .then(|| tui::ServerUi::spawn(tui::ServerUiState::new(udp_dest.to_string())));
    let ctx = ServerCtx::new(ui.as_ref().map(|ui| ui.state.clone()));

    // The server owns a single public iroh endpoint and accepts every client path on it.
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(if cli.relays.is_empty() {
            RelayMode::Default
        } else {
            RelayMode::custom(cli.relays)
        })
        .bind()
        .await
        .context("failed to bind server iroh endpoint")?;

    endpoint.online().await;
    let server_addrs = tui::server_addrs(&endpoint);
    ctx.set_endpoint(endpoint.id().to_string());
    ctx.set_server_addrs(server_addrs);

    let out_socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
            .await
            .context("failed to bind local UDP output socket")?,
    );
    let (tx, rx) = mpsc::channel::<ReceivedPacket>(1024);
    let connections: ConnectionRegistry = Arc::new(RwLock::new(BTreeMap::new()));
    let reply_routes: ReplyRoutes = Arc::new(RwLock::new(Vec::new()));

    {
        // Client-to-server packets are deduped/reordered centrally before hitting the UDP target.
        let out_socket = out_socket.clone();
        let reply_routes = reply_routes.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let _ = reorder_loop(rx, out_socket, udp_dest, reply_routes, ctx).await;
        });
    }

    {
        // Responses from the UDP target are sent back over whichever client paths are active.
        let out_socket = out_socket.clone();
        let connections = connections.clone();
        let reply_routes = reply_routes.clone();
        tokio::spawn(async move {
            let _ = response_loop(out_socket, udp_dest, connections, reply_routes).await;
        });
    }

    loop {
        let shutdown = wait_for_shutdown(ctx.ui_state());
        let incoming = tokio::select! {
            incoming = endpoint.accept() => incoming,
            _ = shutdown => break,
        };
        let Some(incoming) = incoming else {
            break;
        };

        let tx = tx.clone();
        let connections = connections.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
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
                        ctx.record_connection_receive(&remote_key, data.len() as u64);
                        match parse_packet(&data, remote_key.clone()) {
                            Ok(packet) => {
                                if tx.send(packet).await.is_err() {
                                    break;
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
        });
    }

    endpoint.close().await;
    Ok(())
}

fn parse_packet(data: &[u8], remote: String) -> Result<ReceivedPacket> {
    let DecodedPacket { header, payload } = decode_packet(data)?;
    Ok(ReceivedPacket {
        remote,
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
) -> Result<()> {
    let mut current_session_id: Option<u32> = None;
    let mut next_seq = 0_u64;
    let mut buffered = BTreeMap::<u64, Vec<u8>>::new();
    let mut seen = HashSet::<u64>::new();

    while let Some(packet) = rx.recv().await {
        ctx.record_received(packet.payload.len() as u64);

        match current_session_id {
            None => {
                current_session_id = Some(packet.header.session_id);
                set_reply_routes(&reply_routes, &packet.remote, true);
                ctx.set_session(packet.header.session_id, packet.header.seq);
                next_seq = packet.header.seq;
            }
            Some(active) if packet.header.session_id < active => {
                ctx.record_duplicate(buffered.len() as u64, next_seq);
                continue;
            }
            Some(active) if packet.header.session_id > active => {
                set_reply_routes(&reply_routes, &packet.remote, true);
                ctx.record_session_switch(packet.header.session_id, packet.header.seq);
                current_session_id = Some(packet.header.session_id);
                next_seq = packet.header.seq;
                buffered.clear();
                seen.clear();
            }
            Some(_) => {
                set_reply_routes(&reply_routes, &packet.remote, false);
            }
        }

        if packet.header.seq < next_seq {
            ctx.record_duplicate(buffered.len() as u64, next_seq);
            continue;
        }

        if !seen.insert(packet.header.seq) {
            ctx.record_duplicate(buffered.len() as u64, next_seq);
            continue;
        }

        if packet.header.seq == next_seq {
            forward_payload(&out_socket, out_udp, &packet.payload, packet.header).await?;
            ctx.record_forwarded(
                packet.payload.len() as u64,
                buffered.len() as u64,
                next_seq.wrapping_add(1),
            );
            next_seq = next_seq.wrapping_add(1);
            seen.remove(&packet.header.seq);

            while let Some(payload) = buffered.remove(&next_seq) {
                forward_payload(
                    &out_socket,
                    out_udp,
                    &payload,
                    PacketHeader {
                        session_id: current_session_id.expect("session is set"),
                        seq: next_seq,
                    },
                )
                .await?;
                ctx.record_forwarded(
                    payload.len() as u64,
                    buffered.len() as u64,
                    next_seq.wrapping_add(1),
                );
                seen.remove(&next_seq);
                next_seq = next_seq.wrapping_add(1);
            }
        } else {
            buffered.insert(packet.header.seq, packet.payload);
            ctx.record_buffered(buffered.len() as u64, next_seq);
        }
    }

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
        .with_context(|| format!("failed forwarding seq {} to {}", header.seq, out_udp))?;
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
