mod context;
mod runtime;
mod tui;

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};

use anyhow::{Context, Result};
use clap::Parser;
use cli::{InterfaceSpec, parse_interface_configs};
use context::ClientCtx;
use iroh::{EndpointId, RelayUrl};
use parking_lot::RwLock;
use protocol::{PacketHeader, encode_packet};
use runtime::wait_for_shutdown;
use tokio::net::UdpSocket;
use transport::{build_server_addr, connect_path_with_secret, current_session_id};

const MAX_UDP_PACKET_SIZE: usize = 65_507;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
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
    let session_id = current_session_id()?;
    let listen_socket = Arc::new(
        UdpSocket::bind(listen_udp)
            .await
            .with_context(|| format!("failed to bind local UDP ingest socket on {listen_udp}"))?,
    );
    let listen_udp = listen_socket
        .local_addr()
        .context("failed to read local UDP ingest socket address")?;
    let ui = cli.tui.then(|| {
        tui::ClientUi::spawn(tui::ClientUiState::new(
            listen_udp.port(),
            cli.endpoint.to_string(),
            interface_configs
                .iter()
                .map(|config| config.binding.name.clone())
                .collect(),
        ))
    });
    let ctx = ClientCtx::new(ui.as_ref().map(|ui| ui.state.clone()));
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
        let path = connect_path_with_secret(binding, server_addr.clone(), secret_key).await?;
        ctx.record_connection_paths(path.interface_name.clone(), &endpoint_id, &path.connection);
        ctx.connected_path(
            &path.interface_name,
            &endpoint_id,
            SocketAddr::V4(path.bound_addr),
        );
        paths.push(path);
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

    ctx.client_ready(session_id, listen_udp, paths.len());

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

        // Remember the active local UDP peer so reverse traffic has somewhere to go.
        *last_ingest_peer.write() = Some(src);
        ctx.record_ingest(len as u64, src.to_string());

        let packet = Arc::new(encode_packet(PacketHeader { session_id, seq }, &buf[..len]));
        ctx.ingested_packet(seq, len, src);

        // Duplicate each ingested packet over every active interface-bound iroh path.
        for path in &paths {
            match path.send(packet.clone()) {
                Ok(()) => {
                    ctx.record_send(path.interface_name.clone(), len as u64);
                }
                Err(err) => {
                    ctx.record_send_error(path.interface_name.clone(), err.to_string());
                    ctx.send_failure(&path.interface_name, seq, &err.to_string());
                }
            }
        }

        seq = seq.wrapping_add(1);
    }

    Ok(())
}
