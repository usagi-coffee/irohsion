mod tui;

use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    process::Stdio,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use iroh::{Endpoint, RelayMode, SecretKey, endpoint::presets};
use protocol::{DecodedPacket, PacketHeader, decode_packet};
use tokio::{
    net::UdpSocket,
    process::{Child, Command},
    sync::mpsc,
    time::{Instant, interval},
};
use tracing::{error, info, warn};

const ALPN: &[u8] = b"irohsion/v1";

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    rtmp: String,
    #[arg(long)]
    secret: Option<String>,
    #[arg(long, default_value = "127.0.0.1:6000")]
    ffmpeg_input_udp: SocketAddr,
    #[arg(long, default_value = "ffmpeg")]
    ffmpeg_bin: String,
    #[arg(long)]
    tui: bool,
}

#[derive(Debug)]
struct ReceivedPacket {
    header: PacketHeader,
    payload: Vec<u8>,
}

#[derive(Debug, Default)]
struct ServerStats {
    received_packets: u64,
    received_payload_bytes: u64,
    forwarded_packets: u64,
    forwarded_payload_bytes: u64,
    duplicate_or_late_packets: u64,
    buffered_packets: u64,
    invalid_packets: u64,
    session_switches: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let secret_key = load_secret_key(cli.secret.as_deref())?;
    let ui = cli.tui.then(|| {
        tui::ServerUi::spawn(tui::ServerUiState::new(
            cli.rtmp.clone(),
            cli.ffmpeg_input_udp.to_string(),
        ))
    });
    init_tracing(ui.as_ref().map(|ui| ui.state.clone()))?;

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(RelayMode::Default)
        .bind()
        .await
        .context("failed to bind server iroh endpoint")?;

    endpoint.online().await;
    let server_addrs = collect_server_addrs(&endpoint);
    if let Some(ui) = &ui {
        ui.state.set_endpoint(endpoint.id().to_string());
        ui.state.set_server_addrs(server_addrs.clone());
    } else {
        print_server_address(&endpoint, &server_addrs);
    }

    let _ffmpeg = spawn_ffmpeg(&cli.ffmpeg_bin, cli.ffmpeg_input_udp, &cli.rtmp, cli.tui)?;

    let out_socket = Arc::new(
        UdpSocket::bind("127.0.0.1:0")
            .await
            .context("failed to bind local UDP output socket")?,
    );
    let (tx, rx) = mpsc::channel::<ReceivedPacket>(1024);

    {
        let out_socket = out_socket.clone();
        let ffmpeg_input_udp = cli.ffmpeg_input_udp;
        let ui_state = ui.as_ref().map(|ui| ui.state.clone());
        let verbose_logs = !cli.tui;
        tokio::spawn(async move {
            if let Err(err) = reorder_loop(rx, out_socket, ffmpeg_input_udp, ui_state, verbose_logs).await {
                error!(error = %err, "reorder loop exited");
            }
        });
    }

    loop {
        let shutdown = wait_for_shutdown(ui.as_ref().map(|ui| ui.state.clone()));
        let incoming = tokio::select! {
            incoming = endpoint.accept() => incoming,
            _ = shutdown => break,
        };
        let Some(incoming) = incoming else {
            break;
        };
        let tx = tx.clone();
        let ui_state = ui.as_ref().map(|ui| ui.state.clone());
        tokio::spawn(async move {
            let accepting = match incoming.accept() {
                Ok(accepting) => accepting,
                Err(err) => {
                    warn!(error = %err, "incoming connection failed before accept");
                    return;
                }
            };

            let connection = match accepting.await {
                Ok(connection) => connection,
                Err(err) => {
                    warn!(error = %err, "incoming connection failed during handshake");
                    return;
                }
            };

            let remote = connection.remote_id();
            info!(remote = %remote, "accepted interface path");
            log_connection_paths("server", &connection);
            if let Some(ui_state) = &ui_state {
                ui_state.record_connection(
                    remote.to_string(),
                    connection
                        .paths()
                        .into_iter()
                        .map(|path| tui::PathRow {
                            remote_addr: path.remote_addr().to_string(),
                            transport: transport_kind(&path).to_string(),
                            selected: path.is_selected(),
                            status: if path.is_closed() { "closed" } else { "up" }.to_string(),
                        })
                        .collect(),
                );
            }

            loop {
                match connection.read_datagram().await {
                    Ok(data) => {
                        if let Some(ui_state) = &ui_state {
                            ui_state.record_connection_receive(&remote.to_string(), data.len() as u64);
                        }
                        match parse_packet(&data) {
                        Ok(packet) => {
                            if tx.send(packet).await.is_err() {
                                warn!("packet consumer dropped");
                                break;
                            }
                        }
                        Err(err) => {
                            if let Some(ui_state) = &ui_state {
                                ui_state.record_invalid();
                            }
                            warn!(remote = %remote, error = %err, "dropping invalid packet")
                        },
                    }},
                    Err(err) => {
                        if let Some(ui_state) = &ui_state {
                            ui_state.record_disconnect(remote.to_string(), err.to_string());
                        }
                        info!(remote = %remote, error = %err, "connection closed");
                        break;
                    }
                }
            }
        });
    }

    endpoint.close().await;
    Ok(())
}

fn parse_packet(data: &[u8]) -> Result<ReceivedPacket> {
    let DecodedPacket { header, payload } = decode_packet(data)?;
    Ok(ReceivedPacket {
        header,
        payload: payload.to_vec(),
    })
}

async fn reorder_loop(
    mut rx: mpsc::Receiver<ReceivedPacket>,
    out_socket: Arc<UdpSocket>,
    out_udp: SocketAddr,
    ui_state: Option<tui::ServerUiState>,
    verbose_logs: bool,
) -> Result<()> {
    let mut log_tick = interval(Duration::from_secs(5));
    let started_at = Instant::now();
    let mut last_log_at = started_at;
    let mut stats = ServerStats::default();
    let mut last_received_packets = 0_u64;
    let mut last_received_bytes = 0_u64;
    let mut last_forwarded_packets = 0_u64;
    let mut last_forwarded_bytes = 0_u64;

    let mut current_session_id: Option<u32> = None;
    let mut next_seq = 0_u64;
    let mut buffered = BTreeMap::<u64, Vec<u8>>::new();
    let mut seen = HashSet::<u64>::new();

    loop {
        tokio::select! {
            maybe_packet = rx.recv() => {
                let Some(packet) = maybe_packet else {
                    break;
                };
                if let Some(ui_state) = &ui_state {
                    ui_state.record_received(packet.payload.len() as u64);
                }
                stats.received_packets += 1;
                stats.received_payload_bytes += packet.payload.len() as u64;

        match current_session_id {
            None => {
                current_session_id = Some(packet.header.session_id);
                if let Some(ui_state) = &ui_state {
                    ui_state.set_session(packet.header.session_id, packet.header.seq);
                }
                next_seq = packet.header.seq;
                if verbose_logs {
                    info!(session_id = packet.header.session_id, next_seq, "tracking first active session");
                }
            }
            Some(active) if packet.header.session_id < active => {
                if let Some(ui_state) = &ui_state {
                    ui_state.record_duplicate(buffered.len() as u64, next_seq);
                }
                stats.duplicate_or_late_packets += 1;
                if verbose_logs {
                    info!(session_id = packet.header.session_id, active_session = active, seq = packet.header.seq, "dropping old session packet");
                }
                continue;
            }
            Some(active) if packet.header.session_id > active => {
                stats.session_switches += 1;
                if let Some(ui_state) = &ui_state {
                    ui_state.record_session_switch(packet.header.session_id, packet.header.seq);
                }
                if verbose_logs {
                    info!(old_session = active, new_session = packet.header.session_id, "switching active session");
                }
                current_session_id = Some(packet.header.session_id);
                next_seq = packet.header.seq;
                buffered.clear();
                seen.clear();
            }
            Some(_) => {}
        }

        if packet.header.seq < next_seq {
            if let Some(ui_state) = &ui_state {
                ui_state.record_duplicate(buffered.len() as u64, next_seq);
            }
            stats.duplicate_or_late_packets += 1;
            if verbose_logs {
                info!(session_id = packet.header.session_id, seq = packet.header.seq, next_seq, "dropping duplicate or late packet");
            }
            continue;
        }

        if !seen.insert(packet.header.seq) {
            if let Some(ui_state) = &ui_state {
                ui_state.record_duplicate(buffered.len() as u64, next_seq);
            }
            stats.duplicate_or_late_packets += 1;
            if verbose_logs {
                info!(session_id = packet.header.session_id, seq = packet.header.seq, "dropping duplicate packet");
            }
            continue;
        }

        if packet.header.seq == next_seq {
            forward_payload(&out_socket, out_udp, &packet.payload, packet.header).await?;
            if let Some(ui_state) = &ui_state {
                ui_state.record_forwarded(packet.payload.len() as u64, buffered.len() as u64, next_seq.wrapping_add(1));
            }
            stats.forwarded_packets += 1;
            stats.forwarded_payload_bytes += packet.payload.len() as u64;
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
                if let Some(ui_state) = &ui_state {
                    ui_state.record_forwarded(payload.len() as u64, buffered.len() as u64, next_seq.wrapping_add(1));
                }
                stats.forwarded_packets += 1;
                stats.forwarded_payload_bytes += payload.len() as u64;
                seen.remove(&next_seq);
                next_seq = next_seq.wrapping_add(1);
            }
        } else {
            buffered.insert(packet.header.seq, packet.payload);
            if let Some(ui_state) = &ui_state {
                ui_state.record_buffered(buffered.len() as u64, next_seq);
            }
            stats.buffered_packets = buffered.len() as u64;
            if verbose_logs {
                info!(
                    session_id = packet.header.session_id,
                    seq = packet.header.seq,
                    next_seq,
                    buffered = buffered.len(),
                    "buffered out-of-order packet"
                );
            }
        }
                stats.buffered_packets = buffered.len() as u64;
            }
            _ = log_tick.tick() => {
                log_server_stats(
                    &stats,
                    started_at,
                    &mut last_log_at,
                    &mut last_received_packets,
                    &mut last_received_bytes,
                    &mut last_forwarded_packets,
                    &mut last_forwarded_bytes,
                    buffered.len(),
                    seen.len(),
                    current_session_id,
                    next_seq,
                );
            }
        }
    }

    log_server_stats(
        &stats,
        started_at,
        &mut last_log_at,
        &mut last_received_packets,
        &mut last_received_bytes,
        &mut last_forwarded_packets,
        &mut last_forwarded_bytes,
        buffered.len(),
        seen.len(),
        current_session_id,
        next_seq,
    );

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
    info!(session_id = header.session_id, seq = header.seq, bytes = payload.len(), out_udp = %out_udp, "forwarded payload");
    Ok(())
}

fn collect_server_addrs(endpoint: &Endpoint) -> Vec<String> {
    let addr = endpoint.addr();
    let mut lines = Vec::new();
    lines.push(format!("server_id={}", endpoint.id()));
    for ip in addr.ip_addrs() {
        lines.push(format!("server_addr=ip:{ip}"));
    }
    for relay in addr.relay_urls() {
        lines.push(format!("server_addr=relay:{relay}"));
    }
    lines
}

fn print_server_address(endpoint: &Endpoint, lines: &[String]) {
    let _ = endpoint;
    for line in lines {
        println!("{line}");
    }
}

fn load_secret_key(secret: Option<&str>) -> Result<SecretKey> {
    match secret {
        Some(secret) => SecretKey::from_str(secret).context("invalid --secret; expected iroh secret key hex"),
        None => Ok(SecretKey::generate(&mut rand::rng())),
    }
}

fn log_connection_paths(side: &str, connection: &iroh::endpoint::Connection) {
    for path in connection.paths() {
        info!(
            side,
            selected = path.is_selected(),
            closed = path.is_closed(),
            transport = transport_kind(&path),
            remote_addr = %path.remote_addr(),
            "connection path"
        );
    }
}

fn transport_kind(path: &iroh::endpoint::PathInfo) -> &'static str {
    if path.is_ip() {
        "direct"
    } else if path.is_relay() {
        "relay"
    } else {
        "other"
    }
}

fn log_server_stats(
    stats: &ServerStats,
    started_at: Instant,
    last_log_at: &mut Instant,
    last_received_packets: &mut u64,
    last_received_bytes: &mut u64,
    last_forwarded_packets: &mut u64,
    last_forwarded_bytes: &mut u64,
    buffered_len: usize,
    seen_len: usize,
    current_session_id: Option<u32>,
    next_seq: u64,
) {
    let now = Instant::now();
    let total_elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    let interval_elapsed = now.duration_since(*last_log_at).as_secs_f64().max(0.001);

    let recv_packets_delta = stats.received_packets.saturating_sub(*last_received_packets);
    let recv_bytes_delta = stats
        .received_payload_bytes
        .saturating_sub(*last_received_bytes);
    let fwd_packets_delta = stats
        .forwarded_packets
        .saturating_sub(*last_forwarded_packets);
    let fwd_bytes_delta = stats
        .forwarded_payload_bytes
        .saturating_sub(*last_forwarded_bytes);

    let recv_mbps = recv_bytes_delta as f64 * 8.0 / interval_elapsed / 1_000_000.0;
    let fwd_mbps = fwd_bytes_delta as f64 * 8.0 / interval_elapsed / 1_000_000.0;
    let recv_pps = recv_packets_delta as f64 / interval_elapsed;
    let fwd_pps = fwd_packets_delta as f64 / interval_elapsed;
    let avg_recv_mbps = stats.received_payload_bytes as f64 * 8.0 / total_elapsed / 1_000_000.0;
    let avg_fwd_mbps = stats.forwarded_payload_bytes as f64 * 8.0 / total_elapsed / 1_000_000.0;

    info!(
        uptime_secs = total_elapsed,
        session_id = current_session_id.unwrap_or_default(),
        next_seq,
        recv_packets = stats.received_packets,
        recv_payload_bytes = stats.received_payload_bytes,
        recv_pps,
        recv_mbps,
        recv_avg_mbps = avg_recv_mbps,
        forwarded_packets = stats.forwarded_packets,
        forwarded_payload_bytes = stats.forwarded_payload_bytes,
        forwarded_pps = fwd_pps,
        forwarded_mbps = fwd_mbps,
        forwarded_avg_mbps = avg_fwd_mbps,
        duplicate_or_late = stats.duplicate_or_late_packets,
        buffered_packets = buffered_len,
        seen_sequences = seen_len,
        invalid_packets = stats.invalid_packets,
        session_switches = stats.session_switches,
        "server stats"
    );

    *last_log_at = now;
    *last_received_packets = stats.received_packets;
    *last_received_bytes = stats.received_payload_bytes;
    *last_forwarded_packets = stats.forwarded_packets;
    *last_forwarded_bytes = stats.forwarded_payload_bytes;
}

fn spawn_ffmpeg(
    ffmpeg_bin: &str,
    ffmpeg_input_udp: SocketAddr,
    rtmp: &str,
    tui_enabled: bool,
) -> Result<Child> {
    let input = format!(
        "udp://{}?fifo_size=1000000&overrun_nonfatal=1",
        ffmpeg_input_udp
    );

    let mut command = Command::new(ffmpeg_bin);
    command.args([
            "-fflags",
            "nobuffer",
            "-i",
            &input,
            "-c",
            "copy",
            "-f",
            "flv",
            rtmp,
        ]);

    command.stdin(Stdio::null());
    if tui_enabled {
        command.stdout(Stdio::null()).stderr(Stdio::null());
    } else {
        command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    }

    command
        .spawn()
        .with_context(|| format!("failed to spawn ffmpeg for RTMP output `{rtmp}`"))
}

fn init_tracing(ui_state: Option<tui::ServerUiState>) -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| if ui_state.is_some() { "warn".into() } else { "server=info".into() });
    let builder = tracing_subscriber::fmt().with_env_filter(env_filter);

    if let Some(ui_state) = ui_state {
        builder
            .with_writer(ui_state.log_writer())
            .try_init()
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    } else {
        builder
            .try_init()
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    }

    Ok(())
}

async fn wait_for_shutdown(ui_state: Option<tui::ServerUiState>) {
    if let Some(ui_state) = ui_state {
        loop {
            if ui_state.should_quit() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    } else {
        let _ = tokio::signal::ctrl_c().await;
    }
}
