use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Result, bail};
use clap::Parser;
use iroh::{EndpointId, RelayUrl};
use protocol::{PacketHeader, encode_packet};
use tokio::time::{Instant, sleep_until};
use tracing::{error, info};
use transport::{
    build_server_addr, connect_path, current_session_id, resolve_interface_ipv4, transport_kind,
};

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    endpoint: EndpointId,
    #[arg(long = "addr")]
    addrs: Vec<SocketAddr>,
    #[arg(long = "relay")]
    relays: Vec<RelayUrl>,
    #[arg(long = "interfaces", required = true, num_args = 1..)]
    interfaces: Vec<String>,
    #[arg(long, default_value_t = 8.0)]
    throughput_mbps: f64,
    #[arg(long, default_value_t = 1316)]
    packet_size: usize,
    #[arg(long)]
    duration_secs: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    validate_cli(&cli)?;

    let interface_bindings = cli
        .interfaces
        .iter()
        .map(|name| resolve_interface_ipv4(name))
        .collect::<Result<Vec<_>>>()?;
    let remote_addr = build_server_addr(cli.endpoint, &cli.addrs, &cli.relays)?;

    let mut paths = Vec::with_capacity(interface_bindings.len());
    for binding in interface_bindings {
        let path = connect_path(binding, remote_addr.clone()).await?;
        log_connection_paths(&path.interface_name, &path.connection);
        info!(
            interface = %path.interface_name,
            local_addr = %path.bound_addr,
            "connected interface-bound iroh path"
        );
        paths.push(path);
    }

    let session_id = current_session_id()?;
    let payload = vec![0x42_u8; cli.packet_size];
    let bytes_per_second = cli.throughput_mbps * 1_000_000.0 / 8.0;
    let packets_per_second = bytes_per_second / cli.packet_size as f64;
    let interval = Duration::from_secs_f64(1.0 / packets_per_second);
    let start = Instant::now();
    let mut next_tick = start;
    let deadline = cli
        .duration_secs
        .map(|secs| start + Duration::from_secs(secs));

    info!(
        session_id,
        throughput_mbps = cli.throughput_mbps,
        packet_size = cli.packet_size,
        packets_per_second,
        paths = paths.len(),
        "bench sender ready"
    );

    let mut seq = 0_u64;
    let mut sent_packets = 0_u64;
    let mut sent_payload_bytes = 0_u64;
    loop {
        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            break;
        }

        let packet = Arc::new(encode_packet(PacketHeader { session_id, seq }, &payload));
        for path in &paths {
            if let Err(err) = path.send(packet.clone()) {
                error!(interface = %path.interface_name, seq, error = %err, "failed to send bench packet");
            }
        }

        seq = seq.wrapping_add(1);
        sent_packets += 1;
        sent_payload_bytes += cli.packet_size as u64;

        if sent_packets % 1000 == 0 {
            let elapsed = start.elapsed().as_secs_f64().max(0.001);
            let mbps = sent_payload_bytes as f64 * 8.0 / elapsed / 1_000_000.0;
            info!(
                sent_packets,
                sent_payload_bytes,
                effective_mbps = mbps,
                "bench progress"
            );
        }

        next_tick += interval;
        sleep_until(next_tick).await;
    }

    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    let mbps = sent_payload_bytes as f64 * 8.0 / elapsed / 1_000_000.0;
    info!(
        sent_packets,
        sent_payload_bytes,
        elapsed_secs = elapsed,
        effective_mbps = mbps,
        "bench complete"
    );
    Ok(())
}

fn validate_cli(cli: &Cli) -> Result<()> {
    if cli.packet_size == 0 {
        bail!("--packet-size must be greater than zero");
    }
    if !(cli.throughput_mbps.is_finite() && cli.throughput_mbps > 0.0) {
        bail!("--throughput-mbps must be a finite positive number");
    }
    Ok(())
}

fn log_connection_paths(interface_name: &str, connection: &iroh::endpoint::Connection) {
    for path in connection.paths() {
        info!(
            interface = interface_name,
            selected = path.is_selected(),
            closed = path.is_closed(),
            transport = transport_kind(&path),
            remote_addr = %path.remote_addr(),
            "bench connection path"
        );
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "bench=info".into()),
        )
        .try_init();
}
