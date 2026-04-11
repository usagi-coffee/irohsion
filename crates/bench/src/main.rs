use std::{
    ffi::CStr,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ptr,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use clap::Parser;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, RelayUrl, SecretKey, TransportAddr,
    endpoint::presets,
};
use protocol::{PacketHeader, encode_packet};
use tokio::time::{Instant, sleep_until};
use tracing::{error, info};

const ALPN: &[u8] = b"irohsion/v1";
const DEFAULT_RELAY_URL: &str = "https://euc1-1.relay.n0.iroh-canary.iroh.link";

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

#[derive(Debug)]
struct InterfaceBinding {
    name: String,
    bind_addr: SocketAddrV4,
}

struct PathConnection {
    interface_name: String,
    bound_addr: SocketAddrV4,
    connection: iroh::endpoint::Connection,
    _endpoint: Endpoint,
}

impl PathConnection {
    fn send(&self, packet: Arc<Bytes>) -> Result<()> {
        self.connection
            .send_datagram((*packet).clone())
            .map_err(|err| anyhow!("send_datagram failed on {}: {err}", self.interface_name))
    }
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
    let deadline = cli.duration_secs.map(|secs| start + Duration::from_secs(secs));

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

        let packet = Arc::new(encode_packet(
            PacketHeader { session_id, seq },
            &payload,
        ));
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
            info!(sent_packets, sent_payload_bytes, effective_mbps = mbps, "bench progress");
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

async fn connect_path(binding: InterfaceBinding, server_addr: EndpointAddr) -> Result<PathConnection> {
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(SecretKey::generate(&mut rand::rng()))
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(RelayMode::Default)
        .clear_ip_transports()
        .relay_bind_device(binding.name.clone())
        .bind_addr(binding.bind_addr)
        .with_context(|| format!("failed to configure bind for {}", binding.bind_addr))?
        .bind()
        .await
        .with_context(|| format!("failed to bind iroh endpoint for {}", binding.bind_addr))?;

    endpoint.online().await;

    let connection = endpoint
        .connect(server_addr, ALPN)
        .await
        .with_context(|| format!("failed to connect path {}", binding.name))?;
    log_connection_paths(&binding.name, &connection);

    Ok(PathConnection {
        interface_name: binding.name,
        bound_addr: binding.bind_addr,
        connection,
        _endpoint: endpoint,
    })
}

fn build_server_addr(
    endpoint: EndpointId,
    addrs: &[SocketAddr],
    relays: &[RelayUrl],
) -> Result<EndpointAddr> {
    let effective_relays = if relays.is_empty() {
        vec![
            RelayUrl::from_str(DEFAULT_RELAY_URL)
                .context("invalid built-in default relay URL")?,
        ]
    } else {
        relays.to_vec()
    };

    let transports = addrs
        .iter()
        .copied()
        .map(TransportAddr::Ip)
        .chain(effective_relays.into_iter().map(TransportAddr::Relay));

    Ok(EndpointAddr::from_parts(endpoint, transports))
}

fn resolve_interface_ipv4(name: &str) -> Result<InterfaceBinding> {
    let addr = find_interface_ipv4(name)?;
    Ok(InterfaceBinding {
        name: name.to_string(),
        bind_addr: SocketAddrV4::new(addr, 0),
    })
}

fn find_interface_ipv4(name: &str) -> Result<Ipv4Addr> {
    let mut ifaddrs = ptr::null_mut();
    let rc = unsafe { libc::getifaddrs(&mut ifaddrs) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("getifaddrs failed");
    }

    let mut cursor = ifaddrs;
    let mut saw_name = false;
    let mut found = None;

    while !cursor.is_null() {
        let item = unsafe { &*cursor };
        let iface_name = unsafe { CStr::from_ptr(item.ifa_name) }
            .to_string_lossy()
            .into_owned();

        if iface_name == name {
            saw_name = true;
            let addr_ptr = item.ifa_addr;
            if !addr_ptr.is_null() && unsafe { (*addr_ptr).sa_family as i32 } == libc::AF_INET {
                let sin = unsafe { *(addr_ptr as *const libc::sockaddr_in) };
                found = Some(Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr)));
                break;
            }
        }

        cursor = item.ifa_next;
    }

    unsafe {
        libc::freeifaddrs(ifaddrs);
    }

    if let Some(addr) = found {
        return Ok(addr);
    }
    if saw_name {
        bail!("interface `{name}` does not have an IPv4 address");
    }
    bail!("interface `{name}` does not exist");
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

fn transport_kind(path: &iroh::endpoint::PathInfo) -> &'static str {
    if path.is_ip() {
        "direct"
    } else if path.is_relay() {
        "relay"
    } else {
        "other"
    }
}

fn current_session_id() -> Result<u32> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs();
    u32::try_from(seconds).context("current unix timestamp does not fit in u32 session_id")
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "bench=info".into()),
        )
        .try_init();
}
