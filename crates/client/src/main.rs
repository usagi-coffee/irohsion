mod tui;

use std::{
    ffi::CStr,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ptr,
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use clap::Parser;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, RelayUrl, SecretKey, TransportAddr,
    endpoint::presets,
};
use protocol::{PacketHeader, encode_packet};
use tokio::net::UdpSocket;
use tracing::{error, info};

const ALPN: &[u8] = b"irohsion/v1";
const MAX_UDP_PACKET_SIZE: usize = 65_507;
const DEFAULT_RELAY_URL: &str = "https://euc1-1.relay.n0.iroh-canary.iroh.link";

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    port: u16,
    #[arg(long)]
    endpoint: EndpointId,
    #[arg(long = "addr")]
    addrs: Vec<SocketAddr>,
    #[arg(long = "relay")]
    relays: Vec<RelayUrl>,
    #[arg(long = "interfaces", required = true, num_args = 1..)]
    interfaces: Vec<String>,
    #[arg(long)]
    tui: bool,
}

#[derive(Debug)]
struct InterfaceBinding {
    name: String,
    bind_addr: SocketAddrV4,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let ui = cli.tui.then(|| {
        tui::ClientUi::spawn(tui::ClientUiState::new(
            cli.port,
            cli.endpoint.to_string(),
            cli.interfaces.clone(),
        ))
    });
    init_tracing(ui.as_ref().map(|ui| ui.state.clone()))?;

    let interface_bindings = cli
        .interfaces
        .iter()
        .map(|name| resolve_interface_ipv4(name))
        .collect::<Result<Vec<_>>>()?;

    let listen_udp = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, cli.port));
    let server_addr = build_server_addr(cli.endpoint, &cli.addrs, &cli.relays)?;
    let session_id = current_session_id()?;
    let listen_socket = UdpSocket::bind(listen_udp)
        .await
        .with_context(|| format!("failed to bind local UDP ingest socket on {listen_udp}"))?;

    let mut paths = Vec::with_capacity(interface_bindings.len());
    for binding in interface_bindings {
        let path = connect_path(binding, server_addr.clone()).await?;
        if let Some(ui) = &ui {
            ui.state.record_path(
                path.interface_name.clone(),
                path.connection
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
        info!(
            interface = %path.interface_name,
            local_addr = %path.bound_addr,
            "connected interface-bound iroh path"
        );
        paths.push(path);
    }

    info!(session_id, udp_listen = %listen_udp, paths = paths.len(), "client ready");

    let mut seq = 0_u64;
    let mut buf = vec![0_u8; MAX_UDP_PACKET_SIZE];
    loop {
        let shutdown = wait_for_shutdown(ui.as_ref().map(|ui| ui.state.clone()));
        let (len, src) = tokio::select! {
            res = listen_socket.recv_from(&mut buf) => {
                res.context("failed reading from local UDP ingest socket")?
            }
            _ = shutdown => {
                break;
            }
        };
        if let Some(ui) = &ui {
            ui.state.record_ingest(len as u64, src.to_string());
        }
        let packet = Arc::new(encode_packet(
            PacketHeader { session_id, seq },
            &buf[..len],
        ));

        info!(seq, bytes = len, from = %src, "ingested udp packet");

        for path in &paths {
            if let Err(err) = path.send(packet.clone()) {
                if let Some(ui) = &ui {
                    ui.state.record_send_error(path.interface_name.clone(), err.to_string());
                }
                error!(interface = %path.interface_name, seq, error = %err, "failed to send duplicated packet");
            } else if let Some(ui) = &ui {
                ui.state.record_send(path.interface_name.clone(), len as u64);
            }
        }

        seq = seq.wrapping_add(1);
    }

    Ok(())
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

async fn connect_path(binding: InterfaceBinding, server_addr: EndpointAddr) -> Result<PathConnection> {
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(SecretKey::generate(&mut rand::rng()))
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(RelayMode::Default)
        .clear_ip_transports()
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
    log_connection_paths("client", &binding.name, &connection);

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

fn log_connection_paths(side: &str, interface_name: &str, connection: &iroh::endpoint::Connection) {
    for path in connection.paths() {
        info!(
            side,
            interface = interface_name,
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

fn current_session_id() -> Result<u32> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs();
    u32::try_from(seconds).context("current unix timestamp does not fit in u32 session_id")
}

async fn wait_for_shutdown(ui_state: Option<tui::ClientUiState>) {
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

fn init_tracing(ui_state: Option<tui::ClientUiState>) -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| if ui_state.is_some() { "warn".into() } else { "client=info".into() });
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
