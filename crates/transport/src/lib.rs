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
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, RelayUrl, SecretKey, TransportAddr,
    endpoint::presets,
};

pub const ALPN: &[u8] = b"irohsion/v1";
pub const HEALTH_ALPN: &[u8] = b"irohsion/health/v1";
const HEALTH_REPORT_MAGIC: &str = "irohsion-health/v2";
const DEFAULT_RELAY_URL: &str = "https://euc1-1.relay.n0.iroh-canary.iroh.link";

#[derive(Clone, Debug)]
pub struct EndpointHealth {
    pub endpoint_id: String,
    pub target_mbps: f32,
    pub achieved_mbps: f32,
}

#[derive(Clone, Debug)]
pub struct HealthReport {
    pub seq: u64,
    pub unix_ms: u64,
    pub endpoints: Vec<EndpointHealth>,
}

#[derive(Debug)]
pub struct InterfaceBinding {
    pub name: String,
    pub bind_addr: SocketAddrV4,
}

pub struct PathConnection {
    pub interface_name: String,
    pub bound_addr: SocketAddrV4,
    pub connection: iroh::endpoint::Connection,
    _endpoint: Endpoint,
}

impl PathConnection {
    pub fn send(&self, packet: Arc<Bytes>) -> Result<()> {
        self.connection
            .send_datagram((*packet).clone())
            .map_err(|err| anyhow!("send_datagram failed on {}: {err}", self.interface_name))
    }
}

pub async fn connect_path(
    binding: InterfaceBinding,
    server_addr: EndpointAddr,
) -> Result<PathConnection> {
    connect_path_with_secret(binding, server_addr, SecretKey::generate()).await
}

pub async fn connect_path_with_secret(
    binding: InterfaceBinding,
    server_addr: EndpointAddr,
    secret_key: SecretKey,
) -> Result<PathConnection> {
    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(RelayMode::Default)
        .clear_ip_transports();

    builder = builder.relay_bind_device(binding.name.clone());
    let endpoint = builder
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

    Ok(PathConnection {
        interface_name: binding.name,
        bound_addr: binding.bind_addr,
        connection,
        _endpoint: endpoint,
    })
}

pub fn build_server_addr(
    endpoint: EndpointId,
    addrs: &[SocketAddr],
    relays: &[RelayUrl],
) -> Result<EndpointAddr> {
    let effective_relays = if relays.is_empty() {
        vec![RelayUrl::from_str(DEFAULT_RELAY_URL).context("invalid built-in default relay URL")?]
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

pub fn resolve_interface_ipv4(name: &str) -> Result<InterfaceBinding> {
    let addr = find_interface_ipv4(name)?;
    Ok(InterfaceBinding {
        name: name.to_string(),
        bind_addr: SocketAddrV4::new(addr, 0),
    })
}

pub fn transport_kind(path: &iroh::endpoint::PathInfo) -> &'static str {
    if path.is_ip() {
        "direct"
    } else if path.is_relay() {
        "relay"
    } else {
        "other"
    }
}

pub fn current_session_id() -> Result<u32> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs();
    u32::try_from(seconds).context("current unix timestamp does not fit in u32 session_id")
}

pub fn encode_health_report(report: &HealthReport) -> Bytes {
    let mut payload = format!(
        "{HEALTH_REPORT_MAGIC}\t{}\t{}\n",
        report.seq, report.unix_ms
    );
    for endpoint in &report.endpoints {
        payload.push_str(&format!(
            "endpoint\t{}\t{:.4}\t{:.4}\n",
            endpoint.endpoint_id, endpoint.target_mbps, endpoint.achieved_mbps
        ));
    }
    Bytes::from(payload)
}

pub fn decode_health_report(data: &[u8]) -> Result<HealthReport> {
    let text = std::str::from_utf8(data).context("health report is not valid utf-8")?;
    let mut lines = text.lines();
    let header = lines.next().context("missing health report header")?;
    let mut header_fields = header.split('\t');
    let magic = header_fields
        .next()
        .context("missing health report magic")?;
    if magic != HEALTH_REPORT_MAGIC {
        bail!("unexpected health report magic");
    }

    let seq = header_fields
        .next()
        .context("missing health report seq")?
        .parse()
        .context("invalid health report seq")?;
    let unix_ms = header_fields
        .next()
        .context("missing health report unix_ms")?
        .parse()
        .context("invalid health report unix_ms")?;
    let mut endpoints = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }

        let mut fields = line.split('\t');
        let kind = fields.next().context("missing health report row kind")?;
        if kind != "endpoint" {
            bail!("unexpected health report row kind");
        }

        endpoints.push(EndpointHealth {
            endpoint_id: fields.next().context("missing endpoint id")?.to_string(),
            target_mbps: fields
                .next()
                .context("missing target throughput")?
                .parse()
                .context("invalid target throughput")?,
            achieved_mbps: fields
                .next()
                .context("missing achieved throughput")?
                .parse()
                .context("invalid achieved throughput")?,
        });
    }

    Ok(HealthReport {
        seq,
        unix_ms,
        endpoints,
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
