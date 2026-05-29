use std::{
    ffi::CStr,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ptr,
    str::FromStr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
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
    pub achieved_mbps: f32,
    pub last_seq: Option<u64>,
    pub max_seq: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ChatMessage {
    pub id: u64,
    pub unix_ms: u64,
    pub user: String,
    pub text: String,
}

#[derive(Clone, Debug)]
pub struct HealthReport {
    pub seq: u64,
    pub unix_ms: u64,
    pub endpoints: Vec<EndpointHealth>,
    pub chat: Vec<ChatMessage>,
}

#[derive(Clone, Debug)]
pub struct InterfaceBinding {
    pub name: String,
    pub display_name: String,
    pub bind_addr: SocketAddrV4,
}

#[derive(Clone)]
pub struct PathConnection {
    pub interface_name: String,
    pub display_name: String,
    pub bound_addr: SocketAddrV4,
    server_addr: EndpointAddr,
    secret_key: SecretKey,
    relays: Vec<RelayUrl>,
    live: Arc<RwLock<Option<LivePath>>>,
    reconnect_requested: Arc<AtomicBool>,
}

struct LivePath {
    binding: InterfaceBinding,
    connection: iroh::endpoint::Connection,
    endpoint: Endpoint,
}

impl PathConnection {
    pub fn send(&self, packet: Arc<Bytes>) -> Result<()> {
        let connection = self
            .connection()
            .with_context(|| format!("path {} has no live connection", self.display_name))?;
        connection
            .send_datagram((*packet).clone())
            .map_err(|err| anyhow!("send_datagram failed on {}: {err}", self.display_name))
    }

    pub fn connection(&self) -> Option<iroh::endpoint::Connection> {
        self.live
            .read()
            .expect("path live lock poisoned")
            .as_ref()
            .map(|live| live.connection.clone())
    }

    pub fn connection_id(&self) -> Option<usize> {
        self.connection().map(|connection| connection.stable_id())
    }

    pub fn endpoint(&self) -> Option<Endpoint> {
        self.live
            .read()
            .expect("path live lock poisoned")
            .as_ref()
            .map(|live| live.endpoint.clone())
    }

    pub fn current_bound_addr(&self) -> Option<SocketAddrV4> {
        self.live
            .read()
            .expect("path live lock poisoned")
            .as_ref()
            .map(|live| live.binding.bind_addr)
    }

    pub fn is_connected(&self) -> bool {
        self.live.read().expect("path live lock poisoned").is_some()
    }

    pub fn reconnect_requested(&self) -> bool {
        self.reconnect_requested.load(Ordering::Relaxed)
    }

    pub fn request_reconnect(&self) {
        self.reconnect_requested.store(true, Ordering::Relaxed);
    }

    pub fn pending(
        binding: InterfaceBinding,
        server_addr: EndpointAddr,
        secret_key: SecretKey,
        relays: &[RelayUrl],
    ) -> Self {
        Self {
            interface_name: binding.name,
            display_name: binding.display_name,
            bound_addr: binding.bind_addr,
            server_addr,
            secret_key,
            relays: relays.to_vec(),
            live: Arc::new(RwLock::new(None)),
            reconnect_requested: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn mark_failed(&self, connection_id: Option<usize>) -> Option<Endpoint> {
        let mut live = self.live.write().expect("path live lock poisoned");
        let should_clear = match (connection_id, live.as_ref()) {
            (Some(connection_id), Some(live)) => live.connection.stable_id() == connection_id,
            (None, Some(_)) => true,
            _ => false,
        };
        if !should_clear {
            return None;
        }

        self.request_reconnect();
        live.take().map(|live| live.endpoint)
    }

    pub fn mark_interface_changed(&self, binding: InterfaceBinding) -> Option<Endpoint> {
        let mut live = self.live.write().expect("path live lock poisoned");
        let should_clear = live
            .as_ref()
            .is_some_and(|live| live.binding.bind_addr.ip() != binding.bind_addr.ip());
        if !should_clear {
            return None;
        }

        self.request_reconnect();
        live.take().map(|live| live.endpoint)
    }

    pub async fn reconnect(&self) -> Result<iroh::endpoint::Connection> {
        let mut binding = resolve_interface_ipv4(&self.interface_name)?;
        binding.display_name.clone_from(&self.display_name);
        let live = connect_live_path(
            binding,
            self.server_addr.clone(),
            self.secret_key.clone(),
            &self.relays,
        )
        .await?;
        let connection = live.connection.clone();
        let old = {
            let mut current = self.live.write().expect("path live lock poisoned");
            let old = current.replace(live);
            self.reconnect_requested.store(false, Ordering::Relaxed);
            old
        };
        if let Some(old) = old {
            old.endpoint.close().await;
        }

        Ok(connection)
    }
}

pub async fn connect_path(
    binding: InterfaceBinding,
    server_addr: EndpointAddr,
    relays: &[RelayUrl],
) -> Result<PathConnection> {
    connect_path_with_secret(binding, server_addr, SecretKey::generate(), relays).await
}

pub async fn connect_path_with_secret(
    binding: InterfaceBinding,
    server_addr: EndpointAddr,
    secret_key: SecretKey,
    relays: &[RelayUrl],
) -> Result<PathConnection> {
    let interface_name = binding.name.clone();
    let display_name = binding.display_name.clone();
    let live = connect_live_path(binding, server_addr.clone(), secret_key.clone(), relays).await?;
    let bound_addr = live.binding.bind_addr;

    Ok(PathConnection {
        interface_name,
        display_name,
        bound_addr,
        server_addr,
        secret_key,
        relays: relays.to_vec(),
        live: Arc::new(RwLock::new(Some(live))),
        reconnect_requested: Arc::new(AtomicBool::new(false)),
    })
}

async fn connect_live_path(
    binding: InterfaceBinding,
    server_addr: EndpointAddr,
    secret_key: SecretKey,
    relays: &[RelayUrl],
) -> Result<LivePath> {
    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec(), HEALTH_ALPN.to_vec()])
        .relay_mode(relay_mode(relays))
        .clear_ip_transports();

    let bind_device = binding.name.as_bytes().to_vec();
    builder = builder
        .direct_bind_device(bind_device.clone())
        .relay_bind_device(bind_device);
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
        .with_context(|| format!("failed to connect path {}", binding.display_name))?;

    Ok(LivePath {
        binding,
        connection,
        endpoint,
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

pub fn relay_mode(relays: &[RelayUrl]) -> RelayMode {
    if relays.is_empty() {
        RelayMode::Default
    } else {
        RelayMode::custom(relays.to_vec())
    }
}

pub fn resolve_interface_ipv4(name: &str) -> Result<InterfaceBinding> {
    let addr = find_interface_ipv4(name)?;
    Ok(InterfaceBinding {
        name: name.to_string(),
        display_name: name.to_string(),
        bind_addr: SocketAddrV4::new(addr, 0),
    })
}

pub fn transport_kind(path: &iroh::endpoint::Path<'_>) -> &'static str {
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
            "endpoint\t{}\t{:.4}\t{}\t{}\n",
            endpoint.endpoint_id,
            endpoint.achieved_mbps,
            endpoint
                .last_seq
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            endpoint
                .max_seq
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
    }
    for message in &report.chat {
        payload.push_str(&format!(
            "chat\t{}\t{}\t{}\t{}\n",
            message.id,
            message.unix_ms,
            encode_health_text(&message.user),
            encode_health_text(&message.text)
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
    let mut chat = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }

        let mut fields = line.split('\t');
        let kind = fields.next().context("missing health report row kind")?;
        if kind == "chat" {
            let id = fields
                .next()
                .context("missing chat id")?
                .parse()
                .context("invalid chat id")?;
            let unix_ms = fields
                .next()
                .context("missing chat unix_ms")?
                .parse()
                .context("invalid chat unix_ms")?;
            let user = decode_health_text(fields.next().context("missing chat user")?);
            let text = decode_health_text(fields.next().context("missing chat text")?);
            chat.push(ChatMessage {
                id,
                unix_ms,
                user,
                text,
            });
            continue;
        }

        if kind != "endpoint" {
            bail!("unexpected health report row kind");
        }

        let endpoint_id = fields.next().context("missing endpoint id")?.to_string();
        let values = fields.collect::<Vec<_>>();
        let (achieved_mbps, last_seq_value, max_seq_value) = match values.as_slice() {
            [achieved_mbps, last_seq] => (*achieved_mbps, Some(*last_seq), None),
            [achieved_mbps, last_seq, max_seq] => (*achieved_mbps, Some(*last_seq), Some(*max_seq)),
            // Backward-compatible with older rows:
            // endpoint <id> <target_mbps> <achieved_mbps> <last_seq> <max_seq>
            [_target_mbps, achieved_mbps, last_seq, max_seq] => {
                (*achieved_mbps, Some(*last_seq), Some(*max_seq))
            }
            _ => bail!("invalid endpoint health row"),
        };
        let achieved_mbps = achieved_mbps
            .parse()
            .context("invalid achieved throughput")?;
        let last_seq = match last_seq_value {
            Some("-") | None => None,
            Some(value) => Some(value.parse().context("invalid last_seq")?),
        };
        let max_seq = match max_seq_value {
            Some("-") | None => last_seq,
            Some(value) => Some(value.parse().context("invalid max_seq")?),
        };

        endpoints.push(EndpointHealth {
            endpoint_id,
            achieved_mbps,
            last_seq,
            max_seq,
        });
    }

    Ok(HealthReport {
        seq,
        unix_ms,
        endpoints,
        chat,
    })
}

fn encode_health_text(text: &str) -> String {
    let mut encoded = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\\' => encoded.push_str("\\\\"),
            '\t' => encoded.push_str("\\t"),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            _ => encoded.push(ch),
        }
    }
    encoded
}

fn decode_health_text(text: &str) -> String {
    let mut decoded = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }

        match chars.next() {
            Some('\\') => decoded.push('\\'),
            Some('t') => decoded.push('\t'),
            Some('n') => decoded.push('\n'),
            Some('r') => decoded.push('\r'),
            Some(other) => {
                decoded.push('\\');
                decoded.push(other);
            }
            None => decoded.push('\\'),
        }
    }
    decoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_report_round_trips_chat_messages() {
        let report = HealthReport {
            seq: 7,
            unix_ms: 1234,
            endpoints: vec![EndpointHealth {
                endpoint_id: "endpoint".to_string(),
                achieved_mbps: 1.25,
                last_seq: Some(10),
                max_seq: Some(12),
            }],
            chat: vec![ChatMessage {
                id: 42,
                unix_ms: 5678,
                user: "viewer\tname".to_string(),
                text: "hello\\nnot newline\nsecond line".to_string(),
            }],
        };

        let decoded = decode_health_report(&encode_health_report(&report)).unwrap();

        assert_eq!(decoded.seq, report.seq);
        assert_eq!(decoded.unix_ms, report.unix_ms);
        assert_eq!(decoded.endpoints.len(), 1);
        assert_eq!(decoded.chat.len(), 1);
        assert_eq!(decoded.chat[0].id, report.chat[0].id);
        assert_eq!(decoded.chat[0].unix_ms, report.chat[0].unix_ms);
        assert_eq!(decoded.chat[0].user, report.chat[0].user);
        assert_eq!(decoded.chat[0].text, report.chat[0].text);
    }
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
