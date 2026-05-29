use std::{
    collections::BTreeMap,
    io::{self, Stdout},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use parking_lot::RwLock;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap},
};
use transport::transport_kind;

const CONNECTION_MBPS_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct ServerUiState {
    started_at: Instant,
    endpoint: Arc<RwLock<String>>,
    health_endpoint: Arc<RwLock<String>>,
    ticket: Arc<RwLock<Option<String>>>,
    udp_dest: String,
    server_addrs: Arc<RwLock<Vec<String>>>,
    quit_requested: Arc<AtomicBool>,
    received_packets: Arc<AtomicU64>,
    received_bytes: Arc<AtomicU64>,
    forwarded_packets: Arc<AtomicU64>,
    forwarded_bytes: Arc<AtomicU64>,
    duplicate_packets: Arc<AtomicU64>,
    skipped_packets: Arc<AtomicU64>,
    skipped_never_received_packets: Arc<AtomicU64>,
    late_after_skip_packets: Arc<AtomicU64>,
    fragment_incomplete_packets: Arc<AtomicU64>,
    repair_requests: Arc<AtomicU64>,
    fec_recovered_packets: Arc<AtomicU64>,
    send_pressure_drops: Arc<AtomicU64>,
    flow_resets: Arc<AtomicU64>,
    connection_resets: Arc<AtomicU64>,
    invalid_packets: Arc<AtomicU64>,
    buffered_packets: Arc<AtomicU64>,
    last_forwarded_seq: Arc<AtomicU64>,
    next_seq: Arc<AtomicU64>,
    connection_activity_counter: Arc<AtomicU64>,
    connections: Arc<RwLock<BTreeMap<String, ConnectionView>>>,
}

#[derive(Clone)]
pub struct PathRow {
    pub remote_addr: String,
    pub transport: String,
    pub selected: bool,
    pub status: String,
}

#[derive(Clone, Default)]
pub struct ConnectionView {
    pub received_packets: u64,
    pub received_bytes: u64,
    pub max_seq: Option<u64>,
    pub last_seq: Option<u64>,
    pub last_error: Option<String>,
    pub paths: Vec<PathRow>,
    pub last_activity: u64,
}

pub struct ServerUi {
    pub state: ServerUiState,
    _join: thread::JoinHandle<()>,
}

pub fn server_addrs(endpoint: &iroh::Endpoint) -> Vec<String> {
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

pub fn describe_paths(connection: &iroh::endpoint::Connection) -> Vec<PathRow> {
    let paths = connection.paths();
    let has_selected = paths.iter().any(|path| path.is_selected());
    paths
        .iter()
        .filter(|path| !has_selected || path.is_selected())
        .map(|path| PathRow {
            remote_addr: path.remote_addr().to_string(),
            transport: transport_kind(&path).to_string(),
            selected: path.is_selected(),
            status: "up".to_string(),
        })
        .collect()
}

impl ServerUiState {
    pub fn new(udp_dest: String) -> Self {
        Self {
            started_at: Instant::now(),
            endpoint: Arc::new(RwLock::new("-".to_string())),
            health_endpoint: Arc::new(RwLock::new("-".to_string())),
            ticket: Arc::new(RwLock::new(None)),
            udp_dest,
            server_addrs: Arc::new(RwLock::new(Vec::new())),
            quit_requested: Arc::new(AtomicBool::new(false)),
            received_packets: Arc::new(AtomicU64::new(0)),
            received_bytes: Arc::new(AtomicU64::new(0)),
            forwarded_packets: Arc::new(AtomicU64::new(0)),
            forwarded_bytes: Arc::new(AtomicU64::new(0)),
            duplicate_packets: Arc::new(AtomicU64::new(0)),
            skipped_packets: Arc::new(AtomicU64::new(0)),
            skipped_never_received_packets: Arc::new(AtomicU64::new(0)),
            late_after_skip_packets: Arc::new(AtomicU64::new(0)),
            fragment_incomplete_packets: Arc::new(AtomicU64::new(0)),
            repair_requests: Arc::new(AtomicU64::new(0)),
            fec_recovered_packets: Arc::new(AtomicU64::new(0)),
            send_pressure_drops: Arc::new(AtomicU64::new(0)),
            flow_resets: Arc::new(AtomicU64::new(0)),
            connection_resets: Arc::new(AtomicU64::new(0)),
            invalid_packets: Arc::new(AtomicU64::new(0)),
            buffered_packets: Arc::new(AtomicU64::new(0)),
            last_forwarded_seq: Arc::new(AtomicU64::new(0)),
            next_seq: Arc::new(AtomicU64::new(0)),
            connection_activity_counter: Arc::new(AtomicU64::new(0)),
            connections: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn set_endpoint(&self, endpoint: String) {
        *self.endpoint.write() = endpoint;
    }

    pub fn set_server_addrs(&self, addrs: Vec<String>) {
        *self.server_addrs.write() = addrs;
    }

    pub fn set_ticket(&self, ticket: Option<String>) {
        *self.ticket.write() = ticket;
    }

    pub fn set_health_endpoint(&self, endpoint: Option<String>) {
        *self.health_endpoint.write() = endpoint.unwrap_or_else(|| "-".to_string());
    }

    pub fn request_quit(&self) {
        self.quit_requested.store(true, Ordering::Relaxed);
    }

    pub fn should_quit(&self) -> bool {
        self.quit_requested.load(Ordering::Relaxed)
    }

    pub fn record_connection(&self, remote: String, rows: Vec<PathRow>) {
        let mut connections = self.connections.write();
        let activity = self
            .connection_activity_counter
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let entry = connections.entry(remote).or_default();
        entry.paths = rows;
        entry.last_activity = activity;
    }

    pub fn record_disconnect(&self, remote: String, error: String) {
        let mut connections = self.connections.write();
        let activity = self
            .connection_activity_counter
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let entry = connections.entry(remote).or_default();
        entry.last_error = Some(error.clone());
        entry.last_activity = activity;
        entry.paths.iter_mut().for_each(|row| {
            row.status = format!("closed: {error}");
        });
    }

    pub fn record_connection_reset(&self) {
        self.connection_resets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection_receive(&self, remote: &str, bytes: u64, sequence: u64) {
        let mut connections = self.connections.write();
        let activity = self
            .connection_activity_counter
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let entry = connections.entry(remote.to_string()).or_default();
        entry.last_error = None;
        entry.paths.iter_mut().for_each(|row| {
            if row.status.starts_with("closed:") {
                row.status = "up".to_string();
            }
        });
        entry.received_packets += 1;
        entry.received_bytes += bytes;
        entry.last_seq = Some(sequence);
        entry.max_seq = Some(
            entry
                .max_seq
                .map_or(sequence, |current| current.max(sequence)),
        );
        entry.last_activity = activity;
    }

    pub fn record_received(&self, bytes: u64) {
        self.received_packets.fetch_add(1, Ordering::Relaxed);
        self.received_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_forwarded(&self, bytes: u64, buffered: u64, forwarded_seq: u64, next_seq: u64) {
        self.forwarded_packets.fetch_add(1, Ordering::Relaxed);
        self.forwarded_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.buffered_packets.store(buffered, Ordering::Relaxed);
        self.last_forwarded_seq
            .store(forwarded_seq, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    pub fn record_duplicate(&self, buffered: u64, next_seq: u64) {
        self.duplicate_packets.fetch_add(1, Ordering::Relaxed);
        self.buffered_packets.store(buffered, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    pub fn record_never_received_skip(&self, buffered: u64, next_seq: u64) {
        self.skipped_packets.fetch_add(1, Ordering::Relaxed);
        self.skipped_never_received_packets
            .fetch_add(1, Ordering::Relaxed);
        self.buffered_packets.store(buffered, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    pub fn record_fragment_incomplete_skip(&self, buffered: u64, next_seq: u64) {
        self.skipped_packets.fetch_add(1, Ordering::Relaxed);
        self.fragment_incomplete_packets
            .fetch_add(1, Ordering::Relaxed);
        self.buffered_packets.store(buffered, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    pub fn record_late_after_skip(&self, buffered: u64, next_seq: u64) {
        self.late_after_skip_packets.fetch_add(1, Ordering::Relaxed);
        self.buffered_packets.store(buffered, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    pub fn record_send_pressure_drop(&self) {
        self.send_pressure_drops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_repair_request(&self) {
        self.repair_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_fec_recovered(&self) {
        self.fec_recovered_packets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_flow_reset(&self) {
        self.flow_resets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_invalid(&self) {
        self.invalid_packets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_buffered(&self, buffered: u64, next_seq: u64) {
        self.buffered_packets.store(buffered, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    pub fn set_flow_start(&self, next_seq: u64) {
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub fn skipped_never_received_packets(&self) -> u64 {
        self.skipped_never_received_packets.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn late_after_skip_packets(&self) -> u64 {
        self.late_after_skip_packets.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn fragment_incomplete_packets(&self) -> u64 {
        self.fragment_incomplete_packets.load(Ordering::Relaxed)
    }
}

impl ServerUi {
    pub fn spawn(state: ServerUiState) -> Self {
        let thread_state = state.clone();
        let join = thread::spawn(move || {
            let _ = run(thread_state);
        });
        Self { state, _join: join }
    }
}

fn run(state: ServerUiState) -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut snapshot = Snapshot::default();

    loop {
        terminal.draw(|frame| draw(frame, &state, &mut snapshot))?;
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                    || matches!(key.code, KeyCode::Char('c'))
                        && key
                            .modifiers
                            .contains(crossterm::event::KeyModifiers::CONTROL)
                {
                    state.request_quit();
                    break;
                }
                match key.code {
                    KeyCode::Down | KeyCode::Char('j') => {
                        snapshot.connection_scroll = snapshot.connection_scroll.saturating_add(1);
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        snapshot.connection_scroll = snapshot.connection_scroll.saturating_sub(1);
                    }
                    KeyCode::PageDown => {
                        snapshot.connection_scroll = snapshot.connection_scroll.saturating_add(10);
                    }
                    KeyCode::PageUp => {
                        snapshot.connection_scroll = snapshot.connection_scroll.saturating_sub(10);
                    }
                    KeyCode::Home => {
                        snapshot.connection_scroll = 0;
                    }
                    _ => {}
                }
            }
        }
    }

    restore(terminal)
}

fn restore(mut terminal: Terminal<CrosstermBackend<Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()
}

#[derive(Default)]
struct Snapshot {
    last_at: Option<Instant>,
    last_received_bytes: u64,
    last_forwarded_bytes: u64,
    connection_last_bytes: BTreeMap<String, u64>,
    last_connection_rate_at: Option<Instant>,
    connection_mbps: BTreeMap<String, f64>,
    connection_scroll: usize,
}

fn draw(frame: &mut ratatui::Frame<'_>, state: &ServerUiState, snapshot: &mut Snapshot) {
    let area = frame.area();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Min(14),
        ])
        .split(area);

    let now = Instant::now();
    let elapsed = state.started_at.elapsed().as_secs_f64().max(0.001);
    let delta = snapshot
        .last_at
        .map(|last| now.duration_since(last).as_secs_f64().max(0.001))
        .unwrap_or(1.0);
    let recv_bytes = state.received_bytes.load(Ordering::Relaxed);
    let fwd_bytes = state.forwarded_bytes.load(Ordering::Relaxed);
    let recv_mbps = (recv_bytes.saturating_sub(snapshot.last_received_bytes)) as f64 * 8.0
        / delta
        / 1_000_000.0;
    let fwd_mbps = (fwd_bytes.saturating_sub(snapshot.last_forwarded_bytes)) as f64 * 8.0
        / delta
        / 1_000_000.0;
    snapshot.last_at = Some(now);
    snapshot.last_received_bytes = recv_bytes;
    snapshot.last_forwarded_bytes = fwd_bytes;

    if snapshot
        .last_connection_rate_at
        .map(|last| now.duration_since(last) >= CONNECTION_MBPS_INTERVAL)
        .unwrap_or(true)
    {
        let rate_delta = snapshot
            .last_connection_rate_at
            .map(|last| now.duration_since(last).as_secs_f64().max(0.001))
            .unwrap_or(CONNECTION_MBPS_INTERVAL.as_secs_f64());
        let connections = state.connections.read();
        let mut connection_mbps = BTreeMap::new();
        for (remote, connection) in connections.iter() {
            let previous = snapshot
                .connection_last_bytes
                .get(remote)
                .copied()
                .unwrap_or(0);
            connection_mbps.insert(
                remote.clone(),
                (connection.received_bytes.saturating_sub(previous)) as f64 * 8.0
                    / rate_delta
                    / 1_000_000.0,
            );
        }
        snapshot.last_connection_rate_at = Some(now);
        snapshot.connection_last_bytes = connections
            .iter()
            .map(|(remote, connection)| (remote.clone(), connection.received_bytes))
            .collect();
        snapshot.connection_mbps = connection_mbps;
    }

    let header = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                "Irohsion Server",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(
                format!("uptime {}", fmt_uptime(state.started_at.elapsed())),
                Style::default().fg(Color::Gray),
            ),
        ]),
        Line::from(format!("endpoint {}", state.endpoint.read().clone())),
        Line::from(format!(
            "health endpoint {}",
            state.health_endpoint.read().clone()
        )),
        Line::from(format!("udp destination {}", state.udp_dest)),
        Line::from(format!(
            "direct {}",
            format_addrs(&state.server_addrs.read(), "ip:")
        )),
        Line::from(format!(
            "relay  {}",
            format_addrs(&state.server_addrs.read(), "relay:")
        )),
        Line::from(format!(
            "ticket {}",
            state.ticket.read().as_deref().unwrap_or("-")
        )),
    ])
    .block(Block::default().borders(Borders::ALL).title("Overview"))
    .wrap(Wrap { trim: true });
    frame.render_widget(header, layout[0]);

    let stats = Paragraph::new(vec![
        Line::from(format!(
            "receive {:>8.2} Mbps   packets {:>8}   total {:>8.2} MB",
            recv_mbps,
            state.received_packets.load(Ordering::Relaxed),
            recv_bytes as f64 / 1_000_000.0
        )),
        Line::from(format!(
            "forward {:>8.2} Mbps   packets {:>8}   total {:>8.2} MB",
            fwd_mbps,
            state.forwarded_packets.load(Ordering::Relaxed),
            fwd_bytes as f64 / 1_000_000.0
        )),
        Line::from(format!(
            "dupes {:>9}   skipped {:>7}   invalid {:>8}   buffered {:>7}",
            state.duplicate_packets.load(Ordering::Relaxed),
            state.skipped_packets.load(Ordering::Relaxed),
            state.invalid_packets.load(Ordering::Relaxed),
            state.buffered_packets.load(Ordering::Relaxed)
        )),
        Line::from(format!(
            "missing never {:>7}   late {:>7}   frag_incomplete {:>7}",
            state.skipped_never_received_packets.load(Ordering::Relaxed),
            state.late_after_skip_packets.load(Ordering::Relaxed),
            state.fragment_incomplete_packets.load(Ordering::Relaxed)
        )),
        Line::from(format!(
            "fec_recovered {:>5}   repair_req {:>7}   pressure_drop {:>5}   flow_reset {:>5}   connection_reset {:>5}",
            state.fec_recovered_packets.load(Ordering::Relaxed),
            state.repair_requests.load(Ordering::Relaxed),
            state.send_pressure_drops.load(Ordering::Relaxed),
            state.flow_resets.load(Ordering::Relaxed),
            state.connection_resets.load(Ordering::Relaxed)
        )),
        Line::from(format!(
            "last_fwd {:>10}   next_seq {:>10}   avg fwd {:>6.2} Mbps",
            state.last_forwarded_seq.load(Ordering::Relaxed),
            state.next_seq.load(Ordering::Relaxed),
            fwd_bytes as f64 * 8.0 / elapsed / 1_000_000.0
        )),
    ])
    .block(Block::default().borders(Borders::ALL).title("Flow"))
    .wrap(Wrap { trim: true });
    frame.render_widget(stats, layout[1]);

    let connections = state.connections.read();
    let mut ordered = connections
        .iter()
        .map(|(remote, connection)| (remote.clone(), connection.clone()))
        .collect::<Vec<_>>();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));

    let all_rows = ordered
        .iter()
        .flat_map(|(remote, connection)| {
            let conn_mbps = snapshot.connection_mbps.get(remote).copied().unwrap_or(0.0);
            connection.paths.iter().map(move |row| {
                Row::new(vec![
                    Cell::from(remote.clone()),
                    Cell::from(connection.received_packets.to_string()),
                    Cell::from(
                        connection
                            .max_seq
                            .map_or_else(|| "-".to_string(), |value| value.to_string()),
                    ),
                    Cell::from(
                        connection
                            .last_seq
                            .map_or_else(|| "-".to_string(), |value| value.to_string()),
                    ),
                    Cell::from(format!(
                        "{:.2} MB",
                        connection.received_bytes as f64 / 1_000_000.0
                    )),
                    Cell::from(format!("{:.2}", conn_mbps)),
                    Cell::from(row.transport.clone()),
                    Cell::from(if row.selected { "yes" } else { "no" }),
                    Cell::from(row.status.clone()),
                    Cell::from(row.remote_addr.clone()),
                ])
            })
        })
        .collect::<Vec<_>>();

    let visible_rows = layout[2].height.saturating_sub(3) as usize;
    let max_scroll = all_rows.len().saturating_sub(visible_rows);
    snapshot.connection_scroll = snapshot.connection_scroll.min(max_scroll);
    let rows = all_rows
        .into_iter()
        .skip(snapshot.connection_scroll)
        .take(visible_rows)
        .collect::<Vec<_>>();
    let table = Table::new(
        rows,
        [
            Constraint::Length(18),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(18),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new(vec![
            "Remote",
            "Recv Pkts",
            "Max",
            "Last",
            "Recv MB",
            "Mbps",
            "Transport",
            "Selected",
            "Status",
            "Path",
        ])
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(Block::default().borders(Borders::ALL).title(format!(
        "Connections {}-{}",
        snapshot.connection_scroll.saturating_add(1),
        snapshot
            .connection_scroll
            .saturating_add(visible_rows)
            .min(max_scroll.saturating_add(visible_rows))
    )));
    frame.render_widget(table, layout[2]);
}

fn fmt_uptime(elapsed: Duration) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        elapsed.as_secs() / 3600,
        (elapsed.as_secs() / 60) % 60,
        elapsed.as_secs() % 60
    )
}

fn format_addrs(lines: &[String], kind: &str) -> String {
    let values = lines
        .iter()
        .filter_map(|line| {
            line.split_once(&format!("server_addr={kind}"))
                .map(|(_, value)| value)
        })
        .collect::<Vec<_>>();

    if values.is_empty() {
        "-".to_string()
    } else {
        values.join("  |  ")
    }
}
