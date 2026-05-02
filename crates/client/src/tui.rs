use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, Stdout},
    path::PathBuf,
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
use tokio::sync::mpsc::UnboundedSender;
use transport::{EndpointHealth, transport_kind};

const ENDPOINT_PREFIX_LEN: usize = 8;
const CLIENT_PATH_MBPS_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy)]
pub enum UiCommand {
    SetAuto,
    SetRedundant,
    SetSplit,
    SetRoundRobin,
    ToggleWeightedAuto,
}

#[derive(Clone)]
pub struct ClientUiState {
    started_at: Instant,
    port: u16,
    server_endpoint: String,
    health_endpoint: Arc<RwLock<String>>,
    strategy_mode: Arc<RwLock<String>>,
    effective_strategy: Arc<RwLock<String>>,
    interfaces: Vec<String>,
    ingested_packets: Arc<AtomicU64>,
    ingested_bytes: Arc<AtomicU64>,
    sent_packets: Arc<AtomicU64>,
    sent_bytes: Arc<AtomicU64>,
    sent_bytes_by_interface: Arc<RwLock<BTreeMap<String, u64>>>,
    send_errors: Arc<AtomicU64>,
    last_ingest_from: Arc<RwLock<String>>,
    last_health_received: Arc<RwLock<Option<Instant>>>,
    paths: Arc<RwLock<BTreeMap<String, Vec<PathRow>>>>,
    logs: Arc<RwLock<VecDeque<String>>>,
    command_tx: Option<UnboundedSender<UiCommand>>,
    quit_requested: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct PathRow {
    pub endpoint_id: String,
    pub split_percentage: Option<f64>,
    pub server_mbps: String,
    pub last_health_at: Option<Instant>,
    pub remote_addr: String,
    pub transport: String,
    pub selected: bool,
    pub status: String,
}

pub struct ClientUi {
    pub state: ClientUiState,
    join: Option<thread::JoinHandle<()>>,
}

pub fn describe_paths(connection: &iroh::endpoint::Connection, endpoint_id: &str) -> Vec<PathRow> {
    let paths = selected_paths(connection);
    paths
        .map(|path| PathRow {
            endpoint_id: endpoint_id.to_string(),
            split_percentage: None,
            server_mbps: "0.00".to_string(),
            last_health_at: None,
            remote_addr: path.remote_addr().to_string(),
            transport: transport_kind(&path).to_string(),
            selected: path.is_selected(),
            status: if path.is_closed() { "closed" } else { "up" }.to_string(),
        })
        .collect()
}

fn selected_paths(
    connection: &iroh::endpoint::Connection,
) -> impl Iterator<Item = iroh::endpoint::PathInfo> {
    let paths = connection.paths().into_iter().collect::<Vec<_>>();
    let selected = paths
        .iter()
        .filter(|path| path.is_selected())
        .cloned()
        .collect::<Vec<_>>();
    if selected.is_empty() {
        paths.into_iter()
    } else {
        selected.into_iter()
    }
}

impl ClientUiState {
    pub fn new(
        port: u16,
        server_endpoint: String,
        health_endpoint: String,
        interfaces: Vec<String>,
        command_tx: Option<UnboundedSender<UiCommand>>,
    ) -> Self {
        Self {
            started_at: Instant::now(),
            port,
            server_endpoint,
            health_endpoint: Arc::new(RwLock::new(health_endpoint)),
            strategy_mode: Arc::new(RwLock::new("-".to_string())),
            effective_strategy: Arc::new(RwLock::new("-".to_string())),
            interfaces,
            ingested_packets: Arc::new(AtomicU64::new(0)),
            ingested_bytes: Arc::new(AtomicU64::new(0)),
            sent_packets: Arc::new(AtomicU64::new(0)),
            sent_bytes: Arc::new(AtomicU64::new(0)),
            sent_bytes_by_interface: Arc::new(RwLock::new(BTreeMap::new())),
            send_errors: Arc::new(AtomicU64::new(0)),
            last_ingest_from: Arc::new(RwLock::new("-".to_string())),
            last_health_received: Arc::new(RwLock::new(None)),
            paths: Arc::new(RwLock::new(BTreeMap::new())),
            logs: Arc::new(RwLock::new(VecDeque::with_capacity(128))),
            command_tx,
            quit_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn record_ingest(&self, bytes: u64, from: String) {
        self.ingested_packets.fetch_add(1, Ordering::Relaxed);
        self.ingested_bytes.fetch_add(bytes, Ordering::Relaxed);
        *self.last_ingest_from.write() = from;
    }

    pub fn record_send(&self, interface: String, bytes: u64) {
        self.sent_packets.fetch_add(1, Ordering::Relaxed);
        self.sent_bytes.fetch_add(bytes, Ordering::Relaxed);
        *self
            .sent_bytes_by_interface
            .write()
            .entry(interface.clone())
            .or_default() += bytes;
        self.paths
            .write()
            .entry(interface)
            .or_default()
            .iter_mut()
            .for_each(|row| row.status = "sending".to_string());
    }

    pub fn record_send_error(&self, interface: String, error: String) {
        self.send_errors.fetch_add(1, Ordering::Relaxed);
        let mut paths = self.paths.write();
        let rows = paths.entry(interface).or_default();
        if rows.is_empty() {
            rows.push(PathRow {
                endpoint_id: "-".to_string(),
                split_percentage: None,
                server_mbps: "0.00".to_string(),
                last_health_at: None,
                remote_addr: error,
                transport: "error".to_string(),
                selected: false,
                status: "failed".to_string(),
            });
            return;
        }

        for row in rows.iter_mut() {
            row.transport = "error".to_string();
            row.status = "failed".to_string();
            row.remote_addr = error.clone();
            row.selected = false;
            row.split_percentage = None;
        }
    }

    pub fn record_path(&self, interface: String, mut rows: Vec<PathRow>) {
        let mut paths = self.paths.write();
        if let Some(existing_rows) = paths.get(&interface) {
            for row in &mut rows {
                let existing = existing_rows
                    .iter()
                    .find(|existing| {
                        existing.endpoint_id == row.endpoint_id
                            && existing.transport == row.transport
                            && existing.remote_addr == row.remote_addr
                    })
                    .or_else(|| {
                        existing_rows
                            .iter()
                            .find(|existing| existing.endpoint_id == row.endpoint_id)
                    });
                if let Some(existing) = existing {
                    row.split_percentage = existing.split_percentage;
                    row.server_mbps.clone_from(&existing.server_mbps);
                    row.last_health_at = existing.last_health_at;
                }
            }
        }
        paths.insert(interface, rows);
    }

    pub fn record_split_percentages(&self, percentages: &BTreeMap<String, f64>) {
        for (interface, rows) in self.paths.write().iter_mut() {
            let percentage = percentages.get(interface).copied();
            for row in rows {
                row.split_percentage = percentage;
            }
        }
    }

    pub fn set_health_endpoint(&self, endpoint: String) {
        *self.health_endpoint.write() = endpoint;
    }

    pub fn record_strategy_state(&self, mode: String, effective: String) {
        *self.strategy_mode.write() = mode;
        *self.effective_strategy.write() = effective;
    }

    pub fn record_health_received(&self) {
        *self.last_health_received.write() = Some(Instant::now());
    }

    pub fn record_endpoint_health(&self, endpoints: &[EndpointHealth]) {
        let mut throughput_by_endpoint = BTreeMap::new();
        for endpoint in endpoints {
            throughput_by_endpoint.insert(
                endpoint.endpoint_id.clone(),
                format!("{:.2}", endpoint.achieved_mbps),
            );
        }

        let now = Instant::now();
        for rows in self.paths.write().values_mut() {
            for row in rows {
                if let Some(server_mbps) = throughput_by_endpoint.get(&row.endpoint_id) {
                    row.server_mbps = server_mbps.clone();
                    row.last_health_at = Some(now);
                } else {
                    row.server_mbps = "0.00".to_string();
                }
            }
        }
    }

    pub fn push_log_line(&self, line: String) {
        let mut logs = self.logs.write();
        if logs.len() >= 120 {
            logs.pop_front();
        }
        logs.push_back(line);
    }

    pub fn request_quit(&self) {
        self.quit_requested.store(true, Ordering::Relaxed);
    }

    pub fn should_quit(&self) -> bool {
        self.quit_requested.load(Ordering::Relaxed)
    }
}

impl ClientUi {
    pub fn spawn(state: ClientUiState) -> Self {
        let thread_state = state.clone();
        let join = thread::spawn(move || {
            let _ = run(thread_state);
        });
        Self {
            state,
            join: Some(join),
        }
    }
}

pub fn spawn_status_file_writer(
    state: ClientUiState,
    path: PathBuf,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        let mut snapshot = Snapshot::default();
        loop {
            ticker.tick().await;
            if state.should_quit() {
                break;
            }
            let contents = format_status_file(&state, &mut snapshot);
            if let Err(err) = write_status_file(&path, &contents) {
                state.push_log_line(format!(
                    "ERROR failed writing OBS status file path={} error={err}",
                    path.display()
                ));
            }
        }
    })
}

impl Drop for ClientUi {
    fn drop(&mut self) {
        self.state.request_quit();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn run(state: ClientUiState) -> io::Result<()> {
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
                    KeyCode::Char('r')
                        if key
                            .modifiers
                            .contains(crossterm::event::KeyModifiers::CONTROL) =>
                    {
                        if let Some(tx) = &state.command_tx {
                            let _ = tx.send(UiCommand::SetRoundRobin);
                        }
                    }
                    KeyCode::Char('a') => {
                        if let Some(tx) = &state.command_tx {
                            let _ = tx.send(UiCommand::SetAuto);
                        }
                    }
                    KeyCode::Char('r') => {
                        if let Some(tx) = &state.command_tx {
                            let _ = tx.send(UiCommand::SetRedundant);
                        }
                    }
                    KeyCode::Char('s') => {
                        if let Some(tx) = &state.command_tx {
                            let _ = tx.send(UiCommand::SetSplit);
                        }
                    }
                    KeyCode::Char('w') => {
                        if let Some(tx) = &state.command_tx {
                            let _ = tx.send(UiCommand::ToggleWeightedAuto);
                        }
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
    last_ingested_bytes: u64,
    last_sent_bytes: u64,
    last_client_rate_at: Option<Instant>,
    last_sent_bytes_by_interface: BTreeMap<String, u64>,
    client_mbps_by_interface: BTreeMap<String, f64>,
}

fn update_client_path_rates(state: &ClientUiState, snapshot: &mut Snapshot, now: Instant) {
    let sent_bytes_by_interface = state.sent_bytes_by_interface.read().clone();
    if snapshot
        .last_client_rate_at
        .is_some_and(|last| now.duration_since(last) < CLIENT_PATH_MBPS_INTERVAL)
    {
        return;
    }

    let rate_delta = snapshot
        .last_client_rate_at
        .map(|last| now.duration_since(last).as_secs_f64().max(0.001))
        .unwrap_or(CLIENT_PATH_MBPS_INTERVAL.as_secs_f64());
    let mut client_mbps_by_interface = BTreeMap::new();
    for (interface, sent_bytes) in &sent_bytes_by_interface {
        let previous = snapshot
            .last_sent_bytes_by_interface
            .get(interface)
            .copied()
            .unwrap_or(0);
        client_mbps_by_interface.insert(
            interface.clone(),
            (sent_bytes.saturating_sub(previous)) as f64 * 8.0 / rate_delta / 1_000_000.0,
        );
    }
    snapshot.last_client_rate_at = Some(now);
    snapshot.last_sent_bytes_by_interface = sent_bytes_by_interface;
    snapshot.client_mbps_by_interface = client_mbps_by_interface;
}

fn format_status_file(state: &ClientUiState, snapshot: &mut Snapshot) -> String {
    let now = Instant::now();
    update_client_path_rates(state, snapshot, now);

    let paths = state.paths.read();
    let mut lines = Vec::new();
    for interface in &state.interfaces {
        let display_name = fixed_status_name(interface);
        let client_mbps = snapshot
            .client_mbps_by_interface
            .get(interface)
            .map(|mbps| format!("{mbps:.2}"))
            .unwrap_or_else(|| "0.00".to_string());
        let Some(rows) = paths.get(interface) else {
            lines.push(format!(
                "{display_name} {client_mbps:>5} / {:>5}  @ {:>4}  {:>8}  -",
                "0.00", "0%", "-"
            ));
            continue;
        };
        for row in rows {
            lines.push(format!(
                "{display_name} {client_mbps:>5} / {:>5}  @ {:>4}  {:>8}  {}",
                format_file_server_mbps(&row.server_mbps),
                format_split_percentage(row.split_percentage),
                format_health_age(row.last_health_at),
                row.status
            ));
        }
    }
    if lines.is_empty() {
        "-".to_string()
    } else {
        lines.join("\n")
    }
}

fn fixed_status_name(name: &str) -> String {
    format!("{:<10}", name.chars().take(10).collect::<String>())
}

fn format_file_server_mbps(server_mbps: &str) -> String {
    if server_mbps == "-" {
        return "0.00".to_string();
    }
    server_mbps.to_string()
}

fn format_split_percentage(split_percentage: Option<f64>) -> String {
    split_percentage
        .map(|percentage| format!("{percentage:.0}%"))
        .unwrap_or_else(|| "0%".to_string())
}

fn write_status_file(path: &PathBuf, contents: &str) -> io::Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, contents)?;
    std::fs::rename(tmp, path)
}

fn draw(frame: &mut ratatui::Frame<'_>, state: &ClientUiState, snapshot: &mut Snapshot) {
    let area = frame.area();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(9),
            Constraint::Length(7),
            Constraint::Min(6),
            Constraint::Length(8),
        ])
        .split(area);

    let now = Instant::now();
    let elapsed = state.started_at.elapsed().as_secs_f64().max(0.001);
    let delta = snapshot
        .last_at
        .map(|last| now.duration_since(last).as_secs_f64().max(0.001))
        .unwrap_or(1.0);
    let ingested_bytes = state.ingested_bytes.load(Ordering::Relaxed);
    let sent_bytes = state.sent_bytes.load(Ordering::Relaxed);
    let ingest_mbps = (ingested_bytes.saturating_sub(snapshot.last_ingested_bytes)) as f64 * 8.0
        / delta
        / 1_000_000.0;
    let send_mbps =
        (sent_bytes.saturating_sub(snapshot.last_sent_bytes)) as f64 * 8.0 / delta / 1_000_000.0;
    update_client_path_rates(state, snapshot, now);
    snapshot.last_at = Some(now);
    snapshot.last_ingested_bytes = ingested_bytes;
    snapshot.last_sent_bytes = sent_bytes;

    let mut header_lines = vec![Line::from(vec![
        Span::styled(
            "Irohsion Client",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(
            format!("uptime {}", fmt_uptime(state.started_at.elapsed())),
            Style::default().fg(Color::Gray),
        ),
    ])];
    let last_health = state
        .last_health_received
        .read()
        .map(|at| format!("{:.1}s ago", now.duration_since(at).as_secs_f64()))
        .unwrap_or_else(|| "-".to_string());
    header_lines.push(Line::from(format!("server {}", state.server_endpoint)));
    header_lines.push(Line::from(format!("health {}", last_health)));
    header_lines.push(Line::from(format!(
        "port {}  interfaces {}",
        state.port,
        state.interfaces.join(", ")
    )));
    header_lines.push(Line::from(format!(
        "mode {}  effective {}",
        state.strategy_mode.read().clone(),
        state.effective_strategy.read().clone()
    )));
    header_lines.push(Line::from(
        "keys a=auto  w=weighted-auto  r=redundant  s=split  ctrl+r=roundrobin  q=quit",
    ));
    header_lines.push(Line::from(format!(
        "last ingest from {}",
        state.last_ingest_from.read().clone()
    )));

    let header = Paragraph::new(header_lines)
        .block(Block::default().borders(Borders::ALL).title("Overview"))
        .wrap(Wrap { trim: true });
    frame.render_widget(header, layout[0]);

    let stats = Paragraph::new(vec![
        Line::from(format!(
            "ingest  {:>8.2} Mbps   packets {:>8}   total {:>8.2} MB",
            ingest_mbps,
            state.ingested_packets.load(Ordering::Relaxed),
            ingested_bytes as f64 / 1_000_000.0
        )),
        Line::from(format!(
            "send    {:>8.2} Mbps   packets {:>8}   total {:>8.2} MB",
            send_mbps,
            state.sent_packets.load(Ordering::Relaxed),
            sent_bytes as f64 / 1_000_000.0
        )),
        Line::from(format!(
            "errors  {:>8}         avg send {:>8.2} Mbps",
            state.send_errors.load(Ordering::Relaxed),
            sent_bytes as f64 * 8.0 / elapsed / 1_000_000.0
        )),
    ])
    .block(Block::default().borders(Borders::ALL).title("Flow"))
    .wrap(Wrap { trim: true });
    frame.render_widget(stats, layout[1]);

    let mut rows = Vec::new();
    for (interface, path_rows) in state.paths.read().iter() {
        let client_mbps = snapshot
            .client_mbps_by_interface
            .get(interface)
            .map(|mbps| format_client_mbps(*mbps, false))
            .unwrap_or_else(|| "0.00".to_string());
        for row in path_rows {
            rows.push(Row::new(vec![
                Cell::from(interface.clone()),
                Cell::from(short_endpoint(&row.endpoint_id)),
                Cell::from(client_mbps.clone()),
                Cell::from(
                    row.split_percentage
                        .map(|percentage| format!("{percentage:.0}%"))
                        .unwrap_or_else(|| "-".to_string()),
                ),
                Cell::from(row.server_mbps.clone()),
                Cell::from(format_health_age(row.last_health_at)),
                Cell::from(row.transport.clone()),
                Cell::from(if row.selected { "yes" } else { "no" }),
                Cell::from(row.status.clone()),
                Cell::from(row.remote_addr.clone()),
            ]));
        }
    }
    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(8),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new(vec![
            "Interface",
            "Endpoint",
            "Client Mbps",
            "Split %",
            "Server Mbps",
            "Health",
            "Transport",
            "Selected",
            "Status",
            "Remote",
        ])
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(Block::default().borders(Borders::ALL).title("Paths"));
    frame.render_widget(table, layout[2]);

    let log_lines = state
        .logs
        .read()
        .iter()
        .rev()
        .take(5)
        .rev()
        .cloned()
        .map(Line::from)
        .collect::<Vec<_>>();
    let log_panel = Paragraph::new(log_lines)
        .block(Block::default().borders(Borders::ALL).title("Logs"))
        .wrap(Wrap { trim: false })
        .style(Style::default().fg(Color::Gray));
    frame.render_widget(log_panel, layout[3]);
}

fn fmt_uptime(elapsed: Duration) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        elapsed.as_secs() / 3600,
        (elapsed.as_secs() / 60) % 60,
        elapsed.as_secs() % 60
    )
}

fn short_endpoint(endpoint_id: &str) -> String {
    endpoint_id.chars().take(ENDPOINT_PREFIX_LEN).collect()
}

fn format_client_mbps(mbps: f64, include_unit: bool) -> String {
    if mbps <= 0.005 {
        if include_unit {
            "0.00 Mbps".to_string()
        } else {
            "0.00".to_string()
        }
    } else if include_unit {
        format!("{mbps:.2} Mbps")
    } else {
        format!("{mbps:.2}")
    }
}

fn format_health_age(last_health_at: Option<Instant>) -> String {
    let Some(last_health_at) = last_health_at else {
        return "-".to_string();
    };
    let elapsed = last_health_at.elapsed();
    if elapsed.as_secs() < 60 {
        format!("{:.1}s ago", elapsed.as_secs_f64())
    } else {
        format!("{}m ago", elapsed.as_secs() / 60)
    }
}
