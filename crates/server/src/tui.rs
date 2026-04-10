use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, Stdout},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
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
use tracing_subscriber::fmt::writer::MakeWriter;

#[derive(Clone)]
pub struct ServerUiState {
    started_at: Instant,
    endpoint: Arc<RwLock<String>>,
    rtmp: String,
    ffmpeg_input_udp: String,
    server_addrs: Arc<RwLock<Vec<String>>>,
    logs: Arc<RwLock<VecDeque<String>>>,
    quit_requested: Arc<AtomicBool>,
    received_packets: Arc<AtomicU64>,
    received_bytes: Arc<AtomicU64>,
    forwarded_packets: Arc<AtomicU64>,
    forwarded_bytes: Arc<AtomicU64>,
    duplicate_packets: Arc<AtomicU64>,
    invalid_packets: Arc<AtomicU64>,
    session_switches: Arc<AtomicU64>,
    buffered_packets: Arc<AtomicU64>,
    next_seq: Arc<AtomicU64>,
    current_session: Arc<AtomicU32>,
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
    pub last_error: Option<String>,
    pub paths: Vec<PathRow>,
}

pub struct ServerUi {
    pub state: ServerUiState,
    _join: thread::JoinHandle<()>,
}

impl ServerUiState {
    pub fn new(rtmp: String, ffmpeg_input_udp: String) -> Self {
        Self {
            started_at: Instant::now(),
            endpoint: Arc::new(RwLock::new("-".to_string())),
            rtmp,
            ffmpeg_input_udp,
            server_addrs: Arc::new(RwLock::new(Vec::new())),
            logs: Arc::new(RwLock::new(VecDeque::with_capacity(128))),
            quit_requested: Arc::new(AtomicBool::new(false)),
            received_packets: Arc::new(AtomicU64::new(0)),
            received_bytes: Arc::new(AtomicU64::new(0)),
            forwarded_packets: Arc::new(AtomicU64::new(0)),
            forwarded_bytes: Arc::new(AtomicU64::new(0)),
            duplicate_packets: Arc::new(AtomicU64::new(0)),
            invalid_packets: Arc::new(AtomicU64::new(0)),
            session_switches: Arc::new(AtomicU64::new(0)),
            buffered_packets: Arc::new(AtomicU64::new(0)),
            next_seq: Arc::new(AtomicU64::new(0)),
            current_session: Arc::new(AtomicU32::new(0)),
            connections: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn set_endpoint(&self, endpoint: String) {
        *self.endpoint.write() = endpoint;
    }

    pub fn set_server_addrs(&self, addrs: Vec<String>) {
        *self.server_addrs.write() = addrs;
    }

    pub fn push_log_line(&self, line: String) {
        let mut logs = self.logs.write();
        if logs.len() >= 120 {
            logs.pop_front();
        }
        logs.push_back(line);
    }

    pub fn log_writer(&self) -> TuiLogWriterFactory {
        TuiLogWriterFactory {
            state: self.clone(),
        }
    }

    pub fn request_quit(&self) {
        self.quit_requested.store(true, Ordering::Relaxed);
    }

    pub fn should_quit(&self) -> bool {
        self.quit_requested.load(Ordering::Relaxed)
    }

    pub fn record_connection(&self, remote: String, rows: Vec<PathRow>) {
        let mut connections = self.connections.write();
        let entry = connections.entry(remote).or_default();
        entry.paths = rows;
    }

    pub fn record_disconnect(&self, remote: String, error: String) {
        let mut connections = self.connections.write();
        let entry = connections.entry(remote).or_default();
        entry.last_error = Some(error.clone());
        entry.paths.iter_mut().for_each(|row| {
            row.status = format!("closed: {error}");
        });
    }

    pub fn record_connection_receive(&self, remote: &str, bytes: u64) {
        let mut connections = self.connections.write();
        let entry = connections.entry(remote.to_string()).or_default();
        entry.received_packets += 1;
        entry.received_bytes += bytes;
    }

    pub fn record_received(&self, bytes: u64) {
        self.received_packets.fetch_add(1, Ordering::Relaxed);
        self.received_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_forwarded(&self, bytes: u64, buffered: u64, next_seq: u64) {
        self.forwarded_packets.fetch_add(1, Ordering::Relaxed);
        self.forwarded_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.buffered_packets.store(buffered, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    pub fn record_duplicate(&self, buffered: u64, next_seq: u64) {
        self.duplicate_packets.fetch_add(1, Ordering::Relaxed);
        self.buffered_packets.store(buffered, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    pub fn record_invalid(&self) {
        self.invalid_packets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_buffered(&self, buffered: u64, next_seq: u64) {
        self.buffered_packets.store(buffered, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    pub fn set_session(&self, session_id: u32, next_seq: u64) {
        self.current_session.store(session_id, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
    }

    pub fn record_session_switch(&self, session_id: u32, next_seq: u64) {
        self.session_switches.fetch_add(1, Ordering::Relaxed);
        self.current_session.store(session_id, Ordering::Relaxed);
        self.next_seq.store(next_seq, Ordering::Relaxed);
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
        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                if matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                    || matches!(key.code, KeyCode::Char('c'))
                        && key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL)
                {
                    state.request_quit();
                    break;
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
}

fn draw(frame: &mut ratatui::Frame<'_>, state: &ServerUiState, snapshot: &mut Snapshot) {
    let area = frame.area();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7),
            Constraint::Length(8),
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
    let recv_bytes = state.received_bytes.load(Ordering::Relaxed);
    let fwd_bytes = state.forwarded_bytes.load(Ordering::Relaxed);
    let recv_mbps = (recv_bytes.saturating_sub(snapshot.last_received_bytes)) as f64 * 8.0 / delta / 1_000_000.0;
    let fwd_mbps = (fwd_bytes.saturating_sub(snapshot.last_forwarded_bytes)) as f64 * 8.0 / delta / 1_000_000.0;
    snapshot.last_at = Some(now);
    snapshot.last_received_bytes = recv_bytes;
    snapshot.last_forwarded_bytes = fwd_bytes;

    let header = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("Irohsion Server", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(format!("uptime {}", fmt_uptime(state.started_at.elapsed())), Style::default().fg(Color::Gray)),
        ]),
        Line::from(format!("endpoint {}", state.endpoint.read().clone())),
        Line::from(format!("rtmp {}  ffmpeg udp {}", state.rtmp, state.ffmpeg_input_udp)),
        Line::from(format!("direct {}", format_addrs(&state.server_addrs.read(), "ip:"))),
        Line::from(format!("relay  {}", format_addrs(&state.server_addrs.read(), "relay:"))),
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
            "dupes {:>9}   invalid {:>8}   buffered {:>7}",
            state.duplicate_packets.load(Ordering::Relaxed),
            state.invalid_packets.load(Ordering::Relaxed),
            state.buffered_packets.load(Ordering::Relaxed)
        )),
        Line::from(format!(
            "session {:>10}   next_seq {:>10}   switches {:>6}   avg fwd {:>6.2} Mbps",
            state.current_session.load(Ordering::Relaxed),
            state.next_seq.load(Ordering::Relaxed),
            state.session_switches.load(Ordering::Relaxed),
            fwd_bytes as f64 * 8.0 / elapsed / 1_000_000.0
        )),
    ])
    .block(Block::default().borders(Borders::ALL).title("Flow"))
    .wrap(Wrap { trim: true });
    frame.render_widget(stats, layout[1]);

    let connections = state.connections.read();
    let rows = connections
        .iter()
        .flat_map(|(remote, connection)| {
            let previous_bytes = snapshot.connection_last_bytes.get(remote).copied().unwrap_or(0);
            let conn_mbps =
                (connection.received_bytes.saturating_sub(previous_bytes)) as f64 * 8.0 / delta / 1_000_000.0;
            connection.paths.iter().map(move |row| {
                Row::new(vec![
                    Cell::from(remote.clone()),
                    Cell::from(connection.received_packets.to_string()),
                    Cell::from(format!("{:.2} MB", connection.received_bytes as f64 / 1_000_000.0)),
                    Cell::from(format!("{:.2}", conn_mbps)),
                    Cell::from(row.transport.clone()),
                    Cell::from(if row.selected { "yes" } else { "no" }),
                    Cell::from(row.status.clone()),
                    Cell::from(row.remote_addr.clone()),
                ])
            })
        })
        .collect::<Vec<_>>();
    let table = Table::new(
        rows,
        [
            Constraint::Length(18),
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
        Row::new(vec!["Remote", "Recv Pkts", "Recv MB", "Mbps", "Transport", "Selected", "Status", "Path"])
            .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
    )
    .block(Block::default().borders(Borders::ALL).title("Connections"));
    frame.render_widget(table, layout[2]);

    snapshot.connection_last_bytes = connections
        .iter()
        .map(|(remote, connection)| (remote.clone(), connection.received_bytes))
        .collect();

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

fn format_addrs(lines: &[String], kind: &str) -> String {
    let values = lines
        .iter()
        .filter_map(|line| line.split_once(&format!("server_addr={kind}")).map(|(_, value)| value))
        .collect::<Vec<_>>();

    if values.is_empty() {
        "-".to_string()
    } else {
        values.join("  |  ")
    }
}

#[derive(Clone)]
pub struct TuiLogWriterFactory {
    state: ServerUiState,
}

impl<'a> MakeWriter<'a> for TuiLogWriterFactory {
    type Writer = TuiLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        TuiLogWriter {
            state: self.state.clone(),
            buffer: Vec::new(),
        }
    }
}

pub struct TuiLogWriter {
    state: ServerUiState,
    buffer: Vec<u8>,
}

impl io::Write for TuiLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        while let Some(pos) = self.buffer.iter().position(|b| *b == b'\n') {
            let line = self.buffer.drain(..=pos).collect::<Vec<_>>();
            let line = String::from_utf8_lossy(&line).trim().to_string();
            if !line.is_empty() {
                self.state.push_log_line(line);
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            let line = String::from_utf8_lossy(&self.buffer).trim().to_string();
            if !line.is_empty() {
                self.state.push_log_line(line);
            }
            self.buffer.clear();
        }
        Ok(())
    }
}
