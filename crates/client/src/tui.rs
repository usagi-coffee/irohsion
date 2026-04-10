use std::{
    collections::{BTreeMap, VecDeque},
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
use tracing_subscriber::fmt::writer::MakeWriter;

#[derive(Clone)]
pub struct ClientUiState {
    started_at: Instant,
    port: u16,
    endpoint: String,
    interfaces: Vec<String>,
    ingested_packets: Arc<AtomicU64>,
    ingested_bytes: Arc<AtomicU64>,
    sent_packets: Arc<AtomicU64>,
    sent_bytes: Arc<AtomicU64>,
    send_errors: Arc<AtomicU64>,
    last_ingest_from: Arc<RwLock<String>>,
    paths: Arc<RwLock<BTreeMap<String, Vec<PathRow>>>>,
    logs: Arc<RwLock<VecDeque<String>>>,
    quit_requested: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct PathRow {
    pub remote_addr: String,
    pub transport: String,
    pub selected: bool,
    pub status: String,
}

pub struct ClientUi {
    pub state: ClientUiState,
    _join: thread::JoinHandle<()>,
}

impl ClientUiState {
    pub fn new(port: u16, endpoint: String, interfaces: Vec<String>) -> Self {
        Self {
            started_at: Instant::now(),
            port,
            endpoint,
            interfaces,
            ingested_packets: Arc::new(AtomicU64::new(0)),
            ingested_bytes: Arc::new(AtomicU64::new(0)),
            sent_packets: Arc::new(AtomicU64::new(0)),
            sent_bytes: Arc::new(AtomicU64::new(0)),
            send_errors: Arc::new(AtomicU64::new(0)),
            last_ingest_from: Arc::new(RwLock::new("-".to_string())),
            paths: Arc::new(RwLock::new(BTreeMap::new())),
            logs: Arc::new(RwLock::new(VecDeque::with_capacity(128))),
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
        self.paths
            .write()
            .entry(interface)
            .or_default()
            .iter_mut()
            .for_each(|row| row.status = "sending".to_string());
    }

    pub fn record_send_error(&self, interface: String, error: String) {
        self.send_errors.fetch_add(1, Ordering::Relaxed);
        self.paths.write().entry(interface).or_default().push(PathRow {
            remote_addr: error,
            transport: "error".to_string(),
            selected: false,
            status: "failed".to_string(),
        });
    }

    pub fn record_path(&self, interface: String, rows: Vec<PathRow>) {
        self.paths.write().insert(interface, rows);
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
}

impl ClientUi {
    pub fn spawn(state: ClientUiState) -> Self {
        let thread_state = state.clone();
        let join = thread::spawn(move || {
            let _ = run(thread_state);
        });
        Self { state, _join: join }
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
    last_ingested_bytes: u64,
    last_sent_bytes: u64,
}

fn draw(frame: &mut ratatui::Frame<'_>, state: &ClientUiState, snapshot: &mut Snapshot) {
    let area = frame.area();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
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
    let ingest_mbps = (ingested_bytes.saturating_sub(snapshot.last_ingested_bytes)) as f64 * 8.0 / delta / 1_000_000.0;
    let send_mbps = (sent_bytes.saturating_sub(snapshot.last_sent_bytes)) as f64 * 8.0 / delta / 1_000_000.0;
    snapshot.last_at = Some(now);
    snapshot.last_ingested_bytes = ingested_bytes;
    snapshot.last_sent_bytes = sent_bytes;

    let header = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("Irohsion Client", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(format!("uptime {}", fmt_uptime(state.started_at.elapsed())), Style::default().fg(Color::Gray)),
        ]),
        Line::from(format!("endpoint {}  port {}  interfaces {}", state.endpoint, state.port, state.interfaces.join(", "))),
        Line::from(format!("last ingest from {}", state.last_ingest_from.read().clone())),
    ])
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

    let rows = state
        .paths
        .read()
        .iter()
        .flat_map(|(interface, rows)| {
            rows.iter().map(move |row| {
                Row::new(vec![
                    Cell::from(interface.clone()),
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
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new(vec!["Interface", "Transport", "Selected", "Status", "Remote"])
            .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
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

#[derive(Clone)]
pub struct TuiLogWriterFactory {
    state: ClientUiState,
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
    state: ClientUiState,
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
