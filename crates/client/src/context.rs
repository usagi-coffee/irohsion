use crate::tui;
use parking_lot::RwLock;
use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use transport::{ChatMessage, HealthReport, transport_kind};

const MAX_CHAT_MESSAGES: usize = 100;

#[derive(Clone)]
pub struct ClientCtx {
    ui: Option<tui::ClientUiState>,
    last_health_unix_ms: Arc<RwLock<Option<u64>>>,
    chat_messages: Arc<RwLock<VecDeque<ChatMessage>>>,
}

impl Default for ClientCtx {
    fn default() -> Self {
        Self {
            ui: None,
            last_health_unix_ms: Arc::new(RwLock::new(None)),
            chat_messages: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_CHAT_MESSAGES))),
        }
    }
}

impl ClientCtx {
    pub fn new(ui: Option<tui::ClientUiState>) -> Self {
        Self {
            ui,
            last_health_unix_ms: Arc::new(RwLock::new(None)),
            chat_messages: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_CHAT_MESSAGES))),
        }
    }

    pub fn ui_state(&self) -> Option<tui::ClientUiState> {
        self.ui.clone()
    }

    pub fn record_ingest(&self, bytes: u64, from: String) {
        if let Some(ui) = &self.ui {
            ui.record_ingest(bytes, from);
        }
    }

    pub fn record_send(&self, interface: String, bytes: u64) {
        if let Some(ui) = &self.ui {
            ui.record_send(interface, bytes);
        }
    }

    pub fn record_send_error(&self, interface: String, error: String) {
        if let Some(ui) = &self.ui {
            ui.record_send_error(interface, error);
        }
    }

    pub fn set_health_endpoint(&self, endpoint: String) {
        if let Some(ui) = &self.ui {
            ui.set_health_endpoint(endpoint);
        }
    }

    pub fn record_health_report(&self, report: &HealthReport) {
        let mut last = self.last_health_unix_ms.write();
        let previous = *last;
        if previous.is_some_and(|current| report.unix_ms < current) {
            if let Some(ui) = &self.ui {
                ui.push_log_line(format!(
                    "WARN dropped stale health report unix_ms={} last_unix_ms={}",
                    report.unix_ms,
                    previous.expect("previous health unix_ms exists")
                ));
            } else {
                eprintln!(
                    "WARN dropped stale health report unix_ms={} last_unix_ms={}",
                    report.unix_ms,
                    previous.expect("previous health unix_ms exists")
                );
            }
            return;
        }
        *last = Some(report.unix_ms);
        self.record_chat_messages(&report.chat);
        if let Some(ui) = &self.ui {
            ui.record_health_received();
            ui.record_endpoint_health(&report.endpoints);
        }
    }

    pub fn chat_messages(&self) -> Vec<ChatMessage> {
        self.chat_messages.read().iter().cloned().collect()
    }

    fn record_chat_messages(&self, messages: &[ChatMessage]) {
        if messages.is_empty() {
            return;
        }

        let mut stored = self.chat_messages.write();
        for message in messages {
            if stored.iter().any(|existing| existing.id == message.id) {
                continue;
            }
            stored.push_back(message.clone());
        }
        while stored.len() > MAX_CHAT_MESSAGES {
            stored.pop_front();
        }
    }

    pub fn invalid_health_report(&self, error: &str) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!("WARN invalid health report error={error}"));
        } else {
            eprintln!("WARN invalid health report error={error}");
        }
    }

    pub fn connected_path(&self, interface: &str, endpoint_id: &str, local_addr: SocketAddr) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "INFO connected interface-bound iroh path interface={interface} endpoint_id={endpoint_id} local_addr={local_addr}"
            ));
        } else {
            println!(
                "INFO connected interface-bound iroh path interface={interface} endpoint_id={endpoint_id} local_addr={local_addr}"
            );
        }
    }

    pub fn client_ready(&self, udp_listen: SocketAddr, paths: usize, health_endpoint: &str) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "INFO client ready udp_listen={udp_listen} paths={paths} health_endpoint={health_endpoint}"
            ));
        } else {
            println!(
                "INFO client ready udp_listen={udp_listen} paths={paths} health_endpoint={health_endpoint}"
            );
        }
    }

    pub fn ingested_packet(&self, seq: u64, bytes: usize, from: SocketAddr) {
        let _ = (seq, bytes, from);
    }

    pub fn forwarded_return_packet(&self, interface: &str, peer: SocketAddr, bytes: usize) {
        let _ = (interface, peer, bytes);
    }

    pub fn return_path_closed(&self, interface: &str, error: &str) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "INFO return path closed interface={interface} error={error}"
            ));
        } else {
            println!("INFO return path closed interface={interface} error={error}");
        }
    }

    pub fn missing_return_peer(&self, interface: &str, bytes: usize) {
        let _ = (interface, bytes);
    }

    pub fn return_forward_error(&self, interface: &str, peer: SocketAddr, error: &str) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "ERROR failed forwarding return packet to local UDP peer interface={interface} peer={peer} error={error}"
            ));
        } else {
            eprintln!(
                "ERROR failed forwarding return packet to local UDP peer interface={interface} peer={peer} error={error}"
            );
        }
    }

    pub fn send_failure(&self, interface: &str, seq: u64, error: &str) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "ERROR failed to send duplicated packet interface={interface} seq={seq} error={error}"
            ));
        } else {
            eprintln!(
                "ERROR failed to send duplicated packet interface={interface} seq={seq} error={error}"
            );
        }
    }

    pub fn reconnect_failed(&self, interface: &str, error: &str) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "ERROR failed to reconnect path interface={interface} error={error}"
            ));
        } else {
            eprintln!("ERROR failed to reconnect path interface={interface} error={error}");
        }
    }

    pub fn record_strategy_change(&self, strategy: &str, reason: &str) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "INFO path strategy changed strategy={strategy} reason=\"{reason}\""
            ));
        } else {
            println!("INFO path strategy changed strategy={strategy} reason=\"{reason}\"");
        }
    }

    pub fn record_qdisc_reset(
        &self,
        interface: &str,
        device: &str,
        backlog_bytes: u64,
        server_mbps: f32,
        qdisc: &str,
    ) {
        let line = format!(
            "WARN reset interface qdisc interface={interface} device={device} qdisc={qdisc} backlog_bytes={backlog_bytes} server_mbps={server_mbps:.2}"
        );
        if let Some(ui) = &self.ui {
            ui.push_log_line(line);
        } else {
            eprintln!("{line}");
        }
    }

    pub fn record_qdisc_reset_failed(
        &self,
        interface: &str,
        device: &str,
        backlog_bytes: u64,
        server_mbps: f32,
        error: &str,
    ) {
        let line = format!(
            "ERROR failed resetting interface qdisc interface={interface} device={device} backlog_bytes={backlog_bytes} server_mbps={server_mbps:.2} error={error}"
        );
        if let Some(ui) = &self.ui {
            ui.push_log_line(line);
        } else {
            eprintln!("{line}");
        }
    }

    pub fn record_strategy_state(&self, mode: &str, effective: &str) {
        if let Some(ui) = &self.ui {
            ui.record_strategy_state(mode.to_string(), effective.to_string());
        }
    }

    pub fn record_split_percentages(&self, percentages: &BTreeMap<String, f64>) {
        if let Some(ui) = &self.ui {
            ui.record_split_percentages(percentages);
        }
    }

    pub fn record_remote_ready(
        &self,
        adapter: &str,
        name: &str,
        service_uuid: &str,
        status_uuid: &str,
        control_uuid: &str,
    ) {
        let line = format!(
            "INFO remote BLE control ready adapter={adapter} name={name} service={service_uuid} status={status_uuid} control={control_uuid}"
        );
        if let Some(ui) = &self.ui {
            ui.push_log_line(line);
        } else {
            println!("{line}");
        }
    }

    pub fn record_connection_paths(
        &self,
        interface: String,
        endpoint_id: &str,
        connection: &iroh::endpoint::Connection,
        log_paths: bool,
    ) {
        if log_paths {
            let paths = connection.paths().into_iter().collect::<Vec<_>>();
            let selected_paths = paths
                .iter()
                .filter(|path| path.is_selected())
                .collect::<Vec<_>>();
            let paths_to_log = if selected_paths.is_empty() {
                paths.iter().collect::<Vec<_>>()
            } else {
                selected_paths
            };
            for path in paths_to_log {
                if let Some(ui) = &self.ui {
                    ui.push_log_line(format!(
                        "INFO connection path interface={} selected={} closed={} transport={} remote_addr={}",
                        interface,
                        path.is_selected(),
                        path.is_closed(),
                        transport_kind(&path),
                        path.remote_addr()
                    ));
                } else {
                    println!(
                        "INFO connection path interface={} selected={} closed={} transport={} remote_addr={}",
                        interface,
                        path.is_selected(),
                        path.is_closed(),
                        transport_kind(&path),
                        path.remote_addr()
                    );
                }
            }
        }

        if let Some(ui) = &self.ui {
            ui.record_path(interface, tui::describe_paths(connection, endpoint_id));
        }
    }
}
