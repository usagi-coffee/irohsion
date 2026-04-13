use crate::tui;
use std::net::SocketAddr;
use transport::transport_kind;

#[derive(Clone, Default)]
pub struct ClientCtx {
    ui: Option<tui::ClientUiState>,
}

impl ClientCtx {
    pub fn new(ui: Option<tui::ClientUiState>) -> Self {
        Self { ui }
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

    pub fn connected_path(&self, interface: &str, local_addr: SocketAddr) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "INFO connected interface-bound iroh path interface={interface} local_addr={local_addr}"
            ));
        } else {
            println!(
                "INFO connected interface-bound iroh path interface={interface} local_addr={local_addr}"
            );
        }
    }

    pub fn client_ready(&self, session_id: u32, udp_listen: SocketAddr, paths: usize) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "INFO client ready session_id={session_id} udp_listen={udp_listen} paths={paths}"
            ));
        } else {
            println!(
                "INFO client ready session_id={session_id} udp_listen={udp_listen} paths={paths}"
            );
        }
    }

    pub fn ingested_packet(&self, seq: u64, bytes: usize, from: SocketAddr) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "INFO ingested udp packet seq={seq} bytes={bytes} from={from}"
            ));
        } else {
            println!("INFO ingested udp packet seq={seq} bytes={bytes} from={from}");
        }
    }

    pub fn forwarded_return_packet(&self, interface: &str, peer: SocketAddr, bytes: usize) {
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "INFO forwarded return packet to local UDP peer interface={interface} peer={peer} bytes={bytes}"
            ));
        } else {
            println!(
                "INFO forwarded return packet to local UDP peer interface={interface} peer={peer} bytes={bytes}"
            );
        }
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
        if let Some(ui) = &self.ui {
            ui.push_log_line(format!(
                "WARN dropping return packet because no local UDP peer has sent traffic yet interface={interface} bytes={bytes}"
            ));
        } else {
            eprintln!(
                "WARN dropping return packet because no local UDP peer has sent traffic yet interface={interface} bytes={bytes}"
            );
        }
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

    pub fn record_connection_paths(
        &self,
        interface: String,
        connection: &iroh::endpoint::Connection,
    ) {
        for path in connection.paths() {
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

        if let Some(ui) = &self.ui {
            ui.record_path(interface, tui::describe_paths(connection));
        }
    }
}
