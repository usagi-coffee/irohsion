use crate::tui;

#[derive(Clone, Default)]
pub struct ServerCtx {
    ui: Option<tui::ServerUiState>,
}

impl ServerCtx {
    pub fn new(ui: Option<tui::ServerUiState>) -> Self {
        Self { ui }
    }

    pub fn ui_state(&self) -> Option<tui::ServerUiState> {
        self.ui.clone()
    }

    pub fn set_endpoint(&self, endpoint: String) {
        if let Some(ui) = &self.ui {
            ui.set_endpoint(endpoint.clone());
        } else {
            println!("INFO server endpoint endpoint={endpoint}");
        }
    }

    pub fn set_server_addrs(&self, addrs: Vec<String>) {
        if let Some(ui) = &self.ui {
            ui.set_server_addrs(addrs.clone());
        } else {
            for addr in addrs {
                println!("INFO {addr}");
            }
        }
    }

    pub fn set_health_endpoint(&self, endpoint: Option<String>) {
        if let Some(ui) = &self.ui {
            ui.set_health_endpoint(endpoint.clone());
        } else if let Some(endpoint) = endpoint {
            println!("INFO health endpoint endpoint={endpoint}");
        }
    }

    pub fn record_connection(&self, remote: String, rows: Vec<tui::PathRow>) {
        if let Some(ui) = &self.ui {
            ui.record_connection(remote.clone(), rows.clone());
        } else {
            for row in &rows {
                println!(
                    "INFO connection path remote={} selected={} status={} transport={} path={}",
                    remote, row.selected, row.status, row.transport, row.remote_addr
                );
            }
        }
    }

    pub fn record_disconnect(&self, remote: String, error: String) {
        if let Some(ui) = &self.ui {
            ui.record_disconnect(remote.clone(), error.clone());
        } else {
            eprintln!("ERROR connection closed remote={remote} error={error}");
        }
    }

    pub fn record_connection_receive(&self, remote: &str, bytes: u64) {
        if let Some(ui) = &self.ui {
            ui.record_connection_receive(remote, bytes);
        }
    }

    pub fn record_received(&self, bytes: u64) {
        if let Some(ui) = &self.ui {
            ui.record_received(bytes);
        }
    }

    pub fn record_forwarded(&self, bytes: u64, buffered: u64, next_seq: u64) {
        if let Some(ui) = &self.ui {
            ui.record_forwarded(bytes, buffered, next_seq);
        }
    }

    pub fn record_duplicate(&self, buffered: u64, next_seq: u64) {
        if let Some(ui) = &self.ui {
            ui.record_duplicate(buffered, next_seq);
        }
    }

    pub fn record_invalid(&self) {
        if let Some(ui) = &self.ui {
            ui.record_invalid();
        } else {
            eprintln!("WARN invalid packet");
        }
    }

    pub fn record_buffered(&self, buffered: u64, next_seq: u64) {
        if let Some(ui) = &self.ui {
            ui.record_buffered(buffered, next_seq);
        }
    }

    pub fn set_session(&self, session_id: u32, next_seq: u64) {
        if let Some(ui) = &self.ui {
            ui.set_session(session_id, next_seq);
        }
    }

    pub fn record_session_switch(&self, session_id: u32, next_seq: u64) {
        if let Some(ui) = &self.ui {
            ui.record_session_switch(session_id, next_seq);
        } else {
            println!("INFO session switch session_id={session_id} next_seq={next_seq}");
        }
    }
}
