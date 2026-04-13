use crate::tui;

pub async fn wait_for_shutdown(ui_state: Option<tui::ServerUiState>) {
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
