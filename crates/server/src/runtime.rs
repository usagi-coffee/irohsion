use crate::tui;

pub async fn wait_for_shutdown(ui_state: Option<tui::ServerUiState>) {
    if let Some(ui_state) = ui_state {
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);

        loop {
            tokio::select! {
                _ = &mut ctrl_c => return,
                _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {
                    if ui_state.should_quit() {
                        return;
                    }
                }
            }
        }
    } else {
        let _ = tokio::signal::ctrl_c().await;
    }
}
