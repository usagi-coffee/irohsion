use crate::tui;

pub async fn wait_for_shutdown(ui_state: Option<tui::ClientUiState>) {
    let ui_shutdown = async {
        if let Some(ui_state) = ui_state {
            loop {
                if ui_state.should_quit() {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        _ = ui_shutdown => {}
        _ = tokio::signal::ctrl_c() => {}
    };
}
