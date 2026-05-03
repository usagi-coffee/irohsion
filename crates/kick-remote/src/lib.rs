use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use kick_rust::KickClient;
use parking_lot::RwLock;

#[derive(Clone, Debug)]
pub struct KickChatMessage {
    pub id: u64,
    pub unix_ms: u64,
    pub user: String,
    pub text: String,
}

#[derive(Clone)]
pub struct KickChat {
    messages: Arc<RwLock<VecDeque<KickChatMessage>>>,
    next_id: Arc<AtomicU64>,
    max_messages: usize,
}

impl KickChat {
    pub fn new(max_messages: usize) -> Self {
        Self {
            messages: Arc::new(RwLock::new(VecDeque::with_capacity(max_messages))),
            next_id: Arc::new(AtomicU64::new(1)),
            max_messages,
        }
    }

    pub fn recent_messages(&self) -> Vec<KickChatMessage> {
        self.messages.read().iter().cloned().collect()
    }

    fn push(&self, user: String, text: String) -> Result<()> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_millis()
            .try_into()
            .context("current unix timestamp does not fit in u64")?;
        let mut messages = self.messages.write();
        messages.push_back(KickChatMessage {
            id,
            unix_ms,
            user,
            text,
        });
        while messages.len() > self.max_messages {
            messages.pop_front();
        }
        Ok(())
    }
}

pub fn spawn_kick_chat(channel: String, max_messages: usize) -> KickChat {
    let chat = KickChat::new(max_messages);
    let task_chat = chat.clone();
    tokio::spawn(async move {
        let _ = kick_chat_loop(channel, task_chat).await;
    });
    chat
}

async fn kick_chat_loop(channel: String, chat: KickChat) -> Result<()> {
    let client = KickClient::new();
    let message_chat = chat.clone();
    client
        .on_message(move |message| {
            let _ = message_chat.push(message.username, message.content);
        })
        .await;
    client.connect(&channel).await?;

    std::future::pending::<()>().await;
    #[allow(unreachable_code)]
    Ok(())
}
