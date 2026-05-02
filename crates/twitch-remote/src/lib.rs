use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use parking_lot::RwLock;
use twitch_irc::{
    ClientConfig, SecureTCPTransport, TwitchIRCClient, login::StaticLoginCredentials,
    message::ServerMessage,
};

#[derive(Clone, Debug)]
pub struct TwitchChatMessage {
    pub id: u64,
    pub unix_ms: u64,
    pub user: String,
    pub text: String,
}

#[derive(Clone)]
pub struct TwitchChat {
    messages: Arc<RwLock<VecDeque<TwitchChatMessage>>>,
    next_id: Arc<AtomicU64>,
    max_messages: usize,
}

impl TwitchChat {
    pub fn new(max_messages: usize) -> Self {
        Self {
            messages: Arc::new(RwLock::new(VecDeque::with_capacity(max_messages))),
            next_id: Arc::new(AtomicU64::new(1)),
            max_messages,
        }
    }

    pub fn recent_messages(&self) -> Vec<TwitchChatMessage> {
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
        messages.push_back(TwitchChatMessage {
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

pub fn spawn_twitch_chat(channel: String, max_messages: usize) -> TwitchChat {
    let chat = TwitchChat::new(max_messages);
    let task_chat = chat.clone();
    tokio::spawn(async move {
        let _ = twitch_chat_loop(channel, task_chat).await;
    });
    chat
}

async fn twitch_chat_loop(channel: String, chat: TwitchChat) -> Result<()> {
    let config = ClientConfig::new_simple(StaticLoginCredentials::anonymous());
    let (mut incoming_messages, client) =
        TwitchIRCClient::<SecureTCPTransport, StaticLoginCredentials>::new(config);
    client.join(channel)?;

    while let Some(message) = incoming_messages.recv().await {
        if let ServerMessage::Privmsg(message) = message {
            chat.push(message.sender.name, message.message_text)?;
        }
    }

    Ok(())
}
