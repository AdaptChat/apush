#![feature(lazy_cell)]

use std::{sync::LazyLock, time::Duration};

use deadqueue::unlimited::Queue;
pub use fcm_v1::message::Notification;
use fcm_v1::{
    android::{AndroidConfig, AndroidMessagePriority},
    auth::Authenticator,
    message::Message,
    Client, Error,
};
use tokio::sync::OnceCell;

#[derive(Debug)]
pub enum Recipient {
    Token(String),
    Topic(String),
}

#[derive(Debug)]
struct NotificationTask {
    recipient: Recipient,
    msg: Notification,
}

static CLIENT: OnceCell<Client> = OnceCell::const_new();
static QUEUE: LazyLock<Queue<NotificationTask>> = LazyLock::new(Queue::new);

async fn get_client() -> &'static Client {
    CLIENT
        .get_or_init(|| async {
            let auth = Authenticator::service_account_from_file(
                std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
                    .expect("missing google application credentials"),
            )
            .await
            .expect("failed auth");

            Client::new(auth, "adapt-chat", false, Duration::from_secs(5))
        })
        .await
}

pub async fn start_workers(n: usize) {
    for x in 0..n {
        tokio::spawn(worker());

        log::info!("[WORKER:{x}] Spawned push notification worker");
    }
}

pub fn push_to(recipient: Recipient, notif: Notification) {
    QUEUE.push(NotificationTask {
        recipient,
        msg: notif,
    })
}

async fn worker() {
    loop {
        let notif = QUEUE.pop().await;
        let mut message = Message {
            notification: Some(notif.msg),
            android: Some(AndroidConfig {
                priority: Some(AndroidMessagePriority::High),
                ..Default::default()
            }),
            ..Default::default()
        };

        match notif.recipient {
            Recipient::Token(token) => message.token = Some(token),
            Recipient::Topic(topic) => message.topic = Some(topic),
        }

        for tries in 0..=5 {
            if tries != 0 {
                tokio::time::sleep(Duration::from_secs_f32(tries as f32 * 1.5)).await;
            }

            if let Err(e) = get_client().await.send(&message).await {
                match e {
                    Error::FCM { status_code, body } => match status_code {
                        400 | 404 => {
                            log::info!("stale token found, invalidating");
                            todo!("invalid token");
                            // REMIND: break;
                        }
                        500 | 503 => continue,
                        _ => {
                            log::error!("user error received: {status_code} {body}");
                            break;
                        }
                    },
                    Error::Timeout => continue,
                    _ => {
                        log::error!("user error received: {e}");
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }
}
