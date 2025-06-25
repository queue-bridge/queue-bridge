use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::Status;
use crate::queuebridge::QueueMessage;

pub struct Subscriber {
    pub tx: tokio::sync::mpsc::Sender<Result<QueueMessage, Status>>,
    pub last_heartbeat: std::time::SystemTime,
    pub lag: i64
}

pub struct SubscriberMap {
    subscribers: Arc<Mutex<BTreeMap<String, Subscriber>>>,
}

impl SubscriberMap {
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn push_message(&self, msg: QueueMessage) {
        let queue_id = msg.queue_id.clone();
        let mut subscribers = self.subscribers.lock().await;
        if let Some(subscribers) = subscribers.get_mut(&queue_id) {
            if let Err(e) = subscribers.tx.send(Ok(msg)).await {
                println!("subscriber dropped: {}", e);
            }
        }
    }

    pub async fn add_subscriber(&self, queue_id: String, subscriber: Subscriber) {
        let mut subscribers = self.subscribers.lock().await;
        subscribers.insert(queue_id, subscriber);
    }

    pub async fn update_heartbeat(&self, queue_id: &str, lag: i64) {
        let mut subscribers = self.subscribers.lock().await;
        if let Some(subscriber) = subscribers.get_mut(queue_id) {
            subscriber.last_heartbeat = std::time::SystemTime::now();
            subscriber.lag = lag;
        }
    }

    // pub async fn clear_expired(&self, timeout: Duration) {
    //     let mut subscribers = self.subscribers.lock().await;
    //     subscribers.retain(|_, subscriber| {
    //         subscriber.last_heartbeat.elapsed().unwrap() < timeout
    //     });
    // }
}