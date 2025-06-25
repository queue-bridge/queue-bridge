use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Response, Status};
use crate::queuebridge::{QueueMessage, EmptyResponse};

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

    pub async fn push_message(&self, msg: QueueMessage) -> Result<Response<EmptyResponse>, Status> {
        for _ in 0..3 {
            let res = self.push_message_internal(msg.clone()).await;
            if res.is_ok() {
                return res;
            }
        }

        return Err(Status::internal("push message failed"));
    }

    async fn push_message_internal(&self, msg: QueueMessage) -> Result<Response<EmptyResponse>, Status> {
        let queue_id = msg.queue_id.clone();
        let subscribers = self.subscribers.lock().await;

        let start = format!("{}_", &queue_id);
        let end = format!("{}`", &queue_id);
        let selected_subscriber = subscribers
            .range(start..end)
            .min_by_key(|(_, v)| v.lag)
            .map(|(k, v)| (k.clone(), v.tx.clone()));

        drop(subscribers);

        if let Some((k, tx)) = selected_subscriber {
            if tx.is_closed() {
                let mut subscribers = self.subscribers.lock().await;
                subscribers.remove(&k);
                return Err(Status::internal("connection closed."));
            }

            if let Err(e) = tx.send(Ok(msg)).await {
                println!("send message failed, {}", e);
                return Err(Status::internal(format!("send message failed, {}", e)));
            }

            let mut subscribers = self.subscribers.lock().await;
            if let Some(sub) = subscribers.get_mut(&k) {
                sub.lag += 10;
            }

            return Ok(Response::new(EmptyResponse {}));
        }

        return Err(Status::not_found("No available node."));
    }

    pub async fn add_subscriber(&self, id: String, subscriber: Subscriber) {
        let mut subscribers = self.subscribers.lock().await;
        subscribers.insert(id, subscriber);
    }

    pub async fn update_heartbeat(&self, id: &str, lag: i64) {
        let mut subscribers = self.subscribers.lock().await;
        if let Some(subscriber) = subscribers.get_mut(id) {
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