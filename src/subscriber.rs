use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Response, Status};
use std::time::SystemTime;
use crate::queuebridge::{QueueMessage, EmptyResponse};

pub struct Subscriber {
    pub tx: tokio::sync::mpsc::Sender<Result<QueueMessage, Status>>,
    pub last_heartbeat: SystemTime,
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
        let mut keys_to_remove: Vec<String> = Vec::new();
        let selected_subscriber = subscribers
            .range(start..end)
            .min_by_key(|(k, v)| {
                if let Ok(duration) = SystemTime::now().duration_since(v.last_heartbeat) {
                    if duration.as_secs() <= 10 && !v.tx.is_closed() {
                        return v.lag;
                    } else {
                        if duration.as_secs() > 120 || v.tx.is_closed() {
                            keys_to_remove.push((*k).clone());
                        }
                        return i64::MAX;
                    }
                } else {
                    return i64::MAX;
                }
            })
            .map(|(k, v)| (k.clone(), v.tx.clone()));

        drop(subscribers);

        if !keys_to_remove.is_empty() {
            let mut subscribers = self.subscribers.lock().await;
            for k in keys_to_remove {
                subscribers.remove(&k);
            }
        }

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
}