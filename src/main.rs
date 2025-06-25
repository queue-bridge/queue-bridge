use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;

pub mod queuebridge {
    tonic::include_proto!("queuebridge");
}

use queuebridge::{
    queue_bridge_balancer_server::{QueueBridgeBalancer, QueueBridgeBalancerServer},
    EmptyResponse, SubscribeRequest, QueueMessage, HeartbeatRequest,
};

mod subscriber;
use subscriber::{Subscriber, SubscriberMap};

pub struct MyQueueBridge {
    subscribers: SubscriberMap,
}

impl Default for MyQueueBridge {
    fn default() -> Self {
        Self {
            subscribers: SubscriberMap::new(),
        }
    }
}

#[tonic::async_trait]
impl QueueBridgeBalancer for MyQueueBridge {
    type SubscribeStream = ReceiverStream<Result<QueueMessage, Status>>;

    async fn push(
        &self,
        request: Request<QueueMessage>,
    ) -> Result<Response<EmptyResponse>, Status> {
        let msg = request.into_inner();
        println!("Push called for queue_id={}", msg.queue_id);
        self.subscribers.push_message(msg).await;
        Ok(Response::new(EmptyResponse {}))
    }

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let addr = request.remote_addr().unwrap().to_string();
        let qid = request.into_inner().queue_id;
        let sub_id = format!("{}_{}", qid, addr);
        println!("Subscribe called for queue_id={} from {}", qid, addr);

        let (tx, rx) = tokio::sync::mpsc::channel(4);

        let subscriber = Subscriber {
            tx,
            last_heartbeat: std::time::SystemTime::now(),
            lag: 0,
        };
        self.subscribers.add_subscriber(sub_id, subscriber).await;

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        let addr = request.remote_addr().unwrap().to_string();
        let req = request.into_inner();
        for item in &req.queue_lags {
            let sub_id = format!("{}_{}", item.queue_id, addr);
            self.subscribers.update_heartbeat(&sub_id, item.lag).await;
        }

        Ok(Response::new(EmptyResponse {}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::]:50051".parse()?;
    let svc = MyQueueBridge::default();

    println!("Listening on {}", addr);
    Server::builder()
        .add_service(QueueBridgeBalancerServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
