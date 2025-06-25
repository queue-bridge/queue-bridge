use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;
use std::time::Duration;

pub mod queuebridge {
    tonic::include_proto!("queuebridge");
}

use queuebridge::{
    queue_bridge_balancer_server::{QueueBridgeBalancer, QueueBridgeBalancerServer},
    SubscribeRequest, SubscribeResponse, HeartbeatRequest, HeartbeatResponse,
};

#[derive(Debug, Default)]
pub struct MyQueueBridge {}

#[tonic::async_trait]
impl QueueBridgeBalancer for MyQueueBridge {
    type SubscribeStream = ReceiverStream<Result<SubscribeResponse, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let qid = request.into_inner().queue_id;
        println!("Subscribe called for queue_id={}", qid);

        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            for i in 1..=5 {
                let msg = SubscribeResponse {
                    queue_id: qid.clone(),
                    message: format!("Update {} for queue {}", i, qid).into_bytes(),
                };
                if let Err(e) = tx.send(Ok(msg)).await {
                    println!("subscriber dropped: {}", e);
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        println!("Heartbeat: queue_id={}, lag={}", req.queue_id, req.lag);
        let status = if req.lag < 100 { "OK" } else { "Lagging" };
        let reply = HeartbeatResponse { status: status.into() };
        Ok(Response::new(reply))
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
