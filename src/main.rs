use tonic::{transport::Server, Request, Response, Status};

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
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<SubscribeResponse>, Status> {
        let req = request.into_inner();
        println!("Subscribe from queue_id={}", req.queue_id);
        let reply = SubscribeResponse { message: format!("subscribed to {}", req.queue_id) };
        Ok(Response::new(reply))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        println!("Heartbeat: queue_id={} lag={}", req.queue_id, req.lag);
        let status = if req.lag < 100 {
            "OK".into()
        } else {
            "Lagging".into()
        };
        let reply = HeartbeatResponse { status };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::]:50051".parse()?;
    let svc = MyQueueBridge::default();

    println!("QueueBridgeBalancer server listening on {}", addr);
    Server::builder()
        .add_service(QueueBridgeBalancerServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
