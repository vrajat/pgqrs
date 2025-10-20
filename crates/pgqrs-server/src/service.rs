use tonic::{Request, Response, Status};
use super::api::*;
use super::api::queue_service_server::QueueService;
use std::sync::Arc;
use pgqrs_core::traits::{QueueRepo, MessageRepo};

pub struct QueueServiceImpl<RQ, RM> {
    pub queue_repo: Arc<RQ>,
    pub message_repo: Arc<RM>,
}

// Implement tonic service for QueueServiceImpl
// (Stub: actual method implementations to be filled in)
#[tonic::async_trait]
impl<RQ, RM> QueueService for QueueServiceImpl<RQ, RM>
where
    RQ: QueueRepo + Send + Sync + 'static,
    RM: MessageRepo + Send + Sync + 'static,
{
    async fn create_queue(&self, _req: Request<CreateQueueRequest>) -> Result<Response<Queue>, Status> {
        unimplemented!()
    }
    async fn delete_queue(&self, _req: Request<DeleteQueueRequest>) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn get_queue(&self, _req: Request<GetQueueRequest>) -> Result<Response<Queue>, Status> {
        unimplemented!()
    }
    async fn list_queues(&self, _req: Request<ListQueuesRequest>) -> Result<Response<ListQueuesResponse>, Status> {
        unimplemented!()
    }
    async fn enqueue(&self, _req: Request<EnqueueRequest>) -> Result<Response<EnqueueResponse>, Status> {
        unimplemented!()
    }
    async fn dequeue(&self, _req: Request<DequeueRequest>) -> Result<Response<DequeueResponse>, Status> {
        unimplemented!()
    }
    async fn ack(&self, _req: Request<AckRequest>) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn nack(&self, _req: Request<NackRequest>) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn requeue(&self, _req: Request<RequeueRequest>) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn extend_lease(&self, _req: Request<ExtendLeaseRequest>) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn peek(&self, _req: Request<PeekRequest>) -> Result<Response<PeekResponse>, Status> {
        unimplemented!()
    }
    async fn stats(&self, _req: Request<StatsRequest>) -> Result<Response<StatsResponse>, Status> {
        unimplemented!()
    }
    async fn list_in_flight(&self, _req: Request<ListInFlightRequest>) -> Result<Response<PeekResponse>, Status> {
        unimplemented!()
    }
    async fn list_dead_letters(&self, _req: Request<ListDeadLettersRequest>) -> Result<Response<PeekResponse>, Status> {
        unimplemented!()
    }
    async fn purge_dead_letters(&self, _req: Request<PurgeDeadLettersRequest>) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn health_check(&self, _req: Request<HealthCheckRequest>) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse { status: "ok".to_string() }))
    }

    // Liveness probe - lightweight check that server is running
    // Should return OK as soon as the server event loop is running
    // Does NOT check database or dependencies
    async fn liveness(&self, _req: Request<LivenessRequest>) -> Result<Response<LivenessResponse>, Status> {
        Ok(Response::new(LivenessResponse { 
            status: "OK".to_string() 
        }))
    }

    // Readiness probe - checks all dependencies are healthy
    // Verifies database connectivity and other critical services
    async fn readiness(&self, _req: Request<ReadinessRequest>) -> Result<Response<ReadinessResponse>, Status> {
        let mut failing_services = Vec::new();
        
        // Check database connectivity
        // For now, we'll do a lightweight check - in a real implementation,
        // you might want to run a simple query like "SELECT 1"
        match self.queue_repo.list_queues().await {
            Ok(_) => {}, // Database is healthy
            Err(_) => {
                failing_services.push("database".to_string());
            }
        }
        
        let status = if failing_services.is_empty() {
            "OK".to_string()
        } else {
            "FAIL".to_string()
        };
        
        Ok(Response::new(ReadinessResponse { 
            status,
            failing_services,
        }))
    }
}
