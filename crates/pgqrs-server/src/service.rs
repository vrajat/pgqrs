use super::api::queue_service_server::QueueService;
use super::api::*;
use super::db::traits::{MessageRepo, QueueRepo};
use std::sync::Arc;
use tonic::{Request, Response, Status};

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
    async fn create_queue(
        &self,
        _req: Request<CreateQueueRequest>,
    ) -> Result<Response<Queue>, Status> {
        self.queue_repo
            .create_queue(&_req.get_ref().name, _req.get_ref().unlogged)
            .await
            .map_err(|e| Status::internal(format!("Failed to create queue: {}", e)))
            .map(|db_queue: crate::db::traits::Queue| {
                let api_queue: Queue = Queue {
                    id: db_queue.id,
                    name: db_queue.queue_name,
                    unlogged: db_queue.unlogged,
                    created_at_unix: db_queue.created_at.timestamp(),
                };
                Response::new(api_queue)
            })
    }

    async fn delete_queue(
        &self,
        _req: Request<DeleteQueueRequest>,
    ) -> Result<Response<()>, Status> {
        self.queue_repo
            .delete_queue(&_req.get_ref().name)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete queue: {}", e)))
            .map(|_| Response::new(()))
    }

    async fn get_queue(&self, _req: Request<GetQueueRequest>) -> Result<Response<Queue>, Status> {
        self.queue_repo
            .get_queue(&_req.get_ref().name)
            .await
            .map_err(|e| Status::internal(format!("Failed to get queue: {}", e)))
            .map(|db_queue: crate::db::traits::Queue| {
                let api_queue: Queue = Queue {
                    id: db_queue.id,
                    name: db_queue.queue_name,
                    unlogged: db_queue.unlogged,
                    created_at_unix: db_queue.created_at.timestamp(),
                };
                Response::new(api_queue)
            })
    }

    async fn list_queues(
        &self,
        _req: Request<ListQueuesRequest>,
    ) -> Result<Response<ListQueuesResponse>, Status> {
        self.queue_repo
            .list_queues()
            .await
            .map_err(|e| Status::internal(format!("Failed to list queues: {}", e)))
            .map(|db_queues: Vec<crate::db::traits::Queue>| {
                let api_queues: Vec<Queue> = db_queues
                    .into_iter()
                    .map(|db_queue| Queue {
                        id: db_queue.id,
                        name: db_queue.queue_name,
                        unlogged: db_queue.unlogged,
                        created_at_unix: db_queue.created_at.timestamp(),
                    })
                    .collect();
                Response::new(ListQueuesResponse { queues: api_queues })
            })
    }

    async fn enqueue(
        &self,
        _req: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        unimplemented!()
    }

    async fn dequeue(
        &self,
        _req: Request<DequeueRequest>,
    ) -> Result<Response<DequeueResponse>, Status> {
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
    async fn extend_lease(
        &self,
        _req: Request<ExtendLeaseRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn peek(&self, _req: Request<PeekRequest>) -> Result<Response<PeekResponse>, Status> {
        unimplemented!()
    }
    async fn stats(&self, _req: Request<StatsRequest>) -> Result<Response<StatsResponse>, Status> {
        unimplemented!()
    }
    async fn list_in_flight(
        &self,
        _req: Request<ListInFlightRequest>,
    ) -> Result<Response<PeekResponse>, Status> {
        unimplemented!()
    }
    async fn list_dead_letters(
        &self,
        _req: Request<ListDeadLettersRequest>,
    ) -> Result<Response<PeekResponse>, Status> {
        unimplemented!()
    }
    async fn purge_dead_letters(
        &self,
        _req: Request<PurgeDeadLettersRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn health_check(
        &self,
        _req: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: "ok".to_string(),
        }))
    }

    // Liveness probe - lightweight check that server is running
    // Should return OK as soon as the server event loop is running
    // Does NOT check database or dependencies
    async fn liveness(
        &self,
        _req: Request<LivenessRequest>,
    ) -> Result<Response<LivenessResponse>, Status> {
        Ok(Response::new(LivenessResponse {
            status: "OK".to_string(),
        }))
    }

    // Readiness probe - checks all dependencies are healthy
    // Verifies database connectivity and other critical services
    async fn readiness(
        &self,
        _req: Request<ReadinessRequest>,
    ) -> Result<Response<ReadinessResponse>, Status> {
        let mut failing_services = Vec::new();

        // Check database connectivity
        // For now, we'll do a lightweight check - in a real implementation,
        // you might want to run a simple query like "SELECT 1"
        match self.queue_repo.list_queues().await {
            Ok(_) => {} // Database is healthy
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
