use crate::client::PgqrsClient;
use anyhow::Result;
use chrono::DateTime;
use serde::Serialize;
use std::fs;
use tabled::Tabled;

use crate::cli::args::{Cli, HealthCommands, MessageCommands, QueueCommands};
use crate::cli::output::OutputManager;
use crate::error::CliError;

// Data structures for output formatting
#[derive(Debug, Serialize, Tabled)]
pub struct QueueInfo {
    pub id: i64,
    pub name: String,
    pub created_at: String,
    pub unlogged: bool,
}

#[derive(Debug, Serialize, Tabled)]
pub struct MessageInfo {
    pub id: i64,
    pub queue_name: String,
    pub payload: String,
    pub enqueued_at: String,
    pub visibility_timeout: String,
    pub read_count: i32,
}

#[derive(Debug, Serialize, Tabled)]
pub struct QueueStats {
    pub queue: String,
    pub ready: u64,
    pub in_flight: u64,
    pub dead_lettered: u64,
}

#[derive(Debug, Serialize, Tabled)]
pub struct HealthStatus {
    pub status: String,
    #[tabled(display_with = "display_failing_services")]
    pub failing_services: Vec<String>,
}

fn display_failing_services(failing_services: &[String]) -> String {
    if failing_services.is_empty() {
        "None".to_string()
    } else {
        failing_services.join(", ")
    }
}

impl HealthStatus {
    pub fn from_readiness(response: crate::ReadinessResponse) -> Self {
        Self {
            status: response.status,
            failing_services: response.failing_services,
        }
    }

    pub fn from_liveness(response: crate::LivenessResponse) -> Self {
        Self {
            status: response.status,
            failing_services: vec![], // LivenessResponse doesn't have failing_services
        }
    }
}

// Helper functions
fn format_timestamp(unix_timestamp: i64) -> String {
    if let Some(datetime) = DateTime::from_timestamp(unix_timestamp, 0) {
        datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string()
    } else {
        "Invalid timestamp".to_string()
    }
}

fn read_payload(payload_arg: &str) -> Result<String, CliError> {
    if payload_arg.starts_with('@') {
        let file_path = &payload_arg[1..];
        fs::read_to_string(file_path).map_err(CliError::File)
    } else {
        Ok(payload_arg.to_string())
    }
}

fn payload_to_bytes(payload: &str) -> Result<Vec<u8>, CliError> {
    // Validate JSON if it looks like JSON
    if payload.trim_start().starts_with('{') || payload.trim_start().starts_with('[') {
        serde_json::from_str::<serde_json::Value>(payload)?;
    }
    Ok(payload.as_bytes().to_vec())
}

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

// Queue command handlers
pub async fn handle_queue_command(
    mut client: PgqrsClient,
    command: &QueueCommands,
    cli: &Cli,
) -> Result<()> {
    let output = OutputManager::new(cli.output.clone(), cli.quiet);

    match command {
        QueueCommands::Create { name, unlogged } => {
            let queue = client.create_queue(name, *unlogged).await?;
            let queue_info = QueueInfo {
                id: queue.id,
                name: queue.name,
                created_at: format_timestamp(queue.created_at_unix),
                unlogged: queue.unlogged,
            };
            output.print_success(&queue_info)?;
            output.print_success_message(&format!("Queue '{}' created successfully", name));
        }

        QueueCommands::List => {
            let queues = client.list_queues().await?;
            let queue_infos: Vec<QueueInfo> = queues
                .into_iter()
                .map(|q| QueueInfo {
                    id: q.id,
                    name: q.name,
                    created_at: format_timestamp(q.created_at_unix),
                    unlogged: q.unlogged,
                })
                .collect();
            output.print_list(&queue_infos)?;
        }

        QueueCommands::Get { name } => {
            let queue = client.get_queue(name).await?;
            let queue_info = QueueInfo {
                id: queue.id,
                name: queue.name,
                created_at: format_timestamp(queue.created_at_unix),
                unlogged: queue.unlogged,
            };
            output.print_success(&queue_info)?;
        }

        QueueCommands::Delete { name } => {
            client.delete_queue(name).await?;
            output.print_success_message(&format!("Queue '{}' deleted successfully", name));
        }

        QueueCommands::Stats { name } => {
            let stats = client.stats(name).await?;
            let queue_stats = QueueStats {
                queue: name.clone(),
                ready: stats.ready,
                in_flight: stats.in_flight,
                dead_lettered: stats.dead_lettered,
            };
            output.print_success(&queue_stats)?;
        }
    }

    Ok(())
}

// Message command handlers
pub async fn handle_message_command(
    mut client: PgqrsClient,
    command: &MessageCommands,
    cli: &Cli,
) -> Result<()> {
    let output = OutputManager::new(cli.output.clone(), cli.quiet);

    match command {
        MessageCommands::Enqueue {
            queue,
            payload,
            delay,
        } => {
            let payload_content = read_payload(payload)?;
            let payload_bytes = payload_to_bytes(&payload_content)?;

            let message_id = client.enqueue(queue, payload_bytes, *delay as i64).await?;
            output.print_success_message(&format!("Message enqueued with ID: {}", message_id));
        }

        MessageCommands::Dequeue {
            queue,
            max_messages,
            lease_seconds,
        } => {
            let messages = client
                .dequeue(queue, *max_messages as i32, *lease_seconds as i64)
                .await?;
            let message_infos: Vec<MessageInfo> = messages
                .into_iter()
                .map(|m| MessageInfo {
                    id: m.id,
                    queue_name: m.queue_name,
                    payload: bytes_to_string(&m.payload),
                    enqueued_at: format_timestamp(m.enqueued_at_unix),
                    visibility_timeout: format_timestamp(m.vt_unix),
                    read_count: m.read_ct,
                })
                .collect();
            output.print_list(&message_infos)?;
        }

        MessageCommands::Ack { message_id } => {
            client.ack(message_id).await?;
            output.print_success_message(&format!("Message {} acknowledged", message_id));
        }

        MessageCommands::Nack {
            message_id,
            reason,
            dead_letter,
        } => {
            client
                .nack(message_id, reason.clone(), *dead_letter)
                .await?;
            let action = if *dead_letter {
                "negative acknowledged and sent to dead letter queue"
            } else {
                "negative acknowledged"
            };
            output.print_success_message(&format!("Message {} {}", message_id, action));
        }

        MessageCommands::Requeue { message_id, delay } => {
            client.requeue(message_id, *delay as i64).await?;
            output.print_success_message(&format!("Message {} requeued", message_id));
        }

        MessageCommands::ExtendLease {
            message_id,
            additional_seconds,
        } => {
            client
                .extend_lease(message_id, *additional_seconds as i64)
                .await?;
            output.print_success_message(&format!(
                "Message {} lease extended by {} seconds",
                message_id, additional_seconds
            ));
        }

        MessageCommands::Peek { queue, limit } => {
            let messages = client.peek(queue, *limit as i32).await?;
            let message_infos: Vec<MessageInfo> = messages
                .into_iter()
                .map(|m| MessageInfo {
                    id: m.id,
                    queue_name: m.queue_name,
                    payload: bytes_to_string(&m.payload),
                    enqueued_at: format_timestamp(m.enqueued_at_unix),
                    visibility_timeout: format_timestamp(m.vt_unix),
                    read_count: m.read_ct,
                })
                .collect();
            output.print_list(&message_infos)?;
        }

        MessageCommands::ListInFlight { queue, limit } => {
            let messages = client.list_in_flight(queue, *limit as i32).await?;
            let message_infos: Vec<MessageInfo> = messages
                .into_iter()
                .map(|m| MessageInfo {
                    id: m.id,
                    queue_name: m.queue_name,
                    payload: bytes_to_string(&m.payload),
                    enqueued_at: format_timestamp(m.enqueued_at_unix),
                    visibility_timeout: format_timestamp(m.vt_unix),
                    read_count: m.read_ct,
                })
                .collect();
            output.print_list(&message_infos)?;
        }

        MessageCommands::ListDeadLetters { queue, limit } => {
            let messages = client.list_dead_letters(queue, *limit as i32).await?;
            let message_infos: Vec<MessageInfo> = messages
                .into_iter()
                .map(|m| MessageInfo {
                    id: m.id,
                    queue_name: m.queue_name,
                    payload: bytes_to_string(&m.payload),
                    enqueued_at: format_timestamp(m.enqueued_at_unix),
                    visibility_timeout: format_timestamp(m.vt_unix),
                    read_count: m.read_ct,
                })
                .collect();
            output.print_list(&message_infos)?;
        }

        MessageCommands::PurgeDeadLetters { queue } => {
            client.purge_dead_letters(queue).await?;
            output.print_success_message(&format!(
                "Dead letter messages purged from queue '{}'",
                queue
            ));
        }
    }

    Ok(())
}

// Health command handlers
pub async fn handle_health_command(
    mut client: PgqrsClient,
    command: &HealthCommands,
    cli: &Cli,
) -> Result<()> {
    let output = OutputManager::new(cli.output.clone(), cli.quiet);

    match command {
        HealthCommands::Liveness => {
            let response = client.liveness().await?;
            let health_status = HealthStatus::from_liveness(response);
            output.print_success(&health_status)?;
        }

        HealthCommands::Readiness => {
            let response = client.readiness().await?;
            let health_status = HealthStatus::from_readiness(response);
            output.print_success(&health_status)?;
        }

        HealthCommands::Check => {
            // For now, use readiness as the general health check
            let response = client.readiness().await?;
            let health_status = HealthStatus::from_readiness(response);
            output.print_success(&health_status)?;
        }
    }

    Ok(())
}
