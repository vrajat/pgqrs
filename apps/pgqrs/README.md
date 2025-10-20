# pgqrs CLI Application

A comprehensive command-line interface for interacting with the pgqrs queue service.

## Installation

```bash
# Build the CLI application
cargo build -p pgqrs-cli --release

# Install to system (optional)
cargo install --path apps/pgqrs
```

## Quick Start

```bash
# Set your pgqrs server endpoint
export PGQRS_ENDPOINT="http://localhost:50051"

# Check server health
pgqrs health liveness

# Create a queue
pgqrs queue create my-queue

# Add a message
pgqrs message enqueue my-queue "Hello, World!"

# Retrieve messages
pgqrs message dequeue my-queue

# List all queues
pgqrs queue list
```

## Global Options

- `--endpoint <URL>`: Server endpoint (env: `PGQRS_ENDPOINT`)
- `--api-key <KEY>`: API key for authentication (env: `PGQRS_API_KEY`)
- `--connect-timeout <SECONDS>`: Connection timeout (default: 10)
- `--rpc-timeout <SECONDS>`: RPC timeout (default: 30)
- `--output <FORMAT>`: Output format - `table` or `json` (default: table)
- `--quiet`: Suppress output except errors
- `--verbose`: Enable verbose logging

## Queue Management

### Create a Queue
```bash
# Create a regular queue
pgqrs queue create my-queue

# Create an unlogged queue (faster, less durable)
pgqrs queue create fast-queue --unlogged
```

### List Queues
```bash
# Table format (default)
pgqrs queue list

# JSON format
pgqrs queue list --output json
```

### Get Queue Information
```bash
pgqrs queue get my-queue
```

### Delete a Queue
```bash
pgqrs queue delete my-queue
```

### Queue Statistics
```bash
pgqrs queue stats my-queue
```

## Message Operations

### Enqueue Messages
```bash
# Simple text message
pgqrs message enqueue my-queue "Hello, World!"

# JSON message
pgqrs message enqueue my-queue '{"user": "alice", "action": "login"}'

# From file
pgqrs message enqueue my-queue @message.json

# With delay (seconds)
pgqrs message enqueue my-queue "Delayed message" --delay 60
```

### Dequeue Messages
```bash
# Get one message (default)
pgqrs message dequeue my-queue

# Get multiple messages
pgqrs message dequeue my-queue --max-messages 5

# Custom lease time
pgqrs message dequeue my-queue --lease-seconds 120
```

### Message Acknowledgment
```bash
# Acknowledge successful processing
pgqrs message ack <message-id>

# Reject message (requeue)
pgqrs message nack <message-id>

# Reject and send to dead letter queue
pgqrs message nack <message-id> --dead-letter --reason "Processing failed"
```

### Requeue Messages
```bash
# Requeue immediately
pgqrs message requeue <message-id>

# Requeue with delay
pgqrs message requeue <message-id> --delay 300
```

### Extend Message Lease
```bash
# Extend lease by 30 seconds (default)
pgqrs message extend-lease <message-id>

# Custom extension
pgqrs message extend-lease <message-id> --additional-seconds 120
```

### Peek at Messages
```bash
# Peek at messages without removing them
pgqrs message peek my-queue

# Limit number of messages
pgqrs message peek my-queue --limit 5
```

### List In-Flight Messages
```bash
pgqrs message list-in-flight my-queue
```

### Dead Letter Management
```bash
# List dead letter messages
pgqrs message list-dead-letters my-queue

# Purge dead letter messages
pgqrs message purge-dead-letters my-queue
```

## Health Checks

```bash
# Liveness probe (server is running)
pgqrs health liveness

# Readiness probe (server is ready to handle requests)
pgqrs health readiness

# General health check
pgqrs health check
```

## Output Formats

### Table Format (Default)
Human-readable tables with proper formatting and colors.

```bash
pgqrs queue list
```

### JSON Format
Machine-readable JSON output for scripts and automation.

```bash
pgqrs queue list --output json
```

### Quiet Mode
Suppress all output except errors (useful for scripts).

```bash
pgqrs queue create my-queue --quiet
```

## Shell Completions

Generate shell completions for enhanced CLI experience:

```bash
# Bash
pgqrs completions bash > ~/.local/share/bash-completion/completions/pgqrs

# Zsh
pgqrs completions zsh > ~/.zsh/completions/_pgqrs

# Fish
pgqrs completions fish > ~/.config/fish/completions/pgqrs.fish

# PowerShell
pgqrs completions powershell > pgqrs.ps1
```

## Environment Variables

- `PGQRS_ENDPOINT`: Default server endpoint
- `PGQRS_API_KEY`: Default API key for authentication

## Examples

### Basic Workflow
```bash
# Set environment
export PGQRS_ENDPOINT="http://localhost:50051"

# Create and use a queue
pgqrs queue create tasks
pgqrs message enqueue tasks '{"task": "process_data", "id": 123}'
pgqrs message dequeue tasks --max-messages 1 > messages.json

# Process message and acknowledge
MESSAGE_ID=$(jq -r '.[0].id' messages.json)
pgqrs message ack "$MESSAGE_ID"
```

### Monitoring Script
```bash
#!/bin/bash
# Monitor queue statistics
for queue in $(pgqrs queue list --output json | jq -r '.[].name'); do
    echo "=== $queue ==="
    pgqrs queue stats "$queue"
done
```

### Batch Processing
```bash
# Enqueue multiple messages from files
for file in *.json; do
    pgqrs message enqueue my-queue "@$file"
done

# Process all messages
while true; do
    messages=$(pgqrs message dequeue my-queue --max-messages 10 --output json)
    if [ "$(echo "$messages" | jq 'length')" -eq 0 ]; then
        break
    fi
    
    # Process messages...
    echo "$messages" | jq -r '.[].id' | while read -r id; do
        pgqrs message ack "$id"
    done
done
```

## Error Handling

The CLI returns appropriate exit codes:
- `0`: Success
- `1`: General error (connection failed, invalid arguments, etc.)
- `2`: Command line parsing error

Use `--verbose` flag for detailed error information and debugging.