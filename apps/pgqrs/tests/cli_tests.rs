use std::process::Command;

#[test]
fn test_cli_help() {
    let output = Command::new("cargo")
        .args(["run", "-p", "pgqrs-cli", "--", "--help"])
        .output()
        .expect("Failed to run CLI");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("A CLI for interacting with pgqrs queue service"));
    assert!(stdout.contains("queue"));
    assert!(stdout.contains("message"));
    assert!(stdout.contains("health"));
    assert!(stdout.contains("completions"));
}

#[test]
fn test_queue_subcommand_help() {
    let output = Command::new("cargo")
        .args(["run", "-p", "pgqrs-cli", "--", "queue", "--help"])
        .output()
        .expect("Failed to run CLI");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Queue management commands"));
    assert!(stdout.contains("create"));
    assert!(stdout.contains("list"));
    assert!(stdout.contains("delete"));
    assert!(stdout.contains("stats"));
}

#[test]
fn test_message_subcommand_help() {
    let output = Command::new("cargo")
        .args(["run", "-p", "pgqrs-cli", "--", "message", "--help"])
        .output()
        .expect("Failed to run CLI");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Message operations"));
    assert!(stdout.contains("enqueue"));
    assert!(stdout.contains("dequeue"));
    assert!(stdout.contains("ack"));
    assert!(stdout.contains("nack"));
}

#[test]
fn test_health_subcommand_help() {
    let output = Command::new("cargo")
        .args(["run", "-p", "pgqrs-cli", "--", "health", "--help"])
        .output()
        .expect("Failed to run CLI");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Health check commands"));
    assert!(stdout.contains("liveness"));
    assert!(stdout.contains("readiness"));
    assert!(stdout.contains("check"));
}

#[test]
fn test_completions_generation() {
    let output = Command::new("cargo")
        .args(["run", "-p", "pgqrs-cli", "--", "completions", "bash"])
        .output()
        .expect("Failed to run CLI");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("_pgqrs"));
    assert!(stdout.contains("complete"));
}

#[test]
fn test_invalid_endpoint_fails() {
    let output = Command::new("cargo")
        .args([
            "run", "-p", "pgqrs-cli", "--",
            "--endpoint", "http://invalid:9999",
            "--connect-timeout", "1",
            "health", "liveness"
        ])
        .output()
        .expect("Failed to run CLI");

    // Should fail to connect but not panic
    assert!(!output.status.success());
}

#[test]
fn test_output_format_flags() {
    let output = Command::new("cargo")
        .args(["run", "-p", "pgqrs-cli", "--", "--output", "json", "--help"])
        .output()
        .expect("Failed to run CLI");

    assert!(output.status.success());
    // Should accept the output format flag without errors
}

#[test]
fn test_environment_variables() {
    let output = Command::new("cargo")
        .env("PGQRS_ENDPOINT", "http://custom:1234")
        .env("PGQRS_API_KEY", "test-key")
        .args(["run", "-p", "pgqrs-cli", "--", "--help"])
        .output()
        .expect("Failed to run CLI");

    assert!(output.status.success());
    // Should accept environment variables without errors
}