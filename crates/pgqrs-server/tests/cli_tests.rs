use clap::Parser;
use pgqrs_server::{get_config_path, Cli, Commands};

/// Integration tests for CLI argument parsing
#[test]
fn test_cli_integration_install_command() {
    let cli = Cli::try_parse_from(&["pgqrs-server", "install"]).unwrap();
    assert_eq!(cli.command, Commands::Install);
    assert_eq!(cli.config_path, None);
}

#[test]
fn test_cli_integration_install_with_config() {
    let cli = Cli::try_parse_from(&[
        "pgqrs-server",
        "--config-path",
        "/test/config.yaml",
        "install",
    ])
    .unwrap();
    assert_eq!(cli.command, Commands::Install);
    assert_eq!(cli.config_path, Some("/test/config.yaml".to_string()));
}

#[test]
fn test_cli_integration_short_config_flag() {
    let cli = Cli::try_parse_from(&["pgqrs-server", "-c", "/short/config.yaml", "start"]).unwrap();
    assert_eq!(cli.command, Commands::Start);
    assert_eq!(cli.config_path, Some("/short/config.yaml".to_string()));
}

#[test]
fn test_cli_integration_all_subcommands() {
    // Test install
    let cli = Cli::try_parse_from(&["pgqrs-server", "install"]).unwrap();
    assert_eq!(cli.command, Commands::Install);

    // Test uninstall
    let cli = Cli::try_parse_from(&["pgqrs-server", "uninstall"]).unwrap();
    assert_eq!(cli.command, Commands::Uninstall);

    // Test start
    let cli = Cli::try_parse_from(&["pgqrs-server", "start"]).unwrap();
    assert_eq!(cli.command, Commands::Start);
}

#[test]
fn test_config_path_function_integration() {
    // Test CLI arg takes precedence over env var
    std::env::set_var("PGQRS_CONFIG", "/env/config.yaml");
    let result = get_config_path(Some("/cli/config.yaml".to_string()));
    assert_eq!(result, "/cli/config.yaml");

    // Test env var fallback
    let result = get_config_path(None);
    assert_eq!(result, "/env/config.yaml");

    // Test default fallback
    std::env::remove_var("PGQRS_CONFIG");
    let result = get_config_path(None);
    assert_eq!(result, "config.yaml");

    // Clean up
    std::env::remove_var("PGQRS_CONFIG");
}

#[test]
fn test_cli_error_cases() {
    // Test missing subcommand
    let result = Cli::try_parse_from(&["pgqrs-server"]);
    assert!(result.is_err());

    // Test invalid subcommand
    let result = Cli::try_parse_from(&["pgqrs-server", "invalid"]);
    assert!(result.is_err());

    // Test missing config path value
    let result = Cli::try_parse_from(&["pgqrs-server", "-c", "install"]);
    assert!(result.is_err()); // Should fail because "install" is treated as config path value
}
