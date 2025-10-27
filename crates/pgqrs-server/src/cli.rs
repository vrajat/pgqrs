use clap::{Parser, Subcommand};

pub fn get_config_path(cli_path: Option<String>) -> String {
    cli_path
        .or_else(|| std::env::var("PGQRS_CONFIG").ok())
        .unwrap_or_else(|| "config.yaml".to_string())
}

#[derive(Parser, Debug, PartialEq)]
#[command(name = "pgqrs-server")]
#[command(about = "A PostgreSQL queue management server")]
pub struct Cli {
    /// Path to the configuration file
    #[arg(short = 'c', long = "config-path")]
    pub config_path: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug, PartialEq)]
pub enum Commands {
    /// Initialize the database schema
    Install,
    /// Uninstall the database schema
    Uninstall,
    /// Start the gRPC server
    Start,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn test_config_path_precedence() {
        // Test CLI argument takes precedence
        let result = get_config_path(Some("/custom/path.yaml".to_string()));
        assert_eq!(result, "/custom/path.yaml");

        // Test fallback to default when no CLI arg or env var
        std::env::remove_var("PGQRS_CONFIG");
        let result = get_config_path(None);
        assert_eq!(result, "config.yaml");

        // Test env var fallback when no CLI arg
        std::env::set_var("PGQRS_CONFIG", "/env/path.yaml");
        let result = get_config_path(None);
        assert_eq!(result, "/env/path.yaml");

        // Clean up
        std::env::remove_var("PGQRS_CONFIG");
    }

    #[test]
    fn test_cli_parsing_install() {
        let cli = Cli::try_parse_from(&["pgqrs-server", "install"]).unwrap();
        assert_eq!(cli.command, Commands::Install);
        assert_eq!(cli.config_path, None);
    }

    #[test]
    fn test_cli_parsing_with_config_path() {
        let cli =
            Cli::try_parse_from(&["pgqrs-server", "-c", "/test/config.yaml", "start"]).unwrap();
        assert_eq!(cli.command, Commands::Start);
        assert_eq!(cli.config_path, Some("/test/config.yaml".to_string()));
    }

    #[test]
    fn test_cli_parsing_with_long_config_path() {
        let cli = Cli::try_parse_from(&[
            "pgqrs-server",
            "--config-path",
            "/test/config.yaml",
            "uninstall",
        ])
        .unwrap();
        assert_eq!(cli.command, Commands::Uninstall);
        assert_eq!(cli.config_path, Some("/test/config.yaml".to_string()));
    }

    #[test]
    fn test_cli_parsing_invalid_command() {
        let result = Cli::try_parse_from(&["pgqrs-server", "invalid-command"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_parsing_missing_command() {
        let result = Cli::try_parse_from(&["pgqrs-server"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_help_generation() {
        let result = Cli::try_parse_from(&["pgqrs-server", "--help"]);
        assert!(result.is_err()); // --help causes clap to exit with an error code

        // Test that help text can be generated
        let mut cmd = Cli::command();
        let help_text = cmd.render_help();
        let help_string = help_text.to_string();

        // Basic validation that help contains expected content
        assert!(help_string.contains("pgqrs-server"));
        assert!(help_string.contains("install"));
        assert!(help_string.contains("uninstall"));
        assert!(help_string.contains("start"));
        assert!(help_string.contains("config-path"));
    }
}
