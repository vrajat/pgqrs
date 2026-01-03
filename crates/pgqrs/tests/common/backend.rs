//! Test backend selection and configuration.

/// Supported database backends for testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestBackend {
    Postgres,
    Sqlite,
    Turso,
}

impl TestBackend {
    /// Parse from environment variable or default to Postgres.
    pub fn from_env() -> Self {
        match std::env::var("PGQRS_TEST_BACKEND")
            .unwrap_or_else(|_| "postgres".to_string())
            .to_lowercase()
            .as_str()
        {
            "sqlite" => Self::Sqlite,
            "turso" => Self::Turso,
            _ => Self::Postgres, // postgres, pg, or default
        }
    }

    /// Parse from string (for CLI args).
    #[allow(dead_code)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "postgres" | "pg" => Some(Self::Postgres),
            "sqlite" => Some(Self::Sqlite),
            "turso" => Some(Self::Turso),
            _ => None,
        }
    }

    /// Get the DSN environment variable name for this backend.
    #[allow(dead_code)]
    pub fn dsn_env_var(&self) -> &'static str {
        match self {
            Self::Postgres => "PGQRS_TEST_POSTGRES_DSN",
            Self::Sqlite => "PGQRS_TEST_SQLITE_PATH",
            Self::Turso => "PGQRS_TEST_TURSO_DSN",
        }
    }
}
