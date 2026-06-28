use pgrx::prelude::*;

pgrx::pg_module_magic!();

/// Get the compiled version of pgqrs-extension
#[pg_extern]
fn pgqrs_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Simple health check function
#[pg_extern]
fn pgqrs_health_check() -> bool {
    true
}

#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any extra postgresql.conf settings required by the tests
        vec![]
    }
}


#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_pgqrs_version() {
        let version = crate::pgqrs_version();
        assert_eq!(version, "0.1.0");
    }

    #[pg_test]
    fn test_pgqrs_health_check() {
        assert!(crate::pgqrs_health_check());
    }
}
