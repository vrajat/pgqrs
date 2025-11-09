//! Shared database setup and cleanup functionality for test containers

use super::constants::*;
use pgqrs::admin::PgqrsAdmin;
use sqlx::postgres::PgPoolOptions;

/// Common database setup function that can be used by all container types
pub async fn setup_database_common(
    dsn: String,
    schema: &str,
    connection_type: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Test the connection
    {
        let pool = PgPoolOptions::new()
            .max_connections(MAX_CONNECTIONS)
            .acquire_timeout(std::time::Duration::from_secs(CONNECTION_TIMEOUT_SECS))
            .connect(&dsn)
            .await?;

        let _val: i32 = sqlx::query_scalar(VERIFICATION_QUERY)
            .fetch_one(&pool)
            .await?;
        println!("{} connection verified", connection_type);

        // Create custom schema if not using 'public'
        if schema != "public" {
            let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", schema);
            if let Err(e) = sqlx::query(&create_schema_sql).execute(&pool).await {
                // Handle concurrent schema creation
                let error_string = e.to_string();
                if !error_string.contains("already exists")
                    && !error_string.contains("pg_namespace_nspname_index")
                    && !error_string.contains("duplicate key value")
                {
                    return Err(e.into());
                }
            }
            println!("Schema '{}' ensured to exist", schema);
        }
    }

    // Install schema using the configured schema
    let config = if schema == "public" {
        pgqrs::config::Config::from_dsn(dsn)
    } else {
        pgqrs::config::Config::from_dsn_with_schema(dsn, schema)?
    };
    let admin = PgqrsAdmin::new(&config).await?;
    admin.install().await?;
    println!("{} schema installed in '{}'", connection_type, schema);

    Ok(())
}

/// Common database cleanup function that can be used by all container types
pub async fn cleanup_database_common(
    dsn: String,
    schema: &str,
    connection_type: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = if schema == "public" {
        pgqrs::config::Config::from_dsn(dsn)
    } else {
        pgqrs::config::Config::from_dsn_with_schema(dsn, schema)?
    };
    let admin = PgqrsAdmin::new(&config).await?;
    admin.uninstall().await?;
    if schema != "public" {
        let drop_schema_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema);
        sqlx::query(&drop_schema_sql).execute(&admin.pool).await?;
        println!("{} schema uninstalled from '{}'", connection_type, schema);
    } else {
        println!("{} schema uninstalled from '{}'", connection_type, schema);
    }
    Ok(())
}
