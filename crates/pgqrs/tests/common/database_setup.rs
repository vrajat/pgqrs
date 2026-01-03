//! Shared database setup and cleanup functionality for test containers

use super::constants::*;
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

    // Updated to use AnyStore and builder
    let store = pgqrs::store::AnyStore::connect(&config).await?;
    pgqrs::admin(&store).install().await?;

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

    // Updated to use AnyStore
    let store = pgqrs::store::AnyStore::connect(&config).await?;

    // Drop custom schema if not using 'public'
    if schema != "public" {
        use pgqrs::store::Store;
        let drop_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema);
        store.execute_raw(&drop_sql).await?;
        println!(
            "{} schema '{}' dropped successfully",
            connection_type, schema
        );
    }
    Ok(())
}
