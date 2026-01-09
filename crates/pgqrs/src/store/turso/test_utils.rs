#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use turso::Database;
#[cfg(test)]
use uuid::Uuid;

#[cfg(test)]
pub async fn create_test_db() -> Arc<Database> {
    let db_name = format!("test_{}.db", Uuid::new_v4());
    let dir = std::env::temp_dir();
    let path = dir.join(db_name);
    let path_str = path.to_str().expect("Valid path");

    // Create local DB using Builder
    // new_local takes &str
    let db = turso::Builder::new_local(path_str)
        .build()
        .await
        .expect("Failed to open test DB");

    let conn = db.connect().expect("Failed to connect");

    // Run migrations
    for stmt in crate::store::turso::schema::SCHEMA {
        conn.execute(stmt, ())
            .await
            .expect("Failed to run migration");
    }

    Arc::new(db)
}
