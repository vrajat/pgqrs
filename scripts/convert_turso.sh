#!/bin/bash
# Comprehensive conversion script for sqlx → turso API

set -e

cd "$(dirname "$0")/.."

TURSO_DIR="crates/pgqrs/src/store/turso"

echo "=== Phase 1: Fix types and references ==="

find "$TURSO_DIR" -name "*.rs" -type f | while read file; do
    echo "Phase 1: $file"
    
    # Fix imports
    sed -i '' '/use turso::TursoRow;/d' "$file"
    
    # Ensure Arc is imported
    if ! grep -q "use std::sync::Arc" "$file"; then
        sed -i '' '/use turso::Database;/a\
use std::sync::Arc;
' "$file"
    fi
    
    # Fix struct fields: Database → Arc<Database>
    sed -i '' 's/\([ ]*\)pool: Database,/\1db: Arc<Database>,/g' "$file"
    sed -i '' 's/\([ ]*\)pool: Arc<Database>,/\1db: Arc<Database>,/g' "$file"
    
    # Fix constructor signatures
    sed -i '' 's/pub fn new(pool: Database)/pub fn new(db: Arc<Database>)/g' "$file"
    sed -i '' 's/pub fn new(pool: Arc<Database>)/pub fn new(db: Arc<Database>)/g' "$file"
    
    # Fix Self { pool } → Self { db }
    sed -i '' 's/Self { pool }/Self { db }/g' "$file"
    
    # Fix map_row signatures - add & to Row
    sed -i '' 's/fn map_row(row: turso::Row)/fn map_row(row: \&turso::Row)/g' "$file"
    sed -i '' 's/pub fn map_row(row: turso::Row)/pub fn map_row(row: \&turso::Row)/g' "$file"
    
    # Fix all self.pool → self.db
    sed -i '' 's/self\.pool/self.db/g' "$file"
    
    # Fix Error::Internal usage
    sed -i '' 's/Error::Internal(/Error::Internal { message: /g' "$file"
    
done

echo ""
echo "=== Phase 2: Fix row.try_get() calls ==="

find "$TURSO_DIR" -name "*.rs" -type f | while read file; do
    echo "Phase 2: $file"
    
    # Common column patterns (index-based)
    sed -i '' 's/row\.try_get("id")?/row.get::<i64>(0)?/g' "$file"
    sed -i '' 's/row\.try_get("queue_name")?/row.get::<String>(1)?/g' "$file"  
    sed -i '' 's/row\.try_get("created_at")?/row.get::<String>(2)?/g' "$file"
    
    # Mark any remaining try_get for manual review
    sed -i '' 's/row\.try_get(/\/\/ TODO_CONVERT: row.try_get(/g' "$file"
done

echo ""
echo "=== Conversion complete! ==="
echo ""
echo "Remaining work:"
echo "  1. Convert turso::query() calls to Connection API"
echo "  2. Fix remaining row.try_get() marked with TODO_CONVERT"
echo "  3. Run: cargo check --features turso"
echo ""
echo "Hint: Most query patterns need:"
echo "  let conn = self.db.connect()?;"
echo "  let rows = conn.query(SQL, params).await?;"
