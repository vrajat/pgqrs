#!/bin/bash
# Script to convert sqlx patterns to turso Connection API

set -e

TURSO_DIR="crates/pgqrs/src/store/turso"

echo "Converting sqlx patterns to turso API..."

# Find all .rs files in turso directory
find "$TURSO_DIR" -name "*.rs" -type f | while read -r file; do
    echo "Processing: $file"
    
    # 1. Fix imports - remove TursoRow import attempts
    sed -i '' 's/use turso::TursoRow;//g' "$file"
    
    # 2. Change pool field to db with Arc
    sed -i '' 's/pool: Database/db: std::sync::Arc<Database>/g' "$file"
    sed -i '' 's/pool: std::sync::Arc<Database>/db: std::sync::Arc<Database>/g' "$file"
    
    # 3. Fix struct constructors
    sed -i '' 's/Self { pool }/Self { db }/g' "$file"
    sed -i '' 's/Self { pool: db }/Self { db }/g' "$file"
    
    # 4. Replace self.pool with self.db
    sed -i '' 's/&self\.pool/\&self.db/g' "$file"
    sed -i '' 's/self\.pool\.clone()/Arc::clone(\&self.db)/g' "$file"
    
    # 5. Fix Error::Internal enum variant usage
    sed -i '' 's/Error::Internal(/Error::Internal { message: /g' "$file"
    sed -i '' 's/\.map_err(Error::Internal)/.map_err(|e| Error::Internal { message: format!("{}", e) })/g' "$file"
    
    # 6. Fix turso::Row usage
    sed -i '' 's/row: turso::Row/row: \&turso::Row/g' "$file"
    
    # 7. Change try_get to get with index - this is complex, will need manual fixes
    # For now, just add a comment
    sed -i '' 's/row\.try_get(/\/\/ TODO: Convert to row.get(index) - row.try_get(/g' "$file"
    
done

echo "Phase 1 complete. Manual fixes still needed for:"
echo "  - turso::query() calls need Connection API"
echo "  - row.try_get() needs column indices"
echo "  - Transaction handling"
echo ""
echo "Run cargo check --features turso to see remaining errors"
