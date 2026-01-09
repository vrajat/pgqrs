#!/usr/bin/env perl
# Convert sqlx patterns to turso Connection API

use strict;
use warnings;
use File::Find;
use File::Slurp;

my $turso_dir = "crates/pgqrs/src/store/turso";

find(\&process_file, $turso_dir);

sub process_file {
    return unless /\.rs$/;
    my $file = $_;
    my $path = $File::Find::name;
    
    print "Processing: $path\n";
    
    my $content = read_file($file);
    my $original = $content;
    
    # 1. Fix imports
    $content =~ s/use turso::TursoRow;//g;
    $content =~ s/use turso::Row;//g;  # We'll re-add it properly
    
    # 2. Add proper imports at the top if not present
    unless ($content =~ /use std::sync::Arc;/) {
        $content =~ s/(use turso::Database;)/$1\nuse std::sync::Arc;/;
    }
    
    # 3. Fix struct field: pool → db with Arc
    $content =~ s/pool: Database/db: Arc<Database>/g;
    $content =~ s/pool: Arc<Database>/db: Arc<Database>/g;
    
    # 4. Fix constructor parameter
    $content =~ s/pub fn new\(pool: Database\)/pub fn new(db: Arc<Database>)/g;
    $content =~ s/pub fn new\(pool: Arc<Database>\)/pub fn new(db: Arc<Database>)/g;
    
    # 5. Fix Self { pool } → Self { db }
    $content =~ s/Self \{ pool \}/Self { db }/g;
    
    # 6. Fix row type in map_row - add & reference
    $content =~ s/fn map_row\(row: turso::Row\)/fn map_row(row: &turso::Row)/g;
    $content =~ s/pub fn map_row\(row: turso::Row\)/pub fn map_row(row: &turso::Row)/g;
    
    # 7. Fix row.try_get() calls - convert to row.get() with indices
    # This is approximate - may need manual fixes
    $content =~ s/row\.try_get\("id"\)/row.get::<i64>(0)/g;
    $content =~ s/row\.try_get\("queue_name"\)/row.get::<String>(1)/g;
    $content =~ s/row\.try_get\("created_at"\)/row.get::<String>(2)/g;
    $content =~ s/row\.try_get\("worker_id"\)/row.get::<i64>(0)/g;
    $content =~ s/row\.try_get\("hostname"\)/row.get::<String>(1)/g;
    $content =~ s/row\.try_get\("port"\)/row.get::<i32>(2)/g;
    $content =~ s/row\.try_get\("status"\)/row.get::<String>(3)/g;
    $content =~ s/row\.try_get\("started_at"\)/row.get::<String>(4)/g;
    $content =~ s/row\.try_get\("last_heartbeat"\)/row.get::<String>(5)/g;
    $content =~ s/row\.try_get\(([^)]+)\)/_TODO_CONVERT_try_get($1)/g;  # Mark remaining
    
    # 8. Fix Error::Internal usage
    $content =~ s/Error::Internal\(/Error::Internal { message: /g;
    $content =~ s/\.map_err\(Error::Internal\)/.map_err(|e| Error::Internal { message: format!("{}", e) })/g;
    
    # 9. Fix self.pool references
    $content =~ s/&self\.pool/\&self.db/g;
    $content =~ s/self\.pool\.clone\(\)/Arc::clone(\&self.db)/g;
    $content =~ s/pool\.clone\(\)/Arc::clone(\&db)/g;
    
    # Write back if changed
    if ($content ne $original) {
        write_file($file, $content);
        print "  ✓ Modified\n";
    } else {
        print "  - No changes\n";
    }
}

print "\nPhase 1 complete. Now run the query converter...\n";
