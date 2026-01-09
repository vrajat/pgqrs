#!/usr/bin/env python3
"""
Convert sqlx query patterns to turso Connection API
"""

import re
import sys
from pathlib import Path

def convert_query_one(content):
    """Convert turso::query().fetch_one() patterns"""
    
    # Pattern: turso::query(SQL).bind(x).fetch_one(&self.pool)
    # Becomes: conn.query(SQL, params).await?.next().await?
    
    pattern = r'turso::query\(([^)]+)\)((?:\s*\.bind\([^)]+\))*)\s*\.fetch_one\(&self\.(?:pool|db)\)'
    
    def replace(match):
        sql = match.group(1)
        binds = match.group(2)
        
        # Extract bind parameters
        params = re.findall(r'\.bind\(([^)]+)\)', binds)
        
        if params:
            param_str = ', '.join(params)
            return f'self.db.connect()?.query({sql}, ({param_str},)).await?.next().await?'
        else:
            return f'self.db.connect()?.query({sql}, ()).await?.next().await?'
    
    return re.sub(pattern, replace, content)

def convert_query_all(content):
    """Convert turso::query().fetch_all() patterns"""
    
    pattern = r'turso::query\(([^)]+)\)((?:\s*\.bind\([^)]+\))*)\s*\.fetch_all\(&self\.(?:pool|db)\)'
    
    def replace(match):
        sql = match.group(1)
        binds = match.group(2)
        
        params = re.findall(r'\.bind\(([^)]+)\)', binds)
        
        if params:
            param_str = ', '.join(params)
            return f'self.db.connect()?.query({sql}, ({param_str},)).await?'
        else:
            return f'self.db.connect()?.query({sql}, ()).await?'
    
    return re.sub(pattern, replace, content)

def convert_execute(content):
    """Convert .execute() patterns"""
    
    pattern = r'turso::query\(([^)]+)\)((?:\s*\.bind\([^)]+\))*)\s*\.execute\(&self\.(?:pool|db)\)'
    
    def replace(match):
        sql = match.group(1)
        binds = match.group(2)
        
        params = re.findall(r'\.bind\(([^)]+)\)', binds)
        
        if params:
            param_str = ', '.join(params)
            return f'self.db.connect()?.execute({sql}, ({param_str},)).await?'
        else:
            return f'self.db.connect()?.execute({sql}, ()).await?'
    
    return re.sub(pattern, replace, content)

def convert_query_scalar(content):
    """Convert turso::query_scalar() patterns"""
    
    pattern = r'turso::query_scalar\(([^)]+)\)((?:\s*\.bind\([^)]+\))*)\s*\.fetch_one\(&self\.(?:pool|db)\)'
    
    def replace(match):
        sql = match.group(1)
        binds = match.group(2)
        
        params = re.findall(r'\.bind\(([^)]+)\)', binds)
        
        if params:
            param_str = ', '.join(params)
            return f'{{ let mut rows = self.db.connect()?.query({sql}, ({param_str},)).await?; rows.next().await?.unwrap().get(0)? }}'
        else:
            return f'{{ let mut rows = self.db.connect()?.query({sql}, ()).await?; rows.next().await?.unwrap().get(0)? }}'
    
    return re.sub(pattern, replace, content)

def process_file(filepath):
    """Process a single Rust file"""
    
    print(f"Processing {filepath}")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    original = content
    
    # Apply conversions
    content = convert_execute(content)
    content = convert_query_scalar(content)
    content = convert_query_one(content)
    content = convert_query_all(content)
    
    # Only write if changed
    if content != original:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"  ✓ Modified")
    else:
        print(f"  - No changes")

def main():
    turso_dir = Path("crates/pgqrs/src/store/turso")
    
    if not turso_dir.exists():
        print(f"Error: {turso_dir} not found")
        sys.exit(1)
    
    # Process all .rs files
    for rs_file in turso_dir.rglob("*.rs"):
        process_file(rs_file)
    
    print("\nConversion complete!")
    print("Note: Manual fixes still needed for:")
    print("  - row.try_get() → row.get(index)")
    print("  - Transaction handling")
    print("  - Error types")

if __name__ == "__main__":
    main()
