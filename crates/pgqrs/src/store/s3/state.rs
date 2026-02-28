use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

use super::DurabilityMode;

/// Local file layout for S3-backed SQLite state.
///
/// - Local mode: reads and writes share `write_db.sqlite`
/// - Durable mode: writes use `write_db.sqlite`, reads use `read_db.sqlite`
#[derive(Debug, Clone)]
pub struct LocalDbState {
    mode: DurabilityMode,
    root_dir: PathBuf,
    write_path: PathBuf,
    read_path: PathBuf,
}

impl LocalDbState {
    /// Build local state layout from a sqlite cache DSN.
    pub fn from_cache_dsn(sqlite_cache_dsn: &str, mode: DurabilityMode) -> Result<Self> {
        let cache_path = parse_sqlite_path(sqlite_cache_dsn)?;
        let root_dir = cache_path
            .parent()
            .ok_or_else(|| Error::InvalidConfig {
                field: "dsn".to_string(),
                message: format!("Invalid cache path in sqlite DSN: {}", sqlite_cache_dsn),
            })?
            .join(cache_path.file_stem().unwrap_or_default())
            .with_extension("s3state");

        let write_path = root_dir.join("write_db.sqlite");
        let read_path = match mode {
            DurabilityMode::Local => write_path.clone(),
            DurabilityMode::Durable => root_dir.join("read_db.sqlite"),
        };

        Ok(Self {
            mode,
            root_dir,
            write_path,
            read_path,
        })
    }

    pub fn mode(&self) -> DurabilityMode {
        self.mode
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn write_path(&self) -> &Path {
        &self.write_path
    }

    pub fn read_path(&self) -> &Path {
        &self.read_path
    }

    pub fn write_dsn(&self) -> String {
        sqlite_path_to_dsn(&self.write_path)
    }

    pub fn read_dsn(&self) -> String {
        sqlite_path_to_dsn(&self.read_path)
    }

    /// Ensure root directory/files exist.
    pub fn ensure_files(&self) -> Result<()> {
        fs::create_dir_all(&self.root_dir).map_err(|e| Error::InvalidConfig {
            field: "PGQRS_S3_LOCAL_CACHE_DIR".to_string(),
            message: format!("Failed to create S3 local state dir: {}", e),
        })?;

        touch_file(&self.write_path)?;
        if self.mode == DurabilityMode::Durable && !self.read_path.exists() {
            fs::copy(&self.write_path, &self.read_path).map_err(|e| Error::Internal {
                message: format!("Failed to initialize read_db from write_db: {}", e),
            })?;
        }
        Ok(())
    }

    /// Promote write DB as the committed read DB.
    pub fn promote_write_to_read(&self) -> Result<()> {
        if self.mode == DurabilityMode::Local {
            return Ok(());
        }
        fs::copy(&self.write_path, &self.read_path).map_err(|e| Error::Internal {
            message: format!("Failed to promote write_db -> read_db: {}", e),
        })?;
        Ok(())
    }

    /// Overwrite local DB state from remote bytes.
    pub fn restore_from_remote_bytes(&self, bytes: &[u8]) -> Result<()> {
        self.ensure_files()?;
        fs::write(&self.write_path, bytes).map_err(|e| Error::Internal {
            message: format!(
                "Failed writing recovered write_db '{}': {}",
                self.write_path.display(),
                e
            ),
        })?;
        if self.mode == DurabilityMode::Durable {
            fs::write(&self.read_path, bytes).map_err(|e| Error::Internal {
                message: format!(
                    "Failed writing recovered read_db '{}': {}",
                    self.read_path.display(),
                    e
                ),
            })?;
        }
        Ok(())
    }
}

fn sqlite_path_to_dsn(path: &Path) -> String {
    format!("sqlite://{}?mode=rwc", path.to_string_lossy())
}

fn parse_sqlite_path(dsn: &str) -> Result<PathBuf> {
    let raw = dsn
        .strip_prefix("sqlite://")
        .ok_or_else(|| Error::InvalidConfig {
            field: "dsn".to_string(),
            message: format!("Expected sqlite:// DSN, got: {}", dsn),
        })?;
    let path = raw.split('?').next().unwrap_or_default();
    if path.trim().is_empty() {
        return Err(Error::InvalidConfig {
            field: "dsn".to_string(),
            message: format!("SQLite DSN has empty path: {}", dsn),
        });
    }
    Ok(PathBuf::from(path))
}

fn touch_file(path: &Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    std::fs::File::create(path).map_err(|e| Error::Internal {
        message: format!("Failed to create sqlite file '{}': {}", path.display(), e),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::LocalDbState;
    use crate::store::s3::DurabilityMode;

    #[test]
    fn local_mode_uses_single_file() {
        let state = LocalDbState::from_cache_dsn(
            "sqlite:///tmp/pgqrs_s3_cache.db?mode=rwc",
            DurabilityMode::Local,
        )
        .unwrap();
        assert_eq!(state.write_path(), state.read_path());
    }

    #[test]
    fn durable_mode_uses_dual_files() {
        let state = LocalDbState::from_cache_dsn(
            "sqlite:///tmp/pgqrs_s3_cache.db?mode=rwc",
            DurabilityMode::Durable,
        )
        .unwrap();
        assert_ne!(state.write_path(), state.read_path());
        assert!(state.write_path().ends_with("write_db.sqlite"));
        assert!(state.read_path().ends_with("read_db.sqlite"));
    }

    #[test]
    fn ensure_and_promote_work() {
        let root =
            std::env::temp_dir().join(format!("pgqrs_s3_state_test_{}", uuid::Uuid::new_v4()));
        let dsn = format!("sqlite://{}/cache.db?mode=rwc", root.display());
        let state = LocalDbState::from_cache_dsn(&dsn, DurabilityMode::Durable).unwrap();
        state.ensure_files().unwrap();
        assert!(state.write_path().exists());
        assert!(state.read_path().exists());
        state.promote_write_to_read().unwrap();
    }

    #[test]
    fn restore_from_remote_bytes_restores_both_files_in_durable_mode() {
        let root = std::env::temp_dir().join(format!(
            "pgqrs_s3_state_restore_test_{}",
            uuid::Uuid::new_v4()
        ));
        let dsn = format!("sqlite://{}/cache.db?mode=rwc", root.display());
        let state = LocalDbState::from_cache_dsn(&dsn, DurabilityMode::Durable).unwrap();
        state.ensure_files().unwrap();
        state.restore_from_remote_bytes(b"remote-state").unwrap();
        assert_eq!(std::fs::read(state.write_path()).unwrap(), b"remote-state");
        assert_eq!(std::fs::read(state.read_path()).unwrap(), b"remote-state");
    }
}
