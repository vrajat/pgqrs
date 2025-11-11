//! Archive operations and management interface for pgqrs.
//!
//! This module provides backward compatibility by re-exporting the Archive struct
//! from the new tables module structure.

pub use crate::tables::pgqrs_archive::PgqrsArchive as Archive;