use ::pgqrs as rust_pgqrs;
use pyo3::prelude::*;
use rust_pgqrs::types::{WorkerStatus as RustWorkerStatus, WorkflowStatus as RustWorkflowStatus};

#[pyclass(name = "WorkerStatus")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyWorkerStatus {
    Ready,
    Polling,
    Suspended,
    Interrupted,
    Stopped,
}

impl From<RustWorkerStatus> for PyWorkerStatus {
    fn from(status: RustWorkerStatus) -> Self {
        match status {
            RustWorkerStatus::Ready => Self::Ready,
            RustWorkerStatus::Polling => Self::Polling,
            RustWorkerStatus::Suspended => Self::Suspended,
            RustWorkerStatus::Interrupted => Self::Interrupted,
            RustWorkerStatus::Stopped => Self::Stopped,
        }
    }
}

#[pyclass(name = "WorkflowStatus")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyWorkflowStatus {
    Queued,
    Running,
    Paused,
    Cancelled,
    Success,
    Error,
}

impl From<RustWorkflowStatus> for PyWorkflowStatus {
    fn from(status: RustWorkflowStatus) -> Self {
        match status {
            RustWorkflowStatus::Queued => Self::Queued,
            RustWorkflowStatus::Running => Self::Running,
            RustWorkflowStatus::Paused => Self::Paused,
            RustWorkflowStatus::Cancelled => Self::Cancelled,
            RustWorkflowStatus::Success => Self::Success,
            RustWorkflowStatus::Error => Self::Error,
        }
    }
}

#[pyclass(name = "StepResultStatus")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyStepResultStatus {
    Execute,
    Skipped,
}

#[cfg(feature = "s3")]
#[pyclass(name = "DurabilityMode")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyDurabilityMode {
    Durable,
    Local,
}

#[cfg(feature = "s3")]
impl From<rust_pgqrs::store::s3::DurabilityMode> for PyDurabilityMode {
    fn from(inner: rust_pgqrs::store::s3::DurabilityMode) -> Self {
        match inner {
            rust_pgqrs::store::s3::DurabilityMode::Durable => Self::Durable,
            rust_pgqrs::store::s3::DurabilityMode::Local => Self::Local,
        }
    }
}

#[cfg(feature = "s3")]
impl From<PyDurabilityMode> for rust_pgqrs::store::s3::DurabilityMode {
    fn from(mode: PyDurabilityMode) -> Self {
        match mode {
            PyDurabilityMode::Durable => Self::Durable,
            PyDurabilityMode::Local => Self::Local,
        }
    }
}

#[cfg(feature = "s3")]
#[pyclass(name = "SyncState")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PySyncState {
    LocalMissing,
    RemoteMissingClean,
    RemoteMissingDirty,
    InSync,
    LocalChanges,
    RemoteChanges,
    ConcurrentChanges,
}

#[cfg(feature = "s3")]
impl From<rust_pgqrs::store::s3::SyncState> for PySyncState {
    fn from(inner: rust_pgqrs::store::s3::SyncState) -> Self {
        match inner {
            rust_pgqrs::store::s3::SyncState::LocalMissing => Self::LocalMissing,
            rust_pgqrs::store::s3::SyncState::RemoteMissing { local_dirty: false } => {
                Self::RemoteMissingClean
            }
            rust_pgqrs::store::s3::SyncState::RemoteMissing { local_dirty: true } => {
                Self::RemoteMissingDirty
            }
            rust_pgqrs::store::s3::SyncState::InSync => Self::InSync,
            rust_pgqrs::store::s3::SyncState::LocalChanges => Self::LocalChanges,
            rust_pgqrs::store::s3::SyncState::RemoteChanges => Self::RemoteChanges,
            rust_pgqrs::store::s3::SyncState::ConcurrentChanges => Self::ConcurrentChanges,
        }
    }
}
