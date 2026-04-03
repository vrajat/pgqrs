use ::pgqrs as rust_pgqrs;
#[cfg(feature = "s3")]
use pyo3::class::basic::CompareOp;
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
    Success,
    Error,
}

impl From<RustWorkflowStatus> for PyWorkflowStatus {
    fn from(status: RustWorkflowStatus) -> Self {
        match status {
            RustWorkflowStatus::Queued => Self::Queued,
            RustWorkflowStatus::Running => Self::Running,
            RustWorkflowStatus::Paused => Self::Paused,
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
fn durability_mode_to_str(mode: rust_pgqrs::store::s3::DurabilityMode) -> &'static str {
    match mode {
        rust_pgqrs::store::s3::DurabilityMode::Durable => "durable",
        rust_pgqrs::store::s3::DurabilityMode::Local => "local",
    }
}

#[cfg(feature = "s3")]
fn sync_state_to_str(state: rust_pgqrs::store::s3::SyncState) -> &'static str {
    match state {
        rust_pgqrs::store::s3::SyncState::LocalMissing => "local_missing",
        rust_pgqrs::store::s3::SyncState::RemoteMissing { local_dirty: false } => {
            "remote_missing_clean"
        }
        rust_pgqrs::store::s3::SyncState::RemoteMissing { local_dirty: true } => {
            "remote_missing_dirty"
        }
        rust_pgqrs::store::s3::SyncState::InSync => "in_sync",
        rust_pgqrs::store::s3::SyncState::LocalChanges => "local_changes",
        rust_pgqrs::store::s3::SyncState::RemoteChanges => "remote_changes",
        rust_pgqrs::store::s3::SyncState::ConcurrentChanges => "concurrent_changes",
    }
}

#[cfg(feature = "s3")]
#[pyclass(name = "DurabilityMode", frozen)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PyDurabilityMode {
    inner: rust_pgqrs::store::s3::DurabilityMode,
}

#[cfg(feature = "s3")]
impl From<rust_pgqrs::store::s3::DurabilityMode> for PyDurabilityMode {
    fn from(inner: rust_pgqrs::store::s3::DurabilityMode) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "s3")]
impl From<PyDurabilityMode> for rust_pgqrs::store::s3::DurabilityMode {
    fn from(mode: PyDurabilityMode) -> Self {
        mode.inner
    }
}

#[cfg(feature = "s3")]
#[pyclass(name = "SyncState", frozen)]
#[derive(Clone, PartialEq, Eq)]
pub struct PySyncState {
    inner: rust_pgqrs::store::s3::SyncState,
}

#[cfg(feature = "s3")]
impl From<rust_pgqrs::store::s3::SyncState> for PySyncState {
    fn from(inner: rust_pgqrs::store::s3::SyncState) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "s3")]
#[pymethods]
impl PyDurabilityMode {
    #[classattr]
    #[allow(non_snake_case)]
    fn DURABLE() -> Self {
        rust_pgqrs::store::s3::DurabilityMode::Durable.into()
    }

    #[classattr]
    #[allow(non_snake_case)]
    fn LOCAL() -> Self {
        rust_pgqrs::store::s3::DurabilityMode::Local.into()
    }

    #[getter]
    fn value(&self) -> &'static str {
        durability_mode_to_str(self.inner)
    }

    fn __repr__(&self) -> String {
        match self.inner {
            rust_pgqrs::store::s3::DurabilityMode::Durable => "DurabilityMode.DURABLE".to_string(),
            rust_pgqrs::store::s3::DurabilityMode::Local => "DurabilityMode.LOCAL".to_string(),
        }
    }

    fn __str__(&self) -> &'static str {
        durability_mode_to_str(self.inner)
    }

    fn __richcmp__(
        &self,
        other: PyRef<'_, PyDurabilityMode>,
        op: CompareOp,
        py: Python<'_>,
    ) -> PyObject {
        match op {
            CompareOp::Eq => (self.inner == other.inner).into_py(py),
            CompareOp::Ne => (self.inner != other.inner).into_py(py),
            _ => py.NotImplemented(),
        }
    }
}

#[cfg(feature = "s3")]
#[pymethods]
impl PySyncState {
    #[getter]
    fn value(&self) -> &'static str {
        sync_state_to_str(self.inner.clone())
    }

    fn __repr__(&self) -> String {
        format!("SyncState.{}", self.value().to_ascii_uppercase())
    }

    fn __str__(&self) -> &'static str {
        self.value()
    }

    fn __richcmp__(&self, other: PyRef<'_, PySyncState>, op: CompareOp, py: Python<'_>) -> PyObject {
        match op {
            CompareOp::Eq => (self.inner == other.inner).into_py(py),
            CompareOp::Ne => (self.inner != other.inner).into_py(py),
            _ => py.NotImplemented(),
        }
    }
}
