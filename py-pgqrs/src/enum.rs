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
