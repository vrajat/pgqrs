use crate::tables::PyQueueMessage;
use crate::{json_to_py, py_to_json, to_py_err, PgqrsError, PyStore};
use ::pgqrs as rust_pgqrs;
use pyo3::prelude::*;
use rust_pgqrs::store::AnyStore;
use rust_pgqrs::types::QueueMessage as RustQueueMessage;
use rust_pgqrs::{Run, Step};
use std::sync::Arc;

#[pyclass]
#[derive(Clone)]
pub struct WorkflowRecord {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub queue_id: i64,
}

impl From<rust_pgqrs::types::WorkflowRecord> for WorkflowRecord {
    fn from(r: rust_pgqrs::types::WorkflowRecord) -> Self {
        WorkflowRecord {
            id: r.id,
            name: r.name,
            queue_id: r.queue_id,
        }
    }
}

#[pyfunction]
pub fn workflow() -> PyWorkflowBuilder {
    PyWorkflowBuilder::default()
}

#[pyfunction]
pub fn run() -> PyRunBuilder {
    PyRunBuilder::default()
}

#[pyfunction]
pub fn step() -> PyStepBuilder {
    PyStepBuilder::default()
}

#[pyclass(name = "Run")]
pub struct PyRun {
    pub(crate) inner: Arc<Run>,
    pub(crate) store: AnyStore,
    pub(crate) workflow_id: i64,
}

#[pymethods]
impl PyRun {
    fn id(&self) -> PyResult<i64> {
        Ok(self.workflow_id)
    }

    #[getter]
    fn current_time(&self) -> Option<String> {
        self.inner.current_time().map(|dt| dt.to_rfc3339())
    }

    fn with_time(&self, time: String) -> PyResult<Self> {
        let dt = chrono::DateTime::parse_from_rfc3339(&time)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?
            .with_timezone(&chrono::Utc);
        Ok(PyRun {
            inner: Arc::new((*self.inner).clone().with_time(dt)),
            store: self.store.clone(),
            workflow_id: self.workflow_id,
        })
    }

    fn start<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let _ = inner.start().await.map_err(to_py_err)?;
            Ok(true)
        })
    }

    fn fail<'a>(&self, py: Python<'a>, error: String) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let _ = inner.fail(&error).await.map_err(to_py_err)?;
            Ok(true)
        })
    }

    fn success<'a>(&self, py: Python<'a>, result: PyObject) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_res = py_to_json(py, result.as_ref(py))?;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let _ = inner.success(&json_res).await.map_err(to_py_err)?;
            Ok(true)
        })
    }

    fn complete<'a>(&self, py: Python<'a>, result: PyObject) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_res = py_to_json(py, result.as_ref(py))?;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let _ = inner.complete(json_res).await.map_err(to_py_err)?;
            Ok(true)
        })
    }

    fn refresh<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let store = self.store.clone();
        let workflow_id = self.workflow_id;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let refreshed = inner.refresh().await.map_err(to_py_err)?;
            Ok(PyRun {
                inner: Arc::new(refreshed),
                store,
                workflow_id,
            })
        })
    }

    fn pause<'a>(
        &self,
        py: Python<'a>,
        message: String,
        resume_after_seconds: u64,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let _ = inner
                .pause(
                    message,
                    std::time::Duration::from_secs(resume_after_seconds),
                )
                .await
                .map_err(to_py_err)?;
            Ok(true)
        })
    }

    fn complete_step<'a>(
        &self,
        py: Python<'a>,
        step_name: String,
        output: PyObject,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_output = py_to_json(py, output.as_ref(py))?;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .complete_step(&step_name, json_output)
                .await
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (step_name, error, current_time=None))]
    fn fail_step<'a>(
        &self,
        py: Python<'a>,
        step_name: String,
        error: PyObject,
        current_time: Option<String>,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_error = py_to_json(py, error.as_ref(py))?;
        let time = if let Some(time_str) = current_time {
            chrono::DateTime::parse_from_rfc3339(&time_str)
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Invalid ISO 8601 timestamp '{}': {}",
                        time_str, e
                    ))
                })?
                .with_timezone(&chrono::Utc)
        } else {
            chrono::Utc::now()
        };

        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .fail_step(&step_name, json_error, time)
                .await
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (step_name, current_time=None))]
    fn acquire_step<'a>(
        &self,
        py: Python<'a>,
        step_name: String,
        current_time: Option<String>,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let store = self.store.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let time = if let Some(time_str) = current_time {
                chrono::DateTime::parse_from_rfc3339(&time_str)
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Invalid ISO 8601 timestamp '{}': {}",
                            time_str, e
                        ))
                    })?
                    .with_timezone(&chrono::Utc)
            } else {
                chrono::Utc::now()
            };

            let step = rust_pgqrs::step()
                .store(&store)
                .run(&inner)
                .name(&step_name)
                .with_time(time)
                .execute()
                .await
                .map_err(to_py_err)?;

            let record = step.record().clone();

            Python::with_gil(|py| {
                if record.status == rust_pgqrs::WorkflowStatus::Success {
                    let output = record.output.unwrap_or(serde_json::Value::Null);
                    Ok(PyStepResult {
                        status: "SKIPPED".to_string(),
                        value: json_to_py(py, &output)?,
                        guard: None,
                    }
                    .into_py(py))
                } else {
                    let py_guard = PyStepGuard::new(step, time);
                    Ok(PyStepResult {
                        status: "EXECUTE".to_string(),
                        value: py.None(),
                        guard: Some(py_guard),
                    }
                    .into_py(py))
                }
            })
        })
    }
}

#[pyclass(name = "StepResult")]
pub struct PyStepResult {
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub value: PyObject,
    #[pyo3(get)]
    pub guard: Option<PyStepGuard>,
}

#[pyclass(name = "StepGuard")]
#[derive(Clone)]
pub struct PyStepGuard {
    pub(crate) inner: Arc<tokio::sync::Mutex<Step>>,
    pub(crate) current_time: chrono::DateTime<chrono::Utc>,
}

#[pymethods]
impl PyStepGuard {
    fn success<'a>(&self, py: Python<'a>, result: PyObject) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_res = py_to_json(py, result.as_ref(py))?;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard.complete(json_res).await.map_err(to_py_err)
        })
    }

    fn fail<'a>(&self, py: Python<'a>, error: String) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let current_time = self.current_time;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard
                .fail_with_json(serde_json::Value::String(error), current_time)
                .await
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (code, message, retry_after=None))]
    fn fail_transient<'a>(
        &self,
        py: Python<'a>,
        code: String,
        message: String,
        retry_after: Option<f64>,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let current_time = self.current_time;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut error_json = serde_json::json!({
                "is_transient": true,
                "code": code,
                "message": message,
            });

            if let Some(delay_secs) = retry_after {
                if !delay_secs.is_finite() || delay_secs < 0.0 {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Invalid retry_after",
                    ));
                }
                let secs = delay_secs.trunc() as u64;
                let nanos =
                    ((delay_secs.fract() * 1_000_000_000.0).round() as u32).min(999_999_999);
                error_json.as_object_mut().unwrap().insert(
                    "retry_after".to_string(),
                    serde_json::json!({"secs": secs, "nanos": nanos}),
                );
            }

            let mut guard = inner.lock().await;
            guard
                .fail_with_json(error_json, current_time)
                .await
                .map_err(to_py_err)
        })
    }
}

impl PyStepGuard {
    pub fn new(step: Step, current_time: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(step)),
            current_time,
        }
    }
}

#[pyclass(name = "WorkflowBuilder")]
#[derive(Default)]
pub struct PyWorkflowBuilder {
    pub(crate) store: Option<AnyStore>,
    pub(crate) name: Option<String>,
    pub(crate) id: Option<i64>,
}

#[pymethods]
impl PyWorkflowBuilder {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn name(mut slf: PyRefMut<'_, Self>, name: String) -> PyRefMut<'_, Self> {
        slf.name = Some(name);
        slf
    }

    fn store(mut slf: PyRefMut<'_, Self>, store: PyStore) -> PyRefMut<'_, Self> {
        slf.store = Some(store.inner);
        slf
    }

    fn id(mut slf: PyRefMut<'_, Self>, id: i64) -> PyRefMut<'_, Self> {
        slf.id = Some(id);
        slf
    }

    fn create<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        let name = self.name.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut builder = rust_pgqrs::workflow();
            if let Some(ref s) = store {
                builder = builder.store(s);
            }
            if let Some(ref n) = name {
                builder = builder.name(n);
            }
            let res = builder.create().await.map_err(to_py_err)?;
            Ok(WorkflowRecord::from(res))
        })
    }

    fn trigger<'a>(&self, py: Python<'a>, input: PyObject) -> PyResult<PyWorkflowTriggerBuilder> {
        let json_input = py_to_json(py, input.as_ref(py))?;
        Ok(PyWorkflowTriggerBuilder {
            store: self.store.clone(),
            name: self.name.clone(),
            id: self.id,
            input: json_input,
        })
    }
}

#[pyclass(name = "WorkflowTriggerBuilder")]
pub struct PyWorkflowTriggerBuilder {
    pub(crate) store: Option<AnyStore>,
    pub(crate) name: Option<String>,
    pub(crate) id: Option<i64>,
    pub(crate) input: serde_json::Value,
}

#[pymethods]
impl PyWorkflowTriggerBuilder {
    fn execute<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        let name = self.name.clone();
        let id = self.id;
        let input = self.input.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut builder = rust_pgqrs::workflow();
            if let Some(ref s) = store {
                builder = builder.store(s);
            }
            if let Some(ref n) = name {
                builder = builder.name(n);
            }
            if let Some(i) = id {
                builder = builder.id(i);
            }
            let trigger = builder.trigger(&input).map_err(to_py_err)?;
            let msg = trigger.execute().await.map_err(to_py_err)?;
            Ok(PyQueueMessage::from(msg))
        })
    }
}

#[pyclass(name = "RunBuilder")]
#[derive(Default)]
pub struct PyRunBuilder {
    pub(crate) store: Option<AnyStore>,
    pub(crate) message: Option<RustQueueMessage>,
}

#[pymethods]
impl PyRunBuilder {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn store(mut slf: PyRefMut<'_, Self>, store: PyStore) -> PyRefMut<'_, Self> {
        slf.store = Some(store.inner);
        slf
    }

    fn message(mut slf: PyRefMut<'_, Self>, message: Py<PyQueueMessage>) -> PyRefMut<'_, Self> {
        Python::with_gil(|py| {
            slf.message = Some(message.borrow(py).inner.clone());
        });
        slf
    }

    fn execute<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        let message = self.message.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut builder = rust_pgqrs::run();
            if let Some(ref s) = store {
                builder = builder.store(s);
            }
            if let Some(m) = message {
                builder = builder.message(m);
            }
            let run = builder.execute().await.map_err(to_py_err)?;
            let workflow_id = run.id();
            Ok(PyRun {
                inner: Arc::new(run),
                store: store.unwrap(),
                workflow_id,
            })
        })
    }
}

#[pyclass(name = "StepBuilder")]
#[derive(Default)]
pub struct PyStepBuilder {
    pub(crate) run: Option<Py<PyRun>>,
    pub(crate) name: Option<String>,
    pub(crate) current_time: Option<chrono::DateTime<chrono::Utc>>,
}

#[pymethods]
impl PyStepBuilder {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn run(mut slf: PyRefMut<'_, Self>, run: Py<PyRun>) -> PyRefMut<'_, Self> {
        slf.run = Some(run);
        slf
    }

    fn name(mut slf: PyRefMut<'_, Self>, name: String) -> PyRefMut<'_, Self> {
        slf.name = Some(name);
        slf
    }

    fn id(mut slf: PyRefMut<'_, Self>, id: String) -> PyRefMut<'_, Self> {
        slf.name = Some(id);
        slf
    }

    fn with_time(mut slf: PyRefMut<'_, Self>, time: String) -> PyResult<PyRefMut<'_, Self>> {
        let dt = chrono::DateTime::parse_from_rfc3339(&time)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?
            .with_timezone(&chrono::Utc);
        slf.current_time = Some(dt);
        Ok(slf)
    }

    fn execute<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let run_py = self
            .run
            .clone()
            .ok_or_else(|| PgqrsError::new_err("Run is required"))?;
        let step_name = self
            .name
            .clone()
            .ok_or_else(|| PgqrsError::new_err("Step name is required"))?;
        let current_time = self.current_time;

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let run = Python::with_gil(|py| {
                let run_borrow = run_py.borrow(py);
                run_borrow.inner.clone()
            });

            let time = current_time.unwrap_or_else(chrono::Utc::now);
            let step = rust_pgqrs::step()
                .run(&run)
                .name(&step_name)
                .with_time(time)
                .execute()
                .await
                .map_err(to_py_err)?;

            let record = step.record().clone();

            Python::with_gil(|py| {
                if record.status == rust_pgqrs::WorkflowStatus::Success {
                    let output = record.output.unwrap_or(serde_json::Value::Null);
                    Ok(PyStepResult {
                        status: "SKIPPED".to_string(),
                        value: json_to_py(py, &output)?,
                        guard: None,
                    }
                    .into_py(py))
                } else {
                    let py_guard = PyStepGuard::new(step, time);
                    Ok(PyStepResult {
                        status: "EXECUTE".to_string(),
                        value: py.None(),
                        guard: Some(py_guard),
                    }
                    .into_py(py))
                }
            })
        })
    }
}
