#![allow(non_local_definitions)]
use ::pgqrs as rust_pgqrs;
use gethostname::gethostname;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use rust_pgqrs::store::{AnyStore, Store};
use rust_pgqrs::{BackoffStrategy as RustBackoffStrategy, StepRetryPolicy as RustStepRetryPolicy};

use std::future::Future;
use std::sync::{Arc, LazyLock};
use tokio::runtime::Runtime;

mod tables;
mod workers;
mod workflow;

use tables::*;
use workers::*;
use workflow::*;

// Exceptions
pyo3::create_exception!(pgqrs, PgqrsError, pyo3::exceptions::PyException);
pyo3::create_exception!(pgqrs, PgqrsConnectionError, PgqrsError);
pyo3::create_exception!(pgqrs, QueueNotFoundError, PgqrsError);
pyo3::create_exception!(pgqrs, WorkerNotFoundError, PgqrsError);
pyo3::create_exception!(pgqrs, QueueAlreadyExistsError, PgqrsError);
pyo3::create_exception!(pgqrs, MessageNotFoundError, PgqrsError);
pyo3::create_exception!(pgqrs, SerializationError, PgqrsError);
pyo3::create_exception!(pgqrs, ConfigError, PgqrsError);
pyo3::create_exception!(pgqrs, RateLimitedError, PgqrsError);
pyo3::create_exception!(pgqrs, ValidationError, PgqrsError);
pyo3::create_exception!(pgqrs, TimeoutError, PgqrsError);
pyo3::create_exception!(pgqrs, InternalError, PgqrsError);
pyo3::create_exception!(pgqrs, StateTransitionError, PgqrsError);
pyo3::create_exception!(pgqrs, TransientStepError, PgqrsError);
pyo3::create_exception!(pgqrs, RetriesExhaustedError, PgqrsError);
pyo3::create_exception!(pgqrs, StepNotReadyError, PgqrsError);

pub(crate) fn to_py_err(err: rust_pgqrs::Error) -> PyErr {
    match err {
        rust_pgqrs::Error::QueueNotFound { .. } => QueueNotFoundError::new_err(err.to_string()),
        rust_pgqrs::Error::WorkerNotFound { .. }
        | rust_pgqrs::Error::WorkerNotRegistered { .. } => {
            WorkerNotFoundError::new_err(err.to_string())
        }
        rust_pgqrs::Error::QueueAlreadyExists { .. } => {
            QueueAlreadyExistsError::new_err(err.to_string())
        }
        rust_pgqrs::Error::MessageNotFound { .. } => MessageNotFoundError::new_err(err.to_string()),
        rust_pgqrs::Error::Serialization(_) => SerializationError::new_err(err.to_string()),
        rust_pgqrs::Error::MissingConfig { .. } | rust_pgqrs::Error::InvalidConfig { .. } => {
            ConfigError::new_err(err.to_string())
        }
        rust_pgqrs::Error::RateLimited { .. } => RateLimitedError::new_err(err.to_string()),
        rust_pgqrs::Error::ValidationFailed { .. }
        | rust_pgqrs::Error::PayloadTooLarge { .. }
        | rust_pgqrs::Error::SchemaValidation { .. } => ValidationError::new_err(err.to_string()),
        rust_pgqrs::Error::Timeout { .. } => TimeoutError::new_err(err.to_string()),
        rust_pgqrs::Error::ConnectionFailed { .. }
        | rust_pgqrs::Error::PoolExhausted { .. }
        | rust_pgqrs::Error::QueryFailed { .. }
        | rust_pgqrs::Error::TransactionFailed { .. } => {
            PgqrsConnectionError::new_err(err.to_string())
        }
        #[cfg(any(feature = "postgres", feature = "sqlite"))]
        rust_pgqrs::Error::Database(_) => PgqrsConnectionError::new_err(err.to_string()),
        #[cfg(feature = "turso")]
        rust_pgqrs::Error::Turso(_) => PgqrsConnectionError::new_err(err.to_string()),
        rust_pgqrs::Error::InvalidStateTransition { .. }
        | rust_pgqrs::Error::WorkerHasPendingMessages { .. } => {
            StateTransitionError::new_err(err.to_string())
        }
        #[cfg(any(feature = "postgres", feature = "sqlite"))]
        rust_pgqrs::Error::MigrationFailed(_) => InternalError::new_err(err.to_string()),
        rust_pgqrs::Error::Internal { .. } => InternalError::new_err(err.to_string()),
        rust_pgqrs::Error::Transient { .. } => TransientStepError::new_err(err.to_string()),
        rust_pgqrs::Error::RetriesExhausted { .. } => {
            RetriesExhaustedError::new_err(err.to_string())
        }
        rust_pgqrs::Error::StepNotReady { .. } => StepNotReadyError::new_err(err.to_string()),
        _ => PgqrsError::new_err(err.to_string()),
    }
}

pub(crate) struct RuntimeManager {
    pub(crate) rt: Runtime,
}

impl RuntimeManager {
    pub(crate) fn new() -> Self {
        Self {
            rt: Runtime::new().expect("Failed to initialize Tokio runtime"),
        }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.rt.block_on(future)
    }
}

pub(crate) static RUNTIME: LazyLock<RuntimeManager> = LazyLock::new(RuntimeManager::new);

pub(crate) fn get_runtime() -> &'static RuntimeManager {
    &RUNTIME
}

// Helper to convert serde-json value to PyObject
pub(crate) fn json_to_py(py: Python, value: &serde_json::Value) -> PyResult<PyObject> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.to_object(py)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.to_object(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.to_object(py))
            } else {
                Ok(n.to_string().to_object(py)) // Fallback
            }
        }
        serde_json::Value::String(s) => Ok(s.to_object(py)),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_to_py(py, item)?)?;
            }
            Ok(list.to_object(py))
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            Ok(dict.to_object(py))
        }
    }
}

pub(crate) fn py_to_json(py: Python, val: &PyAny) -> PyResult<serde_json::Value> {
    let json_module = py.import("json")?;
    let json_str = json_module
        .call_method1("dumps", (val,))?
        .extract::<String>()?;
    serde_json::from_str(&json_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[pyclass(name = "Config")]
#[derive(Clone)]
pub struct PyConfig {
    #[allow(dead_code)]
    inner: rust_pgqrs::Config,
}

#[pymethods]
impl PyConfig {
    #[new]
    #[pyo3(signature = (dsn, schema=None, max_connections=None))]
    fn new(dsn: String, schema: Option<String>, max_connections: Option<u32>) -> PyResult<Self> {
        let mut inner = rust_pgqrs::Config::from_dsn(&dsn);
        if let Some(s) = schema {
            inner = inner.with_schema(&s);
        }
        if let Some(m) = max_connections {
            inner = inner.with_max_connections(m);
        }
        Ok(PyConfig { inner })
    }

    #[staticmethod]
    fn from_dsn(dsn: String) -> Self {
        PyConfig {
            inner: rust_pgqrs::Config::from_dsn(dsn),
        }
    }

    #[getter]
    fn get_dsn(&self) -> String {
        self.inner.dsn.clone()
    }

    #[getter]
    fn get_schema(&self) -> String {
        self.inner.schema.clone()
    }

    #[setter]
    fn set_schema(&mut self, schema: String) {
        self.inner.schema = schema;
    }

    #[getter]
    fn get_max_connections(&self) -> u32 {
        self.inner.max_connections
    }

    #[setter]
    fn set_max_connections(&mut self, max: u32) {
        self.inner.max_connections = max;
    }

    #[getter]
    fn get_connection_timeout_seconds(&self) -> u64 {
        self.inner.connection_timeout_seconds
    }

    #[setter]
    fn set_connection_timeout_seconds(&mut self, timeout: u64) {
        self.inner.connection_timeout_seconds = timeout;
    }

    #[getter]
    fn get_default_lock_time_seconds(&self) -> u32 {
        self.inner.default_lock_time_seconds
    }

    #[setter]
    fn set_default_lock_time_seconds(&mut self, lock_time: u32) {
        self.inner.default_lock_time_seconds = lock_time;
    }

    #[getter]
    fn get_default_max_batch_size(&self) -> usize {
        self.inner.default_max_batch_size
    }

    #[setter]
    fn set_default_max_batch_size(&mut self, batch_size: usize) {
        self.inner.default_max_batch_size = batch_size;
    }

    #[getter]
    fn get_max_read_ct(&self) -> i32 {
        self.inner.max_read_ct
    }

    #[setter]
    fn set_max_read_ct(&mut self, max_read_ct: i32) {
        self.inner.max_read_ct = max_read_ct;
    }

    #[getter]
    fn get_heartbeat_interval_seconds(&self) -> u64 {
        self.inner.heartbeat_interval
    }

    #[setter]
    fn set_heartbeat_interval_seconds(&mut self, interval: u64) {
        self.inner.heartbeat_interval = interval;
    }
}

/// Backoff strategy for step retries
#[pyclass(name = "BackoffStrategy")]
#[derive(Clone)]
pub struct PyBackoffStrategy {
    inner: RustBackoffStrategy,
}

#[pymethods]
impl PyBackoffStrategy {
    /// Create a fixed delay backoff strategy
    #[staticmethod]
    fn fixed(delay_seconds: u32) -> Self {
        Self {
            inner: RustBackoffStrategy::Fixed { delay_seconds },
        }
    }

    /// Create an exponential backoff strategy
    #[staticmethod]
    fn exponential(base_seconds: u32, max_seconds: u32) -> Self {
        Self {
            inner: RustBackoffStrategy::Exponential {
                base_seconds,
                max_seconds,
            },
        }
    }

    /// Create an exponential backoff with jitter strategy
    #[staticmethod]
    fn exponential_with_jitter(base_seconds: u32, max_seconds: u32) -> Self {
        Self {
            inner: RustBackoffStrategy::ExponentialWithJitter {
                base_seconds,
                max_seconds,
            },
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}

/// Retry policy for workflow steps
#[pyclass(name = "StepRetryPolicy")]
#[derive(Clone)]
pub struct PyStepRetryPolicy {
    inner: RustStepRetryPolicy,
}

#[pymethods]
impl PyStepRetryPolicy {
    #[new]
    #[pyo3(signature = (max_attempts=3, backoff=None))]
    fn new(max_attempts: u32, backoff: Option<PyBackoffStrategy>) -> Self {
        let backoff_inner =
            backoff
                .map(|b| b.inner)
                .unwrap_or(RustBackoffStrategy::ExponentialWithJitter {
                    base_seconds: 1,
                    max_seconds: 60,
                });

        Self {
            inner: RustStepRetryPolicy {
                max_attempts,
                backoff: backoff_inner,
            },
        }
    }

    #[getter]
    fn max_attempts(&self) -> u32 {
        self.inner.max_attempts
    }

    fn __repr__(&self) -> String {
        format!(
            "StepRetryPolicy(max_attempts={}, backoff={:?})",
            self.inner.max_attempts, self.inner.backoff
        )
    }
}

#[pyclass(name = "Store")]
#[derive(Clone)]
pub struct PyStore {
    pub(crate) inner: AnyStore,
}

#[pymethods]
impl PyStore {
    fn producer<'a>(&self, py: Python<'a>, queue: String) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let hostname = format!(
                "{}-{}",
                gethostname().to_string_lossy(),
                uuid::Uuid::new_v4()
            );
            // Use Store trait method directly - returns Box<dyn Producer + 'static>
            let producer = store
                .producer(&queue, &hostname, 0, store.config())
                .await
                .map_err(to_py_err)?;

            Ok(PyProducer {
                inner: Arc::new(producer),
            })
        })
    }

    fn consumer<'a>(&self, py: Python<'a>, queue: String) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let hostname = format!(
                "{}-{}",
                gethostname().to_string_lossy(),
                uuid::Uuid::new_v4()
            );
            // Use Store trait method directly - returns Box<dyn Consumer + 'static>
            let consumer = store
                .consumer(&queue, &hostname, 0, store.config())
                .await
                .map_err(to_py_err)?;

            Ok(PyConsumer {
                inner: Arc::new(consumer),
            })
        })
    }

    fn consume_iter(&self, queue: String, poll_interval_ms: Option<u64>) -> PyConsumerIterator {
        PyConsumerIterator::new(self.inner.clone(), queue, poll_interval_ms.unwrap_or(50))
    }

    fn queue<'a>(&self, py: Python<'a>, name: String) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let q = store.queue(&name).await.map_err(to_py_err)?;
            Ok(PyQueueInfo::from(q))
        })
    }

    fn get_workers<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(PyWorkers { store }) })
    }

    fn get_queues<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(PyQueues { store }) })
    }

    fn get_messages<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(PyMessages { store }) })
    }

    fn get_archive<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(PyArchive { store }) })
    }

    fn get_workflows<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(PyWorkflows { store }) })
    }

    fn get_workflow_runs<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(PyWorkflowRuns { store }) })
    }

    fn get_workflow_steps<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(PyWorkflowSteps { store }) })
    }

    fn bootstrap<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store.bootstrap().await.map_err(to_py_err)
        })
    }
}

#[pyfunction]
fn connect<'a>(py: Python<'a>, dsn: String) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let config = rust_pgqrs::Config::from_dsn(&dsn);
        let store = rust_pgqrs::connect_with_config(&config)
            .await
            .map_err(to_py_err)?;
        Ok(PyStore { inner: store })
    })
}

#[pyfunction]
fn connect_with<'a>(py: Python<'a>, config: PyConfig) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let store = rust_pgqrs::connect_with_config(&config.inner)
            .await
            .map_err(to_py_err)?;
        Ok(PyStore { inner: store })
    })
}

#[pyclass(name = "Admin")]
pub struct PyAdmin {
    pub(crate) store: AnyStore,
}

#[pymethods]
impl PyAdmin {
    #[new]
    fn new(store: PyStore) -> Self {
        PyAdmin { store: store.inner }
    }

    fn install<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store.bootstrap().await.map_err(to_py_err)
        })
    }

    fn verify<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            rust_pgqrs::admin(&store).verify().await.map_err(to_py_err)
        })
    }

    fn delete_queue<'a>(&self, py: Python<'a>, name: String) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            rust_pgqrs::admin(&store)
                .delete_queue_by_name(&name)
                .await
                .map_err(to_py_err)?;
            Ok(true)
        })
    }

    fn release_worker_messages<'a>(&self, py: Python<'a>, worker_id: i64) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let released = rust_pgqrs::admin(&store)
                .release_worker_messages(worker_id)
                .await
                .map_err(to_py_err)?;
            Ok(released)
        })
    }

    #[pyo3(signature = (name, arg=None))]
    fn create_workflow<'a>(
        &self,
        py: Python<'a>,
        name: String,
        arg: Option<PyObject>,
    ) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        let json_arg = if let Some(a) = arg {
            Some(py_to_json(py, a.as_ref(py))?)
        } else {
            None
        };

        pyo3_asyncio::tokio::future_into_py(py, async move {
            // Ensure workflow exists
            let _wf_rec = rust_pgqrs::workflow()
                .store(&store)
                .name(&name)
                .create()
                .await
                .map_err(to_py_err)?;

            if let Some(input) = json_arg {
                // If arg is provided, trigger and return a run handle
                let run_msg = rust_pgqrs::workflow()
                    .store(&store)
                    .name(&name)
                    .trigger(&input)
                    .map_err(to_py_err)?
                    .execute()
                    .await
                    .map_err(to_py_err)?;

                let workflow = store.run(run_msg).await.map_err(to_py_err)?;
                let workflow_id = workflow.id();

                Ok(PyRun {
                    inner: Arc::new(workflow),
                    store,
                    workflow_id,
                })
            } else {
                // Return dummy result for non-triggering create
                Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "create_workflow in Python must have an arg to trigger a run",
                ))
            }
        })
    }
}

#[pyfunction]
fn admin(store: PyStore) -> PyAdmin {
    PyAdmin { store: store.inner }
}

#[pymodule]
fn _pgqrs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyAdmin>()?;
    m.add_class::<PyProducer>()?;
    m.add_class::<PyConsumer>()?;
    m.add_class::<PyWorkers>()?;
    m.add_class::<PyQueues>()?;
    m.add_class::<PyMessages>()?;
    m.add_class::<PyArchive>()?;
    m.add_class::<PyWorkflows>()?;
    m.add_class::<PyWorkflowRuns>()?;
    m.add_class::<PyWorkflowSteps>()?;
    m.add_class::<PyRunRecord>()?;
    m.add_class::<PyStepRecord>()?;
    m.add_class::<PyConfig>()?;
    m.add_class::<PyStore>()?;
    m.add_class::<PyQueueMessage>()?;
    m.add_class::<PyQueueInfo>()?;
    m.add_class::<PyRun>()?;
    m.add_class::<PyStepResult>()?;
    m.add_class::<PyStepGuard>()?;
    m.add_class::<PyArchivedMessage>()?;
    m.add_class::<PyConsumerIterator>()?;
    m.add_class::<PyBackoffStrategy>()?;
    m.add_class::<PyStepRetryPolicy>()?;
    m.add_class::<WorkflowRecord>()?;
    m.add_class::<PyWorkflowBuilder>()?;
    m.add_class::<PyWorkflowTriggerBuilder>()?;
    m.add_class::<PyRunBuilder>()?;
    m.add_class::<PyStepBuilder>()?;

    // Exceptions
    m.add("PgqrsError", py.get_type::<PgqrsError>())?;
    m.add(
        "PgqrsConnectionError",
        py.get_type::<PgqrsConnectionError>(),
    )?;
    m.add("QueueNotFoundError", py.get_type::<QueueNotFoundError>())?;
    m.add("WorkerNotFoundError", py.get_type::<WorkerNotFoundError>())?;
    m.add(
        "QueueAlreadyExistsError",
        py.get_type::<QueueAlreadyExistsError>(),
    )?;
    m.add(
        "MessageNotFoundError",
        py.get_type::<MessageNotFoundError>(),
    )?;
    m.add("SerializationError", py.get_type::<SerializationError>())?;
    m.add("ConfigError", py.get_type::<ConfigError>())?;
    m.add("RateLimitedError", py.get_type::<RateLimitedError>())?;
    m.add("ValidationError", py.get_type::<ValidationError>())?;
    m.add("TimeoutError", py.get_type::<TimeoutError>())?;
    m.add("InternalError", py.get_type::<InternalError>())?;
    m.add(
        "StateTransitionError",
        py.get_type::<StateTransitionError>(),
    )?;
    m.add("TransientStepError", py.get_type::<TransientStepError>())?;
    m.add(
        "RetriesExhaustedError",
        py.get_type::<RetriesExhaustedError>(),
    )?;
    m.add("StepNotReadyError", py.get_type::<StepNotReadyError>())?;

    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(connect_with, m)?)?;
    m.add_function(wrap_pyfunction!(admin, m)?)?;
    m.add_function(wrap_pyfunction!(produce, m)?)?;
    m.add_function(wrap_pyfunction!(produce_batch, m)?)?;
    m.add_function(wrap_pyfunction!(consume, m)?)?;
    m.add_function(wrap_pyfunction!(consume_batch, m)?)?;
    m.add_function(wrap_pyfunction!(enqueue, m)?)?;
    m.add_function(wrap_pyfunction!(enqueue_batch, m)?)?;
    m.add_function(wrap_pyfunction!(dequeue, m)?)?;
    m.add_function(wrap_pyfunction!(archive, m)?)?;
    m.add_function(wrap_pyfunction!(archive_batch, m)?)?;
    m.add_function(wrap_pyfunction!(delete, m)?)?;
    m.add_function(wrap_pyfunction!(enqueue_delayed, m)?)?;
    m.add_function(wrap_pyfunction!(extend_vt, m)?)?;
    m.add_function(wrap_pyfunction!(workflow::workflow, m)?)?;
    m.add_function(wrap_pyfunction!(workflow::run, m)?)?;
    m.add_function(wrap_pyfunction!(workflow::step, m)?)?;

    Ok(())
}
