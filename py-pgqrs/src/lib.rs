#![allow(non_local_definitions)]
use ::pgqrs as rust_pgqrs;
use gethostname::gethostname;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use rust_pgqrs::store::{AnyStore, Store};
use rust_pgqrs::types::{
    QueueMessage as RustQueueMessage, QueueRecord as RustQueueRecord,
    WorkerRecord as RustWorkerInfo,
};
use rust_pgqrs::{BackoffStrategy as RustBackoffStrategy, StepRetryPolicy as RustStepRetryPolicy};
use rust_pgqrs::{Run, StepGuard};

use pyo3::pyasync::IterANextOutput;
use std::future::Future;
use std::sync::{Arc, LazyLock};
use tokio::runtime::Runtime;

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

fn to_py_err(err: rust_pgqrs::Error) -> PyErr {
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

struct RuntimeManager {
    rt: Runtime,
}

impl RuntimeManager {
    fn new() -> Self {
        Self {
            rt: Runtime::new().expect("Failed to initialize Tokio runtime"),
        }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.rt.block_on(future)
    }
}

static RUNTIME: LazyLock<RuntimeManager> = LazyLock::new(RuntimeManager::new);

fn get_runtime() -> &'static RuntimeManager {
    &RUNTIME
}

// Helper to convert serde-json value to PyObject
fn json_to_py(py: Python, value: &serde_json::Value) -> PyResult<PyObject> {
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

fn py_to_json(py: Python, val: &PyAny) -> PyResult<serde_json::Value> {
    let json_module = py.import("json")?;
    let json_str = json_module
        .call_method1("dumps", (val,))?
        .extract::<String>()?;
    serde_json::from_str(&json_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[pyclass(name = "Config")]
#[derive(Clone)]
struct PyConfig {
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

struct IteratorState {
    store: AnyStore,
    queue: String,
    consumer: Option<Arc<Box<dyn rust_pgqrs::Consumer>>>,
    poll_interval: tokio::time::Duration,
}

#[pyclass]
struct ConsumerIterator {
    inner: Arc<tokio::sync::Mutex<IteratorState>>,
}

impl ConsumerIterator {
    fn new(store: AnyStore, queue: String, poll_interval_ms: u64) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(IteratorState {
                store,
                queue,
                consumer: None,
                poll_interval: tokio::time::Duration::from_millis(poll_interval_ms),
            })),
        }
    }
}

#[pymethods]
impl ConsumerIterator {
    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    #[pyo3(name = "__anext__")]
    fn anext<'a>(&self, py: Python<'a>) -> PyResult<IterANextOutput<&'a PyAny, &'a PyAny>> {
        let inner = self.inner.clone();
        let fut = pyo3_asyncio::tokio::future_into_py(py, async move {
            // Acquire lock to get or initialize consumer
            let (consumer, poll_interval) = {
                let mut state = inner.lock().await;

                if state.consumer.is_none() {
                    let c = state
                        .store
                        .consumer_ephemeral(&state.queue, state.store.config())
                        .await
                        .map_err(to_py_err)?;
                    state.consumer = Some(Arc::new(c));
                }

                (
                    state.consumer.as_ref().unwrap().clone(),
                    state.poll_interval,
                )
            }; // Lock dropped here

            loop {
                // Fetch 1 message
                let msgs = consumer.dequeue_many(1).await.map_err(to_py_err)?;

                if let Some(msg) = msgs.into_iter().next() {
                    return Ok(QueueMessage::from(msg));
                }

                // Wait a bit before polling again
                tokio::time::sleep(poll_interval).await;
            }
        })?;
        Ok(IterANextOutput::Yield(fut))
    }

    fn delete<'a>(&self, py: Python<'a>, message_id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let state = inner.lock().await;
            if let Some(consumer) = &state.consumer {
                consumer.delete(message_id).await.map_err(to_py_err)
            } else {
                Err(PgqrsError::new_err("Consumer not initialized"))
            }
        })
    }

    fn archive<'a>(&self, py: Python<'a>, message_id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let state = inner.lock().await;
            if let Some(consumer) = &state.consumer {
                consumer
                    .archive(message_id)
                    .await
                    .map_err(to_py_err)
                    .map(|_| true)
            } else {
                Err(PgqrsError::new_err("Consumer not initialized"))
            }
        })
    }

    fn close<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut state = inner.lock().await;
            if let Some(consumer) = &mut state.consumer {
                consumer.suspend().await.map_err(to_py_err)?;
                consumer.shutdown().await.map_err(to_py_err)
            } else {
                Ok(())
            }
        })
    }

    fn __aenter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let this = slf.into_py(py);
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(this) })
    }

    fn __aexit__<'a>(
        &self,
        py: Python<'a>,
        _exc_type: Option<&PyAny>,
        _exc_value: Option<&PyAny>,
        _traceback: Option<&PyAny>,
    ) -> PyResult<&'a PyAny> {
        self.close(py)
    }
}

#[pyclass(name = "Store")]
#[derive(Clone)]
struct PyStore {
    inner: AnyStore,
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

            Ok(Producer {
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

            Ok(Consumer {
                inner: Arc::new(consumer),
            })
        })
    }

    fn consume_iter(&self, queue: String, poll_interval_ms: Option<u64>) -> ConsumerIterator {
        ConsumerIterator::new(self.inner.clone(), queue, poll_interval_ms.unwrap_or(50))
    }

    fn queue<'a>(&self, py: Python<'a>, name: String) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let q = store.queue(&name).await.map_err(to_py_err)?;
            Ok(QueueInfo::from(q))
        })
    }

    fn get_workers<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(Workers { store }) })
    }

    fn get_queues<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(Queues { store }) })
    }

    fn get_messages<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(Messages { store }) })
    }

    fn get_archive<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move { Ok(Archive { store }) })
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

#[pyclass]
struct Admin {
    store: AnyStore,
}

#[pymethods]
impl Admin {
    #[new]
    fn new(store: PyStore) -> Self {
        Admin { store: store.inner }
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
fn admin(store: PyStore) -> Admin {
    Admin { store: store.inner }
}

#[pyclass]
#[derive(Clone)]
struct WorkflowRecord {
    #[pyo3(get)]
    id: i64,
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    queue_id: i64,
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
fn produce<'a>(
    py: Python<'a>,
    store: PyStore,
    queue: String,
    payload: PyObject,
) -> PyResult<&'a PyAny> {
    let rust_store = store.inner.clone();
    let json_payload = py_to_json(py, payload.as_ref(py))?;
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let msg_ids = rust_pgqrs::enqueue()
            .message(&json_payload)
            .to(&queue)
            .execute(&rust_store)
            .await
            .map_err(to_py_err)?;

        // Return single ID
        Ok(*msg_ids.first().unwrap())
    })
}

#[pyfunction]
fn produce_batch<'a>(
    py: Python<'a>,
    store: PyStore,
    queue: String,
    payloads: Vec<PyObject>,
) -> PyResult<&'a PyAny> {
    let rust_store = store.inner.clone();
    let mut json_payloads = Vec::new();
    for p in payloads {
        json_payloads.push(py_to_json(py, p.as_ref(py))?);
    }
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let msg_ids = rust_pgqrs::enqueue()
            .messages(&json_payloads)
            .to(&queue)
            .execute(&rust_store)
            .await
            .map_err(to_py_err)?;
        Ok(msg_ids)
    })
}

#[pyfunction]
fn consume<'a>(
    py: Python<'a>,
    store: PyStore,
    queue: String,
    handler: PyObject,
) -> PyResult<&'a PyAny> {
    let rust_store = store.inner.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let consumer = rust_store
            .consumer_ephemeral(&queue, rust_store.config())
            .await
            .map_err(to_py_err)?;

        let msgs = consumer.dequeue_many(1).await.map_err(to_py_err)?;

        if let Some(msg) = msgs.first() {
            let py_msg = QueueMessage::from(msg.clone());
            let res: PyResult<PyObject> = Python::with_gil(|py| {
                let fut = handler.call1(py, (py_msg,))?;
                pyo3_asyncio::tokio::into_future(fut.as_ref(py))
            })?
            .await;

            match res {
                Ok(_) => {
                    consumer.archive(msg.id).await.map_err(to_py_err)?;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(())
    })
}

#[pyfunction]
fn consume_batch<'a>(
    py: Python<'a>,
    store: PyStore,
    queue: String,
    batch_size: usize,
    handler: PyObject,
) -> PyResult<&'a PyAny> {
    let rust_store = store.inner.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let consumer = rust_store
            .consumer_ephemeral(&queue, rust_store.config())
            .await
            .map_err(to_py_err)?;

        let msgs = consumer.dequeue_many(batch_size).await.map_err(to_py_err)?;

        if !msgs.is_empty() {
            let py_msgs: Vec<_> = msgs.iter().map(|m| QueueMessage::from(m.clone())).collect();
            let res: PyResult<PyObject> = Python::with_gil(|py| {
                let fut = handler.call1(py, (py_msgs,))?;
                pyo3_asyncio::tokio::into_future(fut.as_ref(py))
            })?
            .await;

            match res {
                Ok(_) => {
                    let msg_ids: Vec<_> = msgs.iter().map(|m| m.id).collect();
                    consumer.archive_many(msg_ids).await.map_err(to_py_err)?;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(())
    })
}

#[pyfunction]
fn enqueue<'a>(py: Python<'a>, producer: &Producer, payload: PyObject) -> PyResult<&'a PyAny> {
    let inner = producer.inner.clone();
    let json_payload = py_to_json(py, payload.as_ref(py))?;
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let msg = inner.enqueue(&json_payload).await.map_err(to_py_err)?;
        Ok(msg.id)
    })
}

#[pyfunction]
fn enqueue_batch<'a>(
    py: Python<'a>,
    producer: &Producer,
    payloads: Vec<PyObject>,
) -> PyResult<&'a PyAny> {
    let inner = producer.inner.clone();
    let mut json_payloads = Vec::new();
    for p in payloads {
        json_payloads.push(py_to_json(py, p.as_ref(py))?);
    }
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let msgs = inner
            .batch_enqueue(&json_payloads)
            .await
            .map_err(to_py_err)?;
        Ok(msgs.iter().map(|m| m.id).collect::<Vec<_>>())
    })
}

#[pyfunction]
fn dequeue<'a>(py: Python<'a>, consumer: &Consumer, batch_size: usize) -> PyResult<&'a PyAny> {
    let inner = consumer.inner.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let messages = inner.dequeue_many(batch_size).await.map_err(to_py_err)?;
        Ok(messages
            .into_iter()
            .map(QueueMessage::from)
            .collect::<Vec<_>>())
    })
}

#[pyfunction]
fn enqueue_delayed<'a>(
    py: Python<'a>,
    producer: &Producer,
    payload: &PyAny,
    delay_seconds: u32,
) -> PyResult<&'a PyAny> {
    producer.enqueue_delayed(py, payload, delay_seconds)
}

#[pyfunction]
fn extend_vt<'a>(
    py: Python<'a>,
    consumer: &Consumer,
    message: &QueueMessage,
    seconds: u32,
) -> PyResult<&'a PyAny> {
    consumer.extend_vt(py, message.id(), seconds)
}

#[pyfunction]
fn archive<'a>(py: Python<'a>, consumer: &Consumer, message: &QueueMessage) -> PyResult<&'a PyAny> {
    let inner = consumer.inner.clone();
    let message_id = message.id();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        inner.archive(message_id).await.map_err(to_py_err)?;
        Ok(true)
    })
}

#[pyfunction]
fn delete<'a>(py: Python<'a>, consumer: &Consumer, message: &QueueMessage) -> PyResult<&'a PyAny> {
    let inner = consumer.inner.clone();
    let message_id = message.id();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        inner.delete(message_id).await.map_err(to_py_err)
    })
}

#[pyfunction]
fn archive_batch<'a>(
    py: Python<'a>,
    consumer: &Consumer,
    messages: Vec<PyObject>,
) -> PyResult<&'a PyAny> {
    let inner = consumer.inner.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let mut msg_ids = Vec::new();
        Python::with_gil(|py| {
            for m in messages {
                // Extract directly from PyObject which should be a QueueMessage
                let msg = m.extract::<Py<QueueMessage>>(py)?;
                msg_ids.push(msg.borrow(py).id());
            }
            Ok::<(), PyErr>(())
        })?;
        inner.archive_many(msg_ids).await.map_err(to_py_err)?;
        Ok(true)
    })
}

#[pyclass]
struct Producer {
    inner: Arc<Box<dyn rust_pgqrs::store::Producer>>,
}

#[pymethods]
impl Producer {
    #[new]
    fn new() -> PyResult<Self> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Direct Producer creation is disabled. Use store.producer(queue).",
        ))
    }

    #[staticmethod]
    #[pyo3(name = "new_producer")]
    fn init(admin: &Admin, queue: String, hostname: String, port: i32) -> PyResult<Self> {
        let store = admin.store.clone();
        let rt = get_runtime();

        let producer = rt.block_on(async {
            // Use Store trait method directly - returns Box<dyn Producer + 'static>
            store
                .producer(&queue, &hostname, port, store.config())
                .await
                .map_err(to_py_err)
        })?;

        Ok(Producer {
            inner: Arc::new(producer),
        })
    }

    #[getter]
    fn worker_id(&self) -> i64 {
        self.inner.worker_id()
    }

    fn enqueue<'a>(&self, py: Python<'a>, payload: &PyAny) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_payload = py_to_json(py, payload)?;

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let msg = inner.enqueue(&json_payload).await.map_err(to_py_err)?;
            Ok(msg.id)
        })
    }

    fn enqueue_delayed<'a>(
        &self,
        py: Python<'a>,
        payload: &PyAny,
        delay_seconds: u32,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_payload = py_to_json(py, payload)?;

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let msg = inner
                .enqueue_delayed(&json_payload, delay_seconds)
                .await
                .map_err(to_py_err)?;
            Ok(msg.id)
        })
    }
}

#[pyclass]
struct Consumer {
    inner: Arc<Box<dyn rust_pgqrs::store::Consumer>>,
}

#[pymethods]
impl Consumer {
    #[new]
    fn new() -> PyResult<Self> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Direct Consumer creation is disabled. Use store.consumer(queue).",
        ))
    }

    #[staticmethod]
    #[pyo3(name = "new_consumer")]
    fn init(admin: &Admin, queue: String, hostname: String, port: i32) -> PyResult<Self> {
        let store = admin.store.clone();
        let rt = get_runtime();

        let consumer = rt.block_on(async {
            // Use Store trait method directly - returns Box<dyn Consumer + 'static>
            store
                .consumer(&queue, &hostname, port, store.config())
                .await
                .map_err(to_py_err)
        })?;

        Ok(Consumer {
            inner: Arc::new(consumer),
        })
    }

    #[getter]
    fn worker_id(&self) -> i64 {
        self.inner.worker_id()
    }

    fn dequeue<'a>(&self, py: Python<'a>, batch_size: Option<usize>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let batch_size = batch_size.unwrap_or(1);
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = inner.dequeue_many(batch_size).await.map_err(to_py_err)?;
            Ok(messages
                .into_iter()
                .map(QueueMessage::from)
                .collect::<Vec<_>>())
        })
    }

    fn extend_vt<'a>(&self, py: Python<'a>, message_id: i64, seconds: u32) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .extend_visibility(message_id, seconds)
                .await
                .map_err(to_py_err)?;
            Ok(true)
        })
    }

    fn archive<'a>(&self, py: Python<'a>, message_id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner.archive(message_id).await.map_err(to_py_err)?;
            Ok(true)
        })
    }

    fn delete<'a>(&self, py: Python<'a>, message_id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner.delete(message_id).await.map_err(to_py_err)
        })
    }
}

#[pyclass]
#[derive(Clone)]
struct Workers {
    store: AnyStore,
}

#[pymethods]
impl Workers {
    fn count<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store.workers().count().await.map_err(to_py_err)
        })
    }

    fn list<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let workers = store.workers().list().await.map_err(to_py_err)?;
            Ok(workers
                .into_iter()
                .map(WorkerInfo::from)
                .collect::<Vec<_>>())
        })
    }
}

#[pyclass]
#[derive(Clone)]
struct Queues {
    store: AnyStore,
}

#[pymethods]
impl Queues {
    fn count<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store.queues().count().await.map_err(to_py_err)
        })
    }
}

#[pyclass]
#[derive(Clone)]
struct Messages {
    store: AnyStore,
}

#[pymethods]
impl Messages {
    fn count<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store.messages().count().await.map_err(to_py_err)
        })
    }
}

#[pyclass]
#[derive(Clone)]
struct Archive {
    store: AnyStore,
}

#[pymethods]
impl Archive {
    fn count<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store.archive().count().await.map_err(to_py_err)
        })
    }

    fn list_by_worker<'a>(
        &self,
        py: Python<'a>,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = store
                .archive()
                .list_by_worker(worker_id, limit, offset)
                .await
                .map_err(to_py_err)?;
            Ok(messages
                .into_iter()
                .map(ArchivedMessage::from)
                .collect::<Vec<_>>())
        })
    }

    fn count_by_worker<'a>(&self, py: Python<'a>, worker_id: i64) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store
                .archive()
                .count_by_worker(worker_id)
                .await
                .map_err(to_py_err)
        })
    }

    fn get<'a>(&self, py: Python<'a>, id: i64) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let msg = store.archive().get(id).await.map_err(to_py_err)?;
            Ok(ArchivedMessage::from(msg))
        })
    }

    fn delete<'a>(&self, py: Python<'a>, id: i64) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store.archive().delete(id).await.map_err(to_py_err)
        })
    }

    fn dlq_count<'a>(&self, py: Python<'a>, max_attempts: i32) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store
                .archive()
                .dlq_count(max_attempts)
                .await
                .map_err(to_py_err)
        })
    }
}

#[pyclass]
#[derive(Clone)]
struct ArchivedMessage {
    #[pyo3(get)]
    id: i64,
    #[pyo3(get)]
    queue_id: i64,
    #[pyo3(get)]
    original_msg_id: i64,
    #[pyo3(get)]
    payload: PyObject,
    #[pyo3(get)]
    producer_worker_id: Option<i64>,
    #[pyo3(get)]
    consumer_worker_id: Option<i64>,
    #[pyo3(get)]
    vt: String,
    #[pyo3(get)]
    dequeued_at: Option<String>,
    #[pyo3(get)]
    archived_at: String,
    #[pyo3(get)]
    enqueued_at: String,
    #[pyo3(get)]
    read_ct: i32,
}

impl From<rust_pgqrs::types::ArchivedMessage> for ArchivedMessage {
    fn from(r: rust_pgqrs::types::ArchivedMessage) -> Self {
        Python::with_gil(|py| ArchivedMessage {
            id: r.id,
            queue_id: r.queue_id,
            original_msg_id: r.original_msg_id,
            payload: json_to_py(py, &r.payload).unwrap_or(py.None()),
            producer_worker_id: r.producer_worker_id,
            consumer_worker_id: r.consumer_worker_id,
            vt: r.vt.to_rfc3339(),
            dequeued_at: r.dequeued_at.map(|dt| dt.to_rfc3339()),
            archived_at: r.archived_at.to_rfc3339(),
            enqueued_at: r.enqueued_at.to_rfc3339(),
            read_ct: r.read_ct,
        })
    }
}

#[pyclass]
struct PyRun {
    inner: Arc<Run>,
    store: AnyStore,
    workflow_id: i64,
}

#[pymethods]
impl PyRun {
    fn id(&self) -> PyResult<i64> {
        Ok(self.workflow_id)
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
            let (_run_id, time) = {
                let id = inner.id();
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
                (id, time)
            };

            let res = rust_pgqrs::step()
                .store(&store)
                .run(&inner)
                .name(&step_name)
                .with_time(time)
                .execute()
                .await
                .map_err(to_py_err)?;

            let step_record_id = res.id;

            Python::with_gil(|py| {
                if res.status == rust_pgqrs::WorkflowStatus::Success {
                    let output = res.output.unwrap_or(serde_json::Value::Null);
                    Ok(PyStepResult {
                        status: "SKIPPED".to_string(),
                        value: json_to_py(py, &output)?,
                        guard: None,
                    }
                    .into_py(py))
                } else {
                    let py_guard = PyStepGuard::new(store, step_record_id, time);
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

#[pyclass]
struct PyStepResult {
    #[pyo3(get)]
    status: String,
    #[pyo3(get)]
    value: PyObject,
    #[pyo3(get)]
    guard: Option<PyStepGuard>,
}

#[pyclass]
#[derive(Clone)]
struct PyStepGuard {
    inner: Arc<tokio::sync::Mutex<Box<dyn StepGuard>>>,
    current_time: chrono::DateTime<chrono::Utc>,
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
    fn new(store: AnyStore, id: i64, current_time: chrono::DateTime<chrono::Utc>) -> Self {
        let guard = store.step_guard(id);
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(guard)),
            current_time,
        }
    }
}

#[pyclass]
struct QueueInfo {
    #[pyo3(get)]
    id: i64,
    #[pyo3(get)]
    queue_name: String,
}

impl From<RustQueueRecord> for QueueInfo {
    fn from(r: RustQueueRecord) -> Self {
        QueueInfo {
            id: r.id,
            queue_name: r.queue_name,
        }
    }
}

#[pyclass]
#[derive(Clone)]
struct QueueMessage {
    inner: RustQueueMessage,
}

#[pymethods]
impl QueueMessage {
    #[getter]
    fn id(&self) -> i64 {
        self.inner.id
    }

    #[getter]
    fn queue_id(&self) -> i64 {
        self.inner.queue_id
    }

    #[getter]
    fn payload(&self, py: Python) -> PyObject {
        json_to_py(py, &self.inner.payload).unwrap_or(py.None())
    }

    #[getter]
    fn vt(&self) -> String {
        self.inner.vt.to_rfc3339()
    }

    #[getter]
    fn enqueued_at(&self) -> String {
        self.inner.enqueued_at.to_rfc3339()
    }

    #[getter]
    fn read_ct(&self) -> i32 {
        self.inner.read_ct
    }

    #[getter]
    fn dequeued_at(&self) -> Option<String> {
        self.inner.dequeued_at.map(|dt| dt.to_rfc3339())
    }

    #[getter]
    fn producer_worker_id(&self) -> Option<i64> {
        self.inner.producer_worker_id
    }

    #[getter]
    fn consumer_worker_id(&self) -> Option<i64> {
        self.inner.consumer_worker_id
    }
}

impl From<RustQueueMessage> for QueueMessage {
    fn from(inner: RustQueueMessage) -> Self {
        Self { inner }
    }
}

#[pyclass]
struct WorkerInfo {
    #[pyo3(get)]
    id: i64,
    #[pyo3(get)]
    hostname: String,
    #[pyo3(get)]
    status: String,
    #[pyo3(get)]
    queue_id: Option<i64>,
}

impl From<RustWorkerInfo> for WorkerInfo {
    fn from(r: RustWorkerInfo) -> Self {
        WorkerInfo {
            id: r.id,
            hostname: r.hostname,
            status: r.status.to_string(),
            queue_id: r.queue_id,
        }
    }
}

#[pyclass(name = "WorkflowBuilder")]
#[derive(Default)]
struct PyWorkflowBuilder {
    store: Option<AnyStore>,
    name: Option<String>,
    id: Option<i64>,
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
struct PyWorkflowTriggerBuilder {
    store: Option<AnyStore>,
    name: Option<String>,
    id: Option<i64>,
    input: serde_json::Value,
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
            Ok(QueueMessage::from(msg))
        })
    }
}

#[pyclass(name = "RunBuilder")]
#[derive(Default)]
struct PyRunBuilder {
    store: Option<AnyStore>,
    message: Option<RustQueueMessage>,
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

    fn message(mut slf: PyRefMut<'_, Self>, message: Py<QueueMessage>) -> PyRefMut<'_, Self> {
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
struct PyStepBuilder {
    run: Option<Py<PyRun>>,
    name: Option<String>,
    current_time: Option<chrono::DateTime<chrono::Utc>>,
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
            let (inner, store) = Python::with_gil(|py| {
                let run_borrow = run_py.borrow(py);
                (run_borrow.inner.clone(), run_borrow.store.clone())
            });

            let time = current_time.unwrap_or_else(chrono::Utc::now);
            let res = rust_pgqrs::step()
                .run(&inner)
                .name(&step_name)
                .with_time(time)
                .execute()
                .await
                .map_err(to_py_err)?;

            let step_record_id = res.id;

            Python::with_gil(|py| {
                if res.status == rust_pgqrs::WorkflowStatus::Success {
                    let output = res.output.unwrap_or(serde_json::Value::Null);
                    Ok(PyStepResult {
                        status: "SKIPPED".to_string(),
                        value: json_to_py(py, &output)?,
                        guard: None,
                    }
                    .into_py(py))
                } else {
                    let time = current_time.unwrap_or_else(chrono::Utc::now);
                    let py_guard = PyStepGuard::new(store, step_record_id, time);
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

#[pyfunction]
fn workflow() -> PyWorkflowBuilder {
    PyWorkflowBuilder::default()
}

#[pyfunction]
fn run() -> PyRunBuilder {
    PyRunBuilder::default()
}

#[pyfunction]
fn step() -> PyStepBuilder {
    PyStepBuilder::default()
}

#[pymodule]
fn _pgqrs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Admin>()?;
    m.add_class::<Producer>()?;
    m.add_class::<Consumer>()?;
    m.add_class::<Workers>()?;
    m.add_class::<Queues>()?;
    m.add_class::<Messages>()?;
    m.add_class::<Archive>()?;
    m.add_class::<PyConfig>()?;
    m.add_class::<PyStore>()?;
    m.add_class::<QueueMessage>()?;
    m.add_class::<QueueInfo>()?;
    m.add_class::<PyRun>()?;
    m.add_class::<PyStepResult>()?;
    m.add_class::<PyStepGuard>()?;
    m.add_class::<ArchivedMessage>()?;
    m.add_class::<ConsumerIterator>()?;
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
    m.add_function(wrap_pyfunction!(workflow, m)?)?;
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(step, m)?)?;

    Ok(())
}
