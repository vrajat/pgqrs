#![allow(non_local_definitions)]
use ::pgqrs as rust_pgqrs;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use rust_pgqrs::tables::{
    Archive as RustArchive, Messages as RustMessages, Queues as RustQueues, Table,
    Workers as RustWorkers,
};
use rust_pgqrs::types::{
    ArchivedMessage as RustArchivedMessage, QueueInfo as RustQueueInfo,
    QueueMessage as RustQueueMessage, WorkerStatus,
};
use rust_pgqrs::{Admin as RustAdmin, Consumer as RustConsumer, Producer as RustProducer};
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().unwrap())
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

fn py_to_json(val: &PyAny) -> PyResult<serde_json::Value> {
    // Simplified conversion, for enqueuing
    // let s = val.call_method0("__str__")?.extract::<String>()?;
    // This expects a JSON string or dict.
    // If it's a dict, we can dump it to string then parse.
    // Or we can rely on user passing a string.
    // Let's assume user passes a dict or other json-serializable object,
    // we use json.dumps
    let json_module = val.py().import("json")?;
    let json_str = json_module
        .call_method1("dumps", (val,))?
        .extract::<String>()?;
    serde_json::from_str(&json_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[pyclass(name = "Config")]
#[derive(Clone)]
struct PyConfig {
    inner: rust_pgqrs::Config,
}

#[pymethods]
impl PyConfig {
    #[staticmethod]
    fn from_dsn(dsn: String) -> Self {
        PyConfig {
            inner: rust_pgqrs::Config::from_dsn(dsn),
        }
    }

    #[setter]
    fn set_schema(&mut self, schema: String) {
        self.inner.schema = schema;
    }

    #[getter]
    fn get_schema(&self) -> String {
        self.inner.schema.clone()
    }

    #[setter]
    fn set_max_connections(&mut self, max: u32) {
        self.inner.max_connections = max;
    }

    #[getter]
    fn get_max_connections(&self) -> u32 {
        self.inner.max_connections
    }

    #[setter]
    fn set_connection_timeout_seconds(&mut self, timeout: u64) {
        self.inner.connection_timeout_seconds = timeout;
    }

    #[getter]
    fn get_connection_timeout_seconds(&self) -> u64 {
        self.inner.connection_timeout_seconds
    }

    #[setter]
    fn set_default_lock_time_seconds(&mut self, seconds: u32) {
        self.inner.default_lock_time_seconds = seconds;
    }

    #[getter]
    fn get_default_lock_time_seconds(&self) -> u32 {
        self.inner.default_lock_time_seconds
    }

    #[setter]
    fn set_default_max_batch_size(&mut self, size: usize) {
        self.inner.default_max_batch_size = size;
    }

    #[getter]
    fn get_default_max_batch_size(&self) -> usize {
        self.inner.default_max_batch_size
    }

    #[setter]
    fn set_max_read_ct(&mut self, count: i32) {
        self.inner.max_read_ct = count;
    }

    #[getter]
    fn get_max_read_ct(&self) -> i32 {
        self.inner.max_read_ct
    }

    #[setter]
    fn set_heartbeat_interval_seconds(&mut self, seconds: u64) {
        self.inner.heartbeat_interval = seconds;
    }

    #[getter]
    fn get_heartbeat_interval_seconds(&self) -> u64 {
        self.inner.heartbeat_interval
    }
}

#[pyclass]
struct Producer {
    inner: Arc<RustProducer>,
}

#[pymethods]
impl Producer {
    #[new]
    fn new(admin: &Admin, queue: &str, hostname: String, port: i32) -> PyResult<Self> {
        let rt = get_runtime();
        let producer = rt.block_on(async {
            let (pool, config) = {
                let locked_admin = admin.inner.lock().await;
                (locked_admin.pool.clone(), locked_admin.config.clone())
            };

            let queues = RustQueues::new(pool.clone());
            let q = queues.get_by_name(queue).await.map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("Queue not found: {}", e))
            })?;

            RustProducer::new(pool, &q, &hostname, port, &config)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })?;
        Ok(Producer {
            inner: Arc::new(producer),
        })
    }

    fn enqueue<'a>(&self, py: Python<'a>, payload: &PyAny) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_payload = py_to_json(payload)?;

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let msg = inner
                .enqueue(&json_payload)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(msg.id)
        })
    }

    fn enqueue_delayed<'a>(
        &self,
        py: Python<'a>,
        payload: &PyAny,
        delay_seconds: u64,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_payload = py_to_json(payload)?;

        let delay: u32 = delay_seconds.try_into().map_err(|_| {
            pyo3::exceptions::PyOverflowError::new_err("Delay seconds must fit in u32")
        })?;

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let msg = inner
                .enqueue_delayed(&json_payload, delay)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(msg.id)
        })
    }

    fn enqueue_batch<'a>(&self, py: Python<'a>, payloads: Vec<PyObject>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_payloads: Result<Vec<serde_json::Value>, _> = payloads
            .into_iter()
            .map(|p| py_to_json(p.as_ref(py)))
            .collect();
        let json_payloads = json_payloads?;

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = inner
                .batch_enqueue(&json_payloads)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(messages
                .into_iter()
                .map(QueueMessage::from)
                .collect::<Vec<_>>())
        })
    }
}

#[pyclass]
struct Consumer {
    inner: Arc<RustConsumer>,
}

#[pymethods]
impl Consumer {
    #[new]
    fn new(admin: &Admin, queue: &str, hostname: String, port: i32) -> PyResult<Self> {
        let rt = get_runtime();
        let consumer = rt.block_on(async {
            let (pool, config) = {
                let locked_admin = admin.inner.lock().await;
                (locked_admin.pool.clone(), locked_admin.config.clone())
            };

            let queues = RustQueues::new(pool.clone());
            let q = queues.get_by_name(queue).await.map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("Queue not found: {}", e))
            })?;

            RustConsumer::new(pool, &q, &hostname, port, &config)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })?;
        Ok(Consumer {
            inner: Arc::new(consumer),
        })
    }

    fn dequeue<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = inner
                .dequeue()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(messages
                .into_iter()
                .map(QueueMessage::from)
                .collect::<Vec<_>>())
        })
    }

    fn delete<'a>(&self, py: Python<'a>, message_id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let result = inner
                .delete(message_id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(result)
        })
    }

    fn archive<'a>(&self, py: Python<'a>, message_id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .archive(message_id)
                .await
                .map(|_| Python::with_gil(|py| py.None()))
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Extends the visibility timeout for a message in the queue.
    ///
    /// Parameters
    /// ----------
    /// message_id : int
    ///     The ID of the message whose visibility timeout is to be extended.
    /// extension_seconds : float
    ///     The number of seconds to extend the visibility timeout.
    ///
    /// Returns
    /// -------
    /// None
    ///     Returns None on success.
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If the operation fails.
    fn extend_visibility<'a>(
        &self,
        py: Python<'a>,
        message_id: i64,
        extension_seconds: u32,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let result = inner
                .extend_visibility(message_id, extension_seconds)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(result)
        })
    }

    fn dequeue_batch<'a>(&self, py: Python<'a>, limit: usize) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = inner
                .dequeue_many(limit)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(messages
                .into_iter()
                .map(QueueMessage::from)
                .collect::<Vec<_>>())
        })
    }

    fn dequeue_batch_with_delay<'a>(
        &self,
        py: Python<'a>,
        limit: usize,
        vt_seconds: u32,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = inner
                .dequeue_many_with_delay(limit, vt_seconds)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(messages
                .into_iter()
                .map(QueueMessage::from)
                .collect::<Vec<_>>())
        })
    }

    fn archive_batch<'a>(&self, py: Python<'a>, message_ids: Vec<i64>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let results = inner
                .archive_many(message_ids)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(results)
        })
    }
}

// Wrappers for Tables

#[pyclass]
#[derive(Clone)]
struct Workers {
    inner: RustWorkers,
}

#[pymethods]
impl Workers {
    fn count<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .count()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn list<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let workers = inner
                .list()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(workers
                .into_iter()
                .map(WorkerInfo::from)
                .collect::<Vec<_>>())
        })
    }

    fn get<'a>(&self, py: Python<'a>, id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let worker = inner
                .get(id)
                .await
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(WorkerInfo::from(worker))
        })
    }
}

#[pyclass]
#[derive(Clone)]
struct Queues {
    inner: RustQueues,
}

#[pymethods]
impl Queues {
    fn count<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .count()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }
}

#[pyclass]
#[derive(Clone)]
struct Messages {
    inner: RustMessages,
}

#[pymethods]
impl Messages {
    fn count<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .count()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }
}

#[pyclass]
#[derive(Clone)]
struct Archive {
    inner: RustArchive,
}

#[pymethods]
impl Archive {
    fn count<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .count()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn get<'a>(&self, py: Python<'a>, id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let msg = inner
                .get(id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(ArchivedMessage::from(msg))
        })
    }

    fn delete<'a>(&self, py: Python<'a>, id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let count = inner
                .delete(id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(count)
        })
    }

    fn list_dlq_messages<'a>(
        &self,
        py: Python<'a>,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = inner
                .list_dlq_messages(max_attempts, limit, offset)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(messages
                .into_iter()
                .map(ArchivedMessage::from)
                .collect::<Vec<_>>())
        })
    }

    fn dlq_count<'a>(&self, py: Python<'a>, max_attempts: i32) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .dlq_count(max_attempts)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn list_by_worker<'a>(
        &self,
        py: Python<'a>,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = inner
                .list_by_worker(worker_id, limit, offset)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(messages
                .into_iter()
                .map(ArchivedMessage::from)
                .collect::<Vec<_>>())
        })
    }

    fn count_by_worker<'a>(&self, py: Python<'a>, worker_id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .count_by_worker(worker_id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn delete_by_worker<'a>(&self, py: Python<'a>, worker_id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .delete_by_worker(worker_id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }
}

#[pyclass]
struct Admin {
    inner: Arc<tokio::sync::Mutex<RustAdmin>>, // Admin is mutable in register, so need Mutex
}

#[pymethods]
impl Admin {
    #[new]
    fn new(dsn: &PyAny, schema: Option<String>) -> PyResult<Self> {
        let mut config = if let Ok(dsn_str) = dsn.extract::<String>() {
            rust_pgqrs::Config::from_dsn(&dsn_str)
        } else if let Ok(config_wrapper) = dsn.extract::<PyConfig>() {
            config_wrapper.inner
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Argument 'dsn' must be a string or a Config instance",
            ));
        };

        if let Some(s) = schema {
            config.schema = s;
        }

        let rt = get_runtime();
        let admin = rt.block_on(async {
            RustAdmin::new(&config)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })?;
        Ok(Admin {
            inner: Arc::new(tokio::sync::Mutex::new(admin)),
        })
    }

    fn install<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let admin = inner.lock().await;
            admin
                .install()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn verify<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let admin = inner.lock().await;
            admin
                .verify()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn create_queue<'a>(&self, py: Python<'a>, name: String) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let admin = inner.lock().await;
            let q = admin
                .create_queue(&name)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(QueueInfo::from(q))
        })
    }

    fn get_workers<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let admin = inner.lock().await;
            Ok(Workers {
                inner: admin.workers.clone(),
            })
        })
    }

    fn get_queues<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let admin = inner.lock().await;
            Ok(Queues {
                inner: admin.queues.clone(),
            })
        })
    }

    fn get_messages<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let admin = inner.lock().await;
            Ok(Messages {
                inner: admin.messages.clone(),
            })
        })
    }

    fn get_archive<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let admin = inner.lock().await;
            Ok(Archive {
                inner: admin.archive.clone(),
            })
        })
    }

    fn reclaim_messages<'a>(
        &self,
        py: Python<'a>,
        queue_name: String,
        older_than_seconds: Option<f64>,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let admin = inner.lock().await;
            let queue = admin
                .get_queue(&queue_name)
                .await
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

            let duration =
                older_than_seconds.map(|s| chrono::Duration::milliseconds((s * 1000.0) as i64));

            let count = admin
                .reclaim_messages(queue.id, duration)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(count)
        })
    }
}

// Data Types Wrappers

#[pyclass]
struct QueueInfo {
    #[pyo3(get)]
    id: i64,
    #[pyo3(get)]
    queue_name: String,
    #[pyo3(get)]
    created_at: String,
}

impl From<RustQueueInfo> for QueueInfo {
    fn from(r: RustQueueInfo) -> Self {
        QueueInfo {
            id: r.id,
            queue_name: r.queue_name,
            created_at: r.created_at.to_rfc3339(),
        }
    }
}

#[pyclass]
struct QueueMessage {
    #[pyo3(get)]
    id: i64,
    #[pyo3(get)]
    queue_id: i64,
    #[pyo3(get)]
    payload: PyObject,
}

impl From<RustQueueMessage> for QueueMessage {
    fn from(r: RustQueueMessage) -> Self {
        Python::with_gil(|py| QueueMessage {
            id: r.id,
            queue_id: r.queue_id,
            payload: json_to_py(py, &r.payload).unwrap_or(py.None()),
        })
    }
}

#[pyclass]
struct ArchivedMessage {
    #[pyo3(get)]
    id: i64,
    #[pyo3(get)]
    original_msg_id: i64,
    #[pyo3(get)]
    queue_id: i64,
    #[pyo3(get)]
    payload: PyObject,
    #[pyo3(get)]
    enqueued_at: String,
    #[pyo3(get)]
    archived_at: String,
    #[pyo3(get)]
    vt: String,
    #[pyo3(get)]
    dequeued_at: Option<String>,
    #[pyo3(get)]
    read_ct: i32,
    #[pyo3(get)]
    producer_worker_id: Option<i64>,
    #[pyo3(get)]
    consumer_worker_id: Option<i64>,
}

impl From<RustArchivedMessage> for ArchivedMessage {
    fn from(r: RustArchivedMessage) -> Self {
        Python::with_gil(|py| ArchivedMessage {
            id: r.id,
            original_msg_id: r.original_msg_id,
            queue_id: r.queue_id,
            payload: json_to_py(py, &r.payload).unwrap_or(py.None()),
            enqueued_at: r.enqueued_at.to_rfc3339(),
            archived_at: r.archived_at.to_rfc3339(),
            vt: r.vt.to_rfc3339(),
            dequeued_at: r.dequeued_at.map(|t| t.to_rfc3339()),
            read_ct: r.read_ct,
            producer_worker_id: r.producer_worker_id,
            consumer_worker_id: r.consumer_worker_id,
        })
    }
}

#[pyclass]
struct WorkerInfo {
    #[pyo3(get)]
    id: i64,
    #[pyo3(get)]
    hostname: String,
    #[pyo3(get)]
    port: i32,
    #[pyo3(get)]
    queue_id: Option<i64>,
    #[pyo3(get)]
    started_at: String,
    #[pyo3(get)]
    heartbeat_at: String,
    #[pyo3(get)]
    shutdown_at: Option<String>,
    #[pyo3(get)]
    status: PyWorkerStatus,
}

impl From<rust_pgqrs::types::WorkerInfo> for WorkerInfo {
    fn from(w: rust_pgqrs::types::WorkerInfo) -> Self {
        WorkerInfo {
            id: w.id,
            hostname: w.hostname,
            port: w.port,
            queue_id: w.queue_id,
            started_at: w.started_at.to_rfc3339(),
            heartbeat_at: w.heartbeat_at.to_rfc3339(),
            shutdown_at: w.shutdown_at.map(|t| t.to_rfc3339()),
            status: w.status.into(),
        }
    }
}

#[pyclass(name = "WorkerStatus")]
#[derive(Clone, PartialEq, Debug)]
enum PyWorkerStatus {
    Ready,
    Suspended,
    Stopped,
}

impl From<WorkerStatus> for PyWorkerStatus {
    fn from(s: WorkerStatus) -> Self {
        match s {
            WorkerStatus::Ready => PyWorkerStatus::Ready,
            WorkerStatus::Suspended => PyWorkerStatus::Suspended,
            WorkerStatus::Stopped => PyWorkerStatus::Stopped,
        }
    }
}

impl From<PyWorkerStatus> for WorkerStatus {
    fn from(s: PyWorkerStatus) -> Self {
        match s {
            PyWorkerStatus::Ready => WorkerStatus::Ready,
            PyWorkerStatus::Suspended => WorkerStatus::Suspended,
            PyWorkerStatus::Stopped => WorkerStatus::Stopped,
        }
    }
}

#[pymodule]
fn pgqrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Producer>()?;
    m.add_class::<Consumer>()?;
    m.add_class::<Admin>()?;
    m.add_class::<Workers>()?;
    m.add_class::<Queues>()?;
    m.add_class::<Messages>()?;
    m.add_class::<Archive>()?;
    m.add_class::<QueueInfo>()?;
    m.add_class::<QueueMessage>()?;
    m.add_class::<ArchivedMessage>()?;
    m.add_class::<WorkerInfo>()?;
    m.add_class::<PyWorkerStatus>()?;
    m.add_class::<PyConfig>()?;
    Ok(())
}
