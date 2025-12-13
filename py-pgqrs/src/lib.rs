#![allow(non_local_definitions)]
use ::pgqrs as rust_pgqrs;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use rust_pgqrs::tables::{
    Archive as RustArchive, Messages as RustMessages, Queues as RustQueues, Table,
    Workers as RustWorkers,
};
use rust_pgqrs::types::{QueueInfo as RustQueueInfo, QueueMessage as RustQueueMessage};
use rust_pgqrs::{Admin as RustAdmin, Config, Consumer as RustConsumer, Producer as RustProducer};
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

#[pyclass]
struct Producer {
    inner: Arc<RustProducer>,
}

#[pymethods]
impl Producer {
    #[new]
    fn new(
        admin: &Admin,
        queue: &str,
        hostname: String,
        port: i32,
    ) -> PyResult<Self> {
        let rt = get_runtime();
        let producer = rt.block_on(async {
            let (pool, config) = {
                let locked_admin = admin.inner.lock().await;
                (locked_admin.pool.clone(), locked_admin.config.clone())
            };

            let queues = RustQueues::new(pool.clone());
            let q = queues
                .get_by_name(queue)
                .await
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Queue not found: {}", e)))?;

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
    fn new(
        admin: &Admin,
        queue: &str,
        hostname: String,
        port: i32,
    ) -> PyResult<Self> {
        let rt = get_runtime();
        let consumer = rt.block_on(async {
            let (pool, config) = {
                let locked_admin = admin.inner.lock().await;
                (locked_admin.pool.clone(), locked_admin.config.clone())
            };

            let queues = RustQueues::new(pool.clone());
            let q = queues
                .get_by_name(queue)
                .await
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Queue not found: {}", e)))?;

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
}

#[pyclass]
struct Admin {
    inner: Arc<tokio::sync::Mutex<RustAdmin>>, // Admin is mutable in register, so need Mutex
}

#[pymethods]
impl Admin {
    #[new]
    fn new(dsn: &str, schema: Option<String>) -> PyResult<Self> {
        let rt = get_runtime();
        let admin = rt.block_on(async {
            let config = if let Some(s) = schema {
                Config::from_dsn_with_schema(dsn.to_string(), &s)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
            } else {
                Config::from_dsn(dsn)
            };
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
    Ok(())
}
