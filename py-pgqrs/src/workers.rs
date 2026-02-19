use crate::tables::PyQueueMessage;
use crate::{get_runtime, to_py_err, PyAdmin};
use ::pgqrs as rust_pgqrs;
use pyo3::prelude::*;
use rust_pgqrs::store::{AnyStore, Store};
use rust_pgqrs::types::WorkerRecord as RustWorkerInfo;
use std::sync::Arc;

#[pyclass(name = "Producer")]
pub struct PyProducer {
    pub(crate) inner: Arc<Box<dyn rust_pgqrs::store::Producer>>,
}

#[pymethods]
impl PyProducer {
    #[new]
    fn new() -> PyResult<Self> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Direct Producer creation is disabled. Use store.producer(queue).",
        ))
    }

    #[staticmethod]
    #[pyo3(name = "new_producer")]
    fn init(admin: &PyAdmin, queue: String, hostname: String, port: i32) -> PyResult<Self> {
        let store = admin.store.clone();
        let rt = get_runtime();

        let producer = rt.block_on(async {
            // Use Store trait method directly - returns Box<dyn Producer + 'static>
            store
                .producer(&queue, &hostname, port, store.config())
                .await
                .map_err(to_py_err)
        })?;

        Ok(PyProducer {
            inner: Arc::new(producer),
        })
    }

    #[getter]
    fn worker_id(&self) -> i64 {
        self.inner.worker_id()
    }

    fn enqueue<'a>(&self, py: Python<'a>, payload: &PyAny) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_payload = crate::py_to_json(py, payload)?;

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let msg = inner.enqueue(&json_payload).await.map_err(to_py_err)?;
            Ok(msg.id)
        })
    }

    pub(crate) fn enqueue_delayed<'a>(
        &self,
        py: Python<'a>,
        payload: &PyAny,
        delay_seconds: u32,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let json_payload = crate::py_to_json(py, payload)?;

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let msg = inner
                .enqueue_delayed(&json_payload, delay_seconds)
                .await
                .map_err(to_py_err)?;
            Ok(msg.id)
        })
    }
}

#[pyclass(name = "Consumer")]
pub struct PyConsumer {
    pub(crate) inner: Arc<Box<dyn rust_pgqrs::store::Consumer>>,
}

impl PyConsumer {
    fn parse_time(time: String) -> PyResult<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::parse_from_rfc3339(&time)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
            .map(|dt| dt.with_timezone(&chrono::Utc))
    }
}

#[pymethods]
impl PyConsumer {
    #[new]
    fn new() -> PyResult<Self> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Direct Consumer creation is disabled. Use store.consumer(queue).",
        ))
    }

    #[staticmethod]
    #[pyo3(name = "new_consumer")]
    fn init(admin: &PyAdmin, queue: String, hostname: String, port: i32) -> PyResult<Self> {
        let store = admin.store.clone();
        let rt = get_runtime();

        let consumer = rt.block_on(async {
            // Use Store trait method directly - returns Box<dyn Consumer + 'static>
            store
                .consumer(&queue, &hostname, port, store.config())
                .await
                .map_err(to_py_err)
        })?;

        Ok(PyConsumer {
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
                .map(PyQueueMessage::from)
                .collect::<Vec<_>>())
        })
    }

    fn dequeue_many_with_delay<'a>(
        &self,
        py: Python<'a>,
        limit: usize,
        vt: u32,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = inner
                .dequeue_many_with_delay(limit, vt)
                .await
                .map_err(to_py_err)?;
            Ok(messages
                .into_iter()
                .map(PyQueueMessage::from)
                .collect::<Vec<_>>())
        })
    }

    fn dequeue_at<'a>(
        &self,
        py: Python<'a>,
        limit: usize,
        vt: u32,
        current_time: String,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let time = Self::parse_time(current_time)?;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = inner.dequeue_at(limit, vt, time).await.map_err(to_py_err)?;
            Ok(messages
                .into_iter()
                .map(PyQueueMessage::from)
                .collect::<Vec<_>>())
        })
    }

    pub(crate) fn extend_vt<'a>(
        &self,
        py: Python<'a>,
        message_id: i64,
        seconds: u32,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .extend_visibility(message_id, seconds)
                .await
                .map_err(to_py_err)
        })
    }

    fn archive<'a>(&self, py: Python<'a>, message_id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let archived = inner.archive(message_id).await.map_err(to_py_err)?;
            Ok(archived.is_some())
        })
    }

    fn archive_many<'a>(&self, py: Python<'a>, message_ids: Vec<i64>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner.archive_many(message_ids).await.map_err(to_py_err)
        })
    }

    fn delete<'a>(&self, py: Python<'a>, message_id: i64) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner.delete(message_id).await.map_err(to_py_err)
        })
    }

    fn delete_many<'a>(&self, py: Python<'a>, message_ids: Vec<i64>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner.delete_many(message_ids).await.map_err(to_py_err)
        })
    }

    fn release_with_visibility<'a>(
        &self,
        py: Python<'a>,
        message_id: i64,
        visible_at: String,
    ) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        let time = Self::parse_time(visible_at)?;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .release_with_visibility(message_id, time)
                .await
                .map_err(to_py_err)
        })
    }

    fn release_messages<'a>(&self, py: Python<'a>, message_ids: Vec<i64>) -> PyResult<&'a PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner
                .release_messages(&message_ids)
                .await
                .map_err(to_py_err)
        })
    }
}

#[pyclass(name = "Workers")]
#[derive(Clone)]
pub struct PyWorkers {
    pub(crate) store: AnyStore,
}

#[pymethods]
impl PyWorkers {
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
                .map(PyWorkerInfo::from)
                .collect::<Vec<_>>())
        })
    }
}

#[pyclass(name = "WorkerInfo")]
pub struct PyWorkerInfo {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub hostname: String,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub queue_id: Option<i64>,
}

impl From<RustWorkerInfo> for PyWorkerInfo {
    fn from(r: RustWorkerInfo) -> Self {
        PyWorkerInfo {
            id: r.id,
            hostname: r.hostname,
            status: r.status.to_string(),
            queue_id: r.queue_id,
        }
    }
}
