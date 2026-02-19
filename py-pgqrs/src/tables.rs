use crate::workers::{PyConsumer, PyProducer};
use crate::{json_to_py, py_to_json, to_py_err, PgqrsError, PyStore, WorkflowRecord};
use ::pgqrs as rust_pgqrs;
use pyo3::prelude::*;
use pyo3::pyasync::IterANextOutput;
use rust_pgqrs::store::AnyStore;
use rust_pgqrs::types::{QueueMessage as RustQueueMessage, QueueRecord as RustQueueRecord};
use rust_pgqrs::Store;
use std::sync::Arc;

pub(crate) struct IteratorState {
    pub(crate) store: AnyStore,
    pub(crate) queue: String,
    pub(crate) consumer: Option<Arc<Box<dyn rust_pgqrs::Consumer>>>,
    pub(crate) poll_interval: tokio::time::Duration,
}

#[pyclass(name = "ConsumerIterator")]
pub struct PyConsumerIterator {
    pub(crate) inner: Arc<tokio::sync::Mutex<IteratorState>>,
}

impl PyConsumerIterator {
    pub fn new(store: AnyStore, queue: String, poll_interval_ms: u64) -> Self {
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
impl PyConsumerIterator {
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
                    return Ok(PyQueueMessage::from(msg));
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

#[pyclass(name = "Queues")]
#[derive(Clone)]
pub struct PyQueues {
    pub(crate) store: AnyStore,
}

#[pyclass(name = "Workflows")]
#[derive(Clone)]
pub struct PyWorkflows {
    pub(crate) store: AnyStore,
}

#[pyclass(name = "WorkflowRuns")]
#[derive(Clone)]
pub struct PyWorkflowRuns {
    pub(crate) store: AnyStore,
}

#[pyclass(name = "WorkflowSteps")]
#[derive(Clone)]
pub struct PyWorkflowSteps {
    pub(crate) store: AnyStore,
}

#[pymethods]
impl PyQueues {
    fn count<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store.queues().count().await.map_err(to_py_err)
        })
    }
}

#[pymethods]
impl PyWorkflows {
    fn get_by_name<'a>(&self, py: Python<'a>, name: String) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let workflow = store
                .workflows()
                .get_by_name(&name)
                .await
                .map_err(to_py_err)?;
            Ok(WorkflowRecord::from(workflow))
        })
    }
}

#[pymethods]
impl PyWorkflowRuns {
    fn list<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let runs = store.workflow_runs().list().await.map_err(to_py_err)?;
            Ok(runs.into_iter().map(PyRunRecord::from).collect::<Vec<_>>())
        })
    }
}

#[pymethods]
impl PyWorkflowSteps {
    fn list<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let steps = store.workflow_steps().list().await.map_err(to_py_err)?;
            Ok(steps
                .into_iter()
                .map(PyStepRecord::from)
                .collect::<Vec<_>>())
        })
    }
}

#[pyclass(name = "Messages")]
#[derive(Clone)]
pub struct PyMessages {
    pub(crate) store: AnyStore,
}

#[pymethods]
impl PyMessages {
    fn count<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            store.messages().count().await.map_err(to_py_err)
        })
    }
}

#[pyclass(name = "Archive")]
#[derive(Clone)]
pub struct PyArchive {
    pub(crate) store: AnyStore,
}

#[pymethods]
impl PyArchive {
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
                .map(PyArchivedMessage::from)
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
            Ok(PyArchivedMessage::from(msg))
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

    fn filter_by_fk<'a>(&self, py: Python<'a>, queue_id: i64) -> PyResult<&'a PyAny> {
        let store = self.store.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let messages = store
                .archive()
                .filter_by_fk(queue_id)
                .await
                .map_err(to_py_err)?;
            Ok(messages
                .into_iter()
                .map(PyArchivedMessage::from)
                .collect::<Vec<_>>())
        })
    }
}

#[pyclass(name = "ArchivedMessage")]
#[derive(Clone)]
pub struct PyArchivedMessage {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub queue_id: i64,
    #[pyo3(get)]
    pub original_msg_id: i64,
    #[pyo3(get)]
    pub payload: PyObject,
    #[pyo3(get)]
    pub producer_worker_id: Option<i64>,
    #[pyo3(get)]
    pub consumer_worker_id: Option<i64>,
    #[pyo3(get)]
    pub vt: String,
    #[pyo3(get)]
    pub dequeued_at: Option<String>,
    #[pyo3(get)]
    pub archived_at: String,
    #[pyo3(get)]
    pub enqueued_at: String,
    #[pyo3(get)]
    pub read_ct: i32,
}

impl From<rust_pgqrs::types::ArchivedMessage> for PyArchivedMessage {
    fn from(r: rust_pgqrs::types::ArchivedMessage) -> Self {
        Python::with_gil(|py| PyArchivedMessage {
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

#[pyclass(name = "QueueInfo")]
pub struct PyQueueInfo {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub queue_name: String,
}

impl From<RustQueueRecord> for PyQueueInfo {
    fn from(r: RustQueueRecord) -> Self {
        PyQueueInfo {
            id: r.id,
            queue_name: r.queue_name,
        }
    }
}

#[pyclass(name = "RunRecord")]
#[derive(Clone)]
pub struct PyRunRecord {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub workflow_id: i64,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub input: Option<PyObject>,
    #[pyo3(get)]
    pub output: Option<PyObject>,
    #[pyo3(get)]
    pub error: Option<PyObject>,
    #[pyo3(get)]
    pub created_at: String,
    #[pyo3(get)]
    pub updated_at: String,
}

impl From<rust_pgqrs::types::RunRecord> for PyRunRecord {
    fn from(r: rust_pgqrs::types::RunRecord) -> Self {
        Python::with_gil(|py| PyRunRecord {
            id: r.id,
            workflow_id: r.workflow_id,
            status: r.status.to_string(),
            input: r
                .input
                .map(|value| json_to_py(py, &value).unwrap_or(py.None())),
            output: r
                .output
                .map(|value| json_to_py(py, &value).unwrap_or(py.None())),
            error: r
                .error
                .map(|value| json_to_py(py, &value).unwrap_or(py.None())),
            created_at: r.created_at.to_rfc3339(),
            updated_at: r.updated_at.to_rfc3339(),
        })
    }
}

#[pyclass(name = "StepRecord")]
#[derive(Clone)]
pub struct PyStepRecord {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub run_id: i64,
    #[pyo3(get)]
    pub step_name: String,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub input: Option<PyObject>,
    #[pyo3(get)]
    pub output: Option<PyObject>,
    #[pyo3(get)]
    pub error: Option<PyObject>,
    #[pyo3(get)]
    pub retry_at: Option<String>,
    #[pyo3(get)]
    pub created_at: String,
    #[pyo3(get)]
    pub updated_at: String,
}

impl From<rust_pgqrs::types::StepRecord> for PyStepRecord {
    fn from(r: rust_pgqrs::types::StepRecord) -> Self {
        Python::with_gil(|py| PyStepRecord {
            id: r.id,
            run_id: r.run_id,
            step_name: r.step_name,
            status: r.status.to_string(),
            input: r
                .input
                .map(|value| json_to_py(py, &value).unwrap_or(py.None())),
            output: r
                .output
                .map(|value| json_to_py(py, &value).unwrap_or(py.None())),
            error: r
                .error
                .map(|value| json_to_py(py, &value).unwrap_or(py.None())),
            retry_at: r.retry_at.map(|dt| dt.to_rfc3339()),
            created_at: r.created_at.to_rfc3339(),
            updated_at: r.updated_at.to_rfc3339(),
        })
    }
}

#[pyclass(name = "QueueMessage")]
#[derive(Clone)]
pub struct PyQueueMessage {
    pub(crate) inner: RustQueueMessage,
}

#[pymethods]
impl PyQueueMessage {
    #[getter]
    pub fn id(&self) -> i64 {
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

impl From<RustQueueMessage> for PyQueueMessage {
    fn from(inner: RustQueueMessage) -> Self {
        Self { inner }
    }
}

#[pyfunction]
pub fn produce<'a>(
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

        Ok(*msg_ids.first().unwrap())
    })
}

#[pyfunction]
pub fn produce_batch<'a>(
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
pub fn consume<'a>(
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
            let py_msg = PyQueueMessage::from(msg.clone());
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
pub fn consume_batch<'a>(
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
            let py_msgs: Vec<_> = msgs
                .iter()
                .map(|m| PyQueueMessage::from(m.clone()))
                .collect();
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
pub fn enqueue<'a>(
    py: Python<'a>,
    producer: &PyProducer,
    payload: PyObject,
) -> PyResult<&'a PyAny> {
    let inner = producer.inner.clone();
    let json_payload = py_to_json(py, payload.as_ref(py))?;
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let msg = inner.enqueue(&json_payload).await.map_err(to_py_err)?;
        Ok(msg.id)
    })
}

#[pyfunction]
pub fn enqueue_batch<'a>(
    py: Python<'a>,
    producer: &PyProducer,
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
pub fn dequeue<'a>(
    py: Python<'a>,
    consumer: &PyConsumer,
    batch_size: usize,
) -> PyResult<&'a PyAny> {
    let inner = consumer.inner.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let messages = inner.dequeue_many(batch_size).await.map_err(to_py_err)?;
        Ok(messages
            .into_iter()
            .map(PyQueueMessage::from)
            .collect::<Vec<_>>())
    })
}

#[pyfunction]
pub fn enqueue_delayed<'a>(
    py: Python<'a>,
    producer: &PyProducer,
    payload: &PyAny,
    delay_seconds: u32,
) -> PyResult<&'a PyAny> {
    producer.enqueue_delayed(py, payload, delay_seconds)
}

#[pyfunction]
pub fn extend_vt<'a>(
    py: Python<'a>,
    consumer: &PyConsumer,
    message: &PyQueueMessage,
    seconds: u32,
) -> PyResult<&'a PyAny> {
    consumer.extend_vt(py, message.id(), seconds)
}

#[pyfunction]
pub fn archive<'a>(
    py: Python<'a>,
    consumer: &PyConsumer,
    message: &PyQueueMessage,
) -> PyResult<&'a PyAny> {
    let inner = consumer.inner.clone();
    let message_id = message.id();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        inner.archive(message_id).await.map_err(to_py_err)?;
        Ok(true)
    })
}

#[pyfunction]
pub fn delete<'a>(
    py: Python<'a>,
    consumer: &PyConsumer,
    message: &PyQueueMessage,
) -> PyResult<&'a PyAny> {
    let inner = consumer.inner.clone();
    let message_id = message.id();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        inner.delete(message_id).await.map_err(to_py_err)
    })
}

#[pyfunction]
pub fn archive_batch<'a>(
    py: Python<'a>,
    consumer: &PyConsumer,
    messages: Vec<PyObject>,
) -> PyResult<&'a PyAny> {
    let inner = consumer.inner.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let mut msg_ids = Vec::new();
        Python::with_gil(|py| {
            for m in messages {
                let msg = m.extract::<Py<PyQueueMessage>>(py)?;
                msg_ids.push(msg.borrow(py).id());
            }
            Ok::<(), PyErr>(())
        })?;
        inner.archive_many(msg_ids).await.map_err(to_py_err)?;
        Ok(true)
    })
}
