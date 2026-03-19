use crate::tables::PyQueueMessage;
use crate::{to_py_err, PyStore};
use ::pgqrs::store::Store as _;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::sync::Arc;
use tokio::time::Duration;

fn validate_handler_configuration(
    batch_size: usize,
    handle_one: &Option<PyObject>,
    handle_batch: &Option<PyObject>,
    handle_workflow: &Option<PyObject>,
) -> PyResult<()> {
    let num_handlers =
        handle_one.is_some() as u8 + handle_batch.is_some() as u8 + handle_workflow.is_some() as u8;
    if num_handlers != 1 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "exactly one of handle(), handle_batch() or handle_workflow() is required",
        ));
    }

    if handle_one.is_some() && batch_size != 1 {
        return Err(to_py_err(::pgqrs::Error::ValidationFailed {
            reason: format!(
                "single-message handlers require batch size = 1, got {}",
                batch_size
            ),
        }));
    }

    Ok(())
}

#[pyclass(name = "DequeueBuilder")]
pub struct PyDequeueBuilder {
    worker: Option<Py<crate::workers::PyConsumer>>,
    batch_size: usize,
    queue_name: Option<String>,
    poll_interval_ms: Option<u64>,
    at: Option<chrono::DateTime<chrono::Utc>>,
    handle_one: Option<PyObject>,
    handle_batch: Option<PyObject>,
    handle_workflow: Option<PyObject>,
}

impl Default for PyDequeueBuilder {
    fn default() -> Self {
        Self {
            worker: None,
            batch_size: 1,
            queue_name: None,
            poll_interval_ms: None,
            at: None,
            handle_one: None,
            handle_batch: None,
            handle_workflow: None,
        }
    }
}

#[pymethods]
impl PyDequeueBuilder {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    pub fn worker(
        mut slf: PyRefMut<'_, Self>,
        consumer: Py<crate::workers::PyConsumer>,
    ) -> PyRefMut<'_, Self> {
        slf.worker = Some(consumer);
        slf
    }

    pub fn batch(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.batch_size = std::cmp::max(1, size);
        slf
    }

    pub fn from_queue(mut slf: PyRefMut<'_, Self>, queue: String) -> PyRefMut<'_, Self> {
        slf.queue_name = Some(queue);
        slf
    }

    pub fn poll_interval(mut slf: PyRefMut<'_, Self>, interval_ms: u64) -> PyRefMut<'_, Self> {
        slf.poll_interval_ms = Some(interval_ms);
        slf
    }

    pub fn at(mut slf: PyRefMut<'_, Self>, time: String) -> PyResult<PyRefMut<'_, Self>> {
        let time = chrono::DateTime::parse_from_rfc3339(&time)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
            .with_timezone(&chrono::Utc);
        slf.at = Some(time);
        Ok(slf)
    }

    #[getter]
    pub fn queue(&self) -> Option<String> {
        self.queue_name.clone()
    }

    #[getter]
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    #[getter]
    pub fn poll_interval_ms(&self) -> Option<u64> {
        self.poll_interval_ms
    }

    pub fn has_worker(&self) -> bool {
        self.worker.is_some()
    }

    pub fn handle(mut slf: PyRefMut<'_, Self>, handler: PyObject) -> PyRefMut<'_, Self> {
        slf.handle_one = Some(handler);
        slf
    }

    pub fn handle_batch(mut slf: PyRefMut<'_, Self>, handler: PyObject) -> PyRefMut<'_, Self> {
        slf.handle_batch = Some(handler);
        slf
    }

    pub fn handle_workflow(mut slf: PyRefMut<'_, Self>, handler: PyObject) -> PyRefMut<'_, Self> {
        slf.handle_workflow = Some(handler);
        slf
    }

    pub fn poll<'a>(&self, py: Python<'a>, store: PyStore) -> PyResult<&'a PyAny> {
        let worker = self.worker.clone();
        let queue_name = self.queue_name.clone();
        let batch_size = self.batch_size;
        let poll_interval_ms = self.poll_interval_ms;
        let at = self.at;
        let handle_one = self.handle_one.clone();
        let handle_batch = self.handle_batch.clone();
        let handle_workflow = self.handle_workflow.clone();

        pyo3_asyncio::tokio::future_into_py::<_, ()>(py, async move {
            validate_handler_configuration(
                batch_size,
                &handle_one,
                &handle_batch,
                &handle_workflow,
            )?;

            let store_inner = store.inner.clone();
            let consumer = worker
                .as_ref()
                .map(|worker| Python::with_gil(|py| worker.borrow(py).inner.as_ref().clone()));

            if let Some(handler) = handle_one {
                let rust_handler = move |msg: ::pgqrs::QueueMessage| {
                    let handler = handler.clone();
                    async move {
                        let py_msg = PyQueueMessage::from(msg);
                        let res = Python::with_gil(|py| -> PyResult<_> {
                            let fut = handler.call1(py, (py_msg,))?;
                            pyo3_asyncio::tokio::into_future(fut.as_ref(py))
                        })
                        .map_err(|e| ::pgqrs::Error::Internal {
                            message: e.to_string(),
                        })?
                        .await;

                        res.map_err(|e| ::pgqrs::Error::Internal {
                            message: e.to_string(),
                        })?;
                        Ok(())
                    }
                };

                let mut builder = ::pgqrs::dequeue().batch(batch_size);
                if let Some(interval_ms) = poll_interval_ms {
                    builder = builder.poll_interval(Duration::from_millis(interval_ms));
                }
                if let Some(at) = at {
                    builder = builder.at(at);
                }

                if let Some(consumer) = consumer.as_ref() {
                    builder
                        .worker(consumer)
                        .handle(rust_handler)
                        .poll(&store_inner)
                        .await
                        .map_err(to_py_err)?;
                } else if let Some(queue_name) = queue_name.as_deref() {
                    builder
                        .from(queue_name)
                        .handle(rust_handler)
                        .poll(&store_inner)
                        .await
                        .map_err(to_py_err)?;
                } else {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Queue name is required. Use .from_queue(\"queue-name\") or .worker(consumer)",
                    ));
                }
            } else if let Some(handler) = handle_batch {
                let rust_handler = move |msgs: Vec<::pgqrs::QueueMessage>| {
                    let handler = handler.clone();
                    async move {
                        let py_msgs = msgs
                            .into_iter()
                            .map(PyQueueMessage::from)
                            .collect::<Vec<_>>();
                        let res = Python::with_gil(|py| -> PyResult<_> {
                            let fut = handler.call1(py, (py_msgs,))?;
                            pyo3_asyncio::tokio::into_future(fut.as_ref(py))
                        })
                        .map_err(|e| ::pgqrs::Error::Internal {
                            message: e.to_string(),
                        })?
                        .await;

                        res.map_err(|e| ::pgqrs::Error::Internal {
                            message: e.to_string(),
                        })?;
                        Ok(())
                    }
                };

                let mut builder = ::pgqrs::dequeue().batch(batch_size);
                if let Some(interval_ms) = poll_interval_ms {
                    builder = builder.poll_interval(Duration::from_millis(interval_ms));
                }
                if let Some(at) = at {
                    builder = builder.at(at);
                }

                if let Some(consumer) = consumer.as_ref() {
                    builder
                        .worker(consumer)
                        .handle_batch(rust_handler)
                        .poll(&store_inner)
                        .await
                        .map_err(to_py_err)?;
                } else if let Some(queue_name) = queue_name.as_deref() {
                    builder
                        .from(queue_name)
                        .handle_batch(rust_handler)
                        .poll(&store_inner)
                        .await
                        .map_err(to_py_err)?;
                } else {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Queue name is required. Use .from_queue(\"queue-name\") or .worker(consumer)",
                    ));
                }
            } else if let Some(handler) = &handle_workflow {
                let consumer = if let Some(consumer) = consumer {
                    consumer
                } else if let Some(queue_name) = queue_name.as_deref() {
                    store_inner
                        .consumer_ephemeral(queue_name, store_inner.config())
                        .await
                        .map_err(to_py_err)?
                } else {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Queue name is required. Use .from_queue(\"queue-name\") or .worker(consumer)",
                    ));
                };

                loop {
                    let mut builder = ::pgqrs::dequeue().batch(batch_size).worker(&consumer);
                    if let Some(interval_ms) = poll_interval_ms {
                        builder = builder.poll_interval(Duration::from_millis(interval_ms));
                    }
                    if let Some(at) = at {
                        builder = builder.at(at);
                    }
                    let msgs = builder.poll(&store_inner).await.map_err(to_py_err)?;

                    for msg in msgs {
                        let inner_msg = msg.clone();
                        let handler = handler.clone();
                        let store_clone = store_inner.clone();
                        let workflow_id = inner_msg.id; // workflow run ID corresponds to msg.id

                        let rust_run = ::pgqrs::run()
                            .store(&store_clone)
                            .message(inner_msg.clone())
                            .execute()
                            .await
                            .map_err(to_py_err)?;

                        let py_run = crate::workflow::PyRun {
                            inner: Arc::new(rust_run),
                            store: store_clone,
                            workflow_id,
                        };

                        let res = Python::with_gil(|py| -> PyResult<_> {
                            let fut = handler.call1(py, (py_run.into_py(py),))?;
                            pyo3_asyncio::tokio::into_future(fut.as_ref(py))
                        })?
                        .await;

                        match res {
                            Ok(_) => {
                                // The work is done by the wrapper, but we need to archive it from the queue
                                consumer.archive(msg.id).await.map_err(to_py_err)?;
                            }
                            Err(e) => {
                                consumer
                                    .release_messages(&[msg.id])
                                    .await
                                    .map_err(to_py_err)?;
                                return Err(e);
                            }
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

#[pyfunction]
pub fn dequeue_builder() -> PyDequeueBuilder {
    PyDequeueBuilder::default()
}
