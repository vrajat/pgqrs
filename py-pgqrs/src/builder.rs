use crate::tables::PyQueueMessage;
use crate::{to_py_err, PyStore};
use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::sync::Arc;
use tokio::time::Duration;

#[pyclass(name = "DequeueBuilder")]
pub struct PyDequeueBuilder {
    worker: Option<Py<crate::workers::PyConsumer>>,
    batch_size: usize,
    queue_name: Option<String>,
    poll_interval_ms: Option<u64>,
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
        let batch_size = self.batch_size;
        let poll_interval = Duration::from_millis(self.poll_interval_ms.unwrap_or(50));
        let handle_one = self.handle_one.clone();
        let handle_batch = self.handle_batch.clone();
        let handle_workflow = self.handle_workflow.clone();

        pyo3_asyncio::tokio::future_into_py::<_, ()>(py, async move {
            let worker = worker
                .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("worker is required"))?;

            let num_handlers = handle_one.is_some() as u8
                + handle_batch.is_some() as u8
                + handle_workflow.is_some() as u8;
            if num_handlers != 1 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "exactly one of handle(), handle_batch() or handle_workflow() is required",
                ));
            }

            let consumer = Python::with_gil(|py| Arc::clone(&worker.borrow(py).inner));
            let _store = store.inner.clone();

            consumer.poll().await.map_err(to_py_err)?;

            loop {
                let status = consumer.status().await.map_err(to_py_err)?;
                if status == ::pgqrs::types::WorkerStatus::Interrupted {
                    consumer.suspend().await.map_err(to_py_err)?;
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "worker interrupted",
                    ));
                }

                let msgs = consumer.dequeue_many(batch_size).await.map_err(to_py_err)?;
                if msgs.is_empty() {
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }

                if let Some(handler) = &handle_one {
                    for msg in msgs {
                        let py_msg = PyQueueMessage::from(msg.clone());
                        let handler = handler.clone();
                        let res = Python::with_gil(|py| -> PyResult<_> {
                            let fut = handler.call1(py, (py_msg,))?;
                            pyo3_asyncio::tokio::into_future(fut.as_ref(py))
                        })?
                        .await;

                        match res {
                            Ok(_) => {
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
                } else if let Some(handler) = &handle_batch {
                    let py_msgs = msgs
                        .iter()
                        .cloned()
                        .map(PyQueueMessage::from)
                        .collect::<Vec<_>>();
                    let handler = handler.clone();
                    let res = Python::with_gil(|py| -> PyResult<_> {
                        let fut = handler.call1(py, (py_msgs,))?;
                        pyo3_asyncio::tokio::into_future(fut.as_ref(py))
                    })?
                    .await;

                    match res {
                        Ok(_) => {
                            let msg_ids = msgs.into_iter().map(|m| m.id).collect::<Vec<_>>();
                            consumer.archive_many(msg_ids).await.map_err(to_py_err)?;
                        }
                        Err(e) => {
                            let msg_ids = msgs.into_iter().map(|m| m.id).collect::<Vec<_>>();
                            consumer
                                .release_messages(&msg_ids)
                                .await
                                .map_err(to_py_err)?;
                            return Err(e);
                        }
                    }
                } else if let Some(handler) = &handle_workflow {
                    for msg in msgs {
                        let inner_msg = msg.clone();
                        let handler = handler.clone();
                        let store_clone = _store.clone();
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

            #[allow(unreachable_code)]
            Ok(())
        })
    }
}

#[pyfunction]
pub fn dequeue_builder() -> PyDequeueBuilder {
    PyDequeueBuilder::default()
}
