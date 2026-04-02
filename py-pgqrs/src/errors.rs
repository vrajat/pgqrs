use ::pgqrs as rust_pgqrs;
use pyo3::prelude::*;

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
pyo3::create_exception!(pgqrs, PausedError, PgqrsError);

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
        rust_pgqrs::Error::Paused { .. } => PausedError::new_err(err.to_string()),
        _ => PgqrsError::new_err(err.to_string()),
    }
}

pub(crate) fn add_exceptions(py: Python<'_>, m: &PyModule) -> PyResult<()> {
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
    m.add("PausedError", py.get_type::<PausedError>())?;
    Ok(())
}
