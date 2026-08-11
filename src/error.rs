use thiserror::Error;

/// Errors surfaced by the Rust daemon and its public adapters.
#[derive(Debug, Error)]
pub enum AppError {
    /// Storage/database failure.
    #[error("storage error: {0}")]
    Storage(#[from] duckdb::Error),
    /// Connection-pool lifecycle failure.
    #[error("connection pool error: {0}")]
    Pool(#[from] r2d2::Error),
    /// Filesystem or process I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON decoding or encoding failure.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// XML parsing failure.
    #[error("XML error: {0}")]
    Xml(#[from] quick_xml::Error),
    /// HTTP request-body decoding failure.
    #[error("request body could not be read: {0}")]
    HttpBody(#[from] hyper::Error),
    /// Validation failure with a stable human-readable message.
    #[error("{0}")]
    Validation(String),
    /// Requested object was not found.
    #[error("{0}")]
    NotFound(String),
    /// A command or daemon operation failed.
    #[error("{0}")]
    Runtime(String),
    /// Another process owns the requested daemon or database lease.
    #[error("resource busy: {resource}{holder}")]
    Busy {
        /// Resource that could not be acquired.
        resource: String,
        /// Best-effort owner metadata or pool diagnostic.
        holder: String,
    },
    /// A DuckDB operation exceeded the configured deadline.
    #[error("{operation} timed out after {timeout_ms} ms")]
    Timeout {
        /// Operation that exceeded its deadline.
        operation: String,
        /// Configured deadline in milliseconds.
        timeout_ms: u64,
    },
}

/// Convenient result alias for application operations.
pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    /// Returns the HTTP status that matches the REST error contract.
    pub fn status_code(&self) -> u16 {
        match self {
            Self::NotFound(_) => 404,
            Self::Validation(_) => 400,
            Self::Storage(_)
            | Self::Pool(_)
            | Self::Io(_)
            | Self::Json(_)
            | Self::Xml(_)
            | Self::HttpBody(_)
            | Self::Runtime(_) => 500,
            Self::Busy { .. } => 503,
            Self::Timeout { .. } => 504,
        }
    }
}
