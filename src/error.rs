use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Errors surfaced by the Rust daemon and its public adapters.
#[derive(Debug)]
pub enum AppError {
    /// Storage/database failure.
    Storage(duckdb::Error),
    /// Connection-pool lifecycle failure.
    Pool(r2d2::Error),
    /// Filesystem or process I/O failure.
    Io(std::io::Error),
    /// JSON decoding or encoding failure.
    Json(serde_json::Error),
    /// XML parsing failure.
    Xml(quick_xml::Error),
    /// HTTP request-body decoding failure.
    HttpBody(hyper::Error),
    /// Validation failure with a stable human-readable message.
    Validation(String),
    /// Requested object was not found.
    NotFound(String),
    /// A command or daemon operation failed.
    Runtime(String),
    /// Another process owns the requested daemon or database lease.
    Busy {
        /// Resource that could not be acquired.
        resource: String,
        /// Best-effort owner metadata or pool diagnostic.
        holder: String,
    },
    /// A DuckDB operation exceeded the configured deadline.
    Timeout {
        /// Operation that exceeded its deadline.
        operation: String,
        /// Configured deadline in milliseconds.
        timeout_ms: u64,
    },
}

/// Convenient result alias for application operations.
pub type AppResult<T> = Result<T, AppError>;

impl Display for AppError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "storage error: {error}"),
            Self::Pool(error) => write!(formatter, "connection pool error: {error}"),
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Json(error) => write!(formatter, "JSON error: {error}"),
            Self::Xml(error) => write!(formatter, "XML error: {error}"),
            Self::HttpBody(error) => {
                write!(formatter, "request body could not be read: {error}")
            }
            Self::Validation(message) | Self::NotFound(message) | Self::Runtime(message) => {
                formatter.write_str(message)
            }
            Self::Busy { resource, holder } => {
                write!(formatter, "resource busy: {resource}{holder}")
            }
            Self::Timeout {
                operation,
                timeout_ms,
            } => write!(formatter, "{operation} timed out after {timeout_ms} ms"),
        }
    }
}

impl Error for AppError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Pool(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Xml(error) => Some(error),
            Self::HttpBody(error) => Some(error),
            Self::Validation(_)
            | Self::NotFound(_)
            | Self::Runtime(_)
            | Self::Busy { .. }
            | Self::Timeout { .. } => None,
        }
    }
}

macro_rules! app_error_from {
    ($source:ty, $variant:ident) => {
        impl From<$source> for AppError {
            fn from(error: $source) -> Self {
                Self::$variant(error)
            }
        }
    };
}

app_error_from!(duckdb::Error, Storage);
app_error_from!(r2d2::Error, Pool);
app_error_from!(std::io::Error, Io);
app_error_from!(serde_json::Error, Json);
app_error_from!(quick_xml::Error, Xml);
app_error_from!(hyper::Error, HttpBody);

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

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::time::Duration;

    use http_body_util::Empty;
    use hyper::body::{Bytes, Incoming};
    use hyper::server::conn::http1;
    use hyper::service::Service;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[derive(Clone, Copy, Debug)]
    struct FailingManager;

    #[derive(Clone, Copy, Debug)]
    struct StaticService;

    impl Service<Request<Incoming>> for StaticService {
        type Response = Response<Empty<Bytes>>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn call(&self, _: Request<Incoming>) -> Self::Future {
            std::future::ready(Ok(Response::new(Empty::new())))
        }
    }

    impl r2d2::ManageConnection for FailingManager {
        type Connection = ();
        type Error = std::io::Error;

        fn connect(&self) -> Result<Self::Connection, Self::Error> {
            Err(std::io::Error::other("connection failed"))
        }

        fn is_valid(&self, _: &mut Self::Connection) -> Result<(), Self::Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn display_and_sources_preserve_wrapped_errors() {
        let mut connection = ();
        assert!(r2d2::ManageConnection::is_valid(&FailingManager, &mut connection).is_ok());
        assert!(!r2d2::ManageConnection::has_broken(
            &FailingManager,
            &mut connection
        ));
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_timeout(Duration::from_millis(5))
            .build(FailingManager)
            .expect_err("failing manager must reject pool creation");
        let (server_io, mut client_io) = tokio::io::duplex(64);
        client_io
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        client_io.shutdown().await.unwrap();
        drop(client_io);
        let http_body = tokio::time::timeout(
            Duration::from_secs(1),
            http1::Builder::new().serve_connection(TokioIo::new(server_io), StaticService),
        )
        .await
        .expect("closed HTTP peer must fail promptly")
        .unwrap_err();

        let wrapped = [
            AppError::from(duckdb::Error::InvalidParameterName("missing".to_owned())),
            AppError::from(pool),
            AppError::from(std::io::Error::other("disk failed")),
            AppError::from(serde_json::from_str::<serde_json::Value>("{").unwrap_err()),
            AppError::from(quick_xml::Error::from(std::io::Error::other("XML failed"))),
            AppError::from(http_body),
        ];
        let prefixes = [
            "storage error:",
            "connection pool error:",
            "I/O error:",
            "JSON error:",
            "XML error:",
            "request body could not be read:",
        ];
        for (error, prefix) in wrapped.iter().zip(prefixes) {
            assert!(error.to_string().starts_with(prefix));
            assert!(error.source().is_some());
        }
    }

    #[test]
    fn display_and_sources_preserve_application_errors() {
        let application_errors = [
            (AppError::Validation("invalid".to_owned()), "invalid"),
            (AppError::NotFound("missing".to_owned()), "missing"),
            (AppError::Runtime("failed".to_owned()), "failed"),
            (
                AppError::Busy {
                    resource: "database".to_owned(),
                    holder: " held by pid 42".to_owned(),
                },
                "resource busy: database held by pid 42",
            ),
            (
                AppError::Timeout {
                    operation: "query".to_owned(),
                    timeout_ms: 250,
                },
                "query timed out after 250 ms",
            ),
        ];
        for (error, message) in application_errors {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none());
        }
    }
}
