use std::{fmt::Display, sync::OnceLock};

#[derive(Debug)]
pub struct DuckDBError {
    error: duckdb::Error,
    message: OnceLock<String>,
}

impl DuckDBError {
    pub fn new(error: duckdb::Error) -> Self {
        Self {
            error,
            message: OnceLock::new(),
        }
    }
    pub fn message(&self) -> &str {
        self.message.get_or_init(|| self.error.to_string())
    }
}

impl std::error::Error for DuckDBError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }
}

impl Display for DuckDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl sqlx_core::error::DatabaseError for DuckDBError {
    fn message(&self) -> &str {
        self.message()
    }

    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        &self.error
    }

    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        &mut self.error
    }

    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self.error.into()
    }

    fn kind(&self) -> sqlx_core::error::ErrorKind {
        sqlx_core::error::ErrorKind::Other
    }
}
