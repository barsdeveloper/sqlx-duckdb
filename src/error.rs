use std::{
    error::Error,
    fmt::{Display, Formatter},
    sync::OnceLock,
};

#[derive(Debug)]
pub struct DuckDBError {
    source: Option<Box<dyn Error + Send + Sync>>,
    message: OnceLock<String>,
}

impl DuckDBError {
    pub fn new(error: String) -> Self {
        let message = OnceLock::new();
        message.set(error);
        Self {
            source: None,
            message,
        }
    }

    pub fn from_source_message(source: Box<dyn Error + Send + Sync>, message: String) -> Self {
        let lock = OnceLock::new();
        lock.set(message);
        Self {
            source: Some(source),
            message: lock,
        }
    }

    pub fn message(&self) -> &str {
        let mut result = self.message.get();
        if result.is_none() && self.source.is_some() {
            self.message.set(self.source.as_ref().unwrap().to_string());
            result = self.message.get();
        }
        result.unwrap()
    }
}

impl Display for DuckDBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.message().fmt(f)?;
        if let Some(source) = &self.source {
            write!(f, "\nCaused by: {}", *source)?;
        }
        Ok(())
    }
}

impl std::error::Error for DuckDBError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn Error + 'static))
    }
}

impl sqlx_core::error::DatabaseError for DuckDBError {
    fn message(&self) -> &str {
        self.message()
    }

    fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self.source
            .as_deref()
            .unwrap_or_else(|| self as &(dyn Error + Send + Sync))
    }

    fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
        if self.source.is_some() {
            self.source.as_deref_mut().unwrap()
        } else {
            self
        }
    }

    fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
        match self.source {
            Some(err) => err,
            None => self,
        }
    }

    fn kind(&self) -> sqlx_core::error::ErrorKind {
        sqlx_core::error::ErrorKind::Other
    }
}
