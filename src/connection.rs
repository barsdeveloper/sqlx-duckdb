use crate::{
    database::DuckDB, error::DuckDBError, options::DuckDBConnectOptions,
    transaction::DuckDBTransactionManager, BoxFuture,
};
use sqlx_core::transaction::TransactionManager;
use sqlx_core::{
    any::AnyConnectionBackend, connection::Connection, database::Database, transaction::Transaction,
};
use std::future;

/// A connection to an open [DuckDB] database.
///
/// Because SQLite is an in-process database accessed by blocking API calls, SQLx uses a background
/// thread and communicates with it via channels to allow non-blocking access to the database.
///
/// Dropping this struct will signal the worker thread to quit and close the database, though
/// if an error occurs there is no way to pass it back to the user this way.
///
/// You can explicitly call [`.close()`][Self::close] to ensure the database is closed successfully
/// or get an error otherwise.
#[derive(Debug)]
pub struct DuckDBConnection {
    pub(crate) connection: duckdb::Connection,
    pub(crate) transaction: bool,
}

impl DuckDBConnection {
    pub(crate) async fn establish(
        options: &DuckDBConnectOptions,
    ) -> Result<Self, sqlx_core::Error> {
        let connection = if options.in_memory {
            duckdb::Connection::open_in_memory_with_flags(options.into())
        } else {
            duckdb::Connection::open_with_flags(&*options.path, options.into())
        }
        .map_err(|e| sqlx_core::Error::Database(Box::new(DuckDBError::new(e.into()))))?;
        Ok(DuckDBConnection {
            connection,
            transaction: false,
        })
    }
}

impl Connection for DuckDBConnection {
    type Database = DuckDB;
    type Options = DuckDBConnectOptions;

    fn close(self) -> BoxFuture<'static, Result<(), sqlx_core::Error>> {
        Box::pin(async move {
            self.connection.close().map_err(|e| {
                sqlx_core::Error::Protocol(format!(
                    "Error while closing the connection: {}",
                    e.1.to_string()
                ))
            })?;
            Ok(())
        })
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), sqlx_core::Error>> {
        Box::pin(async move {
            drop(self);
            Ok(())
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        todo!()
    }

    fn begin(
        &mut self,
    ) -> BoxFuture<
        '_,
        Result<sqlx_core::transaction::Transaction<'_, Self::Database>, sqlx_core::Error>,
    >
    where
        Self: Sized,
    {
        Transaction::begin(self)
    }

    fn shrink_buffers(&mut self) {}

    fn flush(&mut self) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        self.connection.flush_prepared_statement_cache();
        Box::pin(future::ready(Ok(())))
    }

    fn should_flush(&self) -> bool {
        false
    }
}

impl AnyConnectionBackend for DuckDBConnection {
    fn name(&self) -> &str {
        <DuckDB as Database>::NAME
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close(*self)
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close_hard(*self)
    }

    fn ping(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::ping(self)
    }

    fn begin(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        DuckDBTransactionManager::begin(self)
    }

    fn commit(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        DuckDBTransactionManager::commit(self)
    }

    fn rollback(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        DuckDBTransactionManager::rollback(self)
    }

    fn start_rollback(&mut self) {
        DuckDBTransactionManager::start_rollback(self)
    }

    fn shrink_buffers(&mut self) {
        Connection::shrink_buffers(self)
    }

    fn flush(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::flush(self)
    }

    fn should_flush(&self) -> bool {
        Connection::should_flush(self)
    }

    fn fetch_many<'q>(
        &'q mut self,
        query: &'q str,
        persistent: bool,
        arguments: Option<sqlx_core::any::AnyArguments<'q>>,
    ) -> BoxStream<
        'q,
        sqlx_core::Result<
            sqlx_core::Either<sqlx_core::any::AnyQueryResult, sqlx_core::any::AnyRow>,
        >,
    > {
        DuckDBTransactionManager::fetch_many(self)
    }

    fn fetch_optional<'q>(
        &'q mut self,
        query: &'q str,
        persistent: bool,
        arguments: Option<sqlx_core::any::AnyArguments<'q>>,
    ) -> BoxFuture<'q, sqlx_core::Result<Option<sqlx_core::any::AnyRow>>> {
        todo!()
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: &'q str,
        parameters: &[sqlx_core::any::AnyTypeInfo],
    ) -> BoxFuture<'c, sqlx_core::Result<sqlx_core::any::AnyStatement<'q>>> {
        todo!()
    }

    fn describe<'q>(
        &'q mut self,
        sql: &'q str,
    ) -> BoxFuture<'q, sqlx_core::Result<Describe<sqlx_core::any::Any>>> {
        todo!()
    }
}
