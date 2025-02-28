use crate::arguments::DuckDBArguments;
use crate::query_result::DuckDBQueryResult;
use crate::row::DuckDBRow;
use crate::{database::DuckDB, error::DuckDBError, options::DuckDBConnectOptions};
use duckdb::Error;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_core::Stream;
use sqlx_core::database::Database;
use sqlx_core::describe::Describe;
use sqlx_core::executor::{Execute, Executor};
use sqlx_core::{connection::Connection, transaction::Transaction};
use sqlx_core::{try_stream, Either};
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

    pub(crate) async fn run(
        &mut self,
        query: &str,
        arguments: Option<DuckDBArguments>,
        cached: bool,
    ) -> Result<impl Stream<Item = Result<Either<DuckDBQueryResult, DuckDBRow>, Error>>, Error>
    {
        let connection = self.connection;

        tokio::spawn_blocking(move || {
            let statement = if cached {
                connection.prepare_cached(sql)
            } else {
                connection.prepare(sql)
            }
            .map_err(|e| e.into())?;
            let result = statement.query(arguments.unwrap_or_default().into_duckdb_params())?;
            statement.stream_arrow(
                arguments.map_or(duckdb::params![], |a| a.into_duckdb_params()),
                schema,
            )
        });
        Ok(())
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

impl<'c> Executor<'c> for &'c mut DuckDBConnection {
    type Database = DuckDB;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxStream<
        'e,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            sqlx_core::Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + Execute<'q, DuckDB>,
    {
        let sql = query.sql();
        let params = query.take_arguments()?;
        let connection = self.connection;
        let task = tokio::task::spawn_blocking(move || {
            let mut statement = connection.prepare(sql).map_err(|e| DuckDBError::new(e))?;
            statement.execute(params.unwrap_or(duckdb::params![]));
            Ok(())
        });
        Box::pin(Ok(self.connection.prepare(sql)?.query([])?))
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Self::Database as Database>::Row>, sqlx_core::Error>>
    where
        'c: 'e,
        E: 'q + sqlx_core::executor::Execute<'q, Self::Database>,
    {
        todo!()
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement<'q>, sqlx_core::Error>>
    where
        'c: 'e,
    {
        todo!()
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, sqlx_core::Error>>
    where
        'c: 'e,
    {
        todo!()
    }
}
