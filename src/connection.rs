use crate::arguments::DuckDBArguments;
use crate::cbox::CBox;
use crate::column::DuckDBColumn;
use crate::extract_value::extract_value;
use crate::row::DuckDBRow;
use crate::{database::DuckDB, error::DuckDBError, options::DuckDBConnectOptions};
use futures_core::Stream;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use libduckdb_sys::*;
use sqlx_core::{
    Either, Error, Result,
    connection::Connection,
    database::Database,
    describe::Describe,
    executor::{Execute, Executor},
    transaction::Transaction,
};
use std::ffi::{CStr, CString, c_char};
use std::ops::DerefMut;
use std::ptr::null_mut;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::{future, mem, ptr};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// A connection to an open [DuckDB] database.
///
/// Because DuckDB is an in-process database accessed by blocking API calls, SQLx uses tokio
/// to allow non-blocking access to the database.
///
/// Dropping this struct will not close the database because open databases are cached, to avoid
/// opening the same database multiple times. Most likely the database will be closed at the program
/// end.
#[derive(Debug)]
pub struct DuckDBConnection {
    pub(crate) connection: CBox<duckdb_connection>,
    pub(crate) transaction: bool,
}

impl DuckDBConnection {
    pub(crate) fn duckdb_instance_cache() -> &'static AtomicPtr<_duckdb_instance_cache> {
        static DATABASE_CACHE: LazyLock<CBox<AtomicPtr<_duckdb_instance_cache>>> =
            LazyLock::new(|| {
                CBox::new(
                    AtomicPtr::new(unsafe { duckdb_create_instance_cache() }),
                    |ptr| unsafe {
                        duckdb_destroy_instance_cache(&mut ptr.load(Ordering::Relaxed))
                    },
                )
            });
        &**DATABASE_CACHE
    }

    pub(crate) async fn establish(options: &DuckDBConnectOptions) -> Result<Self> {
        let db_cache = DuckDBConnection::duckdb_instance_cache().load(Ordering::Relaxed);
        let config = options.create_duckdb_config()?;
        let mut database: duckdb_database = null_mut();
        let connection = unsafe {
            let mut error: *mut c_char = null_mut();
            if duckdb_get_or_create_from_cache(
                db_cache,
                options.path.as_ptr(),
                &mut database,
                *config,
                &mut error,
            ) != duckdb_state_DuckDBSuccess
            {
                return Err(Error::Configuration(
                    format!(
                        "Error while opening a (possibly cached) database instance: `{}`",
                        CStr::from_ptr(error).to_str().unwrap()
                    )
                    .into(),
                ));
            }
            let mut connection: duckdb_connection = null_mut();
            if duckdb_connect(database, &mut connection) != duckdb_state_DuckDBSuccess {
                return Err(Error::Configuration(
                    format!(
                        "Error connecting to the database `{}`",
                        CStr::from_ptr(error).to_str().unwrap()
                    )
                    .into(),
                ));
            }
            CBox::new(connection, |mut connection| {
                duckdb_disconnect(&mut connection);
            })
        };
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
    ) -> Result<impl Stream<Item = DuckDBRow>> {
        let query = CString::new(query).map_err(|e| DuckDBError::new(e.to_string()))?;
        let (mut tx, mut rx) = mpsc::unbounded_channel::<DuckDBRow>();
        let connection = AtomicPtr::new(*self.connection);
        tokio::task::spawn_blocking(move || -> Result<()> {
            unsafe {
                let mut prepared_statement =
                    CBox::new(ptr::null_mut(), |mut ptr| duckdb_destroy_prepare(&mut ptr));
                let rc = duckdb_prepare(
                    connection.load(Ordering::Relaxed),
                    query.as_ptr(),
                    &mut *prepared_statement,
                );
                if rc != duckdb_state_DuckDBSuccess {
                    let message = CStr::from_ptr(duckdb_prepare_error(*prepared_statement))
                        .to_str()
                        .unwrap()
                        .into();
                    return Err(DuckDBError::new(message).into());
                }
                let mut result: duckdb_result = mem::zeroed();
                let rc = duckdb_execute_prepared_streaming(*prepared_statement, &mut result);
                let mut result = CBox::new(result, |mut r| duckdb_destroy_result(&mut r));
                if rc != duckdb_state_DuckDBSuccess {
                    return Err(DuckDBError::new("Error while executing the query".into()).into());
                }
                let chunk = CBox::new(duckdb_fetch_chunk(*result), |mut v| {
                    duckdb_destroy_data_chunk(&mut v);
                });
                let rows = duckdb_data_chunk_get_size(*chunk);
                let cols = duckdb_data_chunk_get_column_count(*chunk);
                let info = (0..cols)
                    .map(|i| {
                        let vector = duckdb_data_chunk_get_vector(*chunk, i);
                        let logical_type = duckdb_vector_get_column_type(vector);
                        let type_id = duckdb_get_type_id(logical_type);
                        let data = duckdb_vector_get_data(vector);
                        let validity = duckdb_vector_get_validity(vector);
                        let name = CStr::from_ptr(duckdb_column_name(result.deref_mut(), i))
                            .to_str()
                            .unwrap();
                        (vector, logical_type, type_id, data, validity, name)
                    })
                    .collect::<Box<[_]>>();
                (0..rows).for_each(|row| {
                    let columns = (0..cols).map(|col| {
                        let col = col as usize;
                        let info = info[col];
                        Ok(DuckDBColumn {
                            name: info.5.into(),
                            ordinal: col as usize,
                            type_info: extract_value(
                                info.0,
                                row as usize,
                                info.1,
                                info.2,
                                info.3,
                                info.4,
                            )?
                            .into(),
                        })
                    });
                    let message = DuckDBRow(columns.collect::<Result<_>>().unwrap());
                    tx.send(message);
                });
            }
            Ok(())
        });
        Ok(UnboundedReceiverStream::new(rx)).into()
    }
}

impl Connection for DuckDBConnection {
    type Database = DuckDB;
    type Options = DuckDBConnectOptions;

    fn close(mut self) -> BoxFuture<'static, Result<(), sqlx_core::Error>> {
        Box::pin(async move {
            tokio::task::spawn_blocking(move || unsafe {
                duckdb_disconnect(self.connection.deref_mut());
            });
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
        // Nothing to be done
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
        query: E,
    ) -> BoxStream<
        'e,
        std::result::Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        todo!()
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, std::result::Result<Option<<Self::Database as Database>::Row>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        todo!()
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, std::result::Result<<Self::Database as Database>::Statement<'q>, Error>>
    where
        'c: 'e,
    {
        todo!()
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, std::result::Result<Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        todo!()
    }
}
