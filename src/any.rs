use crate::{
    connection::DuckDBConnection, database::DuckDB, transaction::DuckDBTransactionManager,
};
use futures_core::{future::BoxFuture, stream::BoxStream};
use sqlx_core::{
    any::{AnyArguments, AnyConnectionBackend, AnyQueryResult, AnyRow, AnyStatement, AnyTypeInfo},
    connection::Connection,
    database::Database,
    describe::Describe,
    executor::Executor,
    transaction::TransactionManager,
    Either,
};

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
        arguments: Option<AnyArguments<'q>>,
    ) -> BoxStream<'q, sqlx_core::Result<Either<AnyQueryResult, AnyRow>>> {
        <DuckDBConnection as Executor<'q>>::fetch_many(&mut self, query)
    }

    fn fetch_optional<'q>(
        &'q mut self,
        query: &'q str,
        persistent: bool,
        arguments: Option<AnyArguments<'q>>,
    ) -> BoxFuture<'q, sqlx_core::Result<Option<AnyRow>>> {
        todo!()
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: &'q str,
        parameters: &[AnyTypeInfo],
    ) -> BoxFuture<'c, sqlx_core::Result<AnyStatement<'q>>> {
        todo!()
    }

    fn describe<'q>(
        &'q mut self,
        sql: &'q str,
    ) -> BoxFuture<'q, sqlx_core::Result<Describe<sqlx_core::any::Any>>> {
        todo!()
    }
}
