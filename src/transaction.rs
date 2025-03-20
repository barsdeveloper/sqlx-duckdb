use crate::{connection::DuckDBConnection, database::DuckDB};
use futures::future::BoxFuture;
use sqlx_core::transaction::TransactionManager;

pub struct DuckDBTransactionManager;

impl TransactionManager for DuckDBTransactionManager {
    type Database = DuckDB;

    fn begin(conn: &mut DuckDBConnection) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        todo!()
    }

    fn commit(conn: &mut DuckDBConnection) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        todo!()
    }

    fn rollback(conn: &mut DuckDBConnection) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        todo!()
    }

    fn start_rollback(conn: &mut DuckDBConnection) {
        todo!()
    }
}
