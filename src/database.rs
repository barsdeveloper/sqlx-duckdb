use crate::{
    arguments::{DuckDBArgumentBuffer, DuckDBArguments},
    column::DuckDBColumn,
    connection::DuckDBConnection,
    query_result::DuckDBQueryResult,
    row::DuckDBRow,
    statement::DuckDBStatement,
    transaction::DuckDBTransactionManager,
    type_info::DuckdbDBTypeInfo,
    value::{DuckDBValue, DuckDBValueRef},
};
use sqlx_core::{database::Database, declare_driver_with_optional_migrate};

/// DuckDB database driver.
#[derive(Debug)]
pub struct DuckDB;

impl Database for DuckDB {
    type Connection = DuckDBConnection;

    type TransactionManager = DuckDBTransactionManager;

    type Row = DuckDBRow;

    type QueryResult = DuckDBQueryResult;

    type Column = DuckDBColumn;

    type TypeInfo = DuckdbDBTypeInfo;

    type Value = DuckDBValue;
    type ValueRef<'r> = DuckDBValueRef<'r>;

    type Arguments<'q> = DuckDBArguments;
    type ArgumentBuffer<'q> = DuckDBArgumentBuffer;

    type Statement<'q> = DuckDBStatement<'q>;

    const NAME: &'static str = "DuckDB";

    const URL_SCHEMES: &'static [&'static str] = &["duckdb"];
}

declare_driver_with_optional_migrate!(DRIVER = DuckDB);
