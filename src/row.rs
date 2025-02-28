use crate::{column::DuckDBColumn, database::DuckDB, value::DuckDBValue};
use sqlx_core::{ext::ustr::UStr, row::Row, HashMap};
use std::sync::Arc;

pub struct DuckDBRow {
    pub(crate) column_names: Arc<HashMap<UStr, usize>>,
    pub(crate) columns: Box<[DuckDBColumn]>,
    pub(crate) values: Box<[DuckDBValue]>,
}

impl Row for DuckDBRow {
    type Database = DuckDB;

    fn columns(&self) -> &[DuckDBColumn] {
        &self.columns
    }

    fn try_get_raw<I>(
        &self,
        index: I,
    ) -> Result<<Self::Database as sqlx_core::database::Database>::ValueRef<'_>, sqlx_core::Error>
    where
        I: sqlx_core::column::ColumnIndex<Self>,
    {
        todo!()
    }
}

impl<'s> From<duckdb::Row<'s>> for DuckDBRow {
    fn from(value: duckdb::Row) -> Self {
        let stmt = value.as_ref();
        let count = stmt.column_count();
        let name = stmt.column_name(0);
        let tt = stmt.column_type(0);
        let column = value.get_ref(0);
    }
}
