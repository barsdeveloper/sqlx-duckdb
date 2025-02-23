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
