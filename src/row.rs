use crate::{column::DuckDBColumn, database::DuckDB};
use sqlx_core::{column::ColumnIndex, database::Database, row::Row, Result};

pub struct DuckDBRow(pub(crate) Vec<DuckDBColumn>);

impl Row for DuckDBRow {
    type Database = DuckDB;

    fn columns(&self) -> &[DuckDBColumn] {
        &self.0
    }

    fn try_get_raw<I>(&self, index: I) -> Result<<Self::Database as Database>::ValueRef<'_>>
    where
        I: ColumnIndex<Self>,
    {
        todo!()
    }
}
