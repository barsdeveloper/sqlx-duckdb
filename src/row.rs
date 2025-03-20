use crate::value::DuckDBValueRef;
use crate::{column::DuckDBColumn, database::DuckDB};
use sqlx_core::Error;
use sqlx_core::column::Column;
use sqlx_core::{Result, column::ColumnIndex, row::Row};

pub struct DuckDBRow(pub(crate) Vec<DuckDBColumn>);

impl Row for DuckDBRow {
    type Database = DuckDB;

    fn columns(&self) -> &[DuckDBColumn] {
        &self.0
    }

    fn try_get_raw<I>(&self, index: I) -> Result<DuckDBValueRef<'_>>
    where
        I: ColumnIndex<Self>,
    {
        Ok(self.0[index.index(self)?].value_ref())
    }
}

impl ColumnIndex<DuckDBRow> for &'_ str {
    fn index(&self, row: &DuckDBRow) -> Result<usize> {
        row.0.iter().position(|v| v.name() == *self).ok_or_else(|| {
            Error::ColumnNotFound(format!(
                "Column {} not found, possible alternatives: {}",
                self,
                row.0
                    .iter()
                    .map(|v| v.name())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })
    }
}
