use crate::{database::DuckDB, type_info::DuckdbDBTypeInfo, value::DuckDBValueRef};
use sqlx_core::{column::Column, ext::ustr::UStr};

#[derive(Debug)]
pub struct DuckDBColumn {
    pub(crate) name: UStr,
    pub(crate) ordinal: usize,
    pub(crate) type_info: DuckdbDBTypeInfo,
}

impl DuckDBColumn {
    pub fn value_ref(&self) -> DuckDBValueRef<'_> {
        DuckDBValueRef {
            type_info: &self.type_info,
        }
    }
}

impl Column for DuckDBColumn {
    type Database = DuckDB;

    fn ordinal(&self) -> usize {
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &DuckdbDBTypeInfo {
        &self.type_info
    }
}
