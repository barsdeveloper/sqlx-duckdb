use crate::{
    arguments::DuckDBArguments, cbox::CBox, column::DuckDBColumn, database::DuckDB,
    type_info::DuckdbDBTypeInfo,
};
use libduckdb_sys::duckdb_prepared_statement;
use sqlx_core::Either;
use sqlx_core::{HashMap, ext::ustr::UStr, impl_statement_query, statement::Statement};
use std::{borrow::Cow, sync::Arc};

pub struct DuckDBStatement<'q> {
    pub(crate) sql: Cow<'q, str>,
    pub(crate) parameters: usize,
    pub(crate) columns: Arc<Vec<DuckDBColumn>>,
    pub(crate) column_names: Arc<HashMap<UStr, usize>>,
    pub(crate) prepared_statement: CBox<duckdb_prepared_statement>,
}

impl<'q> Statement<'q> for DuckDBStatement<'q> {
    type Database = DuckDB;

    fn to_owned(&self) -> DuckDBStatement<'static> {
        todo!()
    }

    fn sql(&self) -> &str {
        todo!()
    }

    fn parameters(&self) -> Option<Either<&[DuckdbDBTypeInfo], usize>> {
        todo!()
    }

    fn columns(&self) -> &[DuckDBColumn] {
        todo!()
    }

    impl_statement_query!(DuckDBArguments);
}
