use sqlx_core::{arguments::Arguments, error::BoxDynError};

use crate::database::DuckDB;

#[derive(Default)]
pub struct DuckDBArguments<'q>;

impl<'q> Arguments<'q> for DuckDBArguments<'q> {
    type Database = DuckDB;

    fn reserve(&mut self, additional: usize, size: usize) {
        todo!()
    }

    fn add<T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: 'q
            + sqlx_core::encode::Encode<'q, Self::Database>
            + sqlx_core::types::Type<Self::Database>,
    {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }
}

pub struct DuckDBArgumentBuffer<'q>;
