use crate::{database::DuckDB, type_info::DuckDBField};
use sqlx_core::{
    arguments::Arguments, encode::Encode, error::BoxDynError, impl_into_arguments_for_arguments,
    types::Type,
};

#[derive(Default)]
pub struct DuckDBArguments {
    pub(crate) values: Vec<DuckDBField>,
}

impl DuckDBArguments {}

impl<'q> Arguments<'q> for DuckDBArguments {
    type Database = DuckDB;

    fn reserve(&mut self, additional: usize, size: usize) {
        self.values.reserve(size);
    }

    fn add<T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: 'q + Encode<'q, Self::Database> + Type<Self::Database>,
    {
        let type_info = value.produces().unwrap_or_else(T::type_info);
        self.values.push(type_info.into());
        Ok(())
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

pub struct DuckDBArgumentBuffer;

impl_into_arguments_for_arguments!(DuckDBArguments);
