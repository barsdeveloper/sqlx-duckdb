use crate::{database::DuckDB, type_info::DuckdbDBTypeInfo};
use sqlx_core::{
    type_info::TypeInfo,
    value::{Value, ValueRef},
};
use std::borrow::Cow;

pub struct DuckDBValue {
    type_info: DuckdbDBTypeInfo,
}

pub struct DuckDBValueRef<'_> {}

impl Value for DuckDBValue {
    type Database = DuckDB;

    fn as_ref(&self) -> DuckDBValueRef<'_> {
        todo!()
    }

    fn type_info(&self) -> Cow<'_, DuckdbDBTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    fn is_null(&self) -> bool {
        self.type_info.is_null()
    }
}

impl<'a> ValueRef<'a> for DuckDBValueRef<'a> {
    type Database = DuckDB;

    fn to_owned(&self) -> DuckDBValue {
        todo!()
    }

    fn type_info(&self) -> Cow<'_, DuckdbDBTypeInfo> {
        todo!()
    }

    fn is_null(&self) -> bool {
        todo!()
    }
}
