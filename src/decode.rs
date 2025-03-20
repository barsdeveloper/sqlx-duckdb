use std::error::Error;

use crate::type_info::{DuckDBField, DuckdbDBTypeInfo};
use crate::{database::DuckDB, value::DuckDBValueRef};
use sqlx_core::decode::Decode;
use sqlx_core::types::Type;
use sqlx_core::value::ValueRef;

macro_rules! impl_trait {
    ($duckdb_variant:path, $rust_type:path, Type) => {
        impl Type<DuckDB> for $rust_type {
            fn type_info() -> DuckdbDBTypeInfo {
                DuckdbDBTypeInfo::new($duckdb_variant(None))
            }
        }
    };
    ($duckdb_variant:path, $rust_type:path, Decode) => {
        impl<'r> Decode<'r, DuckDB> for $rust_type {
            fn decode(value: DuckDBValueRef<'r>) -> Result<Self, Box<dyn Error + Send + Sync>> {
                if let $duckdb_variant(Some(ref value), ..) = value.type_info().as_ref().field {
                    Ok(value.clone())
                } else {
                    let error =
                        if matches!(value.type_info().as_ref().field, $duckdb_variant(None, ..)) {
                            "Tried to extract a value from a null field".to_string()
                        } else {
                            format!(
                                "Cannot decode {} into {:?}",
                                stringify!($duckdb_variant),
                                value.type_info().as_ref().field,
                            )
                        };
                    Err(error.into())
                }
            }
        }
    };
    ($duckdb_variant:path, $rust_type:path) => {
        impl_trait!($duckdb_variant, $rust_type, Type);
        impl_trait!($duckdb_variant, $rust_type, Decode);
    };
}

impl_trait!(DuckDBField::Boolean, bool);
impl_trait!(DuckDBField::Int8, i8);
impl_trait!(DuckDBField::Int16, i16);
impl_trait!(DuckDBField::Int32, i32);
impl_trait!(DuckDBField::Int64, i64);
impl_trait!(DuckDBField::Int128, i128);
impl_trait!(DuckDBField::UInt8, u8);
impl_trait!(DuckDBField::UInt16, u16);
impl_trait!(DuckDBField::UInt32, u32);
impl_trait!(DuckDBField::UInt64, u64);
impl_trait!(DuckDBField::UInt128, u128);
impl_trait!(DuckDBField::Float32, f32);
impl_trait!(DuckDBField::Float64, f64);
impl_trait!(DuckDBField::Decimal, ::rust_decimal::Decimal, Decode);
impl_trait!(DuckDBField::Varchar, String);
impl_trait!(DuckDBField::Blob, Box<[u8]>);
#[cfg(feature = "time")]
impl_trait!(DuckDBField::Date, ::time::Date);
#[cfg(feature = "time")]
impl_trait!(DuckDBField::Time, ::time::Time);
#[cfg(feature = "time")]
impl_trait!(DuckDBField::Timestamp, ::time::PrimitiveDateTime);
#[cfg(feature = "time")]
impl_trait!(DuckDBField::TimestampWithTimezone, ::time::OffsetDateTime);
impl_trait!(DuckDBField::Interval, crate::interval::Interval);

impl Type<DuckDB> for rust_decimal::Decimal {
    fn type_info() -> DuckdbDBTypeInfo {
        DuckdbDBTypeInfo::new(DuckDBField::Decimal(None, 0, 0))
    }
}
