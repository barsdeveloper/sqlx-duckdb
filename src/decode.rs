use std::error::Error;

use crate::type_info::DuckDBField;
use crate::{database::DuckDB, value::DuckDBValueRef};
use sqlx_core::decode::Decode;
use sqlx_core::value::ValueRef;

macro_rules! impl_decode {
    ($duckdb_variant:path, $rust_type:path) => {
        impl<'r> Decode<'r, DuckDB> for $rust_type {
            fn decode(value: DuckDBValueRef<'r>) -> Result<Self, Box<dyn Error + Send + Sync>> {
                if let $duckdb_variant(Some(value), ..) = value.type_info().as_ref().field {
                    Ok(value)
                } else {
                    let error =
                        if matches!(value.type_info().as_ref().field, $duckdb_variant(None, ..)) {
                            "Null value provided, expected a value".to_string()
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
}

impl_decode!(DuckDBField::Boolean, bool);
impl_decode!(DuckDBField::Int8, i8);
impl_decode!(DuckDBField::Int16, i16);
impl_decode!(DuckDBField::Int32, i32);
impl_decode!(DuckDBField::Int64, i64);
impl_decode!(DuckDBField::Int128, i128);
impl_decode!(DuckDBField::UInt8, u8);
impl_decode!(DuckDBField::UInt16, u16);
impl_decode!(DuckDBField::UInt32, u32);
impl_decode!(DuckDBField::UInt64, u64);
impl_decode!(DuckDBField::UInt128, u128);
impl_decode!(DuckDBField::Float32, f32);
impl_decode!(DuckDBField::Float64, f64);
impl_decode!(DuckDBField::Decimal, rust_decimal::Decimal);
impl_decode!(DuckDBField::Varchar, String);
impl_decode!(DuckDBField::Blob, Box<[u8]>);
impl_decode!(DuckDBField::Date, time::Date);
impl_decode!(DuckDBField::Time, time::Time);
impl_decode!(DuckDBField::Timestamp, time::PrimitiveDateTime);
impl_decode!(DuckDBField::TimestampWithTimezone, time::OffsetDateTime);
impl_decode!(DuckDBField::Interval, crate::interval::Interval);
