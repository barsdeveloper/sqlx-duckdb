use crate::{error::DuckDBError, Interval};
use libduckdb_sys::duckdb_interval;
use rust_decimal::Decimal;
use sqlx_core::{
    ext::ustr::UStr,
    type_info::TypeInfo,
    types::{
        time::{Date, OffsetDateTime, PrimitiveDateTime, Time},
        JsonValue,
    },
    HashMap, Result,
};
use std::fmt::Display;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) enum DuckDBField {
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Int128(Option<i128>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    UInt128(Option<u128>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Decimal(Option<Decimal>, /* prec: */ u8, /* scale: */ u8),
    Varchar(Option<String>),
    Blob(Option<Box<[u8]>>),
    Date(Option<Date>),
    Time(Option<Time>),
    Timestamp(Option<PrimitiveDateTime>),
    TimestampWithTimezone(Option<OffsetDateTime>),
    Interval(Option<Interval>),
    Uuid(Option<Uuid>),
    Json(Option<JsonValue>),
    Array(
        Option<Box<[DuckDBField]>>,
        /* type: */ Box<DuckDBField>,
        /* len: */ u8,
    ),
    List(Option<Vec<DuckDBField>>, /* type: */ Box<DuckDBField>),
    Map(
        Option<HashMap<DuckDBField, DuckDBField>>,
        /* key: */ Box<DuckDBField>,
        /* value: */ Box<DuckDBField>,
    ),
}

/// Type information for a SQLite type.
#[derive(Debug, Clone)]
pub struct DuckdbDBTypeInfo {
    type_name: UStr,
    field: DuckDBField,
}

impl DuckdbDBTypeInfo {
    fn type_name(data_type: &DuckDBField) -> Result<UStr> {
        let result = match data_type {
            DuckDBField::Null => {
                return Err(DuckDBError::new(
                    "The field is null and doesn't contain information about the type".into(),
                )
                .into());
            }
            DuckDBField::Boolean(..) => "BOOLEAN".into(),
            DuckDBField::Int8(..) => "TINYINT".into(),
            DuckDBField::Int16(..) => "SMALLINT".into(),
            DuckDBField::Int32(..) => "INTEGER".into(),
            DuckDBField::Int64(..) => "BIGINT".into(),
            DuckDBField::Int128(..) => "HUGEINT".into(),
            DuckDBField::UInt8(..) => "UTINYINT".into(),
            DuckDBField::UInt16(..) => "USMALLINT".into(),
            DuckDBField::UInt32(..) => "UINTEGER".into(),
            DuckDBField::UInt64(..) => "UBIGINT".into(),
            DuckDBField::UInt128(..) => "UHUGEINT".into(),
            DuckDBField::Float32(..) => "FLOAT".into(),
            DuckDBField::Float64(..) => "DOUBLE".into(),
            DuckDBField::Decimal(_, prec, scale) => format!("DECIMAL({}, {})", prec, scale).into(),
            DuckDBField::Varchar(..) => "VARCHAR".into(),
            DuckDBField::Blob(..) => "BLOB".into(),
            DuckDBField::Date(..) => "DATE".into(),
            DuckDBField::Time(..) => "TIME".into(),
            DuckDBField::Timestamp(..) => "TIMESTAMP".into(),
            DuckDBField::TimestampWithTimezone(..) => "TIMESTAMP WITH TIME ZONE".into(),
            DuckDBField::Interval(..) => "INTERVAL".into(),
            DuckDBField::Uuid(..) => "UUID".into(),
            DuckDBField::Json(..) => "JSON".into(),
            DuckDBField::Array(.., t, l) => format!("{}[{}]", Self::type_name(t)?, l).into(),
            DuckDBField::List(.., data_type) => format!("{}[]", Self::type_name(data_type)?).into(),
            DuckDBField::Map(.., k, v) => {
                format!("MAP({}, {})", Self::type_name(k)?, Self::type_name(v)?).into()
            }
        };
        Ok(result)
    }

    pub fn new(field: DuckDBField) -> Self {
        Self {
            type_name: Self::type_name(&field).unwrap_or("".into()),
            field,
        }
    }
}

impl PartialEq for DuckdbDBTypeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.type_name == other.type_name
    }
}

impl Display for DuckdbDBTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(self.name())
    }
}

impl TypeInfo for DuckdbDBTypeInfo {
    fn is_null(&self) -> bool {
        type F = DuckDBField;
        matches!(
            self.field,
            F::Null
                | F::Boolean(None, ..)
                | F::Int8(None, ..)
                | F::Int16(None, ..)
                | F::Int32(None, ..)
                | F::Int64(None, ..)
                | F::Int128(None, ..)
                | F::UInt8(None, ..)
                | F::UInt16(None, ..)
                | F::UInt32(None, ..)
                | F::UInt64(None, ..)
                | F::UInt128(None, ..)
                | F::Float32(None, ..)
                | F::Float64(None, ..)
                | F::Decimal(None, ..)
                | F::Varchar(None, ..)
                | F::Blob(None, ..)
                | F::Date(None, ..)
                | F::Time(None, ..)
                | F::Timestamp(None, ..)
                | F::TimestampWithTimezone(None, ..)
                | F::Interval(None, ..)
                | F::Uuid(None, ..)
                | F::Json(None, ..)
                | F::Array(None, ..)
                | F::List(None, ..)
                | F::Map(None, ..)
        )
    }

    fn name(&self) -> &str {
        &self.type_name
    }
}

impl From<DuckDBField> for DuckdbDBTypeInfo {
    fn from(field: DuckDBField) -> Self {
        Self::new(field)
    }
}

impl From<DuckdbDBTypeInfo> for DuckDBField {
    fn from(value: DuckdbDBTypeInfo) -> Self {
        value.field
    }
}
