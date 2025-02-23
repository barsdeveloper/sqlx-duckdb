use std::fmt::Display;

use rust_decimal::Decimal;
use sqlx_core::type_info::TypeInfo;

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
pub(crate) enum DuckDBValueKind {
    Null,
    Varchar(String),
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    Float32(f32),
    Float64(f64),
    Decimal(u8, u8, Decimal),
    Blob(Box<[u8]>),
    // Date(),
    // Time,
    // Timestamp,
    // TimestampWithTimezone,
    // Interval,
    // Uuid,
    // Json,
    // Array(Box<DataType>, u8),
    // List(Box<DataType>),
    // Map(Box<DataType>, Box<DataType>),
}

/// Type information for a SQLite type.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
pub struct DuckdbDBTypeInfo {
    type_name: String,
    value_kind: DuckDBValueKind,
}

impl DuckdbDBTypeInfo {
    fn new(data_type: DuckDBValueKind) -> Self {
        Self {
            type_name: Self::type_name(&data_type),
            value_kind: data_type,
        }
    }

    fn type_name(data_type: &DuckDBValueKind) -> String {
        match data_type {
            DuckDBValueKind::Null | DuckDBValueKind::Varchar(_) => "VARCHAR".into(),
            DuckDBValueKind::Boolean(_) => "BOOLEAN".into(),
            DuckDBValueKind::Int8(_) => "TINYINT".into(),
            DuckDBValueKind::Int16(_) => "SMALLINT".into(),
            DuckDBValueKind::Int32(_) => "INTEGER".into(),
            DuckDBValueKind::Int64(_) => "BIGINT".into(),
            DuckDBValueKind::Int128(_) => "HUGEINT".into(),
            DuckDBValueKind::UInt8(_) => "UTINYINT".into(),
            DuckDBValueKind::UInt16(_) => "USMALLINT".into(),
            DuckDBValueKind::UInt32(_) => "UINTEGER".into(),
            DuckDBValueKind::UInt64(_) => "UBIGINT".into(),
            DuckDBValueKind::UInt128(_) => "UHUGEINT".into(),
            DuckDBValueKind::Float32(_) => "FLOAT".into(),
            DuckDBValueKind::Float64(_) => "DOUBLE".into(),
            DuckDBValueKind::Decimal(precision, scale, _) => {
                format!("DECIMAL({},{})", precision, scale)
            }
            DuckDBValueKind::Blob(_) => "BLOB".into(),
            // DataType::Date => "DATE".into(),
            // DataType::Time => "TIME".into(),
            // DataType::Timestamp => "TIMESTAMP".into(),
            // DataType::TimestampWithTimezone => "TIMESTAMP WITH TIME ZONE".into(),
            // DataType::Interval => "INTERVAL".into(),
            // DataType::Uuid => "UUID".into(),
            // DataType::Json => "JSON".into(),
            // DataType::Array(data_type, len) => format!("{}[{}]", Self::type_name(data_type), len),
            // DataType::List(data_type) => format!("{}[]", Self::type_name(data_type)),
            // DataType::Map(k, v) => format!("MAP({},{})", Self::type_name(k), Self::type_name(v)),
        }
    }
}

impl Display for DuckdbDBTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(self.name())
    }
}

impl TypeInfo for DuckdbDBTypeInfo {
    fn is_null(&self) -> bool {
        matches!(self.value_kind, DuckDBValueKind::Null)
    }

    fn name(&self) -> &str {
        &self.type_name
    }
}
