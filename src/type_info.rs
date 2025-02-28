use std::fmt::Display;

use duckdb::arrow::datatypes::ArrowNativeType;
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

impl From<duckdb::arrow::datatypes::DataType> for DuckdbDBTypeInfo {
    fn from(value: duckdb::arrow::datatypes::DataType) -> Self {
        use duckdb::arrow::datatypes::DataType;
        Self {
            type_name: value.to_string(),
            value_kind: match value {
                DataType::Null => DuckDBValueKind::Null,
                DataType::Boolean => DuckDBValueKind::Boolean(false),
                DataType::Int8 => todo!(),
                DataType::Int16 => todo!(),
                DataType::Int32 => todo!(),
                DataType::Int64 => todo!(),
                DataType::UInt8 => todo!(),
                DataType::UInt16 => todo!(),
                DataType::UInt32 => todo!(),
                DataType::UInt64 => todo!(),
                DataType::Float16 => todo!(),
                DataType::Float32 => todo!(),
                DataType::Float64 => todo!(),
                DataType::Timestamp(time_unit, _) => todo!(),
                DataType::Date32 => todo!(),
                DataType::Date64 => todo!(),
                DataType::Time32(time_unit) => todo!(),
                DataType::Time64(time_unit) => todo!(),
                DataType::Duration(time_unit) => todo!(),
                DataType::Interval(interval_unit) => todo!(),
                DataType::Binary => todo!(),
                DataType::FixedSizeBinary(_) => todo!(),
                DataType::LargeBinary => todo!(),
                DataType::BinaryView => todo!(),
                DataType::Utf8 => todo!(),
                DataType::LargeUtf8 => todo!(),
                DataType::Utf8View => todo!(),
                DataType::List(field) => todo!(),
                DataType::ListView(field) => todo!(),
                DataType::FixedSizeList(field, _) => todo!(),
                DataType::LargeList(field) => todo!(),
                DataType::LargeListView(field) => todo!(),
                DataType::Struct(fields) => todo!(),
                DataType::Union(union_fields, union_mode) => todo!(),
                DataType::Dictionary(data_type, data_type1) => todo!(),
                DataType::Decimal128(_, _) => todo!(),
                DataType::Decimal256(_, _) => todo!(),
                DataType::Map(field, _) => todo!(),
                DataType::RunEndEncoded(field, field1) => todo!(),
            },
        }
    }
}

impl From<DuckdbDBTypeInfo> for DuckDBValueKind {
    fn from(type_info: DuckdbDBTypeInfo) -> Self {
        type_info.value_kind
    }
}

impl duckdb::ToSql for DuckDBValueKind {
    fn to_sql(&self) -> duckdb::Result<duckdb::types::ToSqlOutput<'_>> {
        use duckdb::types::*;
        match self {
            DuckDBValueKind::Null => Null.to_sql(),
            DuckDBValueKind::Varchar(v) => v.to_sql(),
            DuckDBValueKind::Boolean(v) => v.to_sql(),
            DuckDBValueKind::Int8(v) => v.to_sql(),
            DuckDBValueKind::Int16(v) => v.to_sql(),
            DuckDBValueKind::Int32(v) => v.to_sql(),
            DuckDBValueKind::Int64(v) => v.to_sql(),
            DuckDBValueKind::Int128(v) => v.to_sql(),
            DuckDBValueKind::UInt8(v) => v.to_sql(),
            DuckDBValueKind::UInt16(v) => v.to_sql(),
            DuckDBValueKind::UInt32(v) => v.to_sql(),
            DuckDBValueKind::UInt64(v) => v.to_sql(),
            // TODO: replace with v.to_sql() when u128 has ToSql implemented
            DuckDBValueKind::UInt128(v) => Ok(ToSqlOutput::Owned(Value::HugeInt(*v as i128))),
            DuckDBValueKind::Float32(v) => v.to_sql(),
            DuckDBValueKind::Float64(v) => v.to_sql(),
            DuckDBValueKind::Decimal(_, _, decimal) => todo!(),
            DuckDBValueKind::Blob(items) => todo!(),
        }
    }
}
