use crate::{error::DuckDBError, type_info::DuckDBField};
use libduckdb_sys::*;
use rust_decimal::Decimal;
use sqlx_core::{Result, types::time};
use std::{ffi::c_void, ptr, slice};

pub(crate) fn convert_date(date: duckdb_date_struct) -> Result<time::Date> {
    time::Date::from_calendar_date(
        date.year,
        (date.month as u8).try_into().unwrap(),
        date.day as u8,
    )
    .map_err(|e| {
        DuckDBError::from_source_message(e.into(), "Unexpected error while creating a date".into())
            .into()
    })
}

pub(crate) fn convert_time(time: duckdb_time_struct) -> Result<time::Time> {
    time::Time::from_hms_micro(
        time.hour as u8,
        time.min as u8,
        time.sec as u8,
        time.micros as u32,
    )
    .map_err(|e| {
        DuckDBError::from_source_message(e.into(), "Unexpected error while creating a time".into())
            .into()
    })
}

pub(crate) fn extract_value(
    vector: duckdb_vector,
    row: usize,
    logical_type: duckdb_logical_type,
    type_id: u32,
    data: *const c_void,
    validity: *mut u64,
) -> Result<DuckDBField> {
    unsafe {
        let is_valid = !data.is_null() && duckdb_validity_row_is_valid(validity, row as u64);
        type K = DuckDBField;
        let result = match type_id {
            DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => K::Boolean(if is_valid {
                Some(*(data as *const bool).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => K::Int8(if is_valid {
                Some(*(data as *const i8).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => K::Int16(if is_valid {
                Some(*(data as *const i16).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => K::Int32(if is_valid {
                Some(*(data as *const i32).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => K::Int64(if is_valid {
                Some(*(data as *const i64).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => K::UInt8(if is_valid {
                Some(*(data as *const u8).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => K::UInt16(if is_valid {
                Some(*(data as *const u16).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => K::UInt32(if is_valid {
                Some(*(data as *const u32).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => K::UInt64(if is_valid {
                Some(*(data as *const u64).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => K::Int128(if is_valid {
                let data = *(data as *const duckdb_hugeint).add(row);
                Some((data.upper as i128) << 64 | data.lower as i128)
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => K::UInt128(if is_valid {
                let data = *(data as *const duckdb_hugeint).add(row);
                Some((data.upper as u128) << 64 | data.lower as u128)
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => K::Float32(if is_valid {
                Some(*(data as *const f32).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => K::Float64(if is_valid {
                Some(*(data as *const f64).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => K::Timestamp(if is_valid {
                let data = *(data as *const duckdb_timestamp).add(row);
                let date_time =
                    time::OffsetDateTime::from_unix_timestamp_nanos(data.micros as i128 * 1000)
                        .map_err(|e| {
                            DuckDBError::from_source_message(
                                e.into(),
                                "Unexpected error while creating a timestamp".into(),
                            )
                        })?;
                Some(time::PrimitiveDateTime::new(
                    date_time.date(),
                    date_time.time(),
                ))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_DATE => K::Date(if is_valid {
                Some(convert_date(duckdb_from_date(
                    *(data as *const duckdb_date).add(row),
                ))?)
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_TIME => K::Time(if is_valid {
                Some(convert_time(duckdb_from_time(
                    *(data as *const duckdb_time).add(row),
                ))?)
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => K::Interval(if is_valid {
                Some((*(data as *const duckdb_interval).add(row)).into())
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR | DUCKDB_TYPE_DUCKDB_TYPE_BLOB => {
                let value = if is_valid {
                    let data = *(data as *const duckdb_string_t).add(row);
                    let parts = if duckdb_string_is_inlined(data) {
                        (
                            &data.value.inlined.inlined as *const i8,
                            data.value.inlined.length,
                        )
                    } else {
                        (
                            data.value.pointer.ptr as *const i8,
                            data.value.pointer.length,
                        )
                    };
                    Some(slice::from_raw_parts(
                        parts.0 as *const u8,
                        parts.1 as usize,
                    ))
                } else {
                    None
                };
                if type_id == DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR {
                    K::Varchar(value.map(|v| String::from_utf8_unchecked(v.into())))
                } else {
                    K::Blob(value.map(|v| v.into()))
                }
            }
            DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
                let width = duckdb_decimal_width(logical_type);
                let scale = duckdb_decimal_scale(logical_type);
                K::Decimal(
                    if is_valid {
                        let num = match duckdb_decimal_internal_type(logical_type) {
                            DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => {
                                *(data as *const i16).add(row) as i128
                            }
                            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => {
                                *(data as *const i32).add(row) as i128
                            }
                            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => {
                                *(data as *const i64).add(row) as i128
                            }
                            DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => {
                                *(data as *const i128).add(row) as i128
                            }
                            _ => {
                                return Err(DuckDBError::new(
                                    "Invalid internal decimal storage type".into(),
                                )
                                .into());
                            }
                        };
                        Some(Decimal::from_i128_with_scale(num, scale as u32))
                    } else {
                        None
                    },
                    width,
                    scale,
                )
            }
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => K::Timestamp(if is_valid {
                let data = duckdb_from_timestamp(*(data as *const duckdb_timestamp).add(row));
                data.time;
                Some(time::PrimitiveDateTime::new(
                    convert_date(data.date)?,
                    convert_time(data.time)?,
                ))
            } else {
                None
            }),
            //  DUCKDB_TYPE_DUCKDB_TYPE_ENUM =>
            DUCKDB_TYPE_DUCKDB_TYPE_ARRAY | DUCKDB_TYPE_DUCKDB_TYPE_LIST => {
                let vector = if type_id == DUCKDB_TYPE_DUCKDB_TYPE_ARRAY {
                    duckdb_array_vector_get_child(vector)
                } else {
                    duckdb_list_vector_get_child(vector)
                };
                let logical_type = duckdb_vector_get_column_type(vector);
                let type_id = duckdb_get_type_id(logical_type);
                let value = if is_valid {
                    let validity = duckdb_vector_get_validity(vector);
                    let range = if type_id == DUCKDB_TYPE_DUCKDB_TYPE_ARRAY {
                        let size = duckdb_array_type_array_size(logical_type) as usize;
                        (row * size)..(row * size + size)
                    } else {
                        let list_info = *(data as *const duckdb_list_entry).add(row);
                        (list_info.offset as usize)
                            ..((list_info.offset + list_info.length) as usize)
                    };
                    Some(
                        range
                            .map(|i| {
                                Ok(extract_value(
                                    vector,
                                    i,
                                    logical_type,
                                    type_id,
                                    data,
                                    validity,
                                )?)
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )
                } else {
                    None
                };
                let values_type = extract_value(
                    vector,
                    0,
                    logical_type,
                    type_id,
                    ptr::null(),
                    ptr::null_mut(),
                )?;
                K::List(value, values_type.into())
            }
            //  DUCKDB_TYPE_DUCKDB_TYPE_STRUCT =>
            //  DUCKDB_TYPE_DUCKDB_TYPE_MAP =>
            //  DUCKDB_TYPE_DUCKDB_TYPE_UUID =>
            //  DUCKDB_TYPE_DUCKDB_TYPE_UNION =>
            //  DUCKDB_TYPE_DUCKDB_TYPE_BIT =>
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => {
                let date_time = duckdb_from_timestamp(*(data as *const duckdb_timestamp).add(row));
                K::Timestamp(if is_valid {
                    Some(time::PrimitiveDateTime::new(
                        convert_date(date_time.date)?,
                        convert_time(date_time.time)?,
                    ))
                } else {
                    None
                })
            }
            //  DUCKDB_TYPE_DUCKDB_TYPE_ANY =>
            //  DUCKDB_TYPE_DUCKDB_TYPE_VARINT =>
            DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => K::Null,
            _ => {
                return Err(DuckDBError::new(
                    format!(
                        "Invalid type value: {}, must be one of the expected DUCKDB_TYPE_DUCKDB_TYPE_* variant",
                        type_id
                    )
                    .into(),
                )
                .into());
            }
        };
        Ok(result)
    }
}
