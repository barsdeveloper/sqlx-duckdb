#[derive(Debug)]
pub struct AccessMode(pub duckdb::AccessMode);
impl Clone for AccessMode {
    fn clone(&self) -> Self {
        Self(match self.0 {
            duckdb::AccessMode::Automatic => duckdb::AccessMode::Automatic,
            duckdb::AccessMode::ReadOnly => duckdb::AccessMode::ReadOnly,
            duckdb::AccessMode::ReadWrite => duckdb::AccessMode::ReadWrite,
        })
    }
}
impl From<duckdb::AccessMode> for AccessMode {
    fn from(value: duckdb::AccessMode) -> Self {
        Self(value)
    }
}
impl From<&AccessMode> for duckdb::AccessMode {
    fn from(value: &AccessMode) -> Self {
        value.clone().0
    }
}

#[derive(Debug)]
pub struct DefaultOrder(pub duckdb::DefaultOrder);
impl Clone for DefaultOrder {
    fn clone(&self) -> Self {
        Self(match self.0 {
            duckdb::DefaultOrder::Asc => duckdb::DefaultOrder::Asc,
            duckdb::DefaultOrder::Desc => duckdb::DefaultOrder::Desc,
        })
    }
}
impl From<duckdb::DefaultOrder> for DefaultOrder {
    fn from(value: duckdb::DefaultOrder) -> Self {
        Self(value)
    }
}
impl From<&DefaultOrder> for duckdb::DefaultOrder {
    fn from(value: &DefaultOrder) -> Self {
        value.clone().0
    }
}

#[derive(Debug)]
pub struct DefaultNullOrder(pub duckdb::DefaultNullOrder);
impl Clone for DefaultNullOrder {
    fn clone(&self) -> Self {
        Self(match self.0 {
            duckdb::DefaultNullOrder::NullsFirst => duckdb::DefaultNullOrder::NullsFirst,
            duckdb::DefaultNullOrder::NullsLast => duckdb::DefaultNullOrder::NullsLast,
        })
    }
}
impl From<duckdb::DefaultNullOrder> for DefaultNullOrder {
    fn from(value: duckdb::DefaultNullOrder) -> Self {
        Self(value)
    }
}
impl From<&DefaultNullOrder> for duckdb::DefaultNullOrder {
    fn from(value: &DefaultNullOrder) -> Self {
        value.clone().0
    }
}
