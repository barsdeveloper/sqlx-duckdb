use std::{future::Future, pin::Pin};

mod arguments;
mod column;
mod connection;
mod database;
mod duckdb_enums;
mod error;
mod options;
mod query_result;
mod row;
mod statement;
mod transaction;
mod type_info;
mod value;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
