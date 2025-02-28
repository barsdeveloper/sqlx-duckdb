use crate::{
    connection::DuckDBConnection,
    duckdb_enums::{AccessMode, DefaultNullOrder, DefaultOrder},
};
use futures_core::future::BoxFuture;
use log::LevelFilter;
use percent_encoding::percent_decode_str;
use sqlx_core::{any::AnyConnectOptions, connection::ConnectOptions, url, Error};
use std::{
    borrow::Cow,
    path::{Path, PathBuf},
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
};

static IN_MEMORY_DB_SEQ: AtomicUsize = AtomicUsize::new(0);

#[derive(Default, Clone, Debug)]
pub struct DuckDBConnectOptions {
    pub(crate) path: Cow<'static, Path>,
    pub(crate) in_memory: bool,
    pub(crate) create_if_missing: bool,
    pub(crate) mode: Option<AccessMode>,
    pub(crate) default_order: Option<DefaultOrder>,
    pub(crate) default_null_order: Option<DefaultNullOrder>,
    pub(crate) external_access: Option<bool>,
    pub(crate) unsigned_extensions: Option<bool>,
    pub(crate) max_memory: String,
    pub(crate) threads: Option<i64>,
    // pub(crate) shared_cache: bool,
    // pub(crate) statement_cache_capacity: usize,
    // pub(crate) busy_timeout: Duration,
    // // pub(crate) log_settings: LogSettings,
    // pub(crate) immutable: bool,
    // pub(crate) vfs: Option<Cow<'static, str>>,

    // pub(crate) pragmas: IndexMap<Cow<'static, str>, Option<Cow<'static, str>>>,
    // /// Extensions are specified as a pair of \<Extension Name : Optional Entry Point>, the majority
    // /// of SQLite extensions will use the default entry points specified in the docs, these should
    // /// be added to the map with a `None` value.
    // /// <https://www.sqlite.org/loadext.html#loading_an_extension>
    // pub(crate) extensions: IndexMap<Cow<'static, str>, Option<Cow<'static, str>>>,

    // pub(crate) command_channel_size: usize,
    // pub(crate) row_channel_size: usize,

    // pub(crate) collations: Vec<Collation>,

    // pub(crate) serialized: bool,
    // pub(crate) thread_name: Arc<DebugFn<dyn Fn(u64) -> String + Send + Sync + 'static>>,

    // pub(crate) optimize_on_close: OptimizeOnClose,
    #[cfg(feature = "regexp")]
    pub(crate) register_regexp_function: bool,
}

impl DuckDBConnectOptions {
    pub fn new(url: &str) -> Result<Self, Error> {
        if !url.starts_with("duckdb:") {
            return Err(Error::Configuration(
                "Expected duckdb connection url to start with \"duckdb:\"".into(),
            ));
        }
        let mut url = url
            .trim_start_matches("duckdb:")
            .trim_start_matches("//")
            .splitn(2, "?");
        let database = url.next().ok_or(Error::Configuration(
            "Expected database reference in the connection string".into(),
        ))?;
        let params = url.next();

        let mut options = Self::default();
        if database == ":memory:" {
            options.in_memory = true;
            let seqno = IN_MEMORY_DB_SEQ.fetch_add(1, Ordering::Relaxed);
            options.path = Cow::Owned(PathBuf::from(format!("file:duckdb-in-memory-{seqno}")));
        } else {
            options.path = Cow::Owned(
                Path::new(
                    &*percent_decode_str(database)
                        .decode_utf8()
                        .map_err(Error::config)?,
                )
                .to_path_buf(),
            )
        }
        if let Some(params) = params {
            for (key, value) in url::form_urlencoded::parse(params.as_bytes()) {
                use duckdb::AccessMode;
                use duckdb::DefaultNullOrder;
                use duckdb::DefaultOrder;
                match &*key {
                    "mode" => match &*value {
                        "ro" => options.mode = Some(AccessMode::ReadOnly.into()),
                        "rw" => options.mode = Some(AccessMode::ReadWrite.into()),
                        "rwc" => options.mode = Some(AccessMode::ReadWrite.into()),
                        "memory" => options.in_memory = true,
                        _ => {
                            return Err(Error::Configuration(
                                format!("Unknown value {value:?} for `mode`, expected one of: `ro`, `rw`, `rwc`, `memory`").into(),
                            ));
                        }
                    },
                    "defaultOrder" => {
                        options.default_order = Some(match &*value {
                            "asc" => DefaultOrder::Asc,
                            "desc" => DefaultOrder::Desc,
                            _ => {
                                return Err(Error::Configuration(
                                format!("Unknown value {value:?} for `defaultOrder`, expected one of: `asc`, `desc`").into(),
                            ));
                            }
                        }.into())
                    }
                    "defaultNullOrder" => {
                        options.default_null_order = Some(match &*value {
                            "first" => DefaultNullOrder::NullsFirst,
                            "last" => DefaultNullOrder::NullsLast,
                            _ => {
                                return Err(Error::Configuration(
                                format!("Unknown value {value:?} for `defaultNullOrder`, expected one of: `first`, `last`").into(),
                            ));
                            }
                        }.into())
                    }
                    "externalAccess" => {
                        options.external_access = Some(match &*value {
                            "true" => true,
                            "false" => false,
                            _ => {
                                return Err(Error::Configuration(
                                            format!("Unknown value {value:?} for `externalAccess`, expected one of: `true`, `false`").into(),
                                        ));
                            }
                        })
                    }
                    "unsignedExtensions" => {
                        options.unsigned_extensions = Some(match &*value {
                            "true" => true,
                            "false" => false,
                            _ => {
                                return Err(Error::Configuration(
                                format!("Unknown value {value:?} for `unsignedExtensions`, expected one of: `true`, `false`").into(),
                            ));
                            }
                        })
                    }
                    "maxMemory" => options.max_memory = value.into(),
                    "threads" => {
                        options.threads = Some(value.parse::<i64>().map_err(|e| {
                            Error::Configuration(
                                format!(
                                    "Unexpected value {value:?} for `threads`, error: {}",
                                    e.to_string()
                                )
                                .into(),
                            )
                        })?)
                    }
                    _ => {
                        return Err(Error::Configuration(
                            format!("Unexpected key {key:?}").into(),
                        ))
                    }
                }
            }
        }
        Ok(options)
    }
}

impl ConnectOptions for DuckDBConnectOptions {
    type Connection = DuckDBConnection;

    fn from_url(url: &sqlx_core::Url) -> Result<Self, sqlx_core::Error> {
        Self::new(url.as_str())
    }

    fn connect(&self) -> BoxFuture<'_, Result<Self::Connection, sqlx_core::Error>>
    where
        Self::Connection: Sized,
    {
        todo!()
    }

    fn log_statements(self, level: LevelFilter) -> Self {
        todo!()
    }

    fn log_slow_statements(self, level: LevelFilter, duration: std::time::Duration) -> Self {
        todo!()
    }
}

impl FromStr for DuckDBConnectOptions {
    type Err = Error;
    fn from_str(url: &str) -> Result<Self, Self::Err> {
        Self::new(url)
    }
}

impl TryFrom<&AnyConnectOptions> for DuckDBConnectOptions {
    type Error = sqlx_core::Error;

    fn try_from(value: &AnyConnectOptions) -> Result<Self, Self::Error> {
        let result = Self::from_url(&value.database_url)?;
        Ok(result)
    }
}

impl From<&DuckDBConnectOptions> for duckdb::Config {
    fn from(options: &DuckDBConnectOptions) -> Self {
        let mut result = Self::default();
        if options.mode.is_some() {
            result = result
                .access_mode(options.mode.as_ref().unwrap().into())
                .unwrap();
        }
        if options.default_order.is_some() {
            result = result
                .default_order(options.default_order.as_ref().unwrap().into())
                .unwrap();
        }
        if options.default_null_order.is_some() {
            result = result
                .default_null_order(options.default_null_order.as_ref().unwrap().into())
                .unwrap();
        }
        if options.external_access.is_some() {
            result = result
                .enable_external_access(options.external_access.unwrap())
                .unwrap();
        }
        if matches!(options.unsigned_extensions, Some(true)) {
            result = result.allow_unsigned_extensions().unwrap();
        }
        if !options.max_memory.is_empty() {
            result = result.max_memory(&options.max_memory).unwrap();
        }
        if options.threads.is_some() {
            result = result.threads(options.threads.unwrap()).unwrap();
        }
        result
    }
}
