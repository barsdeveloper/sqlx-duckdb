use crate::{cbox::CBox, connection::DuckDBConnection};
use futures::future::BoxFuture;
use libduckdb_sys::{
    duckdb_config, duckdb_create_config, duckdb_destroy_config, duckdb_set_config,
    duckdb_state_DuckDBSuccess,
};
use log::LevelFilter;
use percent_encoding::percent_decode_str;
use sqlx_core::{Error, Result, connection::ConnectOptions, url};
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_char},
    ops::{Deref, DerefMut},
    ptr,
    str::FromStr,
};

#[derive(Debug, Clone, Copy)]
pub enum AccessMode {
    ReadOnly,
    ReadWrite,
}

impl TryFrom<&str> for AccessMode {
    type Error = sqlx_core::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match &*value {
            "ro" => Ok(AccessMode::ReadOnly),
            "rw" => Ok(AccessMode::ReadWrite),
            _ => {
                return Err(Error::Configuration(
                    format!("Unknown value {value:?} for `mode`, expected one of: `ro`, `rw`")
                        .into(),
                ));
            }
        }
    }
}

impl From<&AccessMode> for *const c_char {
    fn from(value: &AccessMode) -> Self {
        match value {
            AccessMode::ReadOnly => c"READ_ONLY",
            AccessMode::ReadWrite => c"READ_WRITE",
        }
        .as_ptr()
    }
}

#[derive(Default, Clone, Debug)]
pub struct DuckDBConnectOptions {
    pub(crate) path: CString,
    pub(crate) access_mode: Option<AccessMode>,
    pub(crate) settings: Vec<(CString, CString)>,
}

fn make_cstring(str: Cow<'_, str>) -> Result<CString> {
    Ok(CString::new(&*str).map_err(|e| {
        Error::Configuration(format!("Error while creating the CString: {}", e).into())
    })?)
}

impl DuckDBConnectOptions {
    pub fn new(url: &str) -> Result<Self> {
        if !url.starts_with("duckdb://") {
            return Err(Error::Configuration(
                "Expected duckdb connection url to start with `duckdb://`".into(),
            ));
        }
        let mut url = url.trim_start_matches("duckdb://").splitn(2, '?');
        let path = url.next().ok_or(Error::Configuration(
            "Expected database file path or `:memory:` in the connection string".into(),
        ))?;
        let params = url.next();

        let mut options: DuckDBConnectOptions = Self::default();
        let path = percent_decode_str(path).decode_utf8().map_err(|e| {
            Error::Configuration(format!("Error while decoding path string: {}", e).into())
        })?;
        options.path = make_cstring(path)?;
        for (key, value) in url::form_urlencoded::parse(params.unwrap_or_default().as_bytes()) {
            match &*key {
                "mode" => options.access_mode = Some(value.deref().try_into()?),
                _ => options
                    .settings
                    .push((make_cstring(key)?, make_cstring(value)?)),
            }
        }
        Ok(options)
    }

    pub fn create_duckdb_config(&self) -> Result<CBox<duckdb_config>, sqlx_core::Error> {
        let mut config = CBox::new(ptr::null_mut(), |mut config| unsafe {
            duckdb_destroy_config(&mut config);
        });
        let rc = unsafe { duckdb_create_config(config.deref_mut()) };
        if rc != duckdb_state_DuckDBSuccess {
            return Err(Error::Configuration(
                "Error while creating the configuration object, most likely malloc failure".into(),
            ));
        }
        for (key, value) in self
            .settings
            .iter()
            .map(|(k, v)| (k.as_ptr(), v.as_ptr()))
            .chain(
                self.access_mode
                    .as_ref()
                    .map(|mode| (c"access_mode".as_ptr(), mode.into()))
                    .into_iter(),
            )
        {
            let rc = unsafe { duckdb_set_config(*config, key.into(), value.into()) };
            if rc != duckdb_state_DuckDBSuccess {
                return Err(Error::Configuration(
                    format!(
                        "Error while setting the option property `{}`",
                        unsafe { CStr::from_ptr(key) }.to_str().unwrap()
                    )
                    .into(),
                ));
            }
        }
        Ok(config)
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
        Box::pin(DuckDBConnection::establish(self))
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
