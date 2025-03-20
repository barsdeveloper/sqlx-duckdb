use std::{fs, path::PathBuf, str::FromStr, sync::Mutex};

use sqlx_core::connection::Connection;
use sqlx_duckdb::connection::DuckDBConnection;

const CREATE_PATH: fn(&str) -> PathBuf =
    |name| PathBuf::from_str(format!("target/debug/{}.db", name).as_str()).unwrap();
static MUTEX: Mutex<()> = Mutex::new(());

pub struct EmptyDB {
    path: PathBuf,
    pub connection: DuckDBConnection,
}

impl EmptyDB {
    pub async fn new(db_name: &str) -> Self {
        let path: PathBuf = CREATE_PATH(db_name);
        if path.exists() {
            fs::remove_file(&path).expect(
                format!(
                    "Failed to remove existing test database file {}",
                    path.as_path().to_string_lossy()
                )
                .as_str(),
            );
        }
        assert!(
            !path.exists(),
            "Database file should not exist at this point"
        );
        let connection = DuckDBConnection::connect(
            format!("duckdb://{}?mode=rw", path.to_string_lossy()).as_str(),
        )
        .await
        .expect("Could not connect to the database");
        Self { path, connection }
    }
}

impl Drop for EmptyDB {
    fn drop(&mut self) {
        //fs::remove_file(&self.path).expect("Failed to remove the database file");
    }
}
