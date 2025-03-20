mod tests {
    use sqlx_core::connection::Connection;
    use sqlx_duckdb::connection::DuckDBConnection;
    use std::{fs, path::Path, sync::Mutex};

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn create_database() {
        const DB_PATH: &'static str = "target/debug/test.db";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(DB_PATH).exists() {
            fs::remove_file(DB_PATH).expect(
                format!("Failed to remove existing test database file {}", DB_PATH).as_str(),
            );
        }
        assert!(
            !Path::new(DB_PATH).exists(),
            "Database file should not exist before test"
        );
        DuckDBConnection::connect(format!("duckdb://{}?mode=rw", DB_PATH).as_str())
            .await
            .expect("Could not open the database");
        assert!(
            Path::new(DB_PATH).exists(),
            "Database file should be created after connection"
        );
    }

    #[tokio::test]
    async fn does_not_create_ro_database() {
        const DB_PATH: &'static str = "target/debug/test.db";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(DB_PATH).exists() {
            fs::remove_file(DB_PATH).expect(
                format!("Failed to remove existing test database file {}", DB_PATH).as_str(),
            );
        }
        assert!(
            !Path::new(DB_PATH).exists(),
            "Database file should not exist before test"
        );
        DuckDBConnection::connect(format!("duckdb://{}?mode=ro", DB_PATH).as_str())
            .await
            .expect_err("Connection succeeded but it was expected to fail");
        assert!(
            !Path::new(DB_PATH).exists(),
            "Database file shouldn't be created after connection"
        );
    }
}
