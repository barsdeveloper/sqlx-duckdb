mod tests {
    use std::{fs, path::Path, sync::Mutex};

    use sqlx_core::connection::Connection;
    use sqlx_duckdb::connection::DuckDBConnection;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn create_database() {
        let db_path = "target/debug/test.db";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(db_path).exists() {
            fs::remove_file(db_path).expect("Failed to remove existing test database file");
        }
        assert!(
            !Path::new(db_path).exists(),
            "Database file should not exist before test"
        );
        DuckDBConnection::connect(format!("duckdb://{}?mode=rw", db_path).as_str())
            .await
            .expect("Could not open the database");
        assert!(
            Path::new(db_path).exists(),
            "Database file should be created after connection"
        );
    }

    #[tokio::test]
    async fn does_not_create_ro_database() {
        let db_path = "target/debug/ro_test.db";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(db_path).exists() {
            fs::remove_file(db_path).expect("Failed to remove existing test database file");
        }
        assert!(
            !Path::new(db_path).exists(),
            "Database file should not exist before test"
        );
        DuckDBConnection::connect(format!("duckdb://{}?mode=ro", db_path).as_str())
            .await
            .expect_err("Connection succeeded but it was expected to fail");
        assert!(
            !Path::new(db_path).exists(),
            "Database file shouldn't be created after connection"
        );
    }
}
