mod fixtures;

mod tests {
    use crate::fixtures::empty_db::EmptyDB;
    use sqlx::prelude::*;
    use sqlx_duckdb::database::DuckDB;
    use std::sync::Mutex;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn first() {
        let mut fixture = EmptyDB::new("first").await;
        let connection = &mut fixture.connection;
        let _ = sqlx::query("CREATE TABLE alpha (a INTEGER, b INTEGER)")
            .execute(&mut *connection)
            .await
            .expect("Should succeed");
        let _ = sqlx::query("INSERT INTO alpha VALUES (1, 2), (3, 4)")
            .execute(&mut *connection)
            .await
            .expect("Should succeed");
        let result = sqlx::query("SELECT * FROM alpha ")
            .fetch_all(&mut *connection)
            .await
            .expect("Should succeed");
        assert_eq!(result.len(), 2);

        #[derive(sqlx::FromRow)]
        #[sqlx(type_name = "DuckDB")]
        struct Entry {
            a: i32,
            b: i32,
        }
        let mut stream =
            sqlx::query_as::<DuckDB, Entry>("SELECT * FROM alpha").fetch(&mut *connection);
    }
}
