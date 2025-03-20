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
            .unwrap();
        let _ = sqlx::query("INSERT INTO alpha VALUES (1, 2), (3, 4)")
            .execute(&mut *connection)
            .await
            .unwrap();

        #[derive(sqlx::FromRow, PartialEq, Debug)]
        struct Entry {
            pub a: i32,
            pub b: i32,
        }
        let result = sqlx::query_as::<DuckDB, Entry>("SELECT * FROM alpha")
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        assert_eq!(result, vec![Entry { a: 1, b: 2 }, Entry { a: 3, b: 4 }])
    }
}
