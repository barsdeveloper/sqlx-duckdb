mod fixtures;

mod tests {
    use std::f64;

    use crate::fixtures::empty_db::EmptyDB;
    use rust_decimal::Decimal;
    use sqlx::types::time;
    use sqlx_duckdb::{connection::DuckDBConnection, database::DuckDB};

    #[tokio::test]
    async fn test_multi_type_insert_retrieve() {
        // Create a new test fixture
        let mut fixture = EmptyDB::new("multi_type").await;
        let connection: &mut DuckDBConnection = &mut fixture.connection;

        // Create a table covering several data types.
        // Adjust the SQL types if needed for your DuckDB setup.
        let create_table = r#"
            CREATE TABLE multi_type (
                boolean BOOLEAN,
                int8 TINYINT,
                int16 SMALLINT,
                int32 INTEGER,
                int64 BIGINT,
                int128 HUGEINT,
                uint8 UTINYINT,
                uint16 USMALLINT,
                uint32 UINTEGER,
                uint64 UBIGINT,
                uint128 UHUGEINT,
                float32 FLOAT,
                float64 DOUBLE,
                decimal DECIMAL(10,2),
                varchar VARCHAR,
                blob BLOB,
                date DATE,
                time TIME,
                timestamp TIMESTAMP,
            )
        "#;
        sqlx::query(create_table)
            .execute(&mut *connection)
            .await
            .unwrap();

        // Insert a row with representative values.
        // Some literals (like UHUGEINT) might require special handling in your environment.
        let insert = r#"
            INSERT INTO multi_type VALUES (
                true,
                -8,
                -300,
                12345,
                -9876543210,
                123456789012345678901234567890,
                8,
                300,
                12345,
                9876543210,
                123456789012345678901234567890,
                3.14,
                2.71828,
                12345.67,
                'Hello, world!',
                'FOOBAR'::BLOB,
                '1945-05-08',
                '15:34:56',
                '1969-07-20 20:17:40'
            )
        "#;
        sqlx::query(insert).execute(&mut *connection).await.unwrap();

        // Define a struct that maps to the table's columns.
        #[derive(sqlx::FromRow, PartialEq, Debug)]
        struct MultiType {
            boolean: bool,
            int8: i8,
            int16: i16,
            int32: i32,
            int64: i64,
            int128: i128,
            uint8: u8,
            uint16: u16,
            uint32: u32,
            uint64: u64,
            uint128: u128,
            float32: f32,
            float64: f64,
            decimal: Decimal,
            varchar: String,
            blob: Vec<u8>,
            date: time::Date,
            time: time::Time,
            timestamp: time::PrimitiveDateTime,
        }

        let result = sqlx::query_as::<DuckDB, MultiType>("SELECT * FROM multi_type")
            .fetch_one(&mut *connection)
            .await
            .unwrap();

        // Build the expected value.
        let expected = MultiType {
            boolean: true,
            int8: -8,
            int16: -300,
            int32: 12345,
            int64: -9876543210,
            int128: 123456789012345678901234567890,
            uint8: 8,
            uint16: 300,
            uint32: 12345,
            uint64: 9876543210,
            uint128: 123456789012345678901234567890,
            float32: 3.14,
            float64: 2.71828,
            decimal: Decimal::new(1234567, 2),
            varchar: "Hello, world!".to_string(),
            blob: vec![0x46, 0x4F, 0x4F, 0x42, 0x41, 0x52],
            date: time::Date::from_calendar_date(1945, 5.try_into().unwrap(), 8).unwrap(),
            time: time::Time::from_hms(15, 34, 56).unwrap(),
            timestamp: time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(1969, 7.try_into().unwrap(), 20).unwrap(),
                time::Time::from_hms(20, 17, 40).unwrap(),
            ),
        };

        assert_eq!(result, expected);
    }
}
