mod fixtures;

mod tests {
    use std::f64;

    use crate::fixtures::empty_db::EmptyDB;
    use sqlx::{Execute, prelude::*};
    use sqlx_duckdb::{connection::DuckDBConnection, database::DuckDB};

    #[tokio::test]
    async fn simple() {
        let mut fixture = EmptyDB::new("first").await;
        let connection: &mut DuckDBConnection = &mut fixture.connection;
        #[derive(sqlx::FromRow, PartialEq, Debug)]
        struct Entry {
            pub a: i32,
            pub b: i32,
        }

        let result = sqlx::query("CREATE TABLE simple (a INTEGER, b INTEGER)")
            .execute(&mut *connection)
            .await
            .unwrap();
        assert_eq!(result.rows_affected(), 0);

        let result = sqlx::query("INSERT INTO simple VALUES (1, 2), (3, 4)")
            .execute(&mut *connection)
            .await
            .unwrap();
        assert_eq!(result.rows_affected(), 2);

        let result = sqlx::query_as::<DuckDB, Entry>("SELECT * FROM simple")
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        assert_eq!(result, vec![Entry { a: 1, b: 2 }, Entry { a: 3, b: 4 }]);

        let result = sqlx::query("DELETE FROM simple WHERE true")
            .execute(&mut *connection)
            .await
            .unwrap();
        assert_eq!(result.rows_affected(), 2);

        let result = sqlx::query_as::<DuckDB, Entry>("SELECT * FROM simple")
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        assert_eq!(result, vec![]);

        let many_entries = (-10_000..40_000)
            .map(|v| Entry { a: v, b: -10 * v })
            .collect::<Vec<_>>();
        let result = sqlx::query(&format!(
            "INSERT INTO simple VALUES {}",
            many_entries
                .iter()
                .map(|v| format!("({},{})", v.a, v.b))
                .collect::<Vec<_>>()
                .join(",")
        ))
        .execute(&mut *connection)
        .await
        .unwrap();
        assert_eq!(result.rows_affected(), 50_000);

        let result = sqlx::query_as::<DuckDB, Entry>("SELECT * FROM simple")
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        assert_eq!(result, many_entries);

        let result = sqlx::query("SELECT sum(a) from simple")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        let sum = result.get::<i128, usize>(0);
        assert_eq!(sum, 749975000);
    }

    #[tokio::test]
    async fn simple_2() {
        let mut fixture = EmptyDB::new("second").await;
        let connection: &mut DuckDBConnection = &mut fixture.connection;
        #[derive(sqlx::FromRow, Debug)]
        struct Entry {
            pub alpha: bool,
            pub bravo: f64,
            pub charlie: u128,
        }
        impl PartialEq for Entry {
            fn eq(&self, other: &Self) -> bool {
                self.alpha == other.alpha
                    && self.charlie == other.charlie
                    && (self.bravo == other.bravo || (self.bravo.is_nan() && other.bravo.is_nan()))
            }
        }

        let result =
            sqlx::query("CREATE TABLE simple_2 (alpha BOOLEAN, bravo DOUBLE, charlie UHUGEINT)")
                .execute(&mut *connection)
                .await
                .unwrap();
        assert_eq!(result.rows_affected(), 0);

        let result = sqlx::query(
            r#"INSERT INTO simple_2 VALUES
                (false, 87.2, 0),
                (true, 'nan', 340282366920938463463374607431768211455),
                (true, '-inf', 788372)
            "#,
        )
        .execute(&mut *connection)
        .await
        .unwrap();
        assert_eq!(result.rows_affected(), 3);

        let result = sqlx::query_as::<DuckDB, Entry>("SELECT * FROM simple_2")
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        assert_eq!(result, vec![
            Entry {
                alpha: false,
                bravo: 87.2,
                charlie: 0
            },
            Entry {
                alpha: true,
                bravo: f64::NAN,
                charlie: 340282366920938463463374607431768211455
            },
            Entry {
                alpha: true,
                bravo: f64::NEG_INFINITY,
                charlie: 788372
            },
        ]);

        // let result = sqlx::raw_sql(
        //     r#"
        //         INSERT INTO simple_2 VALUES (false, -1000000.24, 1);
        //         SELECT * FROM simple_2;
        //     "#,
        // )
        // .fetch_all(&mut *connection)
        // .await
        // .unwrap();
        // let entry = Entry::from_row(&result[3]).unwrap();
        // assert_eq!(entry, Entry {
        //     alpha: false,
        //     bravo: -1000000.24,
        //     charlie: 1
        // });
    }
}
