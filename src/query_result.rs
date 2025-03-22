#[derive(Default, Debug)]
pub struct DuckDBQueryResult {
    pub(crate) rows_affected: u64,
    pub(crate) last_insert_id: Option<i64>,
}

impl DuckDBQueryResult {
    pub fn new(rows_affected: u64) -> Self {
        Self {
            rows_affected,
            last_insert_id: None,
        }
    }

    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    pub fn last_insert_id(&self) -> Option<i64> {
        self.last_insert_id
    }
}

impl Extend<DuckDBQueryResult> for DuckDBQueryResult {
    fn extend<T: IntoIterator<Item = DuckDBQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.rows_affected += elem.rows_affected;
            self.last_insert_id = elem.last_insert_id;
        }
    }
}
