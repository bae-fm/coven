//! Stands in for the store the read rule's fixture reads: a statement hands
//! its row mapper a `Row`, and a live query hands `process` what it read.

/// One row a statement read.
pub struct Row {
    values: Vec<i64>,
}

impl Row {
    pub fn get(&self, index: usize) -> i64 {
        self.values[index]
    }

    pub fn column<T: FromColumn>(&self, index: usize) -> Result<T, ()> {
        Ok(T::from_column(self.values[index]))
    }
}

/// A value one column converts to.
pub trait FromColumn {
    fn from_column(value: i64) -> Self;
}

impl FromColumn for i64 {
    fn from_column(value: i64) -> Self {
        value
    }
}

/// What a read receives.
pub struct SqlReadContext {
    rows: Vec<Row>,
}

impl SqlReadContext {
    pub fn query<T>(&self, _sql: &str, mut map: impl FnMut(&Row) -> Result<T, ()>) -> Vec<T> {
        self.rows.iter().filter_map(|row| map(row).ok()).collect()
    }
}

/// What a read receives, as a trait.
pub trait SqlRead {
    fn query<T, F: FnMut(&Row) -> Result<T, ()>>(&self, sql: &str, map: F) -> Vec<T>;

    fn query_row<T, F: FnOnce(&Row) -> Result<T, ()>>(&self, sql: &str, map: F) -> Option<T>;
}

impl SqlRead for SqlReadContext {
    fn query<T, F: FnMut(&Row) -> Result<T, ()>>(&self, sql: &str, map: F) -> Vec<T> {
        SqlReadContext::query(self, sql, map)
    }

    fn query_row<T, F: FnOnce(&Row) -> Result<T, ()>>(&self, _sql: &str, map: F) -> Option<T> {
        self.rows.first().and_then(|row| map(row).ok())
    }
}

/// A live query and the value it read last.
pub struct LiveQuery<T> {
    value: T,
}

impl<T> LiveQuery<T> {
    pub fn latest(&self) -> &T {
        &self.value
    }

    pub fn process<U>(self, process: impl FnOnce(T) -> U) -> U {
        process(self.value)
    }
}
