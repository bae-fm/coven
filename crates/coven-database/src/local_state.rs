use super::*;

/// The single `protocol_state` value stored under `key`, or `None`.
pub fn get_protocol_state_on(conn: &Connection, key: &str) -> Result<Option<String>, DbError> {
    conn.query_row(
        "SELECT value FROM protocol_state WHERE key = ?1",
        [key],
        |row| row.get(0),
    )
    .optional()
    .map_err(DbError::from)
}

/// The `protocol_state` value stored under `key`; an error naming the key
/// when the row is absent, for callers whose protocol guarantees it exists.
pub fn required_protocol_state_on(conn: &Connection, key: &str) -> Result<String, DbError> {
    get_protocol_state_on(conn, key)?
        .ok_or_else(|| DbError::Message(format!("protocol_state key {key:?} is absent")))
}

/// Insert or replace the `protocol_state` value under `key`.
pub fn set_protocol_state_on(conn: &Connection, key: &str, value: &str) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    .map(|_| ())
    .map_err(DbError::from)
}

/// Delete the `protocol_state` row under `key`, returning how many rows
/// (0 or 1) were deleted so a caller can insist the row existed.
pub fn delete_protocol_state_on(conn: &Connection, key: &str) -> Result<usize, DbError> {
    conn.execute("DELETE FROM protocol_state WHERE key = ?1", [key])
        .map_err(DbError::from)
}

#[cfg(any(test, feature = "test-utils"))]
impl Database {
    pub async fn get_protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_string();
        self.connection
            .call_database(move |session| get_protocol_state_on(session.conn, &key))
            .await
    }

    pub async fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        let (key, value) = (key.to_string(), value.to_string());
        self.connection
            .call_database(move |session| set_protocol_state_on(session.conn, &key, &value))
            .await
    }

    pub async fn delete_protocol_state(&self, key: &str) -> Result<(), DbError> {
        let key = key.to_string();
        self.connection
            .call_database(move |session| delete_protocol_state_on(session.conn, &key).map(|_| ()))
            .await
    }
}
