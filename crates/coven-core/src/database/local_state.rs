use super::*;

/// The single `protocol_state` value stored under `key`, or `None`.
pub(crate) fn get_protocol_state_on(
    conn: &Connection,
    key: &str,
) -> Result<Option<String>, DbError> {
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
pub(crate) fn required_protocol_state_on(conn: &Connection, key: &str) -> Result<String, DbError> {
    get_protocol_state_on(conn, key)?
        .ok_or_else(|| DbError::Message(format!("protocol_state key {key:?} is absent")))
}

/// Insert or replace the `protocol_state` value under `key`.
pub(crate) fn set_protocol_state_on(
    conn: &Connection,
    key: &str,
    value: &str,
) -> Result<(), DbError> {
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
pub(crate) fn delete_protocol_state_on(conn: &Connection, key: &str) -> Result<usize, DbError> {
    conn.execute("DELETE FROM protocol_state WHERE key = ?1", [key])
        .map_err(DbError::from)
}

impl Database {
    pub async fn get_protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_string();
        self.call(move |conn| get_protocol_state_on(conn, &key))
            .await
    }

    pub async fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        let (key, value) = (key.to_string(), value.to_string());
        self.call(move |conn| set_protocol_state_on(conn, &key, &value))
            .await
    }

    pub async fn delete_protocol_state(&self, key: &str) -> Result<(), DbError> {
        let key = key.to_string();
        self.call(move |conn| delete_protocol_state_on(conn, &key).map(|_| ()))
            .await
    }

    /// `namespace`'s device-local cache-size budget in bytes, or `None` if the host
    /// has not set one for it. `None` means unlimited — eviction is off for that
    /// namespace and its cache grows without bound; the host opts a namespace into a
    /// budget by calling [`Self::set_cache_budget`]. Budgets are per namespace so a
    /// small namespace (`covers`) is never wiped by pressure from a big one
    /// (`release_files`): each evicts against its own budget. Stored as a single
    /// decimal value under [`crate::blob::cache::cache_budget_state_key`] in
    /// `protocol_state` (config, not per-blob accounting — the cache's truth is still the
    /// folder on disk).
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
        let key = crate::blob::cache::cache_budget_state_key(namespace);
        match self.get_protocol_state(&key).await? {
            Some(raw) => raw.parse::<u64>().map(Some).map_err(|e| {
                DbError::Message(format!(
                    "cache budget for {namespace:?} in protocol_state is not a byte count: {e}"
                ))
            }),
            None => Ok(None),
        }
    }

    /// Set `namespace`'s device-local cache-size budget in bytes. Once set, a populate
    /// into that namespace's cache that pushes `storage/cache/<namespace>/` over this
    /// total evicts its oldest files (by mtime) back under it; `pinned/` is never
    /// counted or touched, and another namespace's files are never walked. Stored
    /// under [`crate::blob::cache::cache_budget_state_key`] in `protocol_state`.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        let key = crate::blob::cache::cache_budget_state_key(namespace);
        self.set_protocol_state(&key, &max_bytes.to_string()).await
    }
}
