use super::*;

impl Database {
    pub async fn get_protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_string();
        self.call(move |conn| {
            conn.query_row(
                "SELECT value FROM protocol_state WHERE key = ?1",
                [key],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)
        })
        .await
    }

    pub async fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        let (key, value) = (key.to_string(), value.to_string());
        self.call(move |conn| {
            conn.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    pub async fn delete_protocol_state(&self, key: &str) -> Result<(), DbError> {
        let key = key.to_string();
        self.call(move |conn| {
            conn.execute("DELETE FROM protocol_state WHERE key = ?1", [key])
                .map(|_| ())
                .map_err(DbError::from)
        })
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
