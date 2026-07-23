use crate::database::*;
use rusqlite::OptionalExtension;

use super::*;

impl StoreDatabase {
    pub(crate) async fn begin_store_creation_attempt(
        &self,
        initialized: crate::sync::store_protocol_root::StoreCreationAttempt,
    ) -> Result<crate::sync::store_protocol_root::StoreCreationAttempt, DbError> {
        let value = serde_json::to_string(&initialized).map_err(|error| {
            DbError::Message(format!("serialize Store creation attempt: {error}"))
        })?;
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                tx.execute(
                    "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (
                        crate::sync::store_protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY,
                        &value,
                    ),
                )
                .map_err(DbError::from)?;
                let actual: String = tx
                    .query_row(
                        "SELECT value FROM protocol_state WHERE key = ?1",
                        [crate::sync::store_protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                tx.commit().map_err(DbError::from)?;
                serde_json::from_str(&actual).map_err(|error| {
                    DbError::Message(format!("parse Store creation attempt: {error}"))
                })
            })
            .await
    }

    pub(crate) async fn load_store_creation_attempt(
        &self,
    ) -> Result<Option<crate::sync::store_protocol_root::StoreCreationAttempt>, DbError> {
        self.sqlite()
            .call(move |conn| {
                let value = conn
                    .query_row(
                        "SELECT value FROM protocol_state WHERE key = ?1",
                        [crate::sync::store_protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(DbError::from)?;
                value
                    .map(|value| {
                        serde_json::from_str(&value).map_err(|error| {
                            DbError::Message(format!("parse Store creation attempt: {error}"))
                        })
                    })
                    .transpose()
            })
            .await
    }

    pub(crate) async fn advance_store_creation_attempt(
        &self,
        previous: crate::sync::store_protocol_root::StoreCreationAttempt,
        next: crate::sync::store_protocol_root::StoreCreationAttempt,
    ) -> Result<(), DbError> {
        let previous = serde_json::to_string(&previous).map_err(|error| {
            DbError::Message(format!("serialize Store creation predecessor: {error}"))
        })?;
        let next = serde_json::to_string(&next).map_err(|error| {
            DbError::Message(format!("serialize Store creation successor: {error}"))
        })?;
        self.sqlite()
            .call(move |conn| {
                let changed = conn
                    .execute(
                        "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                        (
                            &next,
                            crate::sync::store_protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY,
                            &previous,
                        ),
                    )
                    .map_err(DbError::from)?;
                if changed != 1 {
                    return Err(DbError::Message(
                        "Store creation attempt advance lost its exact predecessor".to_string(),
                    ));
                }
                Ok(())
            })
            .await
    }
}
