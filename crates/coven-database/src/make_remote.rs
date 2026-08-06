use super::*;

impl Database {
    pub fn insert_make_remote_intent_on(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
        retain_pinned: bool,
    ) -> Result<(), DbError> {
        conn.execute(
            "INSERT INTO blob_make_remote_intents
             (root_table, root_id, retain_pinned, state, write_id)
             VALUES (?1, ?2, ?3, 'uploading', NULL)
             ON CONFLICT(root_table, root_id) DO UPDATE SET
               retain_pinned = excluded.retain_pinned
             WHERE blob_make_remote_intents.state = 'uploading'",
            (root_table, root_id, retain_pinned),
        )
        .map_err(DbError::from)
        .and_then(|changed| {
            if changed == 1 {
                Ok(())
            } else {
                Err(DbError::Message(format!(
                    "make_remote for {root_table:?}/{root_id:?} is already publishing"
                )))
            }
        })
    }

    pub fn make_remote_intent_state(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<MakeRemoteIntentState>, DbError> {
        let encoded = conn
            .query_row(
                "SELECT state, write_id FROM blob_make_remote_intents
                 WHERE root_table = ?1 AND root_id = ?2",
                (root_table, root_id),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        encoded
            .map(|(state, write_id)| match (state.as_str(), write_id) {
                ("uploading", None) => Ok(MakeRemoteIntentState::Uploading),
                ("cancelling", None) => Ok(MakeRemoteIntentState::Cancelling),
                ("publishing", Some(write_id)) => Ok(MakeRemoteIntentState::Publishing(
                    WriteId::from_generated(write_id),
                )),
                _ => Err(DbError::Message(format!(
                    "make_remote intent {root_table:?}/{root_id:?} has invalid state {state:?}"
                ))),
            })
            .transpose()
    }

    pub fn mark_make_remote_publishing_on(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
        write_id: &WriteId,
    ) -> Result<(), DbError> {
        let updated = conn
            .execute(
                "UPDATE blob_make_remote_intents SET state = 'publishing', write_id = ?3
                 WHERE root_table = ?1 AND root_id = ?2 AND state = 'uploading'",
                (root_table, root_id, write_id.as_str()),
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(format!(
                "make_remote intent {root_table:?}/{root_id:?} changed before publication"
            )));
        }
        Ok(())
    }

    pub fn complete_make_remote_publication_on(
        conn: &Connection,
        write_id: &WriteId,
    ) -> Result<(), DbError> {
        let removed = conn
            .execute(
                "DELETE FROM blob_make_remote_intents
                 WHERE write_id = ?1 AND state = 'publishing'",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
        if removed != 1 {
            return Err(DbError::Message(format!(
                "make_remote publication {write_id} changed before activation"
            )));
        }
        Ok(())
    }

    pub fn make_remote_publication_root_on(
        conn: &Connection,
        write_id: &WriteId,
    ) -> Result<Option<(String, String)>, DbError> {
        conn.query_row(
            "SELECT root_table, root_id FROM blob_make_remote_intents
             WHERE write_id = ?1 AND state = 'publishing'",
            [write_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(DbError::from)
    }
}
