use super::*;

impl Database {
    // ---- Local blob refs (external user files) ----

    pub(crate) fn register_external_blob_on(
        conn: &Connection,
        reference: &RowBlobRef,
        path: &Path,
    ) -> Result<(), DbError> {
        if reference.authority() != &RowBlobAuthority::Local || reference.stored().is_some() {
            return Err(DbError::Message(
                "external file requires an exact Local row blob reference".to_string(),
            ));
        }
        let path = path.to_str().ok_or_else(|| {
            DbError::Message(format!("external blob path is not UTF-8: {path:?}"))
        })?;
        let size = i64::try_from(reference.plaintext_size()).map_err(|_| {
            DbError::Message("external blob plaintext size exceeds SQLite INTEGER".to_string())
        })?;
        super::with_coven_sql_authority(|| {
            conn.execute(
                "INSERT INTO local_blob_refs
             (table_name, row_id, column_name, row_stamp, namespace, blob_id,
              path, plaintext_size, plaintext_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(table_name, row_id, column_name, row_stamp) DO UPDATE SET
               namespace = excluded.namespace,
               blob_id = excluded.blob_id,
               path = excluded.path,
               plaintext_size = excluded.plaintext_size,
               plaintext_hash = excluded.plaintext_hash",
                rusqlite::params![
                    reference.table(),
                    reference.row_id(),
                    reference.column(),
                    reference.row_stamp(),
                    &reference.blob().namespace,
                    &reference.blob().id,
                    path,
                    size,
                    reference.plaintext_hash().to_string(),
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
    }

    pub(crate) fn clear_external_blob_on(
        conn: &Connection,
        reference: &RowBlobRef,
    ) -> Result<(), DbError> {
        super::with_coven_sql_authority(|| {
            conn.execute(
                "DELETE FROM local_blob_refs WHERE table_name = ?1 AND row_id = ?2
                 AND column_name = ?3 AND row_stamp = ?4",
                rusqlite::params![
                    reference.table(),
                    reference.row_id(),
                    reference.column(),
                    reference.row_stamp(),
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
    }

    pub(crate) fn insert_make_remote_intent_on(
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

    #[cfg(test)]
    pub(crate) fn make_remote_intent_exists(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<bool, DbError> {
        conn.query_row(
            "SELECT 1 FROM blob_make_remote_intents WHERE root_table = ?1 AND root_id = ?2",
            (root_table, root_id),
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(DbError::from)
    }

    pub(crate) fn make_remote_intent_state(
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

    pub(crate) fn mark_make_remote_publishing_on(
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

    pub(crate) fn complete_make_remote_publication_on(
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

    pub(crate) fn make_remote_publication_root_on(
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

    #[cfg(test)]
    pub(crate) async fn external_blob_for_row(
        &self,
        reference: &RowBlobRef,
    ) -> Result<Option<ExternalBlob>, DbError> {
        let table = reference.table().to_string();
        let row_id = reference.row_id().to_string();
        let column = reference.column().to_string();
        let row_stamp = reference.row_stamp().to_string();
        let namespace = reference.blob().namespace.clone();
        let blob_id = reference.blob().id.clone();
        let expected_size = reference.plaintext_size();
        let expected_hash = reference.plaintext_hash();
        self.call(move |conn| {
            let row = conn
                .query_row(
                    "SELECT path, plaintext_size, plaintext_hash, namespace, blob_id
                     FROM local_blob_refs
                     WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3
                       AND row_stamp = ?4",
                    rusqlite::params![table, row_id, column, row_stamp],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(DbError::from)?;
            let Some((path, size, hash, stored_namespace, stored_blob_id)) = row else {
                return Ok(None);
            };
            let size = u64::try_from(size).map_err(|_| {
                DbError::Message(format!("external blob {blob_id} has negative size"))
            })?;
            let hash: ObjectHash = hash.parse().map_err(|error| {
                DbError::Message(format!("external blob {blob_id} hash: {error}"))
            })?;
            if size != expected_size
                || hash != expected_hash
                || stored_namespace != namespace
                || stored_blob_id != blob_id
            {
                return Err(DbError::Message(format!(
                    "external blob row {table}/{row_id}/{column} differs from its row reference"
                )));
            }
            Ok(Some(ExternalBlob {
                path: PathBuf::from(path),
                size,
            }))
        })
        .await
    }
}
