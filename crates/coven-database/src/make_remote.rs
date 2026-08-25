use super::*;

impl Database {
    pub(crate) fn insert_make_remote_intent_on(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
        root_label: &str,
        retain_pinned: bool,
    ) -> Result<(), DbError> {
        conn.execute(
            "INSERT INTO blob_make_remote_intents
             (root_table, root_id, root_label, retain_pinned, state, write_id)
             VALUES (?1, ?2, ?3, ?4, 'uploading', NULL)
             ON CONFLICT(root_table, root_id) DO UPDATE SET
               root_label = excluded.root_label,
               retain_pinned = excluded.retain_pinned
             WHERE blob_make_remote_intents.state = 'uploading'",
            (root_table, root_id, root_label, retain_pinned),
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

    /// Mark the transition of every root that just disappeared as cancelling,
    /// in the transaction that removed it.
    ///
    /// A deleted root is not the end of its cloud work. An upload that already
    /// wrote its object has that object to take back out, and only the drain
    /// can do that — it needs the provider. So the queue outlives the row on
    /// purpose, in the one state that says what is left to do. `cancelling` is
    /// not a row awaiting repair; it is the correct durable answer to "the root
    /// is gone and its cloud objects may not be", and the drain settles it the
    /// same way it settles a cancel a person asked for.
    ///
    /// `deleted_roots` may be every row a write removed, unfiltered: the roots
    /// that have cloud work are read first and only those are matched, so this
    /// costs one query plus whatever is actually outstanding — never a pass per
    /// deleted row.
    pub(crate) fn cancel_transitions_for_deleted_roots_on(
        conn: &Connection,
        deleted_roots: &std::collections::HashSet<(String, String)>,
    ) -> Result<(), DbError> {
        if deleted_roots.is_empty() {
            return Ok(());
        }
        let mut statement = conn
            .prepare(
                "SELECT root_table, root_id FROM blob_make_remote_intents
                 UNION
                 SELECT root_table, root_id FROM cloud_outbox WHERE operation = 'upload'",
            )
            .map_err(DbError::from)?;
        let outstanding = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        drop(statement);
        for (root_table, root_id) in outstanding
            .into_iter()
            .filter(|root| deleted_roots.contains(root))
        {
            // A publication caught by the delete gives up its write id with its
            // state: the gate flip it was waiting to activate names a row that
            // is not there to flip.
            conn.execute(
                "UPDATE blob_make_remote_intents
                    SET state = 'cancelling', write_id = NULL
                  WHERE root_table = ?1 AND root_id = ?2 AND state <> 'cancelling'",
                (&root_table, &root_id),
            )
            .map_err(DbError::from)?;
            Self::adopt_cancelling_intent_from_queue_on(conn, &root_table, &root_id)?;
            // Clear the retry backoff the same way a cancel a person asked for
            // does. An upload that failed its last attempt is waiting out a
            // delay before anything looks at it again, and the unwind is not
            // the thing that should be made to wait: the root is already gone.
            conn.execute(
                "UPDATE cloud_outbox
                    SET attempt_count = 0, last_error = NULL, last_attempt_at = NULL
                  WHERE operation = 'upload' AND root_table = ?1 AND root_id = ?2",
                (&root_table, &root_id),
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    /// Give a root's queued uploads a cancelling intent built from the queue
    /// itself, unless the root already has an intent. Answers whether one was
    /// adopted.
    ///
    /// A root with queued uploads and no intent is the ordinary shape of a root
    /// that was already Remote when more work was queued for it, and of one
    /// whose transition ended while its queue had not. Either way the unwind is
    /// owed and the drain needs an intent to act on. The name comes off the
    /// queue rows, which is what the queue's own `root_label` is for — a root
    /// being cancelled or deleted is exactly the one whose row cannot be read.
    pub(crate) fn adopt_cancelling_intent_from_queue_on(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<bool, DbError> {
        conn.execute(
            "INSERT INTO blob_make_remote_intents
                 (root_table, root_id, root_label, retain_pinned, state, write_id)
             SELECT root_table, root_id, root_label, retain_pinned, 'cancelling', NULL
               FROM cloud_outbox
              WHERE operation = 'upload' AND root_table = ?1 AND root_id = ?2
                AND id = (SELECT MIN(id) FROM cloud_outbox
                           WHERE operation = 'upload'
                             AND root_table = ?1 AND root_id = ?2)
             ON CONFLICT(root_table, root_id) DO NOTHING",
            (root_table, root_id),
        )
        .map(|changed| changed == 1)
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
}
