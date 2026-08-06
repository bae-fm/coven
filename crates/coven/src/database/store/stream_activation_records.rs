use super::*;
use crate::protocol::circle_activation::VerifiedStreamActivations;
use crate::protocol::store_commit::StreamActivationId;
use rusqlite::{Connection, OptionalExtension};

impl StoreDatabase {
    pub(crate) fn record_verified_stream_activations_on(
        conn: &Connection,
        verified: &VerifiedStreamActivations,
        activating_commit_json: &str,
    ) -> Result<(), DbError> {
        for activation in verified.as_slice() {
            let activation_id = activation.activation_id().as_hash().to_string();
            let author_stream_id = activation.author_stream_id().to_string();
            let activation_bytes = serde_json::to_vec(activation)
                .map_err(|error| DbError::context("serialize verified stream activation", error))?;
            let existing: Option<(String, Vec<u8>, String)> = conn
                .query_row(
                    "SELECT author_stream_id, activation, activating_commit
                 FROM stream_activations
                 WHERE activation_id = ?1 OR author_stream_id = ?2",
                    (&activation_id, &author_stream_id),
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some(existing) = existing {
                if existing
                    != (
                        author_stream_id.clone(),
                        activation_bytes.clone(),
                        activating_commit_json.to_string(),
                    )
                {
                    return Err(DbError::Message(format!(
                    "stream activation {activation_id} conflicts with durable activation authority"
                )));
                }
                continue;
            }
            conn.execute(
                "INSERT INTO stream_activations
             (activation_id, author_stream_id, activation, activating_commit)
             VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    activation_id,
                    author_stream_id,
                    activation_bytes,
                    activating_commit_json,
                ],
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    pub(crate) async fn registered_stream_activation(
        &self,
        activation_id: StreamActivationId,
    ) -> Result<Option<crate::protocol::store_commit::RegisteredStreamActivation>, DbError> {
        let key = activation_id.as_hash().to_string();
        self.connection
            .call(move |conn| {
                let stored = conn
                    .query_row(
                        "SELECT activation_id, author_stream_id, activation, activating_commit
                         FROM stream_activations WHERE activation_id = ?1",
                        [key],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, Vec<u8>>(2)?,
                                row.get::<_, String>(3)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(DbError::from)?;
                let Some((activation_id, author_stream_id, activation, activating_commit)) = stored
                else {
                    return Ok(None);
                };
                let activation_id = StreamActivationId::from_digest(
                    activation_id
                        .parse()
                        .map_err(|error| DbError::context("stored stream activation id", error))?,
                );
                let author_stream_id = author_stream_id.parse().map_err(|error| {
                    DbError::Message(format!("stored author stream id: {error}"))
                })?;
                let activation = serde_json::from_slice(&activation).map_err(|error| {
                    DbError::context("stored stream activation descriptor", error)
                })?;
                let activating_commit =
                    serde_json::from_str(&activating_commit).map_err(|error| {
                        DbError::context("stored stream activation commit ref", error)
                    })?;
                crate::protocol::store_commit::RegisteredStreamActivation::from_stored(
                    activation_id,
                    author_stream_id,
                    activation,
                    activating_commit,
                )
                .map(Some)
                .map_err(|error| DbError::Message(error.to_string()))
            })
            .await
    }
}
