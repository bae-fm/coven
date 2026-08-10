use super::*;
use coven_protocol::circle_activation::VerifiedStreamActivations;
use coven_protocol::store_commit::StreamActivationId;
use rusqlite::{Connection, OptionalExtension};

impl StoreSession<'_> {
    fn registered_stream_activation(
        &self,
        activation_id: StreamActivationId,
    ) -> Result<Option<coven_protocol::store_commit::RegisteredStreamActivation>, DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .registered_stream_activation(activation_id)
    }
}

impl StoreDatabase {
    pub async fn registered_stream_activation(
        &self,
        activation_id: StreamActivationId,
    ) -> Result<Option<coven_protocol::store_commit::RegisteredStreamActivation>, DbError> {
        self.call_store(move |session| session.registered_stream_activation(activation_id))
            .await
    }
}

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
