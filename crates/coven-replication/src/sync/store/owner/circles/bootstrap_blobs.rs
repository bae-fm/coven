use crate::sync::store::circle_controls::CircleOperationError;

pub(super) trait CircleBootstrapBlobVerification {
    async fn verify_stored_blob(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError>;

    async fn verify_snapshot_blobs(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        snapshot: &coven_database::CreatedSnapshot,
    ) -> Result<Vec<coven_protocol::blob::RowBlobRef>, CircleOperationError> {
        let mut blobs = Vec::with_capacity(snapshot.blobs.len());
        for captured in &snapshot.blobs {
            let coven_database::SnapshotBlobAudience::Circle {
                circle_id: captured_circle,
                ..
            } = captured.audience
            else {
                return Err(CircleOperationError::InvalidState(
                    "Circle bootstrap contains a Store-audience blob".to_string(),
                ));
            };
            let previous = captured.fact.previous.as_ref().ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle bootstrap blob {}/{} has no activated exact remote binding",
                    captured.fact.blob.namespace, captured.fact.blob.id
                ))
            })?;
            if captured_circle != circle_id
                || previous.authority.remote_audience()
                    != coven_protocol::blob::locator::RemoteAudience::Circle(circle_id)
                || previous.stored.locator().audience()
                    != coven_protocol::blob::locator::RemoteAudience::Circle(circle_id)
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle bootstrap blob belongs to another audience".to_string(),
                ));
            }
            self.verify_stored_blob(&previous.stored)
                .await
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "verify Circle bootstrap blob {}/{}: {error}",
                        captured.fact.blob.namespace, captured.fact.blob.id
                    ))
                })?;
            blobs.push(
                coven_protocol::blob::RowBlobRef::new(
                    captured.fact.table.clone(),
                    captured.fact.row_id.clone(),
                    captured.fact.row_stamp.clone(),
                    captured.fact.column.clone(),
                    captured.fact.blob.clone(),
                    captured.fact.plaintext_size,
                    captured.fact.plaintext_hash,
                    coven_protocol::blob::RowBlobAuthority::Remote(previous.authority.clone()),
                    Some(previous.stored.clone()),
                )
                .map_err(CircleOperationError::InvalidState)?,
            );
        }
        blobs.sort_by_cached_key(|blob| {
            serde_json::to_vec(blob).expect("row blob reference serialization cannot fail")
        });
        if blobs.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CircleOperationError::InvalidState(
                "Circle bootstrap repeats an exact row blob binding".to_string(),
            ));
        }
        Ok(blobs)
    }
}
