use crate::sync::store::circles::CircleOperationError;

pub(crate) trait CircleBootstrapBlobVerification {
    async fn verify_stored_blob(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError>;

    async fn verify_snapshot_blobs(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        snapshot_blobs: &[coven_protocol::blob::RowBlobRef],
    ) -> Result<Vec<coven_protocol::blob::RowBlobRef>, CircleOperationError> {
        let mut blobs = Vec::with_capacity(snapshot_blobs.len());
        for captured in snapshot_blobs {
            if captured.audience() != coven_protocol::circle::Audience::Circle(circle_id) {
                return Err(CircleOperationError::InvalidState(
                    "Circle bootstrap blob belongs to another audience".to_string(),
                ));
            }
            let stored = captured.stored().ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle bootstrap blob {}/{} has no activated exact remote binding",
                    captured.blob().namespace,
                    captured.blob().id
                ))
            })?;
            self.verify_stored_blob(stored).await?;
            blobs.push(captured.clone());
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
