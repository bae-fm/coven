use super::*;

impl StoreCommitVerifier<'_> {
    pub(crate) async fn load_owner_promotion_request_publication(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<RetainedOwnerPromotionRequestPublication, StorePullError> {
        let request = commit.owner_promotion_request().ok_or_else(|| {
            StorePullError::InvalidState(
                "publication result requires a promotion request commit".into(),
            )
        })?;
        let prefix = owner_promotion_request_publication_semantic_prefix(request.promotion_id);
        let context = ProtocolObjectContext::signed_plaintext(
            self.store_root_hash(),
            ProtocolObjectDomain::OwnerPromotionRequestPublication,
        );
        let (bytes, object) = self
            .read_protocol_slot(&context, &request.publication_slot, &prefix)
            .await
            .map_err(|source| match source {
                source @ StorageError::NotFound(_) => StorePullError::context(
                    format!(
                        "Owner-promotion request {} is awaiting publication finalization",
                        request.promotion_id
                    ),
                    source,
                ),
                source => StorePullError::Storage(source),
            })?;
        let author = self
            .load_registration(&request.promoter_registration)
            .await?;
        let commit = commit.clone();
        let expected_object = object.clone();
        run_blocking_object_verification(
            &prefix,
            &object,
            Box::new(move || {
                let value: OwnerPromotionRequestPublication = decode_protocol_object(&bytes)?;
                value.verify_for(&commit, &author.value)?;
                if value.to_bytes() != bytes {
                    return Err(StoreProtocolError::Malformed(
                        "Owner-promotion request publication is not canonical".into(),
                    ));
                }
                let publication = RetainedOwnerPromotionRequestPublication {
                    value,
                    object: expected_object,
                };
                publication.validate_for(&commit)?;
                Ok(publication)
            }),
        )
        .await
        .map_err(StorePullError::from)
    }
}
