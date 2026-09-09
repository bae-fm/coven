use super::*;

impl MergeHistoryVerifier<'_> {
    pub(crate) async fn candidate_grant_retirement(
        &mut self,
        database: &coven_database::StoreDatabase,
        candidate: &VerifiedStoreBatchCommit,
    ) -> Result<Option<(MembershipChain, store_commit::StorePublicationRef)>, StorePullError> {
        let (baseline, boundary, entries, inputs, _) = database
            .retained_store_replay(self.root.reference().clone())
            .await?;
        let Some(boundary) = boundary else {
            return Ok(None);
        };
        let Some(publication) = boundary.record().accepted() else {
            return Ok(None);
        };
        self.admit_installed_baseline(baseline)?;
        self.admit_retained_history(&inputs)?;
        let coverage =
            CommitFrontier(self.accepted_frontier_before(boundary.record().next_position()?)?);
        // Held accepted entries cannot be mistaken for an empty author stream.
        if entries.iter().any(|entry| match &entry.value.payload {
            store_commit::StorePublicationPayload::Commit(commit) => {
                !coverage.covers_commit(commit)
            }
            store_commit::StorePublicationPayload::Snapshot(_) => false,
        }) || coverage
            .commits()
            .get(&candidate.reference().coord.stream_id)
            .is_some_and(|tip| tip.coord.sequence() >= candidate.reference().coord.sequence())
        {
            return Ok(None);
        }
        self.verify_refs(coverage.commits().values().cloned())
            .await?;
        let prefix = self.verified_membership_prefix(coverage.commits().values().cloned())?;
        let rooted = self.load_accepted_anchored_membership(&[], None).await?;
        let membership = self
            .project_membership_to_verified_prefix(rooted.head_refs(), &prefix)
            .await?;
        prefix.validate_complete_membership(&membership)?;
        let Some(creation) = &candidate.value().membership_authority else {
            return Ok(None);
        };
        if membership
            .write_authority_retirement(creation, &candidate.author().author_pubkey)
            .is_none()
        {
            return Ok(None);
        }
        Ok(Some((membership, publication.clone())))
    }
}
