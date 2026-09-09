use super::*;

impl MergeHistoryVerifier<'_> {
    pub(crate) fn retain_snapshot_publication_objects(
        &self,
        summary: &mut RetainedVerifiedMergeHistorySummary,
    ) -> Result<(), StorePullError> {
        for (commit, evidence) in &self.accepted_publications {
            if summary.causal_cut.get(&commit.coord) != Some(commit) {
                continue;
            }
            if let AcceptedStoreCommitEvidence::Exact(accepted) = evidence {
                let reference = accepted.reference();
                let id = coven_protocol::remote_object::remote_object_id(&reference.object);
                if summary
                    .reclaim
                    .publications
                    .get(&id)
                    .is_some_and(|old| old != reference)
                {
                    return Err(StorePullError::InvalidState(
                        "retained publication has conflicting exact acceptance".into(),
                    ));
                }
                summary.reclaim.publications.insert(id, reference.clone());
            }
        }
        Ok(())
    }

    pub(crate) async fn prune_absent_snapshot_artifacts(
        &self,
        reclaim: &mut store_commit::RetainedReclaimState,
    ) -> Result<(), StorePullError> {
        let mut absent = Vec::new();
        for (id, snapshot) in &reclaim.snapshots {
            let mut all_absent = true;
            for object in snapshot.objects() {
                all_absent &= self
                    .commit_verifier
                    .exact_protocol_object_is_absent(object)
                    .await?;
            }
            if all_absent {
                absent.push(*id);
            }
        }
        for id in absent {
            reclaim.snapshots.remove(&id);
        }
        let mut absent = Vec::new();
        for (id, publication) in &reclaim.publications {
            if self
                .commit_verifier
                .exact_protocol_object_is_absent(&publication.object)
                .await?
            {
                absent.push(*id);
            }
        }
        for id in absent {
            reclaim.publications.remove(&id);
        }
        Ok(())
    }

    pub(super) async fn verify_snapshot_artifact_retirement(
        &self,
        canonical: &mut store_commit::RetainedReclaimState,
        supplied: &store_commit::RetainedReclaimState,
    ) -> Result<(), StorePullError> {
        // A deletion after capture may leave a harmless pending obligation in
        // the candidate. Only omissions need current exact absence evidence.
        for (id, value) in &supplied.snapshots {
            if canonical.snapshots.get(id) != Some(value) {
                return Err(StorePullError::InvalidState(
                    "snapshot invents or changes accepted artifact ownership".into(),
                ));
            }
        }
        for (id, value) in &supplied.publications {
            if canonical.publications.get(id) != Some(value) {
                return Err(StorePullError::InvalidState(
                    "snapshot invents or changes accepted publication ownership".into(),
                ));
            }
        }
        for (id, value) in &canonical.snapshots {
            if !supplied.snapshots.contains_key(id) {
                for object in value.objects() {
                    if !self
                        .commit_verifier
                        .exact_protocol_object_is_absent(object)
                        .await?
                    {
                        return Err(StorePullError::InvalidState(
                            "snapshot omits a pending accepted artifact deletion".into(),
                        ));
                    }
                }
            }
        }
        for (id, value) in &canonical.publications {
            if !supplied.publications.contains_key(id)
                && !self
                    .commit_verifier
                    .exact_protocol_object_is_absent(&value.object)
                    .await?
            {
                return Err(StorePullError::InvalidState(
                    "snapshot omits a pending accepted publication deletion".into(),
                ));
            }
        }
        canonical.snapshots = supplied.snapshots.clone();
        canonical.publications = supplied.publications.clone();
        Ok(())
    }
}
