use super::*;

impl AuthorizedReclaim<'_, '_> {
    pub(super) async fn retire_snapshot_artifacts(
        &mut self,
        accepted: &VerifiedReclaimSnapshot,
        plan: &crate::sync::store::commit_publication::operation::commit_plan::StoreOperationCommitPlan,
    ) -> Result<u64, StoreReclaimError> {
        let reference = accepted.reference();
        let snapshot = accepted.snapshot();
        let inventory = &snapshot.meta.history_summary.reclaim;
        inventory.validate_before(&reference.publication)?;
        let protected = snapshot
            .meta
            .history_summary
            .pending_device_join_artifacts()?;
        let objects = inventory
            .snapshots
            .values()
            .flat_map(|old| old.objects())
            .chain(
                inventory
                    .publications
                    .values()
                    .map(|publication| &publication.object),
            )
            .filter(|object| !protected.contains(*object))
            .cloned()
            .collect::<BTreeSet<_>>();
        if objects.is_empty() {
            return Ok(0);
        }
        let current = BTreeSet::from([
            &snapshot.reference.object,
            &snapshot.meta.image.object,
            &snapshot.meta.membership_rollup.object,
            &reference.publication.object,
        ]);
        if objects.iter().any(|object| current.contains(object)) {
            return Err(StoreReclaimError::Authorization(
                "snapshot retirement aliases current accepted authority".into(),
            ));
        }
        let coven_protocol::membership::MembershipStatus::Resolved(resolved) =
            plan.membership().status()
        else {
            return Err(StoreReclaimError::Authorization(
                "snapshot retirement requires resolved membership".into(),
            ));
        };
        if plan.owner_grant().is_none()
            || plan
                .effective_provider_admin_grant(resolved.provider_admin.combined_state())
                .is_none()
        {
            return Err(StoreReclaimError::Authorization(
                "snapshot retirement requires an Owner with provider administration authority"
                    .into(),
            ));
        }
        let _permit = self.database.snapshot_publication_permit().await;
        self.database
            .verify_snapshot_artifacts_released(objects.iter().cloned().collect())
            .await?;
        let mut deleted = 0_u64;
        for object in objects {
            match self.storage.observe_exact_slot(object.slot()).await? {
                None => continue,
                Some(observed) if observed == object => {}
                Some(_) => {
                    return Err(StoreReclaimError::Storage(StorageError::SlotCollision(
                        format!(
                            "retired snapshot slot {} contains different bytes",
                            object.slot().logical_key()
                        ),
                    )));
                }
            }
            self.storage
                .delete_protocol_object(&object)
                .await
                .map_err(|source| StoreReclaimError::Delete {
                    object: coven_protocol::remote_object::remote_object_id(&object),
                    source,
                })?;
            deleted = deleted.checked_add(1).ok_or_else(|| {
                StoreReclaimError::Authorization("retired artifact count exceeded u64".into())
            })?;
        }
        Ok(deleted)
    }
}
