use super::*;

pub(crate) async fn cleanup_merge_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    write_id: crate::WriteId,
) -> Result<(), StorePullError> {
    let root = db.local_store_root_ref().await?.ok_or_else(|| {
        StorePullError::Database("Merge candidate cleanup has no Store root".to_string())
    })?;
    for verification in crate::sync::store::database::StoreDatabase::new(db)
        .merge_candidate_terminal_verifications(write_id.clone())
        .await?
    {
        let nonactivation =
            verify_terminal_cleanup_candidate(db, storage, &root, &verification).await?;
        crate::sync::store::database::StoreDatabase::new(db)
            .reconcile_merge_candidate_terminal_head(write_id.clone(), nonactivation)
            .await?;
    }
    let targets = crate::sync::store::database::StoreDatabase::new(db)
        .merge_candidate_cleanup_targets(write_id)
        .await?;
    for target in targets {
        super::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    Ok(())
}

async fn verify_terminal_cleanup_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    verification: &crate::database::TerminalCandidateCleanupVerification,
) -> Result<super::remote_object::VerifiedCandidateNonactivation, StorePullError> {
    let reference = &verification.candidate.head.value.commit;
    let target = super::store_commit::StoreBatchCommitDeletionTarget {
        coord: reference.coord.clone(),
        object: verification.candidate.commit.object.clone(),
        canonical_signed_bytes: verification.candidate.commit.bytes.clone(),
    };
    match &verification.authority {
        crate::database::TerminalCandidateAuthority::AuthorExclusion(locator) => {
            Box::pin(verify_author_exclusion_nonactivation(
                db,
                storage,
                root,
                locator,
                reference,
                &verification.candidate.commit.value,
                &verification.candidate.head.value,
                &verification.candidate.head.object,
            ))
            .await
        }
        crate::database::TerminalCandidateAuthority::MembershipGrantRevocation {
            grant_id,
            membership,
            activation_commit,
            activation_head,
        } => {
            Box::pin(verify_membership_grant_revocation_nonactivation(
                storage,
                root,
                grant_id,
                membership,
                activation_commit,
                activation_head,
                reference,
                &verification.candidate.commit.value,
                &verification.candidate.head.value,
                &verification.candidate.head.object,
            ))
            .await
        }
        crate::database::TerminalCandidateAuthority::DependencyRetraction(authority) => {
            let author = load_registration_ref(
                storage,
                root,
                &verification.candidate.commit.value.author_registration,
            )
            .await
            .map_err(StorePullError::Object)?
            .value;
            super::remote_object::VerifiedCandidateNonactivation::from_verified_dependency_retraction_authority(
                authority.clone(),
                target,
                &author,
                verification.candidate.head.object.clone(),
            )
            .map_err(|error| StorePullError::Database(error.to_string()))
        }
    }
}

pub(crate) async fn resume_merge_retraction_cleanups(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<(), StorePullError> {
    for candidate in crate::sync::store::database::StoreDatabase::new(db)
        .pending_merge_retraction_cleanups()
        .await?
    {
        let verification = crate::sync::store::database::StoreDatabase::new(db)
            .merge_retraction_cleanup_verification(candidate.clone())
            .await?;
        let verified = verify_terminal_cleanup_candidate(db, storage, root, &verification).await?;
        crate::sync::store::database::StoreDatabase::new(db)
            .confirm_merge_retraction_cleanup_nonactivation(candidate.clone(), verified)
            .await?;
        for target in crate::sync::store::database::StoreDatabase::new(db)
            .merge_retraction_cleanup_targets(candidate.clone())
            .await?
        {
            super::store_objects::delete_exact_object(storage, &target.object).await?;
            db.mark_candidate_cleanup_absent(target.object).await?;
        }
        crate::sync::store::database::StoreDatabase::new(db)
            .finish_merge_retraction_cleanup(candidate)
            .await?;
    }
    Ok(())
}
