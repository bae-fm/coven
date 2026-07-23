use super::*;

pub(super) async fn replay_merge_device_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    tip: &StoreBatchCommitRef,
) -> Result<
    (
        ResolvedStoreDeviceState,
        VerifiedStoreDeviceOperations,
        StoreBatchCommit,
        Option<VerifiedCircleActivations>,
    ),
    StorePullError,
> {
    let history = verify_merge_history_refs(storage, root, [tip.clone()]).await?;
    let verified = history.commits.get(tip).ok_or_else(|| {
        StorePullError::Database(
            "author exclusion activation is absent from its verified history".to_string(),
        )
    })?;
    Ok((
        verified.predecessor_state.clone(),
        verified.operations.clone(),
        verified.commit.clone(),
        verified
            .membership_control
            .as_ref()
            .map(|control| control.activations.clone()),
    ))
}

pub(super) async fn verified_terminal_merge_retractions(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activation_head: &StoreDeviceHead,
    activation_head_object: &ExactObjectRef,
    activation_commit_ref: &StoreBatchCommitRef,
    activation_commit: &StoreBatchCommit,
    activation_predecessor_state: &ResolvedStoreDeviceState,
    activation_predecessor_membership: &MembershipChain,
    device_operations: &VerifiedStoreDeviceOperations,
    loaded_predecessor_memberships: &LoadedMergePredecessorMemberships,
) -> Result<Vec<super::remote_object::VerifiedCandidateNonactivation>, StorePullError> {
    let retained = Box::pin(database.retained_merge_replay_inputs()).await?;
    let activation_head_ref = super::store_commit::StoreDeviceHeadRef {
        head_hash: activation_head.head_hash(),
        object: activation_head_object.clone(),
    };
    let current_membership_ref = &activation_commit.membership_state;
    let MembershipStatus::Resolved(current_resolved) = activation_predecessor_membership.status()
    else {
        return Err(StorePullError::Database(
            "Merge terminal retraction witness membership is conflicted".to_string(),
        ));
    };
    let mut retractions = Vec::new();
    for materialization in &retained {
        let mut locator = Box::pin(database.author_exclusion_activation_for_candidate(
            materialization.commit_ref().clone(),
            materialization.commit().author_registration.clone(),
        ))
        .await?;
        if locator.is_none() {
            let expected_stream =
                super::store_commit::StreamActivation::device_authorized_stream_id(
                    root.store_root_hash,
                    &materialization.commit().author_registration,
                    super::store_commit::StreamAnchorDomain::StoreAnnouncements,
                );
            for (exclusion, accepted_cut) in device_operations.exclusions() {
                if exclusion.proposal.target != materialization.commit().author_registration {
                    continue;
                }
                let accepted_cut = &accepted_cut.0;
                let beyond_cutoff = accepted_cut.get(&expected_stream).is_none_or(|reference| {
                    materialization.commit_ref().coord.sequence() > reference.coord.sequence()
                });
                if beyond_cutoff {
                    locator = Some(crate::database::AuthorExclusionActivationLocator::verified(
                        exclusion.clone(),
                        accepted_cut.clone(),
                        activation_commit_ref.clone(),
                        activation_head_ref.clone(),
                    ));
                    break;
                }
            }
        }
        let Some(locator) = locator else {
            let Some(authority) = materialization.commit().membership_authority.as_ref() else {
                continue;
            };
            let predecessor_membership =
                loaded_predecessor_memberships.membership_for(materialization.commit_ref())?;
            let MembershipStatus::Resolved(predecessor_resolved) = predecessor_membership.status()
            else {
                return Err(StorePullError::Database(
                    "retained candidate predecessor membership is conflicted".to_string(),
                ));
            };
            let mut matching = predecessor_resolved
                .active_grants()
                .filter(|(_, record)| &record.creation_authority == authority);
            let Some((grant_id, _)) = matching.next() else {
                return Err(StorePullError::Database(
                    "retained candidate has no exact predecessor grant authority".to_string(),
                ));
            };
            if matching.next().is_some() {
                return Err(StorePullError::Database(
                    "retained candidate authority identifies multiple predecessor grants"
                        .to_string(),
                ));
            }
            if !matches!(
                current_resolved.grants.get(grant_id),
                Some(super::causal_grants::GrantState::Tombstoned { .. })
            ) {
                continue;
            }
            let nonactivation = Box::pin(verify_membership_grant_revocation_nonactivation(
                storage,
                root,
                grant_id,
                current_membership_ref,
                activation_commit_ref,
                &activation_head_ref,
                materialization.commit_ref(),
                materialization.commit(),
                materialization.activation_head(),
                materialization.activation_head_object(),
            ))
            .await?;
            retractions.push(nonactivation);
            continue;
        };
        let nonactivation = Box::pin(
            verify_author_exclusion_nonactivation_with_verified_operation(
                storage,
                root,
                &locator,
                activation_head,
                activation_head_object,
                activation_commit_ref,
                activation_commit,
                activation_predecessor_state,
                device_operations,
                materialization.commit_ref(),
                materialization.commit(),
                materialization.activation_head(),
                materialization.activation_head_object(),
            ),
        )
        .await?;
        retractions.push(nonactivation);
    }
    let mut verified_by_reference = retractions
        .into_iter()
        .map(|verified| {
            let reference = verified
                .candidate_reference()
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            Ok((reference, verified))
        })
        .collect::<Result<BTreeMap<_, _>, StorePullError>>()?;
    loop {
        let mut additions = Vec::new();
        for materialization in &retained {
            if verified_by_reference.contains_key(materialization.commit_ref()) {
                continue;
            }
            let dependency = commit_predecessor_references(materialization.commit())
                .into_iter()
                .filter_map(|reference| {
                    verified_by_reference
                        .get(&reference)
                        .map(|verified| (reference, verified))
                })
                .next();
            let Some((_dependency_reference, dependency)) = dependency else {
                continue;
            };
            let author = Box::pin(database.activated_store_device_registration(
                materialization.commit().author_registration.clone(),
            ))
            .await?;
            let verified =
                super::remote_object::VerifiedCandidateNonactivation::dependency_retraction(
                    dependency,
                    super::store_commit::StoreBatchCommitDeletionTarget {
                        coord: materialization.commit_ref().coord.clone(),
                        object: materialization.commit_ref().object.clone(),
                        canonical_signed_bytes: materialization.commit().to_bytes(),
                    },
                    &author,
                    materialization.activation_head_object().clone(),
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            additions.push((materialization.commit_ref().clone(), verified));
        }
        if additions.is_empty() {
            break;
        }
        for (reference, verified) in additions {
            if verified_by_reference.insert(reference, verified).is_some() {
                return Err(StorePullError::Database(
                    "transitive Merge retraction constructed duplicate proof".to_string(),
                ));
            }
        }
    }
    let removed = verified_by_reference
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if retained.iter().any(|materialization| {
        !removed.contains(materialization.commit_ref())
            && materialization
                .history_summary()
                .causal_cut
                .values()
                .any(|reference| removed.contains(reference))
    }) {
        return Err(StorePullError::Database(
            "surviving retained Merge summary contains a retracted dependency".to_string(),
        ));
    }
    Ok(verified_by_reference.into_values().collect())
}

pub(crate) fn replay_retained_merge_projection_on(
    live: &rusqlite::Transaction<'_>,
    blob_decls: &BlobDecls,
    gates: &super::gate::Gates,
    synced_tables: &[SyncedTable],
    routing_key: Option<&super::circle::RowRoutingKey>,
    retracted: &BTreeSet<StoreBatchCommitRef>,
    history_cut: Option<&CommitFrontier>,
    include_local_write_overlays: bool,
    local_store_membership: LocalStoreMembership,
) -> Result<rusqlite::Connection, DbError> {
    super::retained_replay::validate_merge_generation_zero_preconditions(live)?;
    let baseline =
        crate::sync::store::database::StoreDatabase::generation_zero_replay_baseline_on(live)?;
    let replay = baseline.open_image()?;
    replay
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(DbError::from)?;
    let schema = Arc::new(TableSchema::for_apply(&replay, synced_tables, gates)?);
    let retained =
        crate::sync::store::database::StoreDatabase::load_retained_merge_replay_inputs_on(live)?;
    let circle_epochs =
        crate::sync::store::database::StoreDatabase::circle_replay_epoch_index_on(live)?;
    let active_references = retained
        .iter()
        .filter(|materialization| {
            !retracted.contains(materialization.commit_ref())
                && history_cut
                    .is_none_or(|cutoff| cutoff.covers_commit(materialization.commit_ref()))
        })
        .map(|materialization| materialization.commit_ref().clone())
        .collect::<BTreeSet<_>>();
    for materialization in retained
        .iter()
        .filter(|materialization| active_references.contains(materialization.commit_ref()))
    {
        let mut dependencies = materialization
            .commit()
            .order
            .dependencies()
            .values()
            .cloned()
            .collect::<BTreeSet<_>>();
        if let Some(predecessor) = materialization.commit().order.predecessor() {
            dependencies.insert(predecessor.clone());
        }
        for dependency in dependencies {
            if retracted.contains(&dependency) {
                return Err(DbError::Message(format!(
                    "surviving retained Merge commit {:?} depends on retracted commit {:?}",
                    materialization.commit_ref(),
                    dependency
                )));
            }
            if !active_references.contains(&dependency)
                && !replay_dependency_is_baseline_covered(&dependency, &baseline.exact_cut)
            {
                return Err(DbError::Message(format!(
                    "surviving retained Merge commit {:?} has unretained dependency {:?}",
                    materialization.commit_ref(),
                    dependency
                )));
            }
        }
    }
    let active_accepted_writes = retained
        .iter()
        .filter(|materialization| active_references.contains(materialization.commit_ref()))
        .map(|materialization| materialization.commit().write_id.clone())
        .collect::<BTreeSet<_>>();
    let retracted_writes = retained
        .iter()
        .filter(|materialization| retracted.contains(materialization.commit_ref()))
        .map(|materialization| materialization.commit().write_id.clone())
        .collect::<BTreeSet<_>>();
    let write_overlays = if include_local_write_overlays {
        crate::sync::store::database::StoreDatabase::load_merge_replay_write_overlays_on(
            live,
            &active_accepted_writes,
            &retracted_writes,
        )?
    } else {
        Vec::new()
    };
    let mut pending = retained
        .into_iter()
        .filter(|materialization| active_references.contains(materialization.commit_ref()))
        .map(|materialization| (materialization.commit_ref().clone(), materialization))
        .collect::<BTreeMap<_, _>>();
    let mut applied = BTreeSet::new();
    while !pending.is_empty() {
        let ready = pending
            .iter()
            .filter_map(|(reference, materialization)| {
                let predecessor_ready =
                    materialization
                        .commit()
                        .order
                        .predecessor()
                        .is_none_or(|predecessor| {
                            replay_dependency_is_settled(predecessor, &applied, &baseline.exact_cut)
                        });
                let dependencies_ready = materialization
                    .commit()
                    .order
                    .dependencies()
                    .values()
                    .all(|dependency| {
                        replay_dependency_is_settled(dependency, &applied, &baseline.exact_cut)
                    });
                (predecessor_ready && dependencies_ready).then(|| reference.clone())
            })
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err(DbError::Message(
                "retained Merge replay is cyclic or has an unresolved dependency".to_string(),
            ));
        }
        let mut made_progress = false;
        for reference in ready {
            let materialization = pending
                .get(&reference)
                .expect("ready retained replay input remains pending")
                .clone();
            let timestamp_policy = match materialization.package_application() {
                None => IncomingTimestampPolicy::LocallyAuthored,
                Some(crate::database::RetainedPackageApplication::Received {
                    receiver_wall_ms,
                }) => IncomingTimestampPolicy::Received { receiver_wall_ms },
                Some(crate::database::RetainedPackageApplication::LocallyAuthored) => {
                    IncomingTimestampPolicy::LocallyAuthored
                }
            };
            let mut retained_packages = Vec::new();
            for package in materialization.packages() {
                if let crate::sync::audience_package::PackageAudience::Circle {
                    circle_id,
                    control,
                    ..
                } = package.audience()
                {
                    if !circle_epochs.permits(materialization.commit_ref(), *circle_id, control)? {
                        continue;
                    }
                    if !local_store_membership.retains_circle_rows() {
                        continue;
                    }
                }
                retained_packages.push(package.clone());
            }
            let package_application = if retained_packages.is_empty() {
                None
            } else {
                Some(materialization.package_application().ok_or_else(|| {
                    DbError::Message(
                        "retained Merge packages lack their application timestamp".to_string(),
                    )
                })?)
            };
            let packages = retained_packages
                .into_iter()
                .map(|package| {
                    let changeset =
                        ValidatedChangeset::new(package.changeset().to_vec(), schema.clone())
                            .map_err(|error| {
                                DbError::Message(format!(
                                    "retained Merge replay changeset: {error}"
                                ))
                            })?;
                    Ok(PreparedMergeMaterializationPackage { package, changeset })
                })
                .collect::<Result<Vec<_>, DbError>>()?;
            let membership_remote_objects =
                if let Some(objects) = materialization.membership_objects() {
                    let family = materialization.commit().candidate_family();
                    let owner = materialization.commit_ref();
                    let entry_bytes =
                        retained_membership_bytes_on(live, &objects.entry().object, "entry")?;
                    let head_bytes =
                        retained_membership_bytes_on(live, &objects.head().object, "head")?;
                    let resolution_bytes = objects
                        .resolution()
                        .map(|resolution| {
                            retained_membership_bytes_on(live, &resolution.object, "resolution")
                        })
                        .transpose()?;
                    super::materialization::activated_merge_membership_remote_objects(
                        family,
                        objects,
                        entry_bytes,
                        head_bytes,
                        resolution_bytes,
                        owner,
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?
                } else {
                    Vec::new()
                };
            let replay_materialization = PreparedMergeMaterialization {
                root: materialization.root().clone(),
                commit: materialization.commit().clone(),
                commit_ref: materialization.commit_ref().clone(),
                activation_head: materialization.activation_head().clone(),
                activation_head_object: materialization.activation_head_object().clone(),
                history_summary: materialization.history_summary().clone(),
                membership_objects: materialization.membership_objects().cloned(),
                membership_remote_objects,
                registrations: materialization.registrations().to_vec(),
                packages,
                device_operations: materialization.device_operations().clone(),
                circle_activations: materialization.circle_activations().clone(),
                package_application,
            };
            let tx = replay.unchecked_transaction().map_err(DbError::from)?;
            let outcome = apply_prepared_merge_materialization_on(
                &tx,
                blob_decls,
                gates,
                synced_tables,
                routing_key,
                local_store_membership,
                timestamp_policy,
                replay_materialization,
            )
            .map_err(|error| {
                DbError::Message(format!(
                    "apply retained Merge commit {reference:?} during canonical replay: {error}"
                ))
            })?;
            match outcome.outcome {
                ApplyOutcome::Applied(_) => {
                    tx.commit().map_err(DbError::from)?;
                    pending.remove(&reference);
                    applied.insert(reference);
                    made_progress = true;
                }
                ApplyOutcome::Held(HeldStorePositionReason::ForeignKeyDependency) => {
                    tx.rollback().map_err(DbError::from)?;
                }
                ApplyOutcome::Held(reason) => {
                    tx.rollback().map_err(DbError::from)?;
                    return Err(DbError::Message(format!(
                        "retained Merge replay held accepted commit {reference:?}: {reason:?}"
                    )));
                }
            }
        }
        if !made_progress {
            return Err(DbError::Message(
                "retained Merge replay has an unresolved foreign-key dependency".to_string(),
            ));
        }
    }
    for overlay in write_overlays {
        let tx = replay.unchecked_transaction().map_err(DbError::from)?;
        tx.pragma_update(None, "defer_foreign_keys", "ON")
            .map_err(DbError::from)?;
        let partitions = overlay
            .partitions
            .store
            .into_iter()
            .chain(overlay.partitions.circles)
            .chain(overlay.partitions.local);
        for partition in partitions {
            let changeset =
                ValidatedChangeset::new(partition.changeset, schema.clone()).map_err(|error| {
                    DbError::Message(format!(
                        "local replay write {} changeset: {error}",
                        overlay.write_id
                    ))
                })?;
            let applied = resolve_and_apply_changeset_with_policy_on(
                &tx,
                changeset,
                IncomingTimestampPolicy::LocallyAuthored,
            )?;
            if applied.had_fk_violations || !applied.constraint_conflict_tables.is_empty() {
                return Err(DbError::Message(format!(
                    "local replay write {} conflicts with accepted history",
                    overlay.write_id
                )));
            }
        }
        let violations: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if violations {
            return Err(DbError::Message(format!(
                "local replay write {} violates foreign keys",
                overlay.write_id
            )));
        }
        tx.commit().map_err(DbError::from)?;
    }
    Ok(replay)
}

fn retained_membership_bytes_on(
    live: &rusqlite::Transaction<'_>,
    object: &ExactObjectRef,
    kind: &str,
) -> Result<super::materialization::MembershipAuthorityBytes, DbError> {
    let object_id = super::remote_object::remote_object_id(object);
    let remote = crate::database::load_remote_object_on(live, object_id).map_err(|error| {
        DbError::Message(format!(
            "load retained Merge membership {kind} {object_id} for replay: {error}"
        ))
    })?;
    if remote.object() != object {
        return Err(DbError::Message(format!(
            "retained Merge membership {kind} {object_id} has different exact object"
        )));
    }
    let stored = remote
        .bytes()
        .stored()
        .inline_bytes()
        .ok_or_else(|| {
            DbError::Message(format!(
                "retained Merge membership {kind} {object_id} has no inline stored bytes"
            ))
        })?
        .to_vec();
    Ok(super::materialization::MembershipAuthorityBytes::new(
        remote.bytes().canonical_semantic_bytes().to_vec(),
        stored,
    ))
}

fn replay_dependency_is_settled(
    dependency: &StoreBatchCommitRef,
    applied: &BTreeSet<StoreBatchCommitRef>,
    baseline: &CommitFrontier,
) -> bool {
    if applied.contains(dependency) {
        return true;
    }
    replay_dependency_is_baseline_covered(dependency, baseline)
}

fn replay_dependency_is_baseline_covered(
    dependency: &StoreBatchCommitRef,
    baseline: &CommitFrontier,
) -> bool {
    baseline
        .0
        .get(&dependency.coord.stream_id)
        .is_some_and(|covered| {
            covered.coord.sequence() > dependency.coord.sequence
                || (covered.coord.sequence() == dependency.coord.sequence && covered == dependency)
        })
}
