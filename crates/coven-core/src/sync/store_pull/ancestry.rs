use super::*;

pub(crate) async fn load_commit_with_author(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &StoreBatchCommitRef,
) -> Result<(StoreBatchCommit, StoreDeviceRegistration), StoreObjectError> {
    let root_value = load_store_protocol_root(storage, root).await?.value;
    load_commit_with_author_at_root(storage, root, &root_value, reference).await
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CommitCoverageError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("exact Store ancestry is missing commit {commit_hash}")]
    MissingAncestry { commit_hash: ObjectHash },
}

pub(crate) async fn commit_position_covers(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    covering: &StoreBatchCommitRef,
    covered: &StoreBatchCommitRef,
) -> Result<bool, CommitCoverageError> {
    let same_stream = match (&covering.coord, &covered.coord) {
        (
            super::store_commit::StoreCommitCoord::MergeConcurrent {
                stream_id: covering,
                ..
            },
            super::store_commit::StoreCommitCoord::MergeConcurrent {
                stream_id: covered, ..
            },
        ) => covering == covered,
        (
            super::store_commit::StoreCommitCoord::Serial { .. },
            super::store_commit::StoreCommitCoord::Serial { .. },
        ) => true,
        _ => false,
    };
    if !same_stream || covering.coord.sequence() < covered.coord.sequence() {
        return Ok(false);
    }
    let mut cursor = covering.clone();
    while cursor.coord.sequence() > covered.coord.sequence() {
        let (commit, _) = load_commit_with_author(storage, root, &cursor).await?;
        cursor =
            commit
                .order
                .predecessor()
                .cloned()
                .ok_or(CommitCoverageError::MissingAncestry {
                    commit_hash: cursor.commit_hash,
                })?;
    }
    Ok(cursor == *covered)
}

fn coverage_error(error: CommitCoverageError) -> StorePullError {
    match error {
        CommitCoverageError::Object(error) => StorePullError::Object(error),
        CommitCoverageError::MissingAncestry { commit_hash } => StorePullError::Database(format!(
            "exact Store ancestry is missing commit {commit_hash}"
        )),
    }
}

pub(crate) async fn history_cut_covers(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    cut: &StoreHistoryCut,
    covered: &StoreBatchCommitRef,
) -> Result<bool, StorePullError> {
    let covering = match (cut, &covered.coord) {
        (
            StoreHistoryCut::MergeConcurrent(frontier),
            StoreCommitCoord::MergeConcurrent { stream_id, .. },
        ) => frontier.get(stream_id),
        (
            StoreHistoryCut::Serial(StoreSerialPredecessor::Commit(reference)),
            StoreCommitCoord::Serial { .. },
        ) => Some(reference),
        _ => None,
    };
    match covering {
        Some(covering) => commit_position_covers(storage, root, covering, covered)
            .await
            .map_err(coverage_error),
        None => Ok(false),
    }
}

fn verify_provider_access_evidence<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    verified_root: &'a super::store_commit::StoreProtocolRoot,
    access: &'a super::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &'a super::provider::ProviderAdminGrantRecord,
    administrator: &'a StoreDeviceRegistration,
    accepted_predecessor: Option<&'a VerifiedAcceptedPredecessor<'a>>,
) -> StorePullFuture<'a, StoreBatchCommit> {
    Box::pin(verify_provider_access_evidence_impl(
        storage,
        root,
        verified_root,
        access,
        provider_admin,
        administrator,
        accepted_predecessor,
    ))
}

async fn verify_provider_access_evidence_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    verified_root: &super::store_commit::StoreProtocolRoot,
    access: &super::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &super::provider::ProviderAdminGrantRecord,
    administrator: &StoreDeviceRegistration,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
) -> Result<StoreBatchCommit, StorePullError> {
    let grant = super::store_objects::load_provider_access_grant_ref_with_root(
        storage,
        root,
        verified_root,
        &access.grant_ref,
        administrator,
    )
    .await?;
    if grant.value != access.grant {
        return Err(StorePullError::Database(
            "device provider approval embeds a different access grant than its exact reference"
                .to_string(),
        ));
    }
    if let Some(verified) = accepted_predecessor
        .map(|predecessor| predecessor.serial_history_commit(&access.activation))
        .transpose()?
        .flatten()
    {
        let activation = &verified.commit;
        if activation.provider_access_grants() != std::slice::from_ref(&access.grant_ref)
            || activation.author_registration != access.grant.administrator
            || verified.author != *administrator
        {
            return Err(StorePullError::Database(
                "device provider approval activation is not the administrator's exact sole access grant"
                    .to_string(),
            ));
        }
        let provider_admin_state = &verified.authorization_before.provider_admin;
        if !provider_admin_state.authorizes(
            &access.grant.administrator_grant,
            &activation.author_registration,
        ) || provider_admin_state
            .records()
            .get(&access.grant.administrator_grant)
            != Some(provider_admin)
        {
            return Err(StorePullError::Database(
                "device provider approval activation lacks exact predecessor provider-administrator authority"
                    .to_string(),
            ));
        }
        return Ok(activation.clone());
    }
    if let Some(verified) = accepted_predecessor
        .map(|predecessor| predecessor.merge_history_commit(&access.activation))
        .transpose()?
        .flatten()
    {
        let activation = &verified.commit;
        if activation.provider_access_grants() != std::slice::from_ref(&access.grant_ref)
            || activation.author_registration != access.grant.administrator
        {
            return Err(StorePullError::Database(
                "device provider approval activation is not the administrator's exact sole access grant"
                    .to_string(),
            ));
        }
        let authority =
            RegistrationPredecessorAuthority::MergeConcurrent(&verified.predecessor_membership);
        if !authority.verifies_provider_administrator(
            &access.grant.administrator_grant,
            &activation.author_registration,
            provider_admin,
        ) {
            return Err(StorePullError::Database(
                "device provider approval activation lacks exact predecessor provider-administrator authority"
                    .to_string(),
            ));
        }
        return Ok(activation.clone());
    }
    let (activation, author) =
        load_commit_with_author_at_root(storage, root, verified_root, &access.activation).await?;
    if activation.provider_access_grants() != std::slice::from_ref(&access.grant_ref)
        || activation.author_registration != access.grant.administrator
        || author != *administrator
    {
        return Err(StorePullError::Database(
            "device provider approval activation is not the administrator's exact sole access grant"
                .to_string(),
        ));
    }
    let authorization =
        load_device_join_authorization(storage, root, &activation.membership_state).await?;
    let authority = match &authorization {
        DeviceJoinBootstrapAuthorization::MergeConcurrent { chain, .. } => {
            RegistrationPredecessorAuthority::MergeConcurrent(chain)
        }
        DeviceJoinBootstrapAuthorization::Serial {
            position,
            authorization,
            ..
        } => RegistrationPredecessorAuthority::Serial {
            authorization,
            position: position.clone(),
            history: SerialAuthorizationHistory::ExactPredecessor,
        },
    };
    if !authority.verifies_provider_administrator(
        &access.grant.administrator_grant,
        &activation.author_registration,
        provider_admin,
    ) {
        return Err(StorePullError::Database(
            "device provider approval activation lacks exact predecessor provider-administrator authority"
                .to_string(),
        ));
    }
    Ok(activation)
}

pub(super) fn load_verified_device_join_attempt_evidence_ref<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    reference: &'a super::store_commit::DeviceJoinAttemptRef,
    owner: &'a StoreDeviceRegistration,
    accepted_predecessor: Option<&'a VerifiedAcceptedPredecessor<'a>>,
) -> StorePullFuture<'a, VerifiedObject<DeviceJoinAttempt>> {
    Box::pin(async move {
        let attempt =
            load_owner_signed_device_join_attempt_ref(storage, root, reference, owner).await?;
        let verified_root = load_store_protocol_root(storage, root).await?;
        if attempt.value.store_root != *root {
            return Err(StorePullError::Database(
                "device join attempt names another Store root".to_string(),
            ));
        }
        let offer = &attempt.value.provider_approval.request.offer;
        let administrator =
            load_registration_ref(storage, root, &offer.provider_admin.administrator)
                .await?
                .value;
        attempt
            .value
            .provider_approval
            .verify(&verified_root, owner, &administrator)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        verify_provider_access_evidence(
            storage,
            root,
            &verified_root.value,
            &attempt.value.provider_approval.access_grant,
            &offer.provider_admin,
            &administrator,
            accepted_predecessor,
        )
        .await?;
        if !history_cut_covers(
            storage,
            root,
            &attempt.value.bootstrap_cut,
            &attempt.value.provider_approval.access_grant.activation,
        )
        .await?
        {
            return Err(StorePullError::Database(
            "device join attempt predecessor cut does not include its provider-access activation"
                .to_string(),
        ));
        }
        Ok(attempt)
    })
}

pub(crate) fn load_verified_device_join_attempt_ref<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    reference: &'a super::store_commit::DeviceJoinAttemptRef,
    owner: &'a StoreDeviceRegistration,
) -> StorePullFuture<'a, VerifiedObject<DeviceJoinAttempt>> {
    Box::pin(async move {
        let attempt =
            load_verified_device_join_attempt_evidence_ref(storage, root, reference, owner, None)
                .await?;
        match &attempt.value.bootstrap_cut {
            StoreHistoryCut::MergeConcurrent(_) => {
                Box::pin(crate::sync::store_engine::verify_store_history_authority(
                    storage,
                    None,
                    root,
                    &attempt.value.bootstrap_cut,
                    &attempt.value.membership,
                ))
                .await?;
            }
            StoreHistoryCut::Serial(cut_position) => {
                let authorization =
                    load_device_join_authorization(storage, root, &attempt.value.membership)
                        .await?;
                let DeviceJoinBootstrapAuthorization::Serial { position, .. } = authorization
                else {
                    return Err(StorePullError::Database(
                        "Serial device join attempt carries Merge membership authority".to_string(),
                    ));
                };
                if &position != cut_position {
                    return Err(StorePullError::Serial(
                    "device join attempt cut differs from its exact Serial authorization position"
                        .to_string(),
                ));
                }
            }
        }
        Ok(attempt)
    })
}

pub(crate) async fn verify_accepted_provider_access_activation(
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    access: &super::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &super::provider::ProviderAdminGrantRecord,
    administrator: &StoreDeviceRegistration,
) -> Result<(), StorePullError> {
    let root_value = load_store_protocol_root(storage, root).await?;
    let activation = verify_provider_access_evidence(
        storage,
        root,
        &root_value.value,
        access,
        provider_admin,
        administrator,
        None,
    )
    .await?;
    let accepted = match root_value.value.descriptor.write_policy {
        crate::WritePolicy::MergeConcurrent => {
            if coordination.is_some() {
                return Err(StorePullError::Database(
                    "Merge provider-access verification received Serial coordination".to_string(),
                ));
            }
            let membership =
                load_merge_predecessor_membership(storage, root, &activation.membership_state)
                    .await
                    .map_err(|error| match error {
                        RegistrationLoadError::Object(error) => StorePullError::Object(error),
                        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
                    })?;
            current_merge_history_contains(
                storage,
                root,
                &root_value.value,
                &membership,
                &access.activation,
            )
            .await?
        }
        crate::WritePolicy::Serial => {
            let coordination = coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "provider-access verification requires coordination capability".to_string(),
                )
            })?;
            let head = read_serial_head(storage, coordination, root).await?;
            load_authorized_serial_chain(storage, root, &head.head)
                .await?
                .iter()
                .any(|accepted| accepted.commit_ref == access.activation)
        }
    };
    if !accepted {
        return Err(StorePullError::Database(
            "device provider approval activation is absent from current accepted Store history"
                .to_string(),
        ));
    }
    Ok(())
}

async fn current_merge_history_contains(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    membership: &MembershipChain,
    expected: &StoreBatchCommitRef,
) -> Result<bool, StorePullError> {
    let initial = verify_merge_history_refs(storage, root, [expected.clone()]).await?;
    let mut state = initial
        .commits
        .get(expected)
        .ok_or_else(|| {
            StorePullError::Database(
                "provider-access activation is absent from its verified Merge graph".to_string(),
            )
        })?
        .state_after
        .clone();
    let mut registrations = BTreeMap::new();
    let founder = load_founder_registration_with_root(storage, root, root_value).await?;
    let founder_ref = StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object);
    registrations.insert(founder_ref.device_id, (founder_ref, founder.value));
    for recovered in discover_merge_owner_recoveries(storage, root, root_value, membership).await? {
        registrations.insert(recovered.0.device_id, recovered);
    }
    load_state_registrations(storage, root, &state, &mut registrations).await?;

    let mut accepted = BTreeMap::new();
    let mut observed_states = BTreeSet::new();
    loop {
        let mut next = BTreeMap::new();
        for (registration_ref, registration) in registrations.values() {
            let inactive_cut = match state.devices.get(&registration_ref.device_id) {
                Some(record) if record.registration != *registration_ref => {
                    return Err(StorePullError::Database(
                        "current Merge device state names another registration revision"
                            .to_string(),
                    ));
                }
                Some(record) => match &record.status {
                    StoreDeviceStatus::Active => None,
                    StoreDeviceStatus::Inactive { accepted_cut, .. } => Some(accepted_cut),
                },
                None => None,
            };
            let discovered =
                discover_merge_stream(storage, root, registration_ref, registration, inactive_cut)
                    .await?;
            if matches!(discovered.block, Some(MergeStreamBlock::Authenticated(_))) {
                return Err(StorePullError::Database(
                    "an authenticated Merge stream position cannot be verified".to_string(),
                ));
            }
            if let Some((_, _, reference, _)) = discovered.commits.last() {
                let StoreCommitCoord::MergeConcurrent { stream_id, .. } = reference.coord else {
                    return Err(StorePullError::Database(
                        "Merge stream discovery returned a Serial commit".to_string(),
                    ));
                };
                next.insert(stream_id, reference.clone());
            }
        }
        let history = verify_merge_history_refs(storage, root, next.values().cloned()).await?;
        let next_state = if next.is_empty() {
            history.genesis.clone()
        } else {
            ResolvedStoreDeviceState::merge(
                next.values()
                    .map(|reference| {
                        history
                            .commits
                            .get(reference)
                            .map(|commit| commit.state_after.clone())
                            .ok_or_else(|| {
                                StorePullError::Database(
                                    "current Merge frontier is absent from its verified graph"
                                        .to_string(),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?
        };
        let registration_count = registrations.len();
        load_state_registrations(storage, root, &next_state, &mut registrations).await?;
        let stable =
            next == accepted && next_state == state && registrations.len() == registration_count;
        if stable {
            return Ok(history.commits.contains_key(expected));
        }
        let state_fingerprint = ObjectHash::digest(
            &serde_json::to_vec(&(&next, &next_state))
                .map_err(|error| StorePullError::Database(error.to_string()))?,
        );
        if !observed_states.insert(state_fingerprint) {
            return Err(StorePullError::Database(
                "current Merge authority discovery does not reach one stable frontier".to_string(),
            ));
        }
        accepted = next;
        state = next_state;
    }
}

async fn load_state_registrations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &ResolvedStoreDeviceState,
    registrations: &mut BTreeMap<
        super::store_commit::StoreDeviceId,
        (StoreDeviceRegistrationRef, StoreDeviceRegistration),
    >,
) -> Result<(), StorePullError> {
    for (device_id, record) in &state.devices {
        if registrations
            .get(device_id)
            .is_some_and(|(reference, _)| reference == &record.registration)
        {
            continue;
        }
        let registration = load_registration_ref(storage, root, &record.registration).await?;
        if registration.value.device_id != *device_id {
            return Err(StorePullError::Database(
                "current Merge device state registration has another device id".to_string(),
            ));
        }
        registrations.insert(
            *device_id,
            (record.registration.clone(), registration.value),
        );
    }
    Ok(())
}

pub(crate) fn load_commit_with_author_at_root<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    root_value: &'a super::store_commit::StoreProtocolRoot,
    reference: &'a StoreBatchCommitRef,
) -> super::store_objects::StoreObjectFuture<'a, (StoreBatchCommit, StoreDeviceRegistration)> {
    Box::pin(load_commit_with_author_at_root_impl(
        storage, root, root_value, reference,
    ))
}

async fn load_commit_with_author_at_root_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    reference: &StoreBatchCommitRef,
) -> Result<(StoreBatchCommit, StoreDeviceRegistration), StoreObjectError> {
    let semantic_prefix =
        super::store_commit::semantic_prefix_from_exact_object(&reference.object, ".json")
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: "Store candidate commit".to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let bytes = storage
        .read_protocol_object(&context, &reference.object, &semantic_prefix)
        .await
        .map_err(StoreObjectError::Storage)?;
    #[derive(serde::Deserialize)]
    struct StoreCommitAuthorProjection {
        author_registration: StoreDeviceRegistrationRef,
    }

    let parse_bytes = bytes.clone();
    let author_reference = run_blocking_object_verification(
        &semantic_prefix,
        &reference.object,
        Box::new(move || {
            serde_json::from_slice::<StoreCommitAuthorProjection>(&parse_bytes)
                .map(|projection| projection.author_registration)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
        }),
    )
    .await?;
    let author = load_registration_ref_with_root(storage, root, root_value, &author_reference)
        .await?
        .value;
    let expected_reference = reference.clone();
    let expected_author = author.clone();
    let store_root_hash = root.store_root_hash;
    let verify_bytes = bytes;
    let commit = run_blocking_object_verification(
        &semantic_prefix,
        &reference.object,
        Box::new(move || {
            let commit = StoreBatchCommit::parse_at(
                &verify_bytes,
                store_root_hash,
                &expected_reference.coord,
                &expected_author,
            )?;
            expected_reference.verify_commit(&commit)?;
            Ok(commit)
        }),
    )
    .await?;
    Ok((commit, author))
}
