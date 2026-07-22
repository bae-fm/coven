use super::registration_authority::verify_merge_provider_administrator;
use super::*;

pub(in crate::sync::store_engine) async fn verify_accepted_provider_access_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    access: &crate::sync::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &crate::sync::provider::ProviderAdminGrantRecord,
    administrator: &StoreDeviceRegistration,
) -> Result<(), StorePullError> {
    let activation =
        load_provider_access_activation(storage, root, root_value, access, administrator).await?;
    let membership = load_merge_predecessor_membership(storage, root, &activation.membership_state)
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
    if !verify_merge_provider_administrator(
        &membership,
        &access.grant.administrator_grant,
        &activation.author_registration,
        provider_admin,
    ) {
        return Err(StorePullError::Database(
            "device provider approval activation lacks exact predecessor provider-administrator authority"
                .to_string(),
        ));
    }
    if !current_history_contains(storage, root, root_value, &membership, &access.activation).await?
    {
        return Err(StorePullError::Database(
            "device provider approval activation is absent from current accepted Store history"
                .to_string(),
        ));
    }
    Ok(())
}

async fn current_history_contains(
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
