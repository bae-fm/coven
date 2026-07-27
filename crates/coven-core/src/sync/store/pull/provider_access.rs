use super::registration_authority::verify_merge_provider_administrator;
use super::*;

pub(in crate::sync::store) async fn verify_accepted_provider_access_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    access: &crate::sync::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &crate::sync::provider::ProviderAdminGrantRecord,
    administrator: &StoreDeviceRegistration,
) -> Result<(), StorePullError> {
    let mut history_verifier = MergeHistoryVerifier::new(storage, root).await?;
    let activation = load_provider_access_activation(
        &mut history_verifier,
        storage,
        root,
        access,
        administrator,
    )
    .await?;
    let membership =
        load_merge_predecessor_membership(storage, root, &activation.value().membership_state)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
    if !verify_merge_provider_administrator(
        &membership,
        &access.grant.administrator_grant,
        &activation.value().author_registration,
        provider_admin,
    ) {
        return Err(StorePullError::Database(
            "device provider approval activation lacks exact predecessor provider-administrator authority"
                .to_string(),
        ));
    }
    if !current_history_contains(
        &mut history_verifier,
        storage,
        root,
        &membership,
        &access.activation,
    )
    .await?
    {
        return Err(StorePullError::Database(
            "device provider approval activation is absent from current accepted Store history"
                .to_string(),
        ));
    }
    Ok(())
}

async fn current_history_contains(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    membership: &MembershipChain,
    expected: &StoreBatchCommitRef,
) -> Result<bool, StorePullError> {
    history_verifier.verify_refs([expected.clone()]).await?;
    let mut state = history_verifier
        .history()
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
    let verified_root = history_verifier.verified_root().clone();
    let founder = load_founder_registration_with_root(storage, root, &verified_root).await?;
    let founder_ref = StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object);
    registrations.insert(founder_ref.device_id, (founder_ref, founder.value));
    for recovered in
        discover_merge_owner_recoveries(storage, root, &verified_root, membership).await?
    {
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
            let discovered = discover_merge_stream(
                history_verifier,
                storage,
                root,
                registration_ref,
                registration,
                inactive_cut,
            )
            .await?;
            if matches!(discovered.block, Some(MergeStreamBlock::Authenticated(_))) {
                return Err(StorePullError::Database(
                    "an authenticated Merge stream position cannot be verified".to_string(),
                ));
            }
            if let Some((_, _, reference, _)) = discovered.commits.last() {
                let stream_id = reference.coord.stream_id;
                next.insert(stream_id, reference.clone());
            }
        }
        history_verifier.verify_refs(next.values().cloned()).await?;
        let next_state = if next.is_empty() {
            history_verifier.history().genesis.clone()
        } else {
            ResolvedStoreDeviceState::merge(
                next.values()
                    .map(|reference| {
                        history_verifier
                            .history()
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
            return Ok(history_verifier.history().commits.contains_key(expected));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::test_helpers::{open_test_db, pubkey_hex, TestStore};

    #[tokio::test]
    async fn provider_access_verification_authenticates_its_activation_once() {
        let owner = UserKeypair::generate();
        let db = open_test_db();
        let store = TestStore::create(&db, "provider-access-verification", owner.clone())
            .await
            .expect("create provider-access verification Store");
        let database = StoreDatabase::new(&db);
        let member = UserKeypair::generate();
        crate::sync::store::membership::invite_member(
            store.storage.as_ref(),
            store.home.as_ref(),
            &owner,
            &crate::sync::hlc::Hlc::new("provider-administrator".to_string()),
            &pubkey_hex(&member),
            None,
            crate::sync::membership::MemberRole::Member,
            &crate::encryption::EncryptionService::from_key([42; 32]),
            "provider-access-verification",
            "Provider access verification",
            &database,
        )
        .await
        .expect("invite provider-access recipient");
        let membership = load_cycle_membership(store.storage.as_ref(), &database)
            .await
            .expect("load provider-access membership");
        let pending_dir = tempfile::tempdir().expect("create provider-access join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending.sqlite"),
        )
        .expect("open provider-access join journal");
        let offer = crate::sync::store::begin_device_join(
            &database,
            store.storage.as_ref(),
            &membership,
            &owner,
            &pubkey_hex(&member),
            store
                .protocol_root
                .descriptor
                .founder_provider_admin
                .grant_id
                .clone(),
        )
        .await
        .expect("begin provider-access device join");
        let request = crate::sync::store::prepare_device_provider_access_request(
            &pending,
            store
                .storage
                .provider_binding()
                .await
                .expect("load provider binding"),
            &member,
            offer,
        )
        .await
        .expect("prepare provider-access request");
        let approval = crate::sync::store::authorize_device_provider_access(
            &database,
            store.storage.as_ref(),
            None,
            None,
            &membership,
            &owner,
            request,
        )
        .await
        .expect("authorize provider access");
        let administrator = database
            .activated_store_device_registration(
                approval.request.offer.provider_admin.administrator.clone(),
            )
            .await
            .expect("load provider administrator registration");
        let audit = crate::sync::store_commit::StoreCommitVerificationAudit::begin(&[approval
            .access_grant
            .activation
            .clone()]);

        crate::sync::store::verify_accepted_provider_access_activation(
            store.storage.as_ref(),
            &store.root,
            &approval.access_grant,
            &approval.request.offer.provider_admin,
            &administrator,
        )
        .await
        .expect("verify accepted provider access");

        audit.assert_each_verified_once();
    }
}
