use super::*;
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::store_commit::StoreHistoryCut;

pub(in crate::sync::store_engine) async fn prepare_device_join_bootstrap(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &StoreHistoryCut,
    attempt_activation: &StoreBatchCommitRef,
    membership_state: &StoreMembershipStateRef,
) -> Result<DeviceJoinBootstrapPlan, StorePullError> {
    let StoreHistoryCut::Serial(coverage_position) = coverage else {
        return Err(StorePullError::Serial(
            "Serial device join bootstrap received a Merge history cut".to_string(),
        ));
    };
    if !matches!(attempt_activation.coord, StoreCommitCoord::Serial { .. }) {
        return Err(StorePullError::Serial(
            "Serial device join bootstrap received a Merge activation".to_string(),
        ));
    }
    let (verified_position, verified_authorization) =
        load_device_join_authorization(storage, root, membership_state).await?;
    if &verified_position != coverage_position {
        return Err(StorePullError::Serial(
            "Serial device join bootstrap cut differs from its verified membership position"
                .to_string(),
        ));
    }

    let (authorized, _, _) = Box::pin(load_authorized_serial_prefix(
        storage,
        root,
        Some(attempt_activation.clone()),
    ))
    .await?;
    let activation = authorized.last().ok_or_else(|| {
        StorePullError::Serial(
            "device join attempt activation is absent from its Serial history".to_string(),
        )
    })?;
    if activation.commit_ref != *attempt_activation
        || activation
            .commit
            .order
            .predecessor_cut()
            .map_err(|error| StorePullError::Serial(error.to_string()))?
            != *coverage
    {
        return Err(StorePullError::Serial(
            "device join attempt activation predecessor differs from its signed bootstrap cut"
                .to_string(),
        ));
    }
    if activation.commit.membership_state != *membership_state
        || activation.authorization_before != verified_authorization
    {
        return Err(StorePullError::Serial(
            "device join attempt activation differs from its exact verified membership state"
                .to_string(),
        ));
    }

    let verified_root = load_store_protocol_root(storage, root).await?.value;
    let founder = load_founder_registration_with_root(storage, root, &verified_root).await?;
    let founder_reference =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let genesis = ResolvedStoreDeviceState::founder(
        root,
        founder_reference.clone(),
        &verified_root.descriptor.founder_pubkey,
        verified_root.descriptor.founder_grant.clone(),
        &verified_root.descriptor.founder_recovery,
    )
    .map_err(|error| StorePullError::Serial(error.to_string()))?;
    let commits = authorized
        .into_iter()
        .map(|authorized| DeviceJoinBootstrapCommit {
            reference: authorized.commit_ref,
            commit: authorized.commit,
            registrations: authorized.registrations,
            device_operations: authorized.device_operations,
            activation: DeviceJoinBootstrapActivation::Serial,
        })
        .collect();
    Ok(DeviceJoinBootstrapPlan {
        founder_reference,
        founder: founder.value,
        founder_bytes: founder.bytes,
        genesis,
        coverage: coverage.clone(),
        commits,
    })
}

pub(in crate::sync::store_engine) async fn materialize_device_join_activation(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &StoreBatchCommitRef,
    expected_outcome: &super::store_commit::DeviceJoinOutcomeRef,
    membership_state: &StoreMembershipStateRef,
) -> Result<(), StorePullError> {
    let StoreCommitCoord::Serial { sequence } = reference.coord else {
        return Err(StorePullError::Serial(
            "Serial device join materialization received a Merge commit".to_string(),
        ));
    };
    let stream_id = SERIAL_STREAM_ID.to_string();
    if let Some(materialized) = db.exact_materialized_ref(&stream_id, sequence).await? {
        if materialized == *reference {
            return Ok(());
        }
        return Err(StorePullError::Serial(format!(
            "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
        )));
    }
    let (commit, author) =
        load_device_join_activation_commit(storage, root, reference, expected_outcome).await?;
    let (position, authorization) =
        load_device_join_authorization(storage, root, membership_state).await?;
    let authority = RegistrationPredecessorAuthority::Serial {
        authorization: &authorization,
        position,
        history: SerialAuthorizationHistory::ExactPredecessor,
    };
    let accepted_predecessor = VerifiedAcceptedPredecessor::Exact;
    let registrations = Box::pin(load_commit_registrations(
        storage,
        root,
        &commit,
        &author,
        Some(&authority),
        Some(&accepted_predecessor),
    ))
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
    })?;
    if !authorization.membership.can_write(&author.author_pubkey) {
        return Err(StorePullError::Serial(
            "device join activation author is not authorized by its exact predecessor membership"
                .to_string(),
        ));
    }
    let commit_ref = reference.clone();
    let expected_ref = reference.clone();
    db.call(move |connection| {
        let tx = connection.unchecked_transaction().map_err(DbError::from)?;
        if let Some(materialized) =
            Database::materialized_commit_ref_on(&tx, &stream_id, sequence)?
        {
            if materialized != expected_ref {
                return Err(DbError::Message(format!(
                    "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
                )));
            }
            tx.commit().map_err(DbError::from)?;
            return Ok(());
        }
        Database::record_activated_store_device_registrations_on(
            &tx,
            &commit,
            &registrations,
        )?;
        Database::record_materialized_serial_commit_on(
            &tx,
            &commit,
            &commit_ref,
            &authorization,
        )?;
        tx.commit().map_err(DbError::from)
    })
    .await?;
    Ok(())
}
