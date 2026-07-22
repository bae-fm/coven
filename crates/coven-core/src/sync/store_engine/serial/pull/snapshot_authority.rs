use super::*;
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::store_commit::StoreHistoryCut;

struct VerifiedSerialSnapshotState {
    common: VerifiedSnapshotState,
    authorization: SerialAuthorizationState,
}

async fn accepted_prefix<'a>(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    accepted: &'a [AuthorizedSerialCommit],
    position: &StoreSerialPredecessor,
) -> Result<&'a [AuthorizedSerialCommit], StorePullError> {
    match position {
        StoreSerialPredecessor::Genesis {
            root: cut_root,
            founder_registration,
        } => {
            let founder = load_founder_registration(storage, root).await?;
            let founder_ref =
                StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object);
            if cut_root != root || founder_registration != &founder_ref {
                return Err(StorePullError::Serial(
                    "Serial snapshot cut names another genesis authority".to_string(),
                ));
            }
            Ok(&accepted[..0])
        }
        StoreSerialPredecessor::Commit(reference) => {
            let index = accepted
                .iter()
                .position(|candidate| &candidate.commit_ref == reference)
                .ok_or_else(|| {
                    StorePullError::Serial(
                        "Serial snapshot cut is absent from the signed coordinated chain"
                            .to_string(),
                    )
                })?;
            Ok(&accepted[..=index])
        }
    }
}

async fn verify_history_state(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    position: &StoreSerialPredecessor,
    membership_ref: &StoreMembershipStateRef,
) -> Result<VerifiedSerialSnapshotState, StorePullError> {
    let StoreMembershipStateRef::Serial(_) = membership_ref else {
        return Err(StorePullError::Serial(
            "Serial history carries Merge membership state".to_string(),
        ));
    };
    let verified_head = read_serial_head(storage, coordination, root).await?;
    let accepted = load_authorized_serial_chain(storage, root, &verified_head.head).await?;
    let prefix = accepted_prefix(storage, root, &accepted, position).await?;
    let (_, genesis_authorization, genesis_state) =
        Box::pin(load_authorized_serial_prefix(storage, root, None)).await?;
    let (authorization, device_state) = prefix.last().map_or_else(
        || (genesis_authorization, genesis_state),
        |accepted| {
            (
                accepted.authorization_after.clone(),
                accepted.device_state_after.clone(),
            )
        },
    );
    let expected_membership = StoreMembershipStateRef::serial(
        position.clone(),
        device_state.recovery.clone(),
        &authorization,
    )
    .map_err(|error| StorePullError::Serial(error.to_string()))?;
    if &expected_membership != membership_ref {
        return Err(StorePullError::Serial(
            "Serial history membership differs from its accepted state".to_string(),
        ));
    }
    let active_registrations =
        load_active_history_registrations(storage, root, &device_state).await?;
    Ok(VerifiedSerialSnapshotState {
        common: VerifiedSnapshotState {
            device_state,
            active_registrations,
        },
        authorization,
    })
}

pub(in crate::sync::store_engine) async fn verify_history_authority(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    cut: &StoreHistoryCut,
    membership_ref: &StoreMembershipStateRef,
) -> Result<(), StorePullError> {
    let StoreHistoryCut::Serial(position) = cut else {
        return Err(StorePullError::Serial(
            "Serial history verification received a Merge cut".to_string(),
        ));
    };
    verify_history_state(storage, coordination, root, position, membership_ref)
        .await
        .map(|_| ())
}

async fn verify_authority(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<(StoreHistoryCut, VerifiedSerialSnapshotState), StorePullError> {
    let (CommitFrontier::Serial(_), StoreDeviceStateRef::Serial { position, .. }) =
        (&snapshot.meta.coverage, &snapshot.meta.state.devices)
    else {
        return Err(StorePullError::Serial(
            "Serial snapshot coverage or device state uses Merge policy".to_string(),
        ));
    };
    let state = verify_history_state(
        storage,
        coordination,
        root,
        position,
        &snapshot.meta.state.membership,
    )
    .await?;
    let expected_device_state =
        StoreDeviceStateRef::serial(position.clone(), &state.common.device_state)
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
    if expected_device_state != snapshot.meta.state.devices {
        return Err(StorePullError::Serial(
            "Serial snapshot device state differs from its accepted state".to_string(),
        ));
    }
    if !matches!(
        snapshot.meta.history_summary,
        super::store_commit::StoreSnapshotHistorySummary::Serial
    ) {
        return Err(StorePullError::Serial(
            "Serial snapshot carries a Merge history summary".to_string(),
        ));
    }
    let (_, author) = state
        .common
        .active_registrations
        .get(&snapshot.meta.author_registration.device_id)
        .filter(|(reference, _)| reference == &snapshot.meta.author_registration)
        .ok_or(StorePullError::SnapshotAuthorInactive)?;
    if !state
        .authorization
        .membership
        .is_owner(&author.author_pubkey)
    {
        return Err(StorePullError::SnapshotAuthorNotOwner);
    }
    Ok((StoreHistoryCut::Serial(position.clone()), state))
}

fn head_cut(head: StoreSerialHead) -> StoreHistoryCut {
    StoreHistoryCut::Serial(match head.state {
        StoreSerialHeadState::Genesis {
            root,
            founder_registration,
        } => StoreSerialPredecessor::Genesis {
            root,
            founder_registration,
        },
        StoreSerialHeadState::Commit { commit, .. } => StoreSerialPredecessor::Commit(commit),
    })
}

async fn activated_acknowledgements(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    accepted: &[AuthorizedSerialCommit],
) -> Result<Vec<VerifiedActivatedStoreAck>, StorePullError> {
    let mut acknowledgements = Vec::new();
    for accepted in accepted {
        let Some((reference, value)) = &accepted.acknowledgement else {
            continue;
        };
        let chain = load_acknowledgement_proof_chain(
            storage,
            root,
            reference.clone(),
            value.clone(),
            &accepted.author,
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
        })?;
        acknowledgements.push(VerifiedActivatedStoreAck {
            reference: reference.clone(),
            value: value.clone(),
            chain,
            activating_commit: accepted.commit_ref.clone(),
            activating_commit_value: accepted.commit.clone(),
        });
    }
    Ok(acknowledgements)
}

pub(in crate::sync::store_engine) async fn verify_snapshot_for_acknowledgement(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<(), StorePullError> {
    verify_authority(storage, coordination, root, snapshot)
        .await
        .map(|_| ())
}

pub(in crate::sync::store_engine) async fn verify_snapshot_stability(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<VerifiedStoreSnapshotStability, StorePullError> {
    let (snapshot_cut, state) = verify_authority(storage, coordination, root, snapshot).await?;
    let head = read_serial_head(storage, coordination, root).await?;
    let accepted_cut = head_cut(head.head.clone());
    let accepted = load_authorized_serial_chain(storage, root, &head.head).await?;
    let acknowledgements = activated_acknowledgements(storage, root, &accepted).await?;
    assemble_snapshot_stability(
        storage,
        root,
        snapshot,
        snapshot_cut,
        accepted_cut,
        state.common,
        acknowledgements,
    )
    .await
}
