use super::*;

pub(crate) async fn load_authorized_serial_prefix(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    tip: Option<StoreBatchCommitRef>,
) -> Result<
    (
        Vec<AuthorizedSerialCommit>,
        SerialAuthorizationState,
        ResolvedStoreDeviceState,
    ),
    StorePullError,
> {
    let root_value = load_store_protocol_root(storage, root).await?.value;
    if root_value.descriptor.write_policy != crate::WritePolicy::Serial {
        return Err(StorePullError::Serial(format!(
            "Store protocol root uses {:?}, not Serial",
            root_value.descriptor.write_policy
        )));
    }

    let mut expected = tip;
    let mut reverse = Vec::new();
    while let Some(reference) = expected {
        if !matches!(reference.coord, StoreCommitCoord::Serial { .. }) {
            return Err(StorePullError::Serial(
                "global predecessor chain contains a Merge commit reference".to_string(),
            ));
        }
        let (commit, author) =
            load_commit_with_author_at_root(storage, root, &root_value, &reference).await?;
        expected = commit.order.predecessor().cloned();
        reverse.push((reference, commit, author));
    }
    reverse.reverse();

    let founder = load_founder_registration_with_root(storage, root, &root_value).await?;
    let founder_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let mut authorization =
        SerialAuthorizationState::from_founder(root, &root_value, &founder_ref, &founder.value)
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
    let genesis_authorization = Box::new(authorization.clone());
    let genesis_position = super::store_commit::SerialStorePosition::Genesis {
        root: root.clone(),
        founder_registration: founder_ref.clone(),
    };
    let mut device_state = ResolvedStoreDeviceState::founder(
        root,
        founder_ref.clone(),
        &root_value.descriptor.founder_pubkey,
        root_value.descriptor.founder_grant.clone(),
        &root_value.descriptor.founder_recovery,
    )
    .map_err(|error| StorePullError::Serial(error.to_string()))?;
    let mut predecessor = None;
    let mut authorized = Vec::with_capacity(reverse.len());
    let mut accepted_commits = BTreeSet::new();

    for (reference, commit, author) in reverse {
        match (&predecessor, &commit.order) {
            (
                None,
                super::store_commit::StoreCommitOrder::Serial {
                    seq: 1,
                    predecessor:
                        StoreSerialPredecessor::Genesis {
                            root: genesis_root,
                            founder_registration,
                        },
                },
            ) if genesis_root == root && founder_registration == &founder_ref => {
                let recovery_author =
                    commit
                        .serial_recovery_activation()
                        .as_ref()
                        .is_some_and(|activation| {
                            activation.registration.registration == commit.author_registration
                        });
                if founder.value.author_pubkey != root_value.descriptor.founder_pubkey
                    || (!recovery_author && founder_registration != &commit.author_registration)
                {
                    return Err(StorePullError::Serial(
                        "Serial genesis registration is not the Store founder".to_string(),
                    ));
                }
            }
            (
                Some(previous),
                super::store_commit::StoreCommitOrder::Serial {
                    seq,
                    predecessor: StoreSerialPredecessor::Commit(declared),
                },
            ) if declared == previous
                && *seq
                    == previous.coord.sequence().checked_add(1).ok_or_else(|| {
                        StorePullError::Serial("Serial predecessor sequence overflow".to_string())
                    })? => {}
            _ => {
                return Err(StorePullError::Serial(format!(
                    "Serial commit {} does not extend the exact accepted predecessor",
                    reference.coord.sequence()
                )));
            }
        }

        let expected_device_state = StoreDeviceStateRef::serial(
            match &commit.order {
                super::store_commit::StoreCommitOrder::Serial { predecessor, .. } => {
                    predecessor.clone()
                }
                super::store_commit::StoreCommitOrder::MergeConcurrent { .. } => {
                    return Err(StorePullError::Serial(
                        "Serial chain contains a Merge commit order".to_string(),
                    ));
                }
            },
            &device_state,
        )
        .map_err(|error| StorePullError::Serial(error.to_string()))?;
        if commit.device_state != expected_device_state {
            return Err(StorePullError::Serial(format!(
                "Serial commit {} names a different predecessor device state",
                reference.coord.sequence()
            )));
        }
        if author.device_id != commit.author_registration.device_id {
            return Err(StorePullError::Serial(
                "Serial commit author bytes differ from its exact registration".to_string(),
            ));
        }
        let predecessor_position = match &commit.order {
            super::store_commit::StoreCommitOrder::Serial { predecessor, .. } => {
                predecessor.clone()
            }
            super::store_commit::StoreCommitOrder::MergeConcurrent { .. } => {
                return Err(StorePullError::Serial(
                    "Serial chain contains a Merge commit order".to_string(),
                ));
            }
        };
        let registrations = load_serial_commit_registrations(
            storage,
            root,
            &root_value,
            &commit,
            &author,
            &authorization,
            predecessor_position.clone(),
            SerialAuthorizationHistory::Prefix {
                genesis_position: &genesis_position,
                genesis_authorization: genesis_authorization.as_ref(),
                commits: &authorized,
            },
            &authorized,
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
        })?;
        let predecessor_authority = RegistrationPredecessorAuthority::Serial {
            authorization: &authorization,
            position: predecessor_position,
            history: SerialAuthorizationHistory::Prefix {
                genesis_position: &genesis_position,
                genesis_authorization: genesis_authorization.as_ref(),
                commits: &authorized,
            },
        };
        let acknowledgement = validate_commit_acknowledgement(storage, root, &commit, &author)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
            })?;
        let device_state_before = device_state.clone();
        let (authorized_device_state, recovery_author) =
            predecessor_with_recovery_author(device_state, &commit, &registrations)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
        if !device_state_has_active_registration(
            &authorized_device_state,
            &commit.author_registration,
        ) {
            return Err(StorePullError::Serial(format!(
                "Serial commit {} author registration is not active at its predecessor",
                reference.coord.sequence()
            )));
        }
        let device_operations = load_commit_device_operations(
            None,
            storage,
            root,
            &commit,
            &authorized_device_state,
            Some(&predecessor_authority),
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
        })?;
        super::wrapped_store_key::validate_control_wrapped_keys(storage, root, commit.control())
            .await?;
        validate_serial_provider_admin_control(storage, root, &root_value, commit.control())
            .await?;
        let owner_recovery = verify_commit_owner_recovery_activation(
            storage,
            root,
            &commit,
            Some((&authorization, &authorized_device_state)),
        )
        .await?;
        let authorization_before = authorization.clone();
        authorization = authorization
            .authorize_and_apply(&reference, &commit, &author)
            .map_err(|error| {
                StorePullError::Serial(format!(
                    "commit {} authorization: {error}",
                    reference.coord.sequence()
                ))
            })?;
        let reduced_state = device_operations
            .apply_to(authorized_device_state, &commit.device_state)
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
        device_state = apply_verified_device_lifecycle(
            reduced_state,
            &commit,
            &registrations,
            recovery_author.as_ref(),
            owner_recovery,
        )
        .map_err(|error| StorePullError::Serial(error.to_string()))?;
        predecessor = Some(reference.clone());
        accepted_commits.insert(reference.clone());
        authorized.push(AuthorizedSerialCommit {
            commit_ref: reference,
            commit,
            author,
            registrations,
            device_operations,
            device_state_before,
            device_state_after: device_state.clone(),
            acknowledgement,
            authorization_before,
            authorization_after: authorization.clone(),
        });
    }
    Ok((authorized, authorization, device_state))
}

pub(crate) async fn validate_serial_provider_admin_control(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    control: Option<&super::store_commit::StoreControl>,
) -> Result<(), StorePullError> {
    let Some(super::store_commit::StoreControl::ProviderAdmin {
        change:
            super::provider::ProviderAdminChange::Set {
                administrator,
                provider,
                capability,
                ..
            },
    }) = control
    else {
        return Ok(());
    };
    let registration =
        super::store_objects::load_registration_ref(storage, root, administrator).await?;
    if registration.value.store_root != *root || registration.value.provider != *provider {
        return Err(StorePullError::Serial(
            "provider administrator grant does not match its exact device registration".to_string(),
        ));
    }
    capability
        .verify(&root_value.descriptor.provider, provider, true)
        .map_err(|error| StorePullError::Serial(error.to_string()))
}

pub(crate) async fn load_authorized_serial_chain(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    head: &StoreSerialHead,
) -> Result<Vec<AuthorizedSerialCommit>, StorePullError> {
    let founder = load_founder_registration(storage, root).await?;
    let founder_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let tip = match &head.state {
        StoreSerialHeadState::Genesis {
            root: head_root,
            founder_registration,
        } => {
            if head_root != root || founder_registration != &founder_ref {
                return Err(StorePullError::Serial(
                    "Serial genesis head does not name the exact Store founder".to_string(),
                ));
            }
            None
        }
        StoreSerialHeadState::Commit { commit, .. } => Some(commit.clone()),
    };
    let (authorized, _, _) =
        Box::pin(load_authorized_serial_prefix(storage, root, tip.clone())).await?;
    match (&head.state, authorized.last()) {
        (StoreSerialHeadState::Genesis { .. }, None) => {}
        (
            StoreSerialHeadState::Commit {
                author_registration,
                commit,
            },
            Some(accepted),
        ) if commit == &accepted.commit_ref
            && author_registration == &accepted.commit.author_registration => {}
        _ => {
            return Err(StorePullError::Serial(
                "signed global head is not bound to its exact tip commit".to_string(),
            ));
        }
    }
    Ok(authorized)
}

pub(crate) enum SerialSuccessorObservation {
    Unchanged(super::storage::VersionedObject),
    Advanced(VerifiedSerialAcceptedSuffix),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedSerialAcceptedSuffix {
    store_root_hash: ObjectHash,
    durable: super::remote_object::SerialAcceptedSuffix,
}

impl VerifiedSerialAcceptedSuffix {
    pub(super) fn new(
        store_root_hash: ObjectHash,
        durable: super::remote_object::SerialAcceptedSuffix,
    ) -> Self {
        Self {
            store_root_hash,
            durable,
        }
    }

    pub(crate) fn durable(&self) -> &super::remote_object::SerialAcceptedSuffix {
        &self.durable
    }

    pub(crate) fn commits(&self) -> &[StoreBatchCommitRef] {
        &self.durable.commits
    }

    pub(crate) fn verify_candidate_nonactivation(
        &self,
        losing: Vec<(
            super::store_commit::StoreBatchCommitDeletionTarget,
            StoreDeviceRegistration,
        )>,
    ) -> Result<
        super::remote_object::VerifiedCandidateNonactivation,
        super::remote_object::RemoteObjectRecordError,
    > {
        if losing.is_empty() {
            return Err(super::remote_object::RemoteObjectRecordError::InvalidProof(
                "losing Serial prefix is empty".to_string(),
            ));
        }
        let mut verified = Vec::with_capacity(losing.len());
        for (target, author) in losing {
            target
                .verify_nonactivation_candidate(self.store_root_hash, &author)
                .map_err(|error| {
                    super::remote_object::RemoteObjectRecordError::InvalidProof(error.to_string())
                })?;
            verified.push(target);
        }
        let candidate = verified
            .last()
            .expect("checked nonempty Serial prefix")
            .clone();
        super::remote_object::VerifiedCandidateNonactivation::from_verified_serial_successor(
            candidate,
            self.durable.clone(),
            verified,
        )
    }
}

pub(crate) async fn observe_serial_successors_after(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    predecessor: &super::store_commit::StoreSerialPredecessor,
) -> Result<SerialSuccessorObservation, StorePullError> {
    let verified_head = read_serial_head(storage, coordination, root).await?;
    let authorized = load_authorized_serial_chain(storage, root, &verified_head.head).await?;
    let first = match predecessor {
        super::store_commit::StoreSerialPredecessor::Genesis {
            root: expected_root,
            founder_registration,
        } => {
            let actual = authorized.first().map_or_else(
                || match &verified_head.head.state {
                    StoreSerialHeadState::Genesis {
                        root,
                        founder_registration,
                    } => super::store_commit::StoreSerialPredecessor::Genesis {
                        root: root.clone(),
                        founder_registration: founder_registration.clone(),
                    },
                    StoreSerialHeadState::Commit { .. } => {
                        unreachable!("a commit head has an authorized tip")
                    }
                },
                |first| match &first.commit.order {
                    super::store_commit::StoreCommitOrder::Serial { predecessor, .. } => {
                        predecessor.clone()
                    }
                    super::store_commit::StoreCommitOrder::MergeConcurrent { .. } => {
                        unreachable!("authorized Serial chain contains only Serial commits")
                    }
                },
            );
            let expected = super::store_commit::StoreSerialPredecessor::Genesis {
                root: expected_root.clone(),
                founder_registration: founder_registration.clone(),
            };
            if actual != expected {
                return Err(StorePullError::Serial(
                    "global chain does not descend from the exact Serial genesis".to_string(),
                ));
            }
            0
        }
        super::store_commit::StoreSerialPredecessor::Commit(base) => authorized
            .iter()
            .position(|accepted| &accepted.commit_ref == base)
            .map(|index| index + 1)
            .ok_or_else(|| {
                StorePullError::Serial(
                    "global chain does not descend from the exact Serial predecessor".to_string(),
                )
            })?,
    };
    let commits = authorized[first..]
        .iter()
        .map(|accepted| accepted.commit_ref.clone())
        .collect::<Vec<_>>();
    if commits.is_empty() {
        return Ok(SerialSuccessorObservation::Unchanged(verified_head.object));
    }
    Ok(SerialSuccessorObservation::Advanced(
        VerifiedSerialAcceptedSuffix::new(
            root.store_root_hash,
            super::remote_object::SerialAcceptedSuffix {
                predecessor: match predecessor {
                    super::store_commit::StoreSerialPredecessor::Genesis { .. } => None,
                    super::store_commit::StoreSerialPredecessor::Commit(base) => Some(base.clone()),
                },
                commits,
                canonical_signed_head_bytes: verified_head.object.bytes,
                observed_version_hash: super::store_commit::ObjectHash::digest(
                    verified_head
                        .object
                        .version
                        .cloud()
                        .as_provider()
                        .as_bytes(),
                ),
            },
        ),
    ))
}

pub(crate) struct VerifiedSerialHead {
    pub(crate) head: StoreSerialHead,
    pub(crate) object: super::storage::VersionedObject,
}

pub(crate) async fn read_serial_head(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
) -> Result<VerifiedSerialHead, StorePullError> {
    let object = match coordination.read_head(serial_head_key()).await {
        Ok(object) => object,
        Err(CoordinationError::NotFound(_)) => {
            return Err(StorePullError::Serial("global head is absent".to_string()));
        }
        Err(error) => return Err(StorePullError::Coordination(error)),
    };
    let unverified: StoreSerialHead = serde_json::from_slice(&object.bytes)
        .map_err(|error| StorePullError::Serial(format!("invalid head: {error}")))?;
    let executor_ref = match &unverified.state {
        StoreSerialHeadState::Genesis {
            founder_registration,
            ..
        } => founder_registration,
        StoreSerialHeadState::Commit {
            author_registration,
            ..
        } => author_registration,
    };
    let executor = load_registration_ref(storage, root, executor_ref)
        .await?
        .value;
    let head = StoreSerialHead::parse(&object.bytes, root.store_root_hash, &executor)
        .map_err(|error| StorePullError::Serial(format!("invalid head: {error}")))?;
    Ok(VerifiedSerialHead { head, object })
}

pub(crate) async fn load_serial_authorization_at_head(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    head: &StoreSerialHead,
) -> Result<SerialAuthorizationState, StorePullError> {
    let authorized = load_authorized_serial_chain(storage, root, head).await?;
    match authorized.last() {
        Some(tip) => Ok(tip.authorization_after.clone()),
        None => load_serial_authorization_at_position(storage, root, None).await,
    }
}

pub(crate) struct SerialCycleAuthorization {
    pub authorization: SerialAuthorizationState,
    pub head: Option<StoreBatchCommitRef>,
}

pub(crate) async fn load_serial_cycle_authorization(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
) -> Result<SerialCycleAuthorization, StorePullError> {
    let head = read_serial_head(storage, coordination, root).await?.head;
    let authorized = load_authorized_serial_chain(storage, root, &head).await?;
    let authorization = match authorized.last() {
        Some(tip) => tip.authorization_after.clone(),
        None => load_serial_authorization_at_position(storage, root, None).await?,
    };
    let head = match head.state {
        StoreSerialHeadState::Genesis { .. } => None,
        StoreSerialHeadState::Commit { commit, .. } => Some(commit),
    };
    Ok(SerialCycleAuthorization {
        authorization,
        head,
    })
}

pub(crate) async fn load_serial_authorization_at_position(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: Option<StoreBatchCommitRef>,
) -> Result<SerialAuthorizationState, StorePullError> {
    let (_, authorization, _) =
        Box::pin(load_authorized_serial_prefix(storage, root, reference)).await?;
    Ok(authorization)
}

pub(crate) async fn load_serial_snapshot_authorities_at_position(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: Option<StoreBatchCommitRef>,
) -> Result<Vec<(StoreDeviceRegistrationRef, StoreDeviceRegistration)>, StorePullError> {
    let (authorized, authorization, _) =
        Box::pin(load_authorized_serial_prefix(storage, root, reference)).await?;
    let founder = load_founder_registration(storage, root).await?;
    let founder_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let mut active = BTreeMap::from([(founder_ref, founder.value)]);
    for accepted in authorized {
        for (activated, (registration, _)) in accepted
            .commit
            .device_registrations()
            .iter()
            .zip(accepted.registrations)
        {
            active.insert(activated.registration.clone(), registration);
        }
        for retirement in accepted.commit.device_retirements() {
            active.remove(&retirement.target);
        }
    }
    Ok(active
        .into_iter()
        .filter(|(_, registration)| {
            authorization
                .membership
                .is_owner(&registration.author_pubkey)
        })
        .collect())
}
