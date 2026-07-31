use std::sync::Arc;

use crate::database::StoreDatabase;
use crate::keys::UserKeypair;
use crate::protocol::membership::MembershipChain;
use crate::protocol::store_commit::{
    StoreDeviceRegistration, StoreDeviceRegistrationActivation, StoreDeviceRegistrationRef,
    StoreRootRef,
};
use crate::storage::SyncStorage;
use crate::sync::store::StoreError;

use super::history::AuthorizedStoreHistory;

pub(super) struct LocalStoreDevice {
    registration_ref: StoreDeviceRegistrationRef,
    registration: StoreDeviceRegistration,
    activation: Option<StoreDeviceRegistrationActivation>,
}

impl LocalStoreDevice {
    pub(super) async fn load(
        database: &StoreDatabase,
        root: &StoreRootRef,
        expected_device_id: &str,
    ) -> Result<Self, StoreError> {
        let durable = database
            .latest_local_store_device_registration()
            .await?
            .ok_or(StoreError::MissingState {
                key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            })?;
        if durable.device_id.to_string() != expected_device_id {
            return Err(StoreError::InvalidState {
                key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
                reason: "local registration belongs to another device".to_string(),
            });
        }
        let registration =
            StoreDeviceRegistration::parse_at(&durable.registration_bytes, root, durable.device_id)
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration,
            durable.prepared.reference().clone(),
        );
        if registration_ref.registration_hash != durable.registration_hash {
            return Err(StoreError::InvalidOutbound(
                "local registration differs from its durable hash".to_string(),
            ));
        }
        let activation = match durable.state {
            crate::database::LocalDeviceRegistrationState::Activated { authority } => {
                let activated = database
                    .activated_store_device_registration_with_authority(
                        root,
                        registration_ref.clone(),
                    )
                    .await?;
                if activated != (registration.clone(), authority.clone()) {
                    return Err(StoreError::InvalidOutbound(
                        "local registration differs from its exact activation authority"
                            .to_string(),
                    ));
                }
                Some(authority)
            }
            crate::database::LocalDeviceRegistrationState::Prepared
            | crate::database::LocalDeviceRegistrationState::Created => None,
        };
        Ok(Self {
            registration_ref,
            registration,
            activation,
        })
    }
}

pub(crate) struct AuthorizedStore<'storage> {
    history: AuthorizedStoreHistory<'storage>,
    storage: &'storage Arc<dyn SyncStorage>,
    identity: &'storage UserKeypair,
    local_device: Option<LocalStoreDevice>,
    membership: MembershipChain,
}

impl<'storage> AuthorizedStore<'storage> {
    pub(super) fn new(
        history: AuthorizedStoreHistory<'storage>,
        storage: &'storage Arc<dyn SyncStorage>,
        identity: &'storage UserKeypair,
        local_device: Option<LocalStoreDevice>,
        membership: MembershipChain,
    ) -> Self {
        Self {
            history,
            storage,
            identity,
            local_device,
            membership,
        }
    }

    pub(super) async fn discard_circle_operation(
        &mut self,
        operation_id: &crate::protocol::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::circle_controls::CircleOperationError> {
        self.history
            .circle_operation_discarder()
            .discard(operation_id)
            .await
    }

    fn resolved_membership(
        &self,
    ) -> Result<&MembershipChain, crate::sync::store::membership::MembershipOpsError> {
        match self.membership.conflict() {
            Some(conflict) => Err(
                crate::sync::store::membership::MembershipOpsError::SemanticConflict(Box::new(
                    conflict.clone(),
                )),
            ),
            None => Ok(&self.membership),
        }
    }

    pub(super) fn members(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Result<
        Vec<crate::protocol::membership::MemberInfo>,
        crate::sync::store::membership::MembershipOpsError,
    > {
        Ok(member_info(
            self.resolved_membership()?.current_members(),
            user_pubkey,
        ))
    }

    pub(super) fn membership_conflict(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Option<crate::protocol::membership::MembershipConflictInfo> {
        match self.membership.status() {
            crate::protocol::membership::MembershipStatus::Resolved(_) => None,
            crate::protocol::membership::MembershipStatus::Conflict(
                crate::protocol::membership::MembershipConflict::ConcurrentMemberAssignments {
                    conflict_hash,
                    member_pubkey,
                    conflicting_grants,
                    grants,
                    ..
                },
            ) => Some(
                crate::protocol::membership::MembershipConflictInfo::ConcurrentMemberAssignments {
                    id: conflict_hash.to_string(),
                    member_pubkey: member_pubkey.clone(),
                    choices: conflicting_grants
                        .iter()
                        .map(|(selected_grant, selected_record)| {
                            let selection = crate::protocol::membership::MembershipConflictSelection::MemberAssignment {
                                grant: selected_grant.clone(),
                            };
                            let members = member_info(
                                grants
                                    .iter()
                                    .filter_map(|(grant, state)| {
                                        (!conflicting_grants.contains_key(grant))
                                            .then(|| state.active())
                                            .flatten()
                                            .map(|record| {
                                                (
                                                    record.member_pubkey.clone(),
                                                    record.role.role(),
                                                )
                                            })
                                    })
                                    .chain(std::iter::once((
                                        selected_record.member_pubkey.clone(),
                                        selected_record.role.role(),
                                    )))
                                    .collect(),
                                user_pubkey,
                            );
                            crate::protocol::membership::MembershipConflictChoice::new(
                                membership_conflict_choice_id(&selection),
                                members,
                                *conflict_hash,
                                selection,
                            )
                        })
                        .collect(),
                },
            ),
            crate::protocol::membership::MembershipStatus::Conflict(
                crate::protocol::membership::MembershipConflict::RevocationCycle {
                    conflict_hash,
                    maximal_valid_branches,
                    ..
                },
            ) => Some(
                crate::protocol::membership::MembershipConflictInfo::RevocationCycle {
                    id: conflict_hash.to_string(),
                    choices: maximal_valid_branches
                        .iter()
                        .map(|branch| {
                            let selection = crate::protocol::membership::MembershipConflictSelection::RevocationBranch {
                                heads: branch.heads.clone(),
                            };
                            let members = member_info(
                                branch
                                    .active_grants()
                                    .map(|(_, record)| {
                                        (record.member_pubkey.clone(), record.role.role())
                                    })
                                    .collect(),
                                user_pubkey,
                            );
                            crate::protocol::membership::MembershipConflictChoice::new(
                                membership_conflict_choice_id(&selection),
                                members,
                                *conflict_hash,
                                selection,
                            )
                        })
                        .collect(),
                },
            ),
        }
    }

    pub(super) fn restore_membership(
        &self,
    ) -> Result<super::StoreRestoreMembership, crate::sync::store::membership::MembershipOpsError>
    {
        let founder_pubkey = self
            .membership
            .founder_pubkey()
            .map(str::to_string)
            .ok_or(crate::sync::store::membership::MembershipOpsError::NoFounderChain)?;
        Ok(super::StoreRestoreMembership {
            store_root: self.history.root().clone(),
            founder_pubkey,
            membership_floor: crate::joining::MembershipFloor(self.membership.head_refs().to_vec()),
        })
    }

    #[cfg(test)]
    pub(super) fn membership_for_test(&self) -> MembershipChain {
        self.membership.clone()
    }

    #[cfg(test)]
    pub(super) async fn prepare_wrapped_key_for_test(
        &self,
        recipient: &str,
        value: crate::protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<
        crate::protocol::wrapped_store_key::PreparedWrappedStoreKey,
        crate::storage::StorageError,
    > {
        self.history.prepare_wrapped_key(recipient, value).await
    }

    #[cfg(test)]
    pub(super) async fn open_membership_keyring_for_test(
        &self,
    ) -> Result<crate::encryption::EncryptionService, crate::sync::store::membership::InviteError>
    {
        self.history
            .open_keyring(self.identity, &self.membership)
            .await
    }

    #[cfg(test)]
    pub(super) fn bind_restore_for_test(self) -> super::RestoringStore<'storage> {
        self.history.bind_restore(
            self.storage.as_ref(),
            self.membership,
            self.identity.clone(),
            std::path::PathBuf::new(),
        )
    }

    #[cfg(test)]
    pub(super) async fn authorize_retained_outbound_for_test(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[crate::protocol::membership::MembershipHeadRef],
    ) -> Result<super::verified_history::MergeOutboundAuthorization, crate::sync::store::StoreError>
    {
        let author_registration = self
            .local_device
            .as_ref()
            .ok_or_else(|| {
                crate::sync::store::StoreError::InvalidOutbound(
                    "retained outbound test Store has no local device".to_string(),
                )
            })?
            .registration_ref
            .clone();
        self.history
            .authorize_retained_outbound(order, candidate_membership_heads, &author_registration)
            .await
            .map_err(crate::sync::store::StoreError::from)
    }

    #[cfg(test)]
    pub(super) async fn prepare_merge_history_successor_for_test(
        &mut self,
        verified_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        recovery_author: Option<&StoreDeviceRegistrationRef>,
        evidence: super::verified_history::MergeHistorySuccessorEvidence,
    ) -> Result<
        super::verified_history::PreparedMergeHistorySuccessor,
        crate::sync::store::StoreError,
    > {
        self.history
            .prepare_merge_history_successor_for_test(
                verified_commit,
                &self.membership,
                recovery_author,
                evidence,
            )
            .await
    }

    pub(super) async fn into_writer(
        self,
    ) -> Result<super::AuthorizedWriterOperation<'storage>, super::StoreRegistrationError> {
        let Self {
            history,
            storage,
            identity,
            local_device,
            membership,
        } = self;
        let local_device = local_device.ok_or(crate::sync::store::StoreError::MissingState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        })?;
        if local_device.activation.is_none() {
            return Err(super::StoreRegistrationError::ActivationRequired);
        }
        if &local_device.registration.store_root != history.root() {
            return Err(crate::sync::store::StoreError::InvalidOutbound(
                "local Store writer belongs to another Store root".to_string(),
            )
            .into());
        }
        let live_provider = storage
            .as_ref()
            .provider_binding()
            .await
            .map_err(crate::storage::StoreObjectError::from)
            .map_err(crate::sync::store::StoreError::from)?;
        if live_provider.device != local_device.registration.provider {
            return Err(super::StoreRegistrationError::Invalid(
                "live provider principal differs from the local registration".to_string(),
            ));
        }
        let device_signer = local_device
            .registration
            .device_signer(identity)
            .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))?;
        let writer = super::writer::LocalStoreWriter::from_verified_parts(
            identity,
            local_device.registration_ref,
            local_device.registration,
            device_signer,
        );
        Ok(history.bind_writer(storage, membership, writer))
    }
}

fn membership_conflict_choice_id(
    selection: &crate::protocol::membership::MembershipConflictSelection,
) -> String {
    let selection_bytes =
        serde_json::to_vec(selection).expect("membership conflict selections always serialize");
    let mut bytes = b"coven.membership-conflict-choice.v1\0".to_vec();
    bytes.extend(selection_bytes);
    crate::protocol::store_commit::ObjectHash::digest(&bytes).to_string()
}

fn member_info(
    current: Vec<(String, crate::protocol::membership::MemberRole)>,
    user_pubkey: Option<&[u8]>,
) -> Vec<crate::protocol::membership::MemberInfo> {
    let user_pubkey_hex = user_pubkey.map(hex::encode);
    current
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>()
        .into_iter()
        .map(|(pubkey, role)| crate::protocol::membership::MemberInfo {
            is_self: user_pubkey_hex.as_deref() == Some(&pubkey),
            pubkey,
            role,
        })
        .collect()
}
