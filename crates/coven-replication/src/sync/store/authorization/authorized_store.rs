use crate::sync::store::StoreError;
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;
use coven_protocol::membership::MembershipChain;
use coven_protocol::store_commit::{
    ReferencedStoreDeviceRegistration, StoreDeviceRegistration, StoreDeviceRegistrationActivation,
    StoreDeviceRegistrationRef, StoreRootRef,
};

use super::history::AuthorizedStoreHistory;

pub(super) struct LocalStoreDevice {
    registration: ReferencedStoreDeviceRegistration,
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
                key: coven_database::LOCAL_DEVICE_ID_STATE_KEY,
            })?;
        if durable.device_id.to_string() != expected_device_id {
            return Err(StoreError::InvalidState {
                key: coven_database::LOCAL_DEVICE_ID_STATE_KEY,
                reason: "local registration belongs to another device".to_string(),
            });
        }
        let registration =
            StoreDeviceRegistration::parse_at(&durable.registration_bytes, root, durable.device_id)
                .map_err(StoreError::from)?;
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
            coven_database::LocalDeviceRegistrationState::Activated { authority } => {
                let activated = database
                    .activated_store_device_registration_with_authority(
                        root,
                        registration_ref.clone(),
                    )
                    .await?;
                if activated.value() != &registration || activated.activation() != &authority {
                    return Err(StoreError::InvalidOutbound(
                        "local registration differs from its exact activation authority"
                            .to_string(),
                    ));
                }
                Some(authority)
            }
            coven_database::LocalDeviceRegistrationState::Prepared
            | coven_database::LocalDeviceRegistrationState::RegistrationPublished
            | coven_database::LocalDeviceRegistrationState::RegistrationActivated { .. }
            | coven_database::LocalDeviceRegistrationState::Created => None,
        };
        Ok(Self {
            registration: ReferencedStoreDeviceRegistration::verified(
                registration_ref,
                registration,
            )
            .map_err(StoreError::from)?,
            activation,
        })
    }
}

pub(crate) struct AuthorizedStore<'storage> {
    history: AuthorizedStoreHistory<'storage>,
    identity: &'storage UserKeypair,
    local_device: Option<LocalStoreDevice>,
    membership: MembershipChain,
}

impl<'storage> AuthorizedStore<'storage> {
    pub(super) fn new(
        history: AuthorizedStoreHistory<'storage>,
        identity: &'storage UserKeypair,
        local_device: Option<LocalStoreDevice>,
        membership: MembershipChain,
    ) -> Self {
        Self {
            history,
            identity,
            local_device,
            membership,
        }
    }

    pub(crate) async fn discard_circle_operation(
        &mut self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::circles::CircleOperationError> {
        self.history.circles().discard_operation(operation_id).await
    }

    pub(super) fn members(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Vec<coven_protocol::membership::MemberInfo> {
        member_info(self.membership.current_members(), user_pubkey)
    }

    pub(super) fn restore_membership(
        &self,
    ) -> Result<super::StoreRestoreMembership, crate::sync::store::membership::MembershipOpsError>
    {
        let founder_pubkey = self.membership.founder_pubkey().map(str::to_string).ok_or(
            crate::sync::store::membership::MembershipOpsError::NoFounderChainForAdmission,
        )?;
        Ok(super::StoreRestoreMembership {
            store_root: self.history.root().clone(),
            founder_pubkey,
            membership_floor: coven_protocol::membership::MembershipFloor(
                self.membership.head_refs().to_vec(),
            ),
        })
    }

    pub(super) async fn into_writer(
        self,
    ) -> Result<super::AuthorizedWriterOperation<'storage>, super::StoreRegistrationError> {
        let Self {
            history,
            identity,
            local_device,
            membership,
        } = self;
        let local_device = local_device.ok_or(crate::sync::store::StoreError::MissingState {
            key: coven_database::LOCAL_DEVICE_ID_STATE_KEY,
        })?;
        if local_device.activation.is_none() {
            return Err(super::StoreRegistrationError::ActivationRequired);
        }
        if &local_device.registration.value().store_root != history.root() {
            return Err(crate::sync::store::StoreError::InvalidOutbound(
                "local Store writer belongs to another Store root".to_string(),
            )
            .into());
        }
        let live_provider = history
            .provider_binding()
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)
            .map_err(crate::sync::store::StoreError::from)?;
        if live_provider.device != local_device.registration.value().provider {
            return Err(super::StoreRegistrationError::Invalid(
                "live provider principal differs from the local registration".to_string(),
            ));
        }
        let device_signer = local_device
            .registration
            .value()
            .device_signer(identity)
            .map_err(crate::sync::store::StoreError::from)?;
        Ok(history.authorize_writer(
            membership,
            identity,
            local_device.registration,
            device_signer,
        ))
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn membership_for_test(&self) -> MembershipChain {
        self.membership.clone()
    }

    #[cfg(test)]
    pub(super) fn membership_keyring_facts_for_test(
        &self,
    ) -> Result<([u8; 32], usize), crate::sync::store::membership::MembershipMutationError> {
        let keyring =
            crate::sync::store::authorization::open_store_keyring(self.identity, &self.membership)?;
        Ok((keyring.key_bytes(), keyring.key_count()))
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn bind_restore_for_test(self) -> super::RestoringStore<'storage> {
        self.history
            .bind_restore(self.membership, self.identity.clone())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) async fn authorize_retained_outbound_for_test(
        &self,
        order: &coven_protocol::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[coven_protocol::membership::MembershipHeadRef],
    ) -> Result<
        crate::sync::store::commit_verification::merge_history::MergeOutboundAuthorization,
        crate::sync::store::StoreError,
    > {
        let author_registration = self
            .local_device
            .as_ref()
            .ok_or_else(|| {
                crate::sync::store::StoreError::InvalidOutbound(
                    "retained outbound test Store has no local device".to_string(),
                )
            })?
            .registration
            .reference()
            .clone();
        self.history
            .authorize_retained_outbound(order, candidate_membership_heads, &author_registration)
            .await
            .map_err(crate::sync::store::StoreError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) async fn prepare_merge_history_successor_for_test(
        &mut self,
        verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        recovery_author: Option<&StoreDeviceRegistrationRef>,
        evidence: crate::sync::store::commit_verification::merge_history::MergeHistorySuccessorEvidence,
    ) -> Result<
        crate::sync::store::commit_verification::merge_history::PreparedMergeHistorySuccessor,
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
}

fn member_info(
    current: Vec<(String, coven_protocol::membership::MemberRole)>,
    user_pubkey: Option<&[u8]>,
) -> Vec<coven_protocol::membership::MemberInfo> {
    let user_pubkey_hex = user_pubkey.map(hex::encode);
    current
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>()
        .into_iter()
        .map(|(pubkey, role)| coven_protocol::membership::MemberInfo {
            is_self: user_pubkey_hex.as_deref() == Some(&pubkey),
            pubkey,
            role,
        })
        .collect()
}
