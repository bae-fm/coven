use super::*;

pub(super) struct CircleStreamAuthority {
    pub(super) activation_id: StreamActivationId,
    pub(super) first_slot: coven_protocol::objects::ObjectSlot,
    pub(super) registration: StoreDeviceRegistration,
    pub(super) activated_here: bool,
}

#[derive(Clone, Copy)]
pub(super) enum CircleHeadKind {
    Control,
    Roster,
    Metadata,
}

pub(super) enum CircleHeadValue {
    Control(coven_protocol::circle::CircleControlHead),
    Roster(coven_protocol::circle::CircleRosterHead),
    Metadata(coven_protocol::circle::CircleMetadataHead),
}

pub(super) struct CircleHeadPosition<'a> {
    pub(super) store_root_hash: ObjectHash,
    pub(super) circle_id: CircleId,
    pub(super) author_pubkey: &'a str,
    pub(super) device_id: &'a str,
    pub(super) stream_id: coven_protocol::causal_grants::AuthorStreamId,
    pub(super) author_owner_grant: &'a coven_protocol::membership::MembershipGrantId,
    pub(super) seq: u64,
    pub(super) successor: &'a coven_protocol::store_commit::SuccessorLink,
}

impl CircleHeadValue {
    pub(super) fn parse(kind: CircleHeadKind, bytes: &[u8]) -> Result<Self, CircleOperationError> {
        match kind {
            CircleHeadKind::Control => {
                serde_json::from_slice(bytes)
                    .map(Self::Control)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle control head: {error}"
                        ))
                    })
            }
            CircleHeadKind::Roster => {
                serde_json::from_slice(bytes)
                    .map(Self::Roster)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle roster head: {error}"
                        ))
                    })
            }
            CircleHeadKind::Metadata => {
                serde_json::from_slice(bytes)
                    .map(Self::Metadata)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle metadata head: {error}"
                        ))
                    })
            }
        }
    }

    pub(super) fn position(&self) -> Result<CircleHeadPosition<'_>, CircleOperationError> {
        match self {
            Self::Control(head) => {
                let CircleControlCoord {
                    device_id,
                    stream_id,
                    author_pubkey,
                    author_owner_grant,
                    seq,
                    ..
                } = &head.control;
                Ok(CircleHeadPosition {
                    store_root_hash: head.store_root_hash,
                    circle_id: head.circle_id,
                    author_pubkey,
                    device_id,
                    stream_id: *stream_id,
                    author_owner_grant,
                    seq: *seq,
                    successor: &head.successor,
                })
            }
            Self::Roster(head) => Ok(CircleHeadPosition {
                store_root_hash: head.store_root_hash,
                circle_id: head.circle_id,
                author_pubkey: &head.author_pubkey,
                device_id: &head.device_id,
                stream_id: head.stream_id,
                author_owner_grant: &head.author_owner_grant,
                seq: head.seq,
                successor: &head.successor,
            }),
            Self::Metadata(head) => Ok(CircleHeadPosition {
                store_root_hash: head.store_root_hash,
                circle_id: head.circle_id,
                author_pubkey: &head.author_pubkey,
                device_id: &head.device_id,
                stream_id: head.stream_id,
                author_owner_grant: &head.author_owner_grant,
                seq: head.seq,
                successor: &head.successor,
            }),
        }
    }

    pub(super) fn verify_for_registration(&self, registration: &StoreDeviceRegistration) -> bool {
        match self {
            Self::Control(head) => head.verify(registration),
            Self::Roster(head) => head.verify_for_registration(registration),
            Self::Metadata(head) => head.verify_for_registration(registration),
        }
    }

    pub(super) fn semantic_prefix(&self, object: ExactObjectRef) -> String {
        match self {
            Self::Control(head) => circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id: head.circle_id,
                control: &head.control,
            }),
            Self::Roster(head) => {
                let reference = CircleRosterHeadRef::from_stored_head(head, object);
                circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                    circle_id: head.circle_id,
                    head: &reference,
                })
            }
            Self::Metadata(head) => {
                let reference = CircleMetadataHeadRef::from_stored_head(head, object);
                circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                    circle_id: head.circle_id,
                    head: &reference,
                })
            }
        }
    }
}

impl<'operation, 'storage> CircleActivationVerifier<'operation, 'storage> {
    pub(super) async fn verify_circle_head_chain(
        &self,
        context: &ProtocolObjectContext,
        kind: CircleHeadKind,
        current: CircleHeadValue,
        current_object: ExactObjectRef,
        authority: &CircleStreamAuthority,
    ) -> Result<(), CircleOperationError> {
        let mut current = current;
        let mut current_object = current_object;
        loop {
            let position = current.position()?;
            if !current.verify_for_registration(&authority.registration)
                || position.store_root_hash != authority.registration.store_root.store_root_hash
                || position.author_pubkey != authority.registration.author_pubkey
                || position.device_id != authority.registration.device_id.to_string()
                || position.successor.activation != authority.activation_id
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle head differs from its activated registration".to_string(),
                ));
            }
            if position.seq == 1 {
                if position.successor.predecessor.is_some()
                    || current_object.slot() != &authority.first_slot
                {
                    return Err(CircleOperationError::InvalidState(
                        "first Circle head differs from its activated slot".to_string(),
                    ));
                }
                return Ok(());
            }
            let predecessor_object = position.successor.predecessor.clone().ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "successor Circle head omits its exact predecessor".to_string(),
                )
            })?;
            let predecessor_prefix = predecessor_object
                .slot()
                .logical_key()
                .strip_suffix(".json")
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(
                        "Circle predecessor head has a non-canonical logical key".to_string(),
                    )
                })?;
            let predecessor_bytes = self
                .storage
                .read_protocol_object(context, &predecessor_object, predecessor_prefix)
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
            let predecessor = CircleHeadValue::parse(kind, &predecessor_bytes)?;
            let predecessor_position = predecessor.position()?;
            if predecessor.semantic_prefix(predecessor_object.clone()) != predecessor_prefix
                || predecessor_position.store_root_hash != position.store_root_hash
                || predecessor_position.circle_id != position.circle_id
                || predecessor_position.author_pubkey != position.author_pubkey
                || predecessor_position.device_id != position.device_id
                || predecessor_position.stream_id != position.stream_id
                || predecessor_position.author_owner_grant != position.author_owner_grant
                || predecessor_position.seq.checked_add(1) != Some(position.seq)
                || predecessor_position.successor.next_slot != *current_object.slot()
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle head does not occupy its predecessor-reserved successor slot"
                        .to_string(),
                ));
            }
            current = predecessor;
            current_object = predecessor_object;
        }
    }

    pub(super) async fn verify_covered_control_heads(
        &mut self,
        verified_prefix: &VerifiedStreamActivationPrefix,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        control: &CircleControl,
    ) -> Result<(), CircleOperationError> {
        let access_epoch = control.access_epoch();
        let context = ProtocolObjectContext::store_encrypted(
            commit.store_root_hash,
            ProtocolObjectDomain::CircleControl,
        );
        for reference in &access_epoch.covered_control_heads {
            let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id: control.circle_id,
                control: &reference.coord,
            });
            let bytes = self
                .storage
                .read_protocol_object(&context, &reference.object, &prefix)
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
            let head: coven_protocol::circle::CircleControlHead = serde_json::from_slice(&bytes)
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "parse covered Circle control head: {error}"
                    ))
                })?;
            let CircleControlCoord {
                stream_id,
                author_owner_grant,
                ..
            } = &head.control;
            let authority = self
                .resolve_circle_stream_authority(
                    verified_prefix,
                    commit_ref,
                    commit,
                    head.successor.activation,
                    *stream_id,
                    control.circle_id,
                    author_owner_grant,
                    |circle_id, first_slot| GrantStreamAnchor::CircleControl {
                        circle_id,
                        first_slot,
                    },
                )
                .await?;
            if authority.activated_here
                || head.control != reference.coord
                || head.head_hash() != reference.head_hash
            {
                return Err(CircleOperationError::InvalidState(
                    "covered Circle control head differs from its exact reference".to_string(),
                ));
            }
            self.verify_circle_head_chain(
                &context,
                CircleHeadKind::Control,
                CircleHeadValue::Control(head),
                reference.object.clone(),
                &authority,
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn resolve_circle_stream_authority(
        &mut self,
        verified_prefix: &VerifiedStreamActivationPrefix,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        claimed_activation_id: StreamActivationId,
        stream_id: coven_protocol::causal_grants::AuthorStreamId,
        circle_id: CircleId,
        grant_id: &coven_protocol::membership::MembershipGrantId,
        expected_anchor: fn(CircleId, coven_protocol::objects::ObjectSlot) -> GrantStreamAnchor,
    ) -> Result<CircleStreamAuthority, CircleOperationError> {
        let root = self.root().clone();
        let current = commit
            .stream_activations()
            .iter()
            .find(|activation| activation.activation_id() == claimed_activation_id)
            .cloned();
        let (activation, activating_commit, activated_here) = if let Some(activation) = current {
            (activation, commit_ref.clone(), true)
        } else if let Some((activation, activating_commit)) =
            verified_prefix.activation(claimed_activation_id)
        {
            (activation.clone(), activating_commit.clone(), false)
        } else {
            let registered = self
                .database
                .registered_stream_activation(claimed_activation_id)
                .await
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle author stream {stream_id} has no verified activation"
                    ))
                })?;
            (
                registered.activation().clone(),
                registered.activating_commit().clone(),
                false,
            )
        };
        let StreamActivation::GrantAuthorized {
            store_root_hash,
            author_registration,
            grant_id: activation_grant,
            anchor,
        } = &activation
        else {
            return Err(CircleOperationError::InvalidState(
                "Circle author stream uses device authority".to_string(),
            ));
        };
        let expected = expected_anchor(circle_id, anchor.first_slot().clone());
        if *store_root_hash != root.store_root_hash
            || activation.author_stream_id() != stream_id
            || activation_grant != grant_id
            || anchor != &expected
        {
            return Err(CircleOperationError::InvalidState(
                "Circle author stream differs from its activation descriptor".to_string(),
            ));
        }
        if activated_here {
            if activating_commit != *commit_ref {
                return Err(CircleOperationError::InvalidState(
                    "same-commit Circle activation names another Store commit".to_string(),
                ));
            }
        } else {
            let reached = self
                .history
                .predecessor_commit_matching(
                    &commit.order,
                    Box::new(|predecessor| {
                        predecessor.reference() == &activating_commit
                            && predecessor
                                .value()
                                .stream_activations()
                                .binary_search(&activation)
                                .is_ok()
                    }),
                )
                .await
                .map_err(|error| match error {
                    crate::sync::store::commit_verification::merge_history::registration::RegistrationLoadError::Object(error) => {
                        CircleOperationError::Object(error)
                    }
                    crate::sync::store::commit_verification::merge_history::registration::RegistrationLoadError::Invalid(error) => {
                        CircleOperationError::InvalidState(error)
                    }
                })?
                .is_some();
            if !reached {
                return Err(CircleOperationError::InvalidState(
                    "Circle author stream activation is outside the commit predecessor history"
                        .to_string(),
                ));
            }
        }
        let registration = self
            .history
            .load_registration(author_registration)
            .await?
            .value;
        Ok(CircleStreamAuthority {
            activation_id: activation.activation_id(),
            first_slot: anchor.first_slot().clone(),
            registration,
            activated_here,
        })
    }
}
