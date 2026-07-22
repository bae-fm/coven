//! Verification and materialization of Store-activated Circle state.

use std::collections::BTreeSet;

use super::CircleOperationError;
use crate::database::Database;
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};
use crate::sync::circle::{
    circle_semantic_prefix, recipient_slot_with_peer, verify_circle_semantic_prefix,
    AccessEnvelope, CircleAccessDisposition, CircleAccessLeaf, CircleControl, CircleControlCoord,
    CircleId, CircleMetadataHeadRef, CircleRosterHeadRef, CircleSemanticSlot,
    MergeCircleOwnerAuthorityRef, PreparedAccessLeaf, PreparedCircleControl, ResolvedCircleRoster,
};
use crate::sync::circle_roster::CircleMaterializedRoster;
use crate::sync::storage::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use crate::sync::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    CircleAccessObjectRef, CircleActivationObjects, GrantStreamAnchor, ObjectHash,
    StoreBatchCommit, StoreBatchCommitRef, StoreDeviceRegistration, StoreRootRef, StreamActivation,
    StreamActivationId,
};

mod context;
mod metadata;
mod roster;
mod state;

pub(crate) use context::{load_exact_slot_bytes, verify_control_context};
use context::{read_exact_circle_object, verify_control_membership};
use metadata::load_circle_metadata_state;
use roster::{load_circle_authority_roster, load_circle_roster_state};
#[cfg(test)]
use state::CircleCurrentControl;
pub(crate) use state::{
    CircleAuthoringState, CircleCurrentState, VerifiedCircleAccess, VerifiedCircleActivations,
    VerifiedCircleActive, VerifiedCircleReference, VerifiedStreamActivationPrefix,
    VerifiedStreamActivations,
};

struct VerifiedAccessPair {
    reference: CircleAccessObjectRef,
    envelope: AccessEnvelope,
    leaf_bytes: Vec<u8>,
}

async fn load_verified_access_pairs(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
) -> Result<Vec<VerifiedAccessPair>, CircleOperationError> {
    let family = commit.candidate_family();
    let mut verified = Vec::with_capacity(objects.access.len());
    for reference in &objects.access {
        if reference.leaf.owner_pubkey != reference.envelope.owner_pubkey
            || reference.leaf.recipient_slot != reference.envelope.recipient_slot
            || reference.leaf.leaf_id != reference.envelope.leaf_id
            || reference.leaf.leaf_hash != reference.envelope.leaf_hash
            || reference.leaf.leaf_hash != reference.leaf.object.stored_hash()
            || reference.envelope.control_hash != control.coord.control_hash()
        {
            return Err(CircleOperationError::InvalidState(
                "paired Circle access references differ".to_string(),
            ));
        }
        let envelope_prefix = circle_access_envelope_semantic_prefix(
            circle_id,
            family,
            &reference.envelope.owner_pubkey,
            &reference.envelope.recipient_slot,
            reference.envelope.control_hash,
        );
        let envelope_bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleAccessEnvelope,
            ),
            &reference.envelope.object,
            &envelope_prefix,
        )
        .await?;
        let envelope: AccessEnvelope =
            serde_json::from_slice(&envelope_bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse circle access envelope: {error}"))
            })?;
        if envelope.candidate_family != family
            || envelope.circle_id != circle_id
            || envelope.owner_pubkey != reference.envelope.owner_pubkey
            || envelope.recipient_slot != reference.envelope.recipient_slot
            || envelope.control_hash != reference.envelope.control_hash
            || envelope.leaf_id != reference.envelope.leaf_id
            || envelope.leaf_hash != reference.envelope.leaf_hash
            || !envelope.verify(control, family)
        {
            return Err(CircleOperationError::InvalidState(
                "circle access envelope failed verification".to_string(),
            ));
        }
        let leaf_prefix = circle_access_leaf_semantic_prefix(
            circle_id,
            family,
            &reference.leaf.owner_pubkey,
            reference.leaf.epoch_id,
            &reference.leaf.recipient_slot,
            reference.leaf.leaf_id,
        );
        let leaf_bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::recipient_sealed(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleAccessLeaf,
            ),
            &reference.leaf.object,
            &leaf_prefix,
        )
        .await?;
        if ObjectHash::digest(&leaf_bytes) != reference.leaf.leaf_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle access leaf bytes differ from the paired leaf hash".to_string(),
            ));
        }
        verified.push(VerifiedAccessPair {
            reference: reference.clone(),
            envelope,
            leaf_bytes,
        });
    }
    Ok(verified)
}

fn verify_circle_owner_authority(
    author_pubkey: &str,
    control: &CircleControl,
    roster: &CircleMaterializedRoster,
) -> bool {
    verify_merge_circle_owner_authority(author_pubkey, &control.value.author_authority, roster)
}

fn verify_merge_circle_owner_authority(
    author_pubkey: &str,
    authority: &MergeCircleOwnerAuthorityRef,
    roster: &ResolvedCircleRoster,
) -> bool {
    match authority {
        MergeCircleOwnerAuthorityRef::Roster {
            grant_id,
            created_at,
            ..
        } => roster.authorizes_owner_grant(author_pubkey, grant_id, created_at),
        MergeCircleOwnerAuthorityRef::ConflictResolution {
            conflict_hash,
            resolution_hash,
        } => {
            let grant_id = crate::sync::circle_roster::derive_circle_resolution_grant(
                conflict_hash,
                author_pubkey,
            );
            roster.authorizes_resolution_grant(
                author_pubkey,
                &grant_id,
                &crate::sync::circle_roster::CircleRosterConflictResolutionRef {
                    conflict_hash: *conflict_hash,
                    resolver_pubkey: author_pubkey.to_string(),
                    resolution_hash: *resolution_hash,
                },
            )
        }
    }
}

struct CircleStreamAuthority {
    activation_id: StreamActivationId,
    first_slot: crate::storage::cloud::ObjectSlot,
    registration: StoreDeviceRegistration,
    activated_here: bool,
}

#[derive(Clone, Copy)]
enum CircleHeadKind {
    Control,
    Roster,
    Metadata,
}

enum CircleHeadValue {
    Control(crate::sync::circle::CircleControlHead),
    Roster(crate::sync::circle::CircleRosterHead),
    Metadata(crate::sync::circle::CircleMetadataHead),
}

struct CircleHeadPosition<'a> {
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    author_pubkey: &'a str,
    device_id: &'a str,
    stream_id: crate::sync::causal_grants::AuthorStreamId,
    author_owner_grant: &'a crate::sync::membership::MembershipGrantId,
    seq: u64,
    successor: &'a crate::sync::store_commit::SuccessorLink,
}

impl CircleHeadValue {
    fn parse(kind: CircleHeadKind, bytes: &[u8]) -> Result<Self, CircleOperationError> {
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

    fn position(&self) -> Result<CircleHeadPosition<'_>, CircleOperationError> {
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

    fn verify_for_registration(&self, registration: &StoreDeviceRegistration) -> bool {
        match self {
            Self::Control(head) => head.verify(registration),
            Self::Roster(head) => head.verify_for_registration(registration),
            Self::Metadata(head) => head.verify_for_registration(registration),
        }
    }

    fn semantic_prefix(&self, object: ExactObjectRef) -> String {
        match self {
            Self::Control(head) => circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id: head.circle_id,
                control: &head.control,
                head_hash: head.head_hash(),
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

async fn verify_circle_head_chain(
    storage: &dyn SyncStorage,
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
        let predecessor_bytes =
            read_exact_circle_object(storage, context, &predecessor_object, predecessor_prefix)
                .await?;
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
                "Circle head does not occupy its predecessor-reserved successor slot".to_string(),
            ));
        }
        current = predecessor;
        current_object = predecessor_object;
    }
}

async fn verify_covered_control_heads(
    db: &Database,
    verified_prefix: &VerifiedStreamActivationPrefix,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    control: &CircleControl,
) -> Result<(), CircleOperationError> {
    let active_epoch = &control.value.active_epoch;
    let context = ProtocolObjectContext::store_encrypted(
        commit.store_root_hash,
        ProtocolObjectDomain::CircleControl,
    );
    for reference in &active_epoch.covered_control_heads {
        let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
            circle_id: control.circle_id,
            control: &reference.coord,
            head_hash: reference.head_hash,
        });
        let bytes = read_exact_circle_object(storage, &context, &reference.object, &prefix).await?;
        let head: crate::sync::circle::CircleControlHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse covered Circle control head: {error}"
                ))
            })?;
        let CircleControlCoord {
            stream_id,
            author_owner_grant,
            ..
        } = &head.control;
        let authority = resolve_circle_stream_authority(
            db,
            verified_prefix,
            storage,
            root,
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
        verify_circle_head_chain(
            storage,
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

async fn resolve_circle_stream_authority(
    db: &Database,
    verified_prefix: &VerifiedStreamActivationPrefix,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    claimed_activation_id: StreamActivationId,
    stream_id: crate::sync::causal_grants::AuthorStreamId,
    circle_id: CircleId,
    grant_id: &crate::sync::membership::MembershipGrantId,
    expected_anchor: fn(CircleId, crate::storage::cloud::ObjectSlot) -> GrantStreamAnchor,
) -> Result<CircleStreamAuthority, CircleOperationError> {
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
        let registered = db
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
        let reached = crate::sync::store::pull::predecessor_commit_matching(
            storage,
            root,
            &commit.order,
            Box::new(|reference, predecessor| {
                reference == &activating_commit
                    && predecessor
                        .stream_activations()
                        .binary_search(&activation)
                        .is_ok()
            }),
        )
        .await
        .map_err(|error| match error {
            crate::sync::store::pull::RegistrationLoadError::Object(error) => {
                CircleOperationError::Object(error)
            }
            crate::sync::store::pull::RegistrationLoadError::Invalid(error) => {
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
    let registration =
        crate::sync::store_objects::load_registration_ref(storage, root, author_registration)
            .await?
            .value;
    Ok(CircleStreamAuthority {
        activation_id: activation.activation_id(),
        first_slot: anchor.first_slot().clone(),
        registration,
        activated_here,
    })
}

pub(crate) async fn load_circle_activations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    identity: &UserKeypair,
    founder_pubkey: &str,
) -> Result<VerifiedCircleActivations, CircleOperationError> {
    let verified_prefix = VerifiedStreamActivationPrefix::empty();
    Box::pin(load_circle_activations_with_prefix(
        db,
        storage,
        root,
        commit_ref,
        commit,
        author,
        identity,
        founder_pubkey,
        &verified_prefix,
    ))
    .await
}

pub(crate) async fn load_circle_activations_with_prefix(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    identity: &UserKeypair,
    founder_pubkey: &str,
    verified_prefix: &VerifiedStreamActivationPrefix,
) -> Result<VerifiedCircleActivations, CircleOperationError> {
    commit_ref
        .verify_commit(commit)
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if root.store_root_hash != commit.store_root_hash
        || commit
            .author_registration
            .verify_registration(author)
            .is_err()
    {
        return Err(CircleOperationError::InvalidState(
            "Circle activation authority differs from its exact Store commit".to_string(),
        ));
    }
    let mut activations = Vec::with_capacity(commit.circle_controls().len());
    let mut consumed_stream_activations = BTreeSet::new();
    for reference in commit.circle_controls() {
        let objects = reference.objects();
        let control_prefix = circle_semantic_prefix(CircleSemanticSlot::Control {
            circle_id: reference.circle_id(),
            control: reference.control(),
        });
        let control_bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            &objects.control,
            &control_prefix,
        )
        .await?;
        if ObjectHash::digest(&control_bytes) != reference.control().control_hash() {
            return Err(CircleOperationError::InvalidState(
                "Circle control bytes differ from the signed control hash".to_string(),
            ));
        }
        let control_value: CircleControl =
            serde_json::from_slice(&control_bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse Circle control: {error}"))
            })?;
        let declared_coord = control_value.coord();
        if !control_value.verify()
            || verify_circle_semantic_prefix(
                &control_prefix,
                CircleSemanticSlot::Control {
                    circle_id: control_value.circle_id,
                    control: &declared_coord,
                },
            )
            .is_err()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle control failed exact verification".to_string(),
            ));
        }
        let control = PreparedCircleControl {
            coord: reference.control().clone(),
            bytes: control_bytes,
            value: control_value,
        };
        let circle_id = reference.circle_id;
        let control_coord = &reference.control;
        let head_hash = reference.head_hash;
        let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
            circle_id,
            control: control_coord,
            head_hash,
        });
        let head_object = reference.head_object();
        let bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            head_object,
            &prefix,
        )
        .await?;
        let head: crate::sync::circle::CircleControlHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse exact Circle control head: {error}"
                ))
            })?;
        let CircleControlCoord {
            stream_id,
            author_pubkey,
            author_owner_grant,
            seq,
            ..
        } = &head.control;
        let authority = resolve_circle_stream_authority(
            db,
            verified_prefix,
            storage,
            root,
            commit_ref,
            commit,
            head.successor.activation,
            *stream_id,
            circle_id,
            author_owner_grant,
            |circle_id, first_slot| GrantStreamAnchor::CircleControl {
                circle_id,
                first_slot,
            },
        )
        .await?;
        verify_circle_head_chain(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            CircleHeadKind::Control,
            CircleHeadValue::Control(head.clone()),
            head_object.clone(),
            &authority,
        )
        .await?;
        if !head.verify(author)
            || !head.verify(&authority.registration)
            || authority.registration.author_pubkey != *author_pubkey
            || (authority.activated_here && *seq != 1)
            || head.successor.activation != authority.activation_id
            || (*seq == 1
                && (head.successor.predecessor.is_some()
                    || head_object.slot() != &authority.first_slot))
            || head.head_hash() != head_hash
            || head.entry != objects.control
            || verify_circle_semantic_prefix(
                &prefix,
                CircleSemanticSlot::ControlHead {
                    circle_id: head.circle_id,
                    control: &head.control,
                    head_hash: head.head_hash(),
                },
            )
            .is_err()
            || head.store_root_hash != commit.store_root_hash
            || head.circle_id != circle_id
        {
            return Err(CircleOperationError::InvalidState(
                "Circle control head failed exact verification".to_string(),
            ));
        }
        if authority.activated_here {
            consumed_stream_activations.insert(authority.activation_id);
        }
        verify_covered_control_heads(
            db,
            verified_prefix,
            storage,
            root,
            commit_ref,
            commit,
            &control.value,
        )
        .await?;
        verify_control_context(reference, &control, commit_ref, commit, author)?;
        consume_public_private_stream_activations(
            commit,
            author,
            reference.circle_id(),
            &control,
            objects,
            &mut consumed_stream_activations,
        )?;
        let verified_access =
            load_verified_access_pairs(storage, commit, reference.circle_id(), &control, objects)
                .await?;
        let checkpoint_members = Box::pin(verify_control_membership(
            storage,
            root,
            &control,
            founder_pubkey,
        ))
        .await?;
        let own_pubkey = keys::public_key_hex(identity);
        if !checkpoint_members
            .iter()
            .any(|(pubkey, _)| pubkey == &own_pubkey)
        {
            activations.push(VerifiedCircleReference {
                reference: reference.clone(),
                circle_id: reference.circle_id(),
                control,
                local_access: None,
            });
            continue;
        }
        let owner_pubkey = &control.value.author_pubkey;
        let owner = (
            owner_pubkey.clone(),
            recipient_slot_with_peer(identity, owner_pubkey, reference.circle_id()).map_err(
                |error| {
                    CircleOperationError::InvalidState(format!(
                        "derive circle Owner recipient slot: {error}"
                    ))
                },
            )?,
        );
        let access = verified_access
            .iter()
            .find(|candidate| {
                candidate.reference.envelope.owner_pubkey == owner.0
                    && candidate.reference.envelope.recipient_slot == owner.1
                    && candidate.reference.envelope.control_hash
                        == reference.control().control_hash()
            })
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle activation lacks the recipient's exact access envelope".to_string(),
                )
            })?;
        let envelope = &access.envelope;
        let leaf_bytes = access.leaf_bytes.clone();
        let plaintext = keys::seal_box_decrypt(&leaf_bytes, &identity.to_x25519_secret_key())
            .map_err(|error| {
                CircleOperationError::InvalidState(format!("open circle access leaf: {error}"))
            })?;
        let leaf: CircleAccessLeaf = serde_json::from_slice(&plaintext).map_err(|error| {
            CircleOperationError::InvalidState(format!("parse circle access leaf: {error}"))
        })?;
        let prepared_leaf = PreparedAccessLeaf {
            bytes: leaf_bytes,
            value: leaf,
            leaf_hash: envelope.leaf_hash,
        };
        let leaf = &prepared_leaf.value;
        if leaf.candidate_family != commit.candidate_family()
            || leaf.owner_pubkey != owner.0
            || leaf.recipient_pubkey != own_pubkey
            || leaf.recipient_slot != owner.1
            || leaf.store_membership != control.value.store_membership_state_ref()
            || leaf.epoch_id != access.reference.leaf.epoch_id
            || leaf.leaf_id != access.reference.leaf.leaf_id
            || !prepared_leaf.verify_envelope(&control, envelope, commit.candidate_family())
        {
            return Err(CircleOperationError::InvalidState(
                "circle access leaf failed context verification".to_string(),
            ));
        }
        let active = match &leaf.disposition {
            CircleAccessDisposition::Active { keyring, .. } => {
                let encryption = EncryptionService::from(
                    MasterKeyring::from_serialized(keyring).map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse circle access keyring: {error}"
                        ))
                    })?,
                );
                let authority_roster = load_circle_authority_roster(
                    db,
                    verified_prefix,
                    storage,
                    commit,
                    reference.circle_id(),
                    &control,
                    encryption.clone(),
                    objects,
                    root,
                    commit_ref,
                    &mut consumed_stream_activations,
                )
                .await?;
                if !verify_circle_owner_authority(
                    &control.value.author_pubkey,
                    &control.value,
                    &authority_roster,
                ) {
                    return Err(CircleOperationError::InvalidState(
                        "circle control author lacks its exact historical Owner grant".to_string(),
                    ));
                }
                let resolved = load_circle_roster_state(
                    db,
                    verified_prefix,
                    storage,
                    root,
                    commit_ref,
                    commit,
                    reference.circle_id(),
                    &control.value.value.active_epoch.roster,
                    encryption.clone(),
                    objects,
                    &mut consumed_stream_activations,
                )
                .await?;
                let resolved_members = resolved.members();
                if !resolved_members.contains_key(&leaf.recipient_pubkey) {
                    return Err(CircleOperationError::InvalidState(
                        "circle Active access recipient is absent from its resolved roster"
                            .to_string(),
                    ));
                }
                let roster_owners = resolved_members
                    .iter()
                    .filter_map(|(pubkey, role)| {
                        (*role == crate::sync::circle::CircleRole::Owner).then_some(pubkey.clone())
                    })
                    .collect::<Vec<_>>();
                if roster_owners != control.value.owners() {
                    return Err(CircleOperationError::InvalidState(
                        "circle control Owners differ from its roster".to_string(),
                    ));
                }
                let metadata_state = control.value.metadata_state_ref();
                let metadata = load_circle_metadata_state(
                    db,
                    verified_prefix,
                    storage,
                    commit,
                    reference.circle_id(),
                    &metadata_state,
                    encryption,
                    objects,
                    root,
                    commit_ref,
                    &mut consumed_stream_activations,
                )
                .await?;
                Some(VerifiedCircleActive {
                    roster: resolved,
                    metadata,
                })
            }
            CircleAccessDisposition::Inactive => None,
        };
        activations.push(VerifiedCircleReference {
            reference: reference.clone(),
            circle_id: reference.circle_id(),
            control,
            local_access: Some(VerifiedCircleAccess {
                envelope: envelope.clone(),
                leaf: prepared_leaf,
                active,
            }),
        });
    }
    let declared = commit
        .stream_activations()
        .iter()
        .map(StreamActivation::activation_id)
        .collect::<BTreeSet<_>>();
    if consumed_stream_activations != declared {
        return Err(CircleOperationError::InvalidState(
            "Store commit stream activations do not exactly introduce its first Circle heads"
                .to_string(),
        ));
    }
    let stream_activations =
        VerifiedStreamActivations::from_verified_circle_commit(commit, commit_ref)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    Ok(VerifiedCircleActivations {
        circles: activations,
        stream_activations,
    })
}

fn consume_public_private_stream_activations(
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
    consumed: &mut BTreeSet<StreamActivationId>,
) -> Result<(), CircleOperationError> {
    let roster = control.value.roster_state_ref();
    let metadata = control.value.metadata_state_ref();
    for activation in commit.stream_activations() {
        let StreamActivation::GrantAuthorized {
            store_root_hash,
            author_registration,
            grant_id,
            anchor,
        } = activation
        else {
            continue;
        };
        let valid = match anchor {
            GrantStreamAnchor::CircleRoster {
                circle_id: anchor_circle,
                first_slot,
            } if *anchor_circle == circle_id => roster.heads.iter().any(|head| {
                head.coord.seq == 1
                    && head.coord.author_pubkey == author.author_pubkey
                    && head.coord.device_id == author.device_id.to_string()
                    && head.coord.author_owner_grant == *grant_id
                    && head.coord.stream_id == activation.author_stream_id()
                    && head.object.slot() == first_slot
                    && objects.roster_heads.contains(head)
            }),
            GrantStreamAnchor::CircleMetadata {
                circle_id: anchor_circle,
                first_slot,
            } if *anchor_circle == circle_id => metadata.heads.iter().any(|head| {
                head.coord.seq == 1
                    && head.coord.author_pubkey == author.author_pubkey
                    && head.coord.device_id == author.device_id.to_string()
                    && head.coord.author_owner_grant == *grant_id
                    && head.coord.stream_id == activation.author_stream_id()
                    && head.object.slot() == first_slot
                    && objects.metadata_heads.contains(head)
            }),
            _ => continue,
        };
        if *store_root_hash != commit.store_root_hash
            || author_registration != &commit.author_registration
            || grant_id != &control.value.author_grant_id()
            || !valid
        {
            return Err(CircleOperationError::InvalidState(
                "private Circle stream activation differs from its signed public first-head reference"
                    .to_string(),
            ));
        }
        consumed.insert(activation.activation_id());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
