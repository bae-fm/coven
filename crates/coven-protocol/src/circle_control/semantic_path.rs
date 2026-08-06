use super::*;

#[derive(Debug, Clone, Copy)]
pub enum CircleSemanticSlot<'a> {
    Control {
        circle_id: CircleId,
        control: &'a CircleControlCoord,
    },
    ControlHead {
        circle_id: CircleId,
        control: &'a CircleControlCoord,
    },
    RosterEntry {
        circle_id: CircleId,
        coord: &'a crate::circle_roster::CircleRosterCoord,
    },
    RosterHead {
        circle_id: CircleId,
        head: &'a CircleRosterHeadRef,
    },
    RosterResolution {
        circle_id: CircleId,
        resolution: &'a crate::circle_roster::CircleRosterConflictResolutionRef,
    },
    MetadataEntry {
        circle_id: CircleId,
        coord: &'a CircleMetadataCoord,
    },
    MetadataHead {
        circle_id: CircleId,
        head: &'a CircleMetadataHeadRef,
    },
}

pub fn circle_semantic_prefix(slot: CircleSemanticSlot<'_>) -> String {
    match slot {
        CircleSemanticSlot::Control { circle_id, control } => format!(
            "circle-control/{}/merge/entries/{author_pubkey}/{device_id}/{author_owner_grant}/{stream_id}/{seq}/{control_hash}",
            circle_id,
            author_pubkey = control.author_pubkey,
            device_id = control.device_id,
            author_owner_grant = control.author_owner_grant,
            stream_id = control.stream_id,
            seq = control.seq,
            control_hash = control.control_hash,
        ),
        CircleSemanticSlot::ControlHead { circle_id, control } => {
            circle_control_head_prefix(
                circle_id,
                &CircleAuthorStreamKey {
                    author_pubkey: control.author_pubkey.clone(),
                    device_id: control.device_id.clone(),
                    stream_id: control.stream_id,
                    author_owner_grant: control.author_owner_grant.clone(),
                },
                control.seq,
            )
        }
        CircleSemanticSlot::RosterEntry { circle_id, coord } => format!(
            "circles/{circle_id}/roster/entries/{}/{}/{}/{}/{}/{}",
            coord.author_pubkey,
            coord.device_id,
            coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
            coord.entry_hash
        ),
        CircleSemanticSlot::RosterHead { circle_id, head } => {
            circle_roster_head_prefix(circle_id, &head.coord.stream_key(), head.coord.seq)
        }
        CircleSemanticSlot::RosterResolution {
            circle_id,
            resolution,
        } => format!(
            "circles/{circle_id}/roster/resolutions/{}/{}/{}",
            resolution.conflict_hash,
            resolution.resolver_pubkey,
            resolution.resolution_hash
        ),
        CircleSemanticSlot::MetadataEntry { circle_id, coord } => format!(
            "circles/{circle_id}/metadata/entries/{}/{}/{}/{}/{}/{}",
            coord.author_pubkey,
            coord.device_id,
            coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
            coord.metadata_hash
        ),
        CircleSemanticSlot::MetadataHead { circle_id, head } => {
            circle_metadata_head_prefix(circle_id, &head.coord.stream_key(), head.coord.seq)
        }
    }
}

pub fn circle_control_head_prefix(
    circle_id: CircleId,
    stream: &CircleAuthorStreamKey,
    seq: u64,
) -> String {
    format!(
        "circle-control/{circle_id}/merge/heads/{}/{}/{}/{}/{seq}",
        stream.author_pubkey, stream.device_id, stream.author_owner_grant, stream.stream_id
    )
}

pub fn circle_roster_head_prefix(
    circle_id: CircleId,
    stream: &CircleAuthorStreamKey,
    seq: u64,
) -> String {
    format!(
        "circles/{circle_id}/roster/heads/{}/{}/{}/{}/{seq}",
        stream.author_pubkey, stream.device_id, stream.author_owner_grant, stream.stream_id
    )
}

pub fn circle_metadata_head_prefix(
    circle_id: CircleId,
    stream: &CircleAuthorStreamKey,
    seq: u64,
) -> String {
    format!(
        "circles/{circle_id}/metadata/heads/{}/{}/{}/{}/{seq}",
        stream.author_pubkey, stream.device_id, stream.author_owner_grant, stream.stream_id
    )
}

pub fn circle_epoch_close_outcome_semantic_prefix(
    circle_id: CircleId,
    close_id: CircleEpochCloseId,
) -> String {
    format!("circles/{circle_id}/epoch-close/{close_id}/outcome")
}

pub fn circle_epoch_close_intent_semantic_prefix(
    circle_id: CircleId,
    close_id: CircleEpochCloseId,
    intent_hash: ObjectHash,
) -> String {
    format!("circles/{circle_id}/epoch-close/{close_id}/intent/{intent_hash}")
}

pub fn circle_epoch_close_response_semantic_prefix(
    circle_id: CircleId,
    close_id: CircleEpochCloseId,
    device_id: crate::store_commit::StoreDeviceId,
) -> String {
    format!("circles/{circle_id}/epoch-close/{close_id}/responses/{device_id}")
}

pub fn verify_circle_semantic_prefix(
    actual: &str,
    slot: CircleSemanticSlot<'_>,
) -> Result<(), CircleSemanticPathError> {
    let expected = circle_semantic_prefix(slot);
    if actual == expected {
        Ok(())
    } else {
        Err(CircleSemanticPathError {
            expected,
            actual: actual.to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Circle object path {actual:?} does not match signed coordinate path {expected:?}")]
pub struct CircleSemanticPathError {
    pub expected: String,
    pub actual: String,
}

pub fn recipient_slot(
    owner: &dyn coven_keys::keys::IdentityKeyAuthority,
    recipient_pubkey: &str,
    circle_id: CircleId,
) -> Result<String, CircleTransitionError> {
    recipient_slot_with_peer(owner, recipient_pubkey, circle_id)
}

pub fn recipient_slot_with_peer(
    local_identity: &dyn coven_keys::keys::IdentityKeyAuthority,
    peer_pubkey: &str,
    circle_id: CircleId,
) -> Result<String, CircleTransitionError> {
    let peer_x25519 = keys::ed25519_hex_to_x25519_public_key(peer_pubkey)
        .map_err(|_| CircleTransitionError::InvalidRecipient(peer_pubkey.to_string()))?;
    let shared = keys::x25519_shared_secret(local_identity.to_x25519_secret_key(), peer_x25519)
        .map_err(|_| CircleTransitionError::InvalidRecipient(peer_pubkey.to_string()))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&shared).expect("HMAC accepts X25519 output");
    mac.update(RECIPIENT_SLOT_DOMAIN);
    mac.update(circle_id.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}
