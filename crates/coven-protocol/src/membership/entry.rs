use super::*;

#[cfg(any(test, feature = "test-utils"))]
pub fn test_wrapped_key_ref(
    owner_pubkey: &str,
    recipient_pubkey: &str,
    generation: u64,
    label: &[u8],
) -> WrappedStoreKeyRef {
    let wrap_hash = ObjectHash::digest(
        &[
            label,
            owner_pubkey.as_bytes(),
            recipient_pubkey.as_bytes(),
            &generation.to_le_bytes(),
        ]
        .concat(),
    );
    let logical_key =
        format!("keys/{owner_pubkey}/{recipient_pubkey}/{generation}/{wrap_hash}.json");
    WrappedStoreKeyRef {
        owner_pubkey: owner_pubkey.to_string(),
        recipient_pubkey: recipient_pubkey.to_string(),
        generation,
        wrap_hash,
        object: ExactObjectRef::new(
            crate::objects::ObjectSlot::logical(logical_key)
                .expect("test wrapped-key slot is valid"),
            label.len() as u64,
            ObjectHash::digest(label),
        ),
    }
}

pub fn derive_founder_stream_id(store_id: &str, owner_pubkey: &str) -> AuthorStreamId {
    AuthorStreamId::from_digest(ObjectHash::digest(
        format!("coven.membership-founder-stream.v1\0{store_id}\0{owner_pubkey}").as_bytes(),
    ))
}

pub(super) fn store_membership_anchor_stream(
    owner_pubkey: &str,
    owner_grant: &MembershipGrantId,
    anchor: &GrantStreamAnchor,
) -> Option<AuthorStreamId> {
    let GrantStreamAnchor::StoreMembership { first_slot } = anchor else {
        return None;
    };
    let prefix = format!(
        "{}{owner_pubkey}/{owner_grant}/",
        crate::store_commit::STORE_MEMBERSHIP_HEAD_PREFIX,
    );
    first_slot
        .logical_key()
        .strip_prefix(&prefix)?
        .strip_suffix("/1.json")?
        .parse()
        .ok()
}

pub fn derive_grant_id(
    store_id: &str,
    author_pubkey: &str,
    author_grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
    user_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!(
            "coven.membership-grant.v1\0{store_id}\0{author_pubkey}\0{author_grant}\0{stream_id}\0{seq}\0{user_pubkey}"
        )
        .as_bytes(),
    ))
}

pub fn founder_entry_for_creation(
    store_id: &str,
    creation_id: StoreCreationId,
    owner: &UserKeypair,
    owner_grant_id: MembershipGrantId,
    created_at: &str,
    membership: GrantStreamAnchor,
    provider_admin: crate::provider::FounderProviderAdminGrant,
) -> MembershipEntry {
    let owner_pubkey = keys::public_key_hex(owner);
    let stream_id = derive_founder_stream_id(store_id, &owner_pubkey);
    Signed::sign(
        MembershipEntryBody {
            store_id: store_id.to_string(),
            author_pubkey: owner_pubkey.clone(),
            author_owner_grant: owner_grant_id.clone(),
            stream_id,
            seq: 1,
            previous_hash: None,
            dependencies: Vec::new(),
            resolution_dependencies: Vec::new(),
            created_at: created_at.to_string(),
            change: MembershipChange::Founder {
                creation_id,
                owner_pubkey,
                owner_grant_id,
                membership,
                provider_admin,
            },
            provider_admin: None,
        },
        owner,
    )
}

#[cfg(any(test, feature = "test-utils"))]
pub fn founder_entry(
    store_id: &str,
    owner: &UserKeypair,
    owner_grant_id: MembershipGrantId,
    created_at: &str,
    membership: GrantStreamAnchor,
    provider_admin: crate::provider::FounderProviderAdminGrant,
) -> MembershipEntry {
    founder_entry_for_creation(
        store_id,
        StoreCreationId::from_nonce(store_id),
        owner,
        owner_grant_id,
        created_at,
        membership,
        provider_admin,
    )
}

pub fn verify_membership_entry(entry: &MembershipEntry) -> bool {
    let activation_position_is_valid = match &entry.change {
        MembershipChange::ResolutionActivation { .. } => causal_grants::starts_author_stream(
            entry.seq,
            entry.previous_hash,
            &entry.coord().stream_key(),
            entry.dependencies.iter().map(MembershipCoord::stream_key),
        ),
        _ => true,
    };
    activation_position_is_valid
        && entry
            .resolution_dependencies
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        && entry.verify_by(&entry.author_pubkey).is_ok()
}
