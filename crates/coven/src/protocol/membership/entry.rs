use super::*;

#[cfg(test)]
pub(crate) fn test_wrapped_key_ref(
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
            crate::protocol::objects::ObjectSlot::logical(logical_key)
                .expect("test wrapped-key slot is valid"),
            label.len() as u64,
            ObjectHash::digest(label),
        ),
    }
}

pub(crate) fn derive_founder_stream_id(store_id: &str, owner_pubkey: &str) -> AuthorStreamId {
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
        crate::protocol::store_commit::STORE_MEMBERSHIP_HEAD_PREFIX,
    );
    first_slot
        .logical_key()
        .strip_prefix(&prefix)?
        .strip_suffix("/1.json")?
        .parse()
        .ok()
}

pub(crate) fn derive_grant_id(
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

pub(crate) fn founder_entry_for_creation(
    store_id: &str,
    creation_id: StoreCreationId,
    owner: &UserKeypair,
    owner_grant_id: MembershipGrantId,
    created_at: &str,
    membership: GrantStreamAnchor,
    provider_admin: crate::protocol::provider::FounderProviderAdminGrant,
) -> MembershipEntry {
    let owner_pubkey = keys::public_key_hex(owner);
    let stream_id = derive_founder_stream_id(store_id, &owner_pubkey);
    let mut entry = MembershipEntry {
        version: STORE_PROTOCOL_VERSION,
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
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, owner);
    entry
}

#[cfg(test)]
pub(crate) fn founder_entry(
    store_id: &str,
    owner: &UserKeypair,
    owner_grant_id: MembershipGrantId,
    created_at: &str,
    membership: GrantStreamAnchor,
    provider_admin: crate::protocol::provider::FounderProviderAdminGrant,
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

pub(crate) fn canonical_bytes(entry: &MembershipEntry) -> Vec<u8> {
    #[derive(Serialize)]
    struct Signed<'a> {
        version: u32,
        store_id: &'a str,
        author_pubkey: &'a str,
        author_owner_grant: &'a MembershipGrantId,
        stream_id: AuthorStreamId,
        seq: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        previous_hash: Option<ObjectHash>,
        dependencies: &'a [MembershipCoord],
        resolution_dependencies: &'a [StoreMembershipConflictResolutionRef],
        created_at: &'a str,
        change: &'a MembershipChange,
        #[serde(skip_serializing_if = "Option::is_none")]
        provider_admin: Option<&'a crate::protocol::provider::ProviderAdminMembershipChange>,
    }
    serde_json::to_vec(&Signed {
        version: entry.version,
        store_id: &entry.store_id,
        author_pubkey: &entry.author_pubkey,
        author_owner_grant: &entry.author_owner_grant,
        stream_id: entry.stream_id,
        seq: entry.seq,
        previous_hash: entry.previous_hash,
        dependencies: &entry.dependencies,
        resolution_dependencies: &entry.resolution_dependencies,
        created_at: &entry.created_at,
        change: &entry.change,
        provider_admin: entry.provider_admin.as_ref(),
    })
    .expect("membership signed fields serialize")
}

pub(crate) fn entry_hash(entry: &MembershipEntry) -> ObjectHash {
    ObjectHash::digest(
        &serde_json::to_vec(entry).expect("membership entry serialization cannot fail"),
    )
}

pub(crate) fn sign_membership_entry(entry: &mut MembershipEntry, keypair: &UserKeypair) {
    entry.author_pubkey = keys::public_key_hex(keypair);
    let (_, signature) = keys::sign_hex(keypair, &canonical_bytes(entry));
    entry.signature = signature;
}

pub(crate) fn verify_membership_entry(entry: &MembershipEntry) -> bool {
    let activation_position_is_valid = match &entry.change {
        MembershipChange::ResolutionActivation { .. } => {
            entry.seq == 1
                && entry.previous_hash.is_none()
                && entry
                    .dependencies
                    .iter()
                    .all(|dependency| dependency.stream_key() != entry.coord().stream_key())
        }
        _ => true,
    };
    activation_position_is_valid
        && entry
            .resolution_dependencies
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        && keys::verify_signature_hex(
            &entry.author_pubkey,
            &entry.signature,
            &canonical_bytes(entry),
        )
}
