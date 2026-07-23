use super::*;

/// Invite a member through Merge membership authority.
pub fn invite_member<'a>(
    storage: &'a dyn SyncStorage,
    cloud_home: &'a dyn crate::storage::cloud::CloudHome,
    user_keypair: &'a UserKeypair,
    hlc: &'a Hlc,
    public_key_hex: &'a str,
    invitee_email: Option<&'a str>,
    role: MemberRole,
    encryption: &'a EncryptionService,
    store_id: &'a str,
    store_name: &'a str,
    database: &'a crate::sync::store::database::StoreDatabase,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<crate::join_code::InviteCode, MembershipOpsError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(invite_merge_member_impl(
        storage,
        cloud_home,
        user_keypair,
        hlc,
        public_key_hex,
        invitee_email,
        role,
        encryption,
        store_id,
        store_name,
        database,
    ))
}
async fn invite_merge_member_impl(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    store_id: &str,
    store_name: &str,
    database: &crate::sync::store::database::StoreDatabase,
) -> Result<crate::join_code::InviteCode, MembershipOpsError> {
    validate_invitation(user_keypair, public_key_hex, &role)?;

    // Download existing membership entries
    let db = database.sqlite();
    let root_ref = required_store_root_ref(database).await?;
    let store_root_hash = root_ref.store_root_hash;

    // The founder is written once, when a store is created and first connects
    // its cloud (issue #102) — never lazily here. An empty listing at invite time
    // means the chain is missing (a fresh store that never founded, or a wiped
    // `membership/*`); bootstrapping a new founder on the spot is the takeover
    // primitive #104 describes, so refuse instead.
    let pinned_owner = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoFounderChain)?;
    let mut chain = Box::pin(load_current_exact_chain(
        storage,
        &root_ref,
        Some(&pinned_owner),
        Some(database),
    ))
    .await?;
    require_resolved_membership(&chain)?;
    let protocol_store_id = root_ref.store_root_id.to_string();

    // Create the invitation
    let invite_ts = hlc.now().to_string();
    let (join_info, wrapped_key) = Box::pin(super::create_invitation_with_encryption_durable(
        storage,
        cloud_home,
        store_root_hash,
        &mut chain,
        user_keypair,
        public_key_hex,
        invitee_email,
        role,
        encryption,
        &protocol_store_id,
        &invite_ts,
        database,
    ))
    .await?;

    info!(
        "Invited member {}...",
        &public_key_hex[..public_key_hex.len().min(16)]
    );

    // The invite anchors the joiner to the store's OWNER — the chain founder,
    // not whichever Owner happens to send the invite. A second Owner inviting must
    // still hand the joiner the founder's pubkey, or the joiner would pin the wrong
    // owner and reject the (correctly founder-anchored) chain forever (issue #102).
    let owner_pubkey = chain
        .founder_pubkey()
        .ok_or(MembershipOpsError::ChainHasNoFounder)?
        .to_string();

    // The chain as it stands after this invite is committed (including the
    // invitee's own Add and the head just published for it): its per-author-stream
    // heads are the floor the joiner seeds its watermark from, so a provider
    // can never roll the joiner back to a state before this invite.
    let membership_floor = chain.head_refs().to_vec();
    let store_protocol_root =
        crate::sync::store_objects::load_store_protocol_root(storage, &root_ref).await?;
    if store_protocol_root.value.descriptor.store_root_id() != root_ref.store_root_id
        || store_protocol_root.value.descriptor.founder_pubkey != owner_pubkey
    {
        return Err(MembershipOpsError::Chain(AnchoredChainError::LoadFailed(
            "Store protocol root differs from the invite authority".to_string(),
        )));
    }

    // Build the invite code
    Ok(crate::join_code::InviteCode {
        v: crate::join_code::INVITE_CODE_VERSION,
        store_id: store_id.to_string(),
        store_name: store_name.to_string(),
        join_info,
        owner_pubkey,
        wrapped_key,
        store_root: root_ref,
        membership_floor: crate::join_code::MembershipFloor(membership_floor),
    })
}

/// Remove a member through Merge membership authority and adopt the rotated key.
pub async fn remove_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    current_encryption: &EncryptionService,
    custody: &dyn MasterKeyCustody,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    database: &crate::sync::store::database::StoreDatabase,
) -> Result<String, MembershipOpsError> {
    remove_merge_member_impl(
        storage,
        cloud_home,
        user_keypair,
        hlc,
        public_key_hex,
        current_encryption,
        custody,
        cipher,
        pending_rotation,
        database,
    )
    .await
}
async fn remove_merge_member_impl(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    current_encryption: &EncryptionService,
    custody: &dyn MasterKeyCustody,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    database: &crate::sync::store::database::StoreDatabase,
) -> Result<String, MembershipOpsError> {
    let db = database.sqlite();
    let root_ref = required_store_root_ref(database).await?;
    let protocol_store_id = root_ref.store_root_id.to_string();
    // Download existing membership entries and build the chain.
    let store_root_hash = root_ref.store_root_hash;

    let pinned_owner = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoMembershipChain)?;
    let mut chain =
        load_current_exact_chain(storage, &root_ref, Some(&pinned_owner), Some(database)).await?;
    require_resolved_membership(&chain)?;

    // Revoke the member and rotate the cloud key. On return the rotation is
    // committed for every remaining member.
    let revoke_ts = hlc.now().to_string();
    let new_key = super::revoke_member_durable(
        storage,
        cloud_home,
        store_root_hash,
        &mut chain,
        user_keypair,
        public_key_hex,
        &protocol_store_id,
        &revoke_ts,
        current_encryption,
        pending_rotation,
        database,
    )
    .await?;

    info!(
        "Revoked member {}... and rotated encryption key",
        &public_key_hex[..public_key_hex.len().min(16)]
    );

    // Adopt the rotated key into this device's live cipher and custody. The cloud
    // rotation is already committed, so a failure here is not a generic membership
    // error but the specific half-applied state its own variant names.
    let generation = new_key.current_generation();
    let fingerprint = apply_key_rotation(new_key, custody, cipher)
        .map_err(|source| MembershipOpsError::RotationCommittedAdoptionFailed { source })?;
    super::complete_revoke_rotation_adoption(database, pending_rotation, generation).await?;
    Ok(fingerprint)
}
