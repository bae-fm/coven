use super::*;

pub async fn current_membership_floor(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    pinned_owner: Option<&str>,
    watermark_db: Option<&Database>,
) -> Result<Vec<MembershipHeadRef>, MembershipOpsError> {
    let chain = load_current_exact_chain(storage, root, pinned_owner, watermark_db).await?;
    Ok(chain.head_refs().to_vec())
}

/// Read the membership chain from the sync storage and return the current members.
pub async fn get_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
    db: &Database,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => get_merge_members(storage, user_pubkey, db).await,
        crate::WritePolicy::Serial => get_serial_members(storage, user_pubkey, db).await,
    }
}

async fn get_serial_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
    db: &Database,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    let root_ref = required_store_root_ref(db).await?;
    let state = match db
        .serial_authorization_state()
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
    {
        Some(state) => state.membership,
        None => {
            if db
                .latest_local_store_position()
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?
                .is_some()
            {
                return Err(MembershipOpsError::Database(
                    "Serial membership state is absent after a Serial commit was materialized"
                        .to_string(),
                ));
            }
            let root = crate::sync::store_objects::load_store_protocol_root(storage, &root_ref)
                .await?
                .value;
            let founder =
                crate::sync::store_objects::load_founder_registration(storage, &root_ref).await?;
            let founder_ref =
                crate::sync::store_commit::StoreDeviceRegistrationRef::from_registration(
                    &founder.value,
                    founder.object.clone(),
                );
            crate::sync::membership::SerialAuthorizationState::from_founder(
                &root_ref,
                &root,
                &founder_ref,
                &founder.value,
            )
            .map_err(|error| MembershipOpsError::Database(error.to_string()))?
            .membership
        }
    };
    let user_pubkey_hex = user_pubkey.map(hex::encode);
    Ok(state
        .current_members()
        .into_iter()
        .map(|(pubkey, role)| MemberInfo {
            is_self: user_pubkey_hex.as_deref() == Some(&pubkey),
            pubkey,
            role,
        })
        .collect())
}

async fn get_merge_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
    db: &Database,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    let root_ref = required_store_root_ref(db).await?;
    let pinned_owner = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoFounderChain)?;
    let chain = load_current_exact_chain(storage, &root_ref, Some(&pinned_owner), Some(db)).await?;
    require_resolved_membership(&chain)?;
    let user_pubkey_hex = user_pubkey.map(hex::encode);

    let current = chain.current_members();
    let members = current
        .into_iter()
        .map(|(pubkey, role)| {
            let is_self = user_pubkey_hex.as_deref() == Some(&pubkey);
            MemberInfo {
                pubkey,
                role,
                is_self,
            }
        })
        .collect();

    Ok(members)
}
