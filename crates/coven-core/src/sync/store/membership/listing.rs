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
    get_merge_members(storage, user_pubkey, db).await
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
