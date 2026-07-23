use super::*;

pub(crate) async fn current_membership_floor(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    pinned_owner: Option<&str>,
    watermark_database: Option<&StoreDatabase>,
) -> Result<Vec<MembershipHeadRef>, MembershipOpsError> {
    let chain = load_current_exact_chain(storage, root, pinned_owner, watermark_database).await?;
    Ok(chain.head_refs().to_vec())
}

/// Read the membership chain from the sync storage and return the current members.
pub(crate) async fn get_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
    database: &StoreDatabase,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    get_merge_members(storage, user_pubkey, database).await
}

async fn get_merge_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
    database: &StoreDatabase,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    let db = database.sqlite();
    let root_ref = required_store_root_ref(database).await?;
    let pinned_owner = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoFounderChain)?;
    let chain =
        load_current_exact_chain(storage, &root_ref, Some(&pinned_owner), Some(database)).await?;
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
