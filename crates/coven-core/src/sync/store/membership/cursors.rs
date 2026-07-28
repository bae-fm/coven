use super::*;

pub(super) fn head_cursor_key(reference: &MembershipHeadRef) -> String {
    format!(
        "{}{}/{}",
        MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX,
        reference.coord.author_owner_grant,
        reference.coord.stream_id
    )
}

pub(super) async fn read_head_cursors(db: &Database) -> Result<Vec<MembershipHeadRef>, String> {
    let prefix = MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX.to_string();
    db.call(move |conn| {
        let mut statement = conn
            .prepare(
                "SELECT value FROM protocol_state \
                 WHERE substr(key, 1, length(?1)) = ?1 ORDER BY key",
            )
            .map_err(crate::database::DbError::from)?;
        let rows = statement
            .query_map([&prefix], |row| row.get::<_, String>(0))
            .map_err(crate::database::DbError::from)?;
        let mut cursors = Vec::new();
        for row in rows {
            let value = row.map_err(crate::database::DbError::from)?;
            let reference: MembershipHeadRef = serde_json::from_str(&value).map_err(|error| {
                crate::database::DbError::Message(format!(
                    "membership head cursor is malformed: {error}"
                ))
            })?;
            if reference.coord.seq == 0 {
                return Err(crate::database::DbError::Message(
                    "membership head cursor has sequence zero".to_string(),
                ));
            }
            cursors.push(reference);
        }
        Ok(cursors)
    })
    .await
    .map_err(|error| format!("read membership head cursors: {error}"))
}

pub(crate) fn upsert_head_cursor_on(
    conn: &rusqlite::Connection,
    reference: &MembershipHeadRef,
) -> Result<(), crate::database::DbError> {
    let key = head_cursor_key(reference);
    let existing = crate::database::get_protocol_state_on(conn, &key)?;
    if let Some(existing) = existing {
        let existing: MembershipHeadRef = serde_json::from_str(&existing).map_err(|error| {
            crate::database::DbError::Message(format!(
                "membership head cursor is malformed: {error}"
            ))
        })?;
        if existing.coord.stream_key() != reference.coord.stream_key() {
            return Err(crate::database::DbError::Message(
                "membership head cursor key names a different stream".to_string(),
            ));
        }
        if existing.coord.seq > reference.coord.seq {
            return Ok(());
        }
        if existing.coord.seq == reference.coord.seq {
            if existing == *reference {
                return Ok(());
            }
            return Err(crate::database::DbError::Message(
                "membership head cursor forks at the same sequence".to_string(),
            ));
        }
    }
    let value = serde_json::to_string(reference).map_err(|error| {
        crate::database::DbError::Message(format!("serialize membership head cursor: {error}"))
    })?;
    crate::database::set_protocol_state_on(conn, &key, &value)
}

pub(super) async fn persist_head_cursors(
    db: &Database,
    cursors: &[MembershipHeadRef],
) -> Result<(), String> {
    let cursors = cursors.to_vec();
    db.call(move |conn| {
        let transaction = conn
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        for reference in &cursors {
            upsert_head_cursor_on(&transaction, reference)?;
        }
        transaction.commit().map_err(crate::database::DbError::from)
    })
    .await
    .map_err(|error| format!("persist membership head cursors: {error}"))
}

pub async fn seed_head_watermark(db: &Database, floor: &[MembershipHeadRef]) -> Result<(), String> {
    persist_head_cursors(db, floor).await
}

pub(crate) async fn load_and_persist_owner_anchor(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    owner_pubkey: &str,
    database: &StoreDatabase,
) -> Result<MembershipChain, AnchoredChainError> {
    let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(storage, root)
        .await
        .map_err(super::exact_chain::map_membership_history_error)?;
    load_and_persist_owner_anchor_with_history(
        &mut history_verifier,
        storage,
        root,
        owner_pubkey,
        database,
    )
    .await
}

pub(crate) async fn load_and_persist_owner_anchor_with_history(
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    owner_pubkey: &str,
    database: &StoreDatabase,
) -> Result<MembershipChain, AnchoredChainError> {
    let db = database.sqlite();
    let _membership_load = database.lock_membership_load().await;
    let cursors = read_head_cursors(db)
        .await
        .map_err(AnchoredChainError::LoadFailed)?;
    let chain = load_exact_anchored_chain_with_history(
        history_verifier,
        storage,
        root,
        &cursors,
        Some(owner_pubkey),
    )
    .await?;
    let founder = chain.founder_coord().ok_or_else(|| {
        AnchoredChainError::LoadFailed("owner-anchored membership chain is empty".to_string())
    })?;
    let founder_head_ref = chain
        .head_ref_for_stream(
            &founder.author_pubkey,
            &founder.author_owner_grant,
            founder.stream_id,
        )
        .cloned()
        .ok_or_else(|| {
            AnchoredChainError::LoadFailed(
                "owner-anchored membership chain has no exact founder head".to_string(),
            )
        })?;
    let founder_head = load_exact_membership_head(storage, root, &founder_head_ref).await?;
    let founder_registration_ref = founder_head.body.author_registration.clone();
    let founder_registration =
        crate::sync::store_objects::load_registration_ref(storage, root, &founder_registration_ref)
            .await
            .map_err(map_membership_object_error)?;
    let founder_registration_bytes = founder_registration.bytes;
    let founder_registration = founder_registration.value;
    if founder_registration.author_pubkey != owner_pubkey
        || !matches!(
            founder_registration.origin,
            crate::sync::store_commit::StoreDeviceRegistrationOrigin::Founder { .. }
        )
    {
        return Err(AnchoredChainError::LoadFailed(
            "founder head registration is not activated by the Store root".to_string(),
        ));
    }
    let protocol_root = crate::sync::store_objects::load_store_protocol_root(storage, root)
        .await
        .map_err(map_membership_object_error)?
        .value;
    if protocol_root.descriptor.founder_pubkey != owner_pubkey {
        return Err(AnchoredChainError::LoadFailed(
            "owner anchor differs from the Store root founder".to_string(),
        ));
    }
    let founder_genesis = ResolvedStoreDeviceState::founder(
        root,
        founder_registration_ref.clone(),
        &protocol_root.descriptor.founder_pubkey,
        protocol_root.descriptor.founder_grant.clone(),
        &protocol_root.descriptor.founder_recovery,
    )
    .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    database
        .install_store_owner_anchor(
            root.clone(),
            founder_registration_ref,
            founder_registration,
            founder_registration_bytes,
            founder_genesis,
            owner_pubkey.to_string(),
            crate::database::InitialStoreMembershipAuthority {
                head_refs: chain.head_refs().to_vec(),
            },
        )
        .await
        .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    Ok(chain)
}
