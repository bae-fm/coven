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
    let existing: Option<String> = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [&key],
            |row| row.get(0),
        )
        .optional()
        .map_err(crate::database::DbError::from)?;
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
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )
    .map(|_| ())
    .map_err(crate::database::DbError::from)
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

pub(crate) fn validate_membership_floor(floor: &[MembershipHeadRef]) -> Result<(), String> {
    if floor.is_empty() {
        return Err("membership floor is empty".to_string());
    }
    for (index, reference) in floor.iter().enumerate() {
        if reference.coord.seq == 0 {
            return Err("membership floor contains sequence zero".to_string());
        }
        if index > 0 && floor[index - 1].coord.stream_key() >= reference.coord.stream_key() {
            return Err("membership floor is not strictly ordered by author stream".to_string());
        }
    }
    Ok(())
}

pub(crate) async fn load_and_persist_owner_anchor(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    owner_pubkey: &str,
    db: &Database,
) -> Result<MembershipChain, AnchoredChainError> {
    let _membership_load = db.lock_membership_load().await;
    let cursors = read_head_cursors(db)
        .await
        .map_err(AnchoredChainError::LoadFailed)?;
    let chain = load_exact_anchored_chain(storage, root, &cursors, Some(owner_pubkey)).await?;
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
    db.install_store_owner_anchor(
        root.clone(),
        founder_registration_ref,
        founder_registration,
        founder_registration_bytes,
        founder_genesis,
        owner_pubkey.to_string(),
        crate::database::InitialStoreMembershipAuthority::MergeConcurrent {
            head_refs: chain.head_refs().to_vec(),
        },
    )
    .await
    .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    Ok(chain)
}
