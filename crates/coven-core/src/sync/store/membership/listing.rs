use super::*;

#[cfg(test)]
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
#[cfg(test)]
pub(crate) async fn get_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
    database: &StoreDatabase,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    let root = required_store_root_ref(database).await?;
    let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(storage, &root)
        .await
        .map_err(super::exact_chain::map_membership_history_error)?;
    let chain = load_current_membership_chain_with_history(&mut history_verifier, database).await?;
    members_from_chain(&chain, user_pubkey)
}

pub(crate) fn members_from_chain(
    chain: &MembershipChain,
    user_pubkey: Option<&[u8]>,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    require_resolved_membership(chain)?;
    Ok(member_info(chain.current_members(), user_pubkey))
}

pub(crate) fn membership_conflict_from_chain(
    chain: &MembershipChain,
    user_pubkey: Option<&[u8]>,
) -> Option<crate::sync::membership::MembershipConflictInfo> {
    let conflict = match chain.status() {
        crate::sync::membership::MembershipStatus::Resolved(_) => None,
        crate::sync::membership::MembershipStatus::Conflict(
            MembershipConflict::ConcurrentMemberAssignments {
                conflict_hash,
                member_pubkey,
                conflicting_grants,
                grants,
                ..
            },
        ) => Some(
            crate::sync::membership::MembershipConflictInfo::ConcurrentMemberAssignments {
                id: conflict_hash.to_string(),
                member_pubkey: member_pubkey.clone(),
                choices: conflicting_grants
                    .iter()
                    .map(|(selected_grant, selected_record)| {
                        let selection =
                            crate::sync::membership::MembershipConflictSelection::MemberAssignment {
                                grant: selected_grant.clone(),
                            };
                        let members = member_info(
                            grants
                                .iter()
                                .filter_map(|(grant, state)| {
                                    (!conflicting_grants.contains_key(grant))
                                        .then(|| state.active())
                                        .flatten()
                                        .map(|record| {
                                            (record.member_pubkey.clone(), record.role.role())
                                        })
                                })
                                .chain(std::iter::once((
                                    selected_record.member_pubkey.clone(),
                                    selected_record.role.role(),
                                )))
                                .collect(),
                            user_pubkey,
                        );
                        crate::sync::membership::MembershipConflictChoice::new(
                            membership_conflict_choice_id(&selection),
                            members,
                            *conflict_hash,
                            selection,
                        )
                    })
                    .collect(),
            },
        ),
        crate::sync::membership::MembershipStatus::Conflict(
            MembershipConflict::RevocationCycle {
                conflict_hash,
                maximal_valid_branches,
                ..
            },
        ) => Some(
            crate::sync::membership::MembershipConflictInfo::RevocationCycle {
                id: conflict_hash.to_string(),
                choices: maximal_valid_branches
                    .iter()
                    .map(|branch| {
                        let heads = branch.heads.clone();
                        let selection =
                            crate::sync::membership::MembershipConflictSelection::RevocationBranch {
                                heads,
                            };
                        let members = member_info(
                            branch
                                .active_grants()
                                .map(|(_, record)| {
                                    (record.member_pubkey.clone(), record.role.role())
                                })
                                .collect(),
                            user_pubkey,
                        );
                        crate::sync::membership::MembershipConflictChoice::new(
                            membership_conflict_choice_id(&selection),
                            members,
                            *conflict_hash,
                            selection,
                        )
                    })
                    .collect(),
            },
        ),
    };
    conflict
}

#[cfg(test)]
async fn load_current_membership_chain_with_history(
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    database: &StoreDatabase,
) -> Result<MembershipChain, MembershipOpsError> {
    let db = database.sqlite();
    let pinned_owner = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoFounderChain)?;
    let chain = load_current_exact_chain_with_history(
        history_verifier,
        Some(&pinned_owner),
        Some(database),
    )
    .await?;
    Ok(chain)
}

fn membership_conflict_choice_id(
    selection: &crate::sync::membership::MembershipConflictSelection,
) -> String {
    let selection_bytes =
        serde_json::to_vec(selection).expect("membership conflict selections always serialize");
    let mut bytes = b"coven.membership-conflict-choice.v1\0".to_vec();
    bytes.extend(selection_bytes);
    crate::sync::store_commit::ObjectHash::digest(&bytes).to_string()
}

fn member_info(current: Vec<(String, MemberRole)>, user_pubkey: Option<&[u8]>) -> Vec<MemberInfo> {
    let user_pubkey_hex = user_pubkey.map(hex::encode);
    current
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(|(pubkey, role)| {
            let is_self = user_pubkey_hex.as_deref() == Some(&pubkey);
            MemberInfo {
                pubkey,
                role,
                is_self,
            }
        })
        .collect()
}
