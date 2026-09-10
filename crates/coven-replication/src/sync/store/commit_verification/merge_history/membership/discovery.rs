use super::*;
use coven_protocol::membership::MembershipStreamKey;

impl MembershipActivationAuthority<'_, '_> {
    pub(super) async fn load_exact_anchored_chain(
        &mut self,
        cursors: &[MembershipHeadRef],
        owner_pubkey: Option<&str>,
    ) -> Result<(MembershipChain, Vec<TraversedMembershipStream>), AnchoredChainError> {
        let root = self.root().clone();
        let root_value = self.verified_root().clone();
        if let Some(owner) = owner_pubkey {
            if root_value.descriptor.founder_pubkey != owner {
                return Err(AnchoredChainError::FounderMismatch {
                    founder: Some(root_value.descriptor.founder_pubkey.clone()),
                    owner: owner.to_string(),
                });
            }
        }
        let anchor = &root_value.descriptor.founder_membership;
        let founder_stream = coven_protocol::membership::derive_founder_stream_id(
            &root.store_root_id.to_string(),
            &root_value.descriptor.founder_pubkey,
        );
        let cursor = cursors.iter().find(|cursor| {
            cursor.coord.author_pubkey == root_value.descriptor.founder_pubkey
                && cursor.coord.author_owner_grant == root_value.descriptor.founder_grant
                && cursor.coord.stream_id == founder_stream
        });
        let founder_loaded = Box::pin(self.traverse_exact_membership_stream(
            &root_value.descriptor.founder_pubkey,
            &root_value.descriptor.founder_grant,
            founder_stream,
            anchor,
            cursor,
        ))
        .await?;
        let founder_latest = founder_loaded.heads.last().cloned().ok_or_else(|| {
            AnchoredChainError::LoadFailed("founder membership head is absent".to_string())
        })?;
        let founder = founder_loaded
            .entries
            .first()
            .map(|(_, entry)| entry)
            .ok_or_else(|| {
                AnchoredChainError::LoadFailed("founder membership entry is absent".to_string())
            })?;
        if root_value
            .descriptor
            .validate_merge_founder_entry(founder)
            .is_err()
        {
            return Err(AnchoredChainError::LoadFailed(
                "first exact membership entry differs from the signed Store founder".to_string(),
            ));
        }
        let mut discovered =
            std::collections::BTreeSet::from([founder_latest.0.coord.stream_key()]);
        let mut consumed_cursors = std::collections::BTreeSet::new();
        if let Some(cursor) = cursor {
            consumed_cursors.insert(cursor.clone());
        }
        let mut latest_heads = vec![founder_latest];
        let mut traversed = vec![TraversedMembershipStream {
            author_pubkey: root_value.descriptor.founder_pubkey.clone(),
            author_owner_grant: root_value.descriptor.founder_grant.clone(),
            stream_id: founder_stream,
            heads: zip_traversed_heads(&founder_loaded),
        }];

        loop {
            // A later entry on a known stream may depend on a peer stream
            // introduced by an earlier entry. Discover those exact anchors
            // before asking the causal reducer to validate the complete graph.
            // Pending heads never enter `traversed`, so they cannot add streams.
            let pending = referenced_membership_streams(&traversed)?
                .into_iter()
                .filter(|(stream, _)| !discovered.contains(stream))
                .collect::<Vec<_>>();
            if pending.is_empty() {
                let exact_heads = latest_heads
                    .iter()
                    .map(|(reference, _)| reference.clone())
                    .collect::<Vec<_>>();
                let chain = Box::pin(self.load_anchored_chain_at_exact_heads(&exact_heads)).await?;
                let activated = chain.activated_membership_streams();
                if consumed_cursors.len() != cursors.len()
                    || cursors.iter().any(|cursor| {
                        !activated
                            .iter()
                            .any(|(stream, _)| *stream == cursor.coord.stream_key())
                    })
                {
                    return Err(AnchoredChainError::LoadFailed(
                        "membership cursor names a stream that is not activated by the anchored chain"
                            .to_string(),
                    ));
                }
                traversed.sort_by(|left, right| {
                    (
                        &left.author_pubkey,
                        &left.author_owner_grant,
                        left.stream_id,
                    )
                        .cmp(&(
                            &right.author_pubkey,
                            &right.author_owner_grant,
                            right.stream_id,
                        ))
                });
                if let Self::AcceptedHeads {
                    commit_verifier,
                    device_authority,
                    root,
                } = self
                {
                    device_authority
                        .validate(commit_verifier, root, &chain, &traversed)
                        .await?;
                }
                return Ok((chain, traversed));
            }

            for (stream, anchor) in pending {
                let cursor = cursors
                    .iter()
                    .find(|cursor| cursor.coord.stream_key() == stream);
                let loaded = Box::pin(self.traverse_exact_membership_stream(
                    &stream.author_pubkey,
                    &stream.author_owner_grant,
                    stream.stream_id,
                    &anchor,
                    cursor,
                ))
                .await?;
                if let Some(cursor) = cursor {
                    consumed_cursors.insert(cursor.clone());
                }
                traversed.push(TraversedMembershipStream {
                    author_pubkey: stream.author_pubkey.clone(),
                    author_owner_grant: stream.author_owner_grant.clone(),
                    stream_id: stream.stream_id,
                    heads: zip_traversed_heads(&loaded),
                });
                if let Some(latest) = loaded.heads.last().cloned() {
                    latest_heads.push(latest);
                    latest_heads.sort_by_key(|(reference, _)| reference.coord.stream_key());
                }
                discovered.insert(stream);
            }
        }
    }
}

/// References used to discover objects, before the complete graph decides
/// which granting entries remain effective. The entries stay in the traversal
/// for that final authority check.
fn referenced_membership_streams(
    streams: &[TraversedMembershipStream],
) -> Result<BTreeMap<MembershipStreamKey, GrantStreamAnchor>, AnchoredChainError> {
    let mut anchors = BTreeMap::new();
    let grants = streams
        .iter()
        .flat_map(|stream| &stream.heads)
        .filter_map(|(_, _, entry)| match &entry.change {
            StoreAuthorityChange::SetMember {
                user_pubkey,
                grant_id,
                membership: Some(anchor),
                ..
            } => Some((user_pubkey.as_str(), grant_id, anchor)),
            _ => None,
        });
    for (author, grant, anchor) in grants {
        let stream = MembershipStreamKey::from_anchor(author, grant, anchor).ok_or_else(|| {
            AnchoredChainError::LoadFailed(
                "membership grant does not name its exact Store author stream".into(),
            )
        })?;
        match anchors.entry(stream) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(anchor.clone());
            }
            std::collections::btree_map::Entry::Occupied(slot) if slot.get() != anchor => {
                return Err(AnchoredChainError::LoadFailed(
                    "membership author stream has inconsistent grant anchors".into(),
                ));
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    Ok(anchors)
}
