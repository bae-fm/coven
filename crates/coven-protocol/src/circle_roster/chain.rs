use super::reduction::*;
use super::*;

fn causal_owner_barriers(
    owner_barriers: &BTreeMap<MembershipGrantId, CircleOwnerGrantBarrier>,
) -> BTreeMap<MembershipGrantId, OwnerGrantBarrier<CircleRosterCoord>> {
    owner_barriers
        .iter()
        .map(|(grant, barrier)| {
            (
                grant.clone(),
                OwnerGrantBarrier::from_observed(barrier.observed_streams.iter().cloned()),
            )
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct CircleRosterChain {
    pub(super) entries: Vec<CircleRosterEntry>,
    pub(super) reduced: Option<causal_grants::ReducedGrants<CircleRosterCoord, CircleRole>>,
    pub(super) status: CircleRosterStatus,
    pub(super) head_refs: Vec<CircleRosterHeadRef>,
}

impl CircleRosterChain {
    pub fn from_entries(entries: Vec<CircleRosterEntry>) -> Result<Self, CircleRosterError> {
        Self::from_entries_and_head_refs(entries, Vec::new())
    }

    pub fn from_entries_with_heads(
        entries: Vec<CircleRosterEntry>,
        heads: Vec<ExactCircleRosterHead>,
    ) -> Result<Self, CircleRosterError> {
        let head_refs = Self::validate_exact_heads(&entries, &heads)?;
        Self::from_entries_and_head_refs(entries, head_refs)
    }

    pub fn with_exact_successor(
        &self,
        entry: CircleRosterEntry,
        head: ExactCircleRosterHead,
    ) -> Result<Self, CircleRosterError> {
        if head.head().entry_coord() != entry.coord() {
            return Err(CircleRosterError::HeadEntryMismatch);
        }
        let stream = entry.coord().stream_key();
        let mut entries = self.entries.clone();
        entries.push(entry);
        let mut head_refs = self.head_refs.clone();
        head_refs.retain(|reference| reference.coord.stream_key() != stream);
        head_refs.push(head.reference().clone());
        head_refs.sort_by_key(|reference| reference.coord.stream_key());
        Self::from_entries_and_head_refs(entries, head_refs)
    }

    pub fn resolved_with_successor(
        &self,
        entry: CircleRosterEntry,
    ) -> Result<ResolvedCircleRoster, CircleRosterError> {
        let mut entries = self.entries.clone();
        entries.push(entry);
        Self::from_entries_and_head_refs(entries, self.head_refs.clone())?.try_resolved()
    }

    fn validate_exact_heads(
        entries: &[CircleRosterEntry],
        heads: &[ExactCircleRosterHead],
    ) -> Result<Vec<CircleRosterHeadRef>, CircleRosterError> {
        let founder = entries.first().ok_or(CircleRosterError::Empty)?;
        if heads.iter().any(|bound| {
            let head = bound.head();
            let reference = bound.reference();
            head.store_root_hash != founder.store_root_hash
                || head.circle_id != founder.circle_id
                || head.entry_coord() != reference.coord
                || !entries.iter().any(|entry| entry.coord() == reference.coord)
        }) {
            return Err(CircleRosterError::HeadEntryMismatch);
        }
        Ok(heads.iter().map(|head| head.reference().clone()).collect())
    }

    fn from_entries_and_head_refs(
        entries: Vec<CircleRosterEntry>,
        head_refs: Vec<CircleRosterHeadRef>,
    ) -> Result<Self, CircleRosterError> {
        let founder = entries.first().ok_or(CircleRosterError::Empty)?;
        let expected_store = founder.store_root_hash;
        let expected_circle = founder.circle_id;
        for (index, entry) in entries.iter().enumerate() {
            if !entry.verify() {
                return Err(CircleRosterError::InvalidEntry(index));
            }
            if entry.store_root_hash != expected_store || entry.circle_id != expected_circle {
                return Err(CircleRosterError::ContextMismatch { index });
            }
        }
        let normalized = entries
            .iter()
            .map(|entry| CausalEntry {
                coord: entry.coord(),
                previous_hash: entry.previous_hash,
                dependencies: entry
                    .dependencies
                    .iter()
                    .cloned()
                    .map(|coord| (coord.stream_key(), coord))
                    .collect(),
                change: match &entry.change {
                    CircleRosterChange::Founder {
                        member_pubkey,
                        grant_id,
                    } => CausalChange::Founder {
                        member_pubkey: member_pubkey.clone(),
                        grant_id: grant_id.clone(),
                        assignment: CircleRole::Owner,
                    },
                    CircleRosterChange::SetMember {
                        member_pubkey,
                        role,
                        grant_id,
                        replaces,
                        owner_barriers,
                    } => CausalChange::SetMember {
                        member_pubkey: member_pubkey.clone(),
                        assignment: *role,
                        grant_id: grant_id.clone(),
                        replaces: replaces.clone(),
                        owner_barriers: causal_owner_barriers(owner_barriers),
                    },
                    CircleRosterChange::RemoveMember {
                        member_pubkey,
                        removes,
                        owner_barriers,
                    } => CausalChange::RemoveMember {
                        member_pubkey: member_pubkey.clone(),
                        removes: removes.clone(),
                        owner_barriers: causal_owner_barriers(owner_barriers),
                    },
                },
            })
            .collect::<Vec<_>>();
        let reduction = causal_grants::reduce(&normalized)?;
        let founder_entry = entries
            .iter()
            .find(|entry| matches!(entry.change, CircleRosterChange::Founder { .. }))
            .expect("shared reducer requires one founder");
        if founder_entry.circle_id
            != CircleId::founder(
                founder_entry.store_root_hash,
                &founder_entry.author_pubkey,
                &founder_entry.author_owner_grant,
            )
        {
            return Err(CircleRosterError::InvalidFounderIdentity);
        }
        let (reduced, status) = match reduction {
            CausalGrantStatus::Resolved(reduced) => {
                let resolved = resolved_circle_roster(&reduced);
                (Some(reduced), CircleRosterStatus::Resolved(resolved))
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::ConcurrentMemberAssignments {
                raw_heads,
                effective_frontier,
                member_pubkey,
                conflicting_grants,
                uncontested_grants,
                reduced,
            }) => (
                Some(reduced),
                CircleRosterStatus::Conflict(CircleRosterConflict::ConcurrentMemberAssignments {
                    heads: exact_circle_head_refs(&head_refs, &raw_heads)?,
                    effective_frontier,
                    member_pubkey,
                    conflicting_grants: map_circle_grants(conflicting_grants),
                    uncontested_grants: map_circle_grants(uncontested_grants),
                }),
            ),
            CausalGrantStatus::Conflict(CausalGrantConflict::RevocationCycle {
                raw_heads,
                cyclic_sources,
                involved_owner_grants,
            }) => (
                None,
                CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                    heads: exact_circle_head_refs(&head_refs, &raw_heads)?,
                    cyclic_sources,
                    involved_owner_grants,
                }),
            ),
        };
        Ok(Self {
            entries,
            reduced,
            status,
            head_refs,
        })
    }

    pub fn entries(&self) -> &[CircleRosterEntry] {
        &self.entries
    }

    pub fn status(&self) -> &CircleRosterStatus {
        &self.status
    }

    pub fn resolved(&self) -> ResolvedCircleRoster {
        self.try_resolved()
            .expect("caller must inspect Circle roster status before consuming resolved state")
    }

    pub fn try_resolved(&self) -> Result<ResolvedCircleRoster, CircleRosterError> {
        match &self.status {
            CircleRosterStatus::Resolved(resolved) => Ok(resolved.clone()),
            CircleRosterStatus::Conflict(_) => Err(CircleRosterError::Conflict),
        }
    }

    pub fn author_heads(&self) -> Vec<CircleRosterCoord> {
        causal_grants::stream_frontier(self.entries.iter().map(CircleRosterEntry::coord))
    }

    pub fn effective_frontier(&self) -> Vec<CircleRosterCoord> {
        let Some(reduced) = &self.reduced else {
            return Vec::new();
        };
        causal_grants::stream_frontier(
            self.entries
                .iter()
                .map(CircleRosterEntry::coord)
                .filter(|coord| reduced.includes_coord(coord)),
        )
    }

    fn active_grants(&self, member_pubkey: &str) -> BTreeSet<MembershipGrantId> {
        let reduced = self
            .reduced
            .as_ref()
            .expect("resolved roster has reduced grants");
        reduced
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                state
                    .active()
                    .is_some_and(|record| record.member_pubkey == member_pubkey)
                    .then_some(grant.clone())
            })
            .collect()
    }

    fn active_owner_grant(&self, member_pubkey: &str) -> Option<MembershipGrantId> {
        self.active_grants(member_pubkey).into_iter().find(|grant| {
            self.reduced
                .as_ref()
                .expect("resolved roster has reduced grants")
                .active_grant(grant)
                .is_some_and(|record| record.assignment == CircleRole::Owner)
        })
    }

    pub fn reusable_author_streams(
        &self,
        author_pubkey: &str,
        device_id: &str,
        grant: &MembershipGrantId,
    ) -> BTreeSet<AuthorStreamId> {
        self.effective_frontier()
            .into_iter()
            .filter(|effective_tip| {
                effective_tip.author_pubkey == author_pubkey
                    && effective_tip.device_id == device_id
                    && effective_tip.author_owner_grant == *grant
                    && self
                        .entries
                        .iter()
                        .map(CircleRosterEntry::coord)
                        .filter(|coord| coord.stream_key() == effective_tip.stream_key())
                        .max_by_key(|coord| coord.seq)
                        .as_ref()
                        == Some(effective_tip)
            })
            .map(|coord| coord.stream_id)
            .collect()
    }

    fn owner_barriers(
        &self,
        grants: &BTreeSet<MembershipGrantId>,
        dependencies: &[CircleRosterCoord],
    ) -> BTreeMap<MembershipGrantId, CircleOwnerGrantBarrier> {
        grants
            .iter()
            .filter(|grant| {
                self.reduced
                    .as_ref()
                    .expect("resolved roster has reduced grants")
                    .active_grant(grant)
                    .is_some_and(|record| record.assignment == CircleRole::Owner)
            })
            .map(|grant| {
                let observed_streams = dependencies
                    .iter()
                    .filter(|coord| coord.author_owner_grant == *grant)
                    .cloned()
                    .collect();
                (grant.clone(), CircleOwnerGrantBarrier { observed_streams })
            })
            .collect()
    }

    pub(super) fn next_position(
        &self,
        stream: &CircleAuthorStreamKey,
    ) -> Result<(u64, Option<ObjectHash>), CircleRosterError> {
        let raw_tip = self
            .entries
            .iter()
            .map(CircleRosterEntry::coord)
            .filter(|coord| coord.stream_key() == *stream)
            .max_by_key(|coord| coord.seq);
        let effective_tip = self
            .effective_frontier()
            .into_iter()
            .find(|coord| coord.stream_key() == *stream);
        if raw_tip.is_some()
            && !self
                .reusable_author_streams(
                    &stream.author_pubkey,
                    &stream.device_id,
                    &stream.author_owner_grant,
                )
                .contains(&stream.stream_id)
        {
            return Err(CircleRosterError::PrunedAuthorStream);
        }
        match effective_tip {
            Some(tip) => Ok((
                tip.seq
                    .checked_add(1)
                    .ok_or(CircleRosterError::SequenceExhausted { current: tip.seq })?,
                Some(tip.entry_hash),
            )),
            None => Ok((1, None)),
        }
    }

    pub fn signed_set_member(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        member_pubkey: String,
        role: CircleRole,
        signer: &dyn coven_keys::keys::IdentityKeyAuthority,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
        self.signed_change(device_id, stream_id, member_pubkey, Some(role), signer)
    }

    pub fn signed_remove_member(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        member_pubkey: String,
        signer: &dyn coven_keys::keys::IdentityKeyAuthority,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
        if self.active_grants(&member_pubkey).is_empty() {
            return Err(CircleRosterError::NotAMember(member_pubkey));
        }
        self.signed_change(device_id, stream_id, member_pubkey, None, signer)
    }

    fn signed_change(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        member_pubkey: String,
        role: Option<CircleRole>,
        signer: &dyn coven_keys::keys::IdentityKeyAuthority,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
        if matches!(self.status, CircleRosterStatus::Conflict(_)) {
            return Err(CircleRosterError::Conflict);
        }
        let author_pubkey = keys::public_key_hex(signer);
        let author_owner_grant = self
            .active_owner_grant(&author_pubkey)
            .ok_or_else(|| CircleRosterError::SignerIsNotOwner(author_pubkey.clone()))?;
        let stream = CircleAuthorStreamKey {
            author_pubkey: author_pubkey.clone(),
            device_id: device_id.to_string(),
            stream_id,
            author_owner_grant: author_owner_grant.clone(),
        };
        let (seq, previous_hash) = self.next_position(&stream)?;
        let dependencies = self.effective_frontier();
        let replaced = self.active_grants(&member_pubkey);
        let owner_barriers = self.owner_barriers(&replaced, &dependencies);
        let change = match role {
            Some(role) => CircleRosterChange::SetMember {
                member_pubkey: member_pubkey.clone(),
                role,
                grant_id: MembershipGrantId(ObjectHash::digest(
                    format!(
                        "coven.circle-roster-grant.v1\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
                        self.entries[0].circle_id,
                        author_pubkey,
                        device_id,
                        stream_id,
                        author_owner_grant,
                        seq,
                        member_pubkey
                    )
                    .as_bytes(),
                )),
                replaces: replaced,
                owner_barriers,
            },
            None => CircleRosterChange::RemoveMember {
                member_pubkey,
                removes: replaced,
                owner_barriers,
            },
        };
        let entry = Signed::sign(
            CircleRosterEntryBody {
                store_root_hash: self.entries[0].store_root_hash,
                circle_id: self.entries[0].circle_id,
                author_pubkey,
                device_id: device_id.to_string(),
                stream_id,
                author_owner_grant,
                seq,
                previous_hash,
                dependencies,
                change,
            },
            signer,
        );
        let mut candidate_history = self.entries.clone();
        candidate_history.push(entry.clone());
        Self::from_entries_and_head_refs(candidate_history, self.head_refs.clone())?;
        Ok(entry)
    }
}
