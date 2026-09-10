use super::*;
use coven_protocol::membership::{MembershipHeadAcceptance, MembershipHeadActivation};
use coven_protocol::objects::VerifiedObject;

type ReadHead = (MembershipHeadRef, AuthorHead);
pub(super) type LoadedHeadAcceptance =
    Result<VerifiedObject<MembershipHeadAcceptance>, AnchoredChainError>;

/// Reads remain provisional until the forward walk reaches them. In particular,
/// an unfinished head prevents an unrelated later tail error from taking authority.
struct ReadMembershipStream {
    heads: Vec<ReadHead>,
    tail: Result<(), AnchoredChainError>,
}

impl MembershipActivationAuthority<'_, '_> {
    async fn read_membership_stream(
        &self,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: coven_protocol::membership::AuthorStreamId,
        first_slot: &coven_protocol::objects::ObjectSlot,
    ) -> ReadMembershipStream {
        let mut heads = Vec::new();
        let tail = async {
            let prefetched = self
                .prefetch_membership_stream(author, grant, stream_id, first_slot)
                .await?;
            let mut slot = first_slot.clone();
            let mut sequence = 1_u64;
            loop {
                let read = match prefetched.get(&slot) {
                    Some(read) => {
                        self.verify_membership_head_at_slot(
                            read, author, grant, stream_id, sequence,
                        )
                        .await
                    }
                    None => {
                        self.load_membership_head_at_slot(&slot, author, grant, stream_id, sequence)
                            .await
                    }
                };
                let loaded = match read {
                    Ok(value) => value,
                    Err(StoreObjectError::Storage(StorageError::NotFound(_))) => break,
                    Err(StoreObjectError::Storage(source)) if source.is_transport() => {
                        return Err(AnchoredChainError::StorageUnavailable {
                            operation: format!(
                                "read membership head {author}/{grant}/{stream_id}/{sequence}"
                            ),
                            source,
                        });
                    }
                    Err(error) => return Err(map_membership_object_error(error)),
                };
                let reference = MembershipHeadRef {
                    coord: loaded.value.entry_coord(),
                    head_hash: loaded.value.head_hash(),
                    object: loaded.object,
                };
                slot = loaded.value.body.successor.next_slot.clone();
                heads.push((reference, loaded.value));
                sequence = sequence.checked_add(1).ok_or_else(|| {
                    AnchoredChainError::LoadFailed("membership head sequence overflow".into())
                })?;
            }
            Ok(())
        }
        .await;
        ReadMembershipStream { heads, tail }
    }

    /// Only a finalized successor selects an exact predecessor receipt. Reading
    /// backwards starts with the provider's terminal result; an unfinished or
    /// invalid successor leaves its predecessor's own result slot authoritative.
    /// These results do not activate entries or grant streams. The forward walk
    /// still checks every reached path, entry, issuer and accepted floor.
    async fn read_membership_stream_acceptances(
        &self,
        heads: &[ReadHead],
    ) -> Vec<Option<LoadedHeadAcceptance>> {
        let mut results = Vec::with_capacity(heads.len());
        let Self::AcceptedHeads {
            commit_verifier, ..
        } = self
        else {
            results.resize_with(heads.len(), || None);
            return results;
        };
        let mut predecessor = None;
        for (reference, head) in heads.iter().rev() {
            let exact = predecessor.as_ref().and_then(
                |previous: &coven_protocol::membership::MembershipHeadPredecessor| {
                    (previous.head() == reference)
                        .then(|| previous.acceptance())
                        .flatten()
                },
            );
            let result = match head.activation {
                MembershipHeadActivation::Direct => None,
                MembershipHeadActivation::StoreCommit { .. } => Some(
                    commit_verifier
                        .membership_objects()
                        .load_head_acceptance_at(reference, head, exact)
                        .await,
                ),
            };
            predecessor = match &result {
                Some(Ok(_)) => head.body.predecessor.clone(),
                Some(Err(_)) | None => None,
            };
            results.push(result);
        }
        results
    }

    pub(super) async fn traverse_exact_membership_stream(
        &mut self,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: coven_protocol::membership::AuthorStreamId,
        anchor: &GrantStreamAnchor,
        cursor: Option<&MembershipHeadRef>,
    ) -> Result<ExactMembershipStream, AnchoredChainError> {
        let GrantStreamAnchor::StoreMembership { first_slot } = anchor else {
            return Err(AnchoredChainError::LoadFailed(
                "membership stream uses a recovery anchor".to_string(),
            ));
        };
        let read = self
            .read_membership_stream(author, grant, stream_id, first_slot)
            .await;
        let mut acceptances = self.read_membership_stream_acceptances(&read.heads).await;
        let mut predecessor: Option<MembershipHeadRef> = None;
        let mut entries = Vec::new();
        let mut heads = Vec::new();
        let mut reached_cursor = cursor.is_none();
        let mut reached_tail = true;

        for (reference, head) in read.heads {
            let coord = reference.coord.clone();
            if head.body.predecessor_head() != predecessor.as_ref()
                || head.body.successor.predecessor
                    != predecessor
                        .as_ref()
                        .map(|reference| reference.object.clone())
            {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "membership head {coord:?} does not extend its exact predecessor"
                )));
            }
            if let Some((_, previous_head)) = heads.last() {
                head.body
                    .predecessor
                    .as_ref()
                    .ok_or_else(|| {
                        AnchoredChainError::LoadFailed(
                            "membership successor omits its predecessor".into(),
                        )
                    })?
                    .verify_head(previous_head)
                    .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
            }
            if head.body.successor.activation
                != coven_protocol::store_commit::StreamActivation::grant_authorized(
                    self.root().store_root_hash,
                    head.body.author_registration.clone(),
                    grant.clone(),
                    anchor.clone(),
                )
                .activation_id()
            {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "membership head {coord:?} is not signed by its activated certified device"
                )));
            }
            let loaded_entry = self
                .load_membership_entry(&head.body.entry)
                .await
                .map_err(map_membership_object_error)?;
            let acceptance = acceptances.pop().expect("one result for each read head");
            if !self
                .validate_head_activation(&reference, &head, &loaded_entry.value, acceptance)
                .await?
            {
                if cursor == Some(&reference) {
                    return Err(AnchoredChainError::LoadFailed(
                        "membership cursor names an unactivated Store-bound head".to_string(),
                    ));
                }
                reached_tail = false;
                break;
            }
            if cursor == Some(&reference) {
                reached_cursor = true;
            }
            entries.push((coord, loaded_entry.value));
            heads.push((reference.clone(), head.clone()));
            predecessor = Some(reference);
        }
        if reached_tail {
            read.tail?;
        }

        if !reached_cursor {
            return Err(AnchoredChainError::LoadFailed(
                "membership head successor chain regressed below its durable cursor".to_string(),
            ));
        }
        Ok(ExactMembershipStream { entries, heads })
    }
}
