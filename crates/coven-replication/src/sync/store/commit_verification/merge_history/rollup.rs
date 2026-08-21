//! Publishing and taking the membership rollup a snapshot carries.
//!
//! The membership chain is a Store's oldest history and its most re-read: a
//! device opens the Store keyring out of it, so every reader that has not
//! opened the Store yet reaches the chain first and can use nothing that lives
//! behind the keyring. Reading it change by change is what made a device join
//! spend most of its provider round trips on membership that had not moved in
//! months.
//!
//! A snapshot names a rollup carrying every membership object up to the
//! frontier the snapshot pins. This is the reading half: finding the rollup a
//! Store has published, and putting its objects in front of the anchored walk
//! that follows. The publishing half is the anchored walk itself — what a
//! publisher puts in a rollup is what that walk read
//! (`load_exact_anchored_membership_traversal`).

use super::{membership, MergeHistoryVerifier};
use std::collections::BTreeMap;

impl<'a> MergeHistoryVerifier<'a> {
    /// Find the membership rollup the Store's newest published snapshot names
    /// and hold its objects for the walk that follows.
    ///
    /// Every step is advisory and every failure is dropped, because none of it
    /// changes what the walk concludes — it changes how many round trips the
    /// walk spends getting there. A Store with no published snapshot, a
    /// snapshot whose rollup is gone, a rollup that does not authenticate: each
    /// leaves the verifier exactly as it was, walking the provider the way it
    /// always did.
    ///
    /// This is the one place a dropped failure is right. Elsewhere a swallowed
    /// read error is a silent retry — the real path re-reads and believes the
    /// second answer — but there is no second answer here: what the walk then
    /// reads is the provider's own membership slots, checked in full, and it
    /// fails loudly on its own reads if they fail.
    ///
    /// Returns whether a rollup was adopted.
    pub async fn adopt_published_membership_rollup(&self) -> bool {
        let meta = match self.commit_verifier.newest_listed_store_snapshot().await {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                tracing::debug!("no published Store snapshot names a membership rollup");
                return false;
            }
            Err(error) => {
                tracing::debug!(%error, "newest published Store snapshot did not open");
                return false;
            }
        };
        let rollup = match self.commit_verifier.load_membership_rollup(&meta).await {
            Ok(rollup) => rollup,
            Err(error) => {
                tracing::debug!(%error, "published membership rollup did not open");
                return false;
            }
        };
        let streams = rollup.streams.len();
        let heads = rollup
            .streams
            .iter()
            .map(|stream| stream.heads.len())
            .sum::<usize>();
        match self.admit_membership_rollup(&rollup).await {
            Ok(()) => {
                tracing::info!(
                    generation = meta.generation,
                    streams,
                    heads,
                    "took the membership chain from a published rollup"
                );
                true
            }
            Err(error) => {
                tracing::debug!(%error, "published membership rollup did not verify");
                false
            }
        }
    }

    /// Take a published membership rollup as bytes for the walk that follows.
    ///
    /// A joining device opens the Store keyring out of the membership chain, so
    /// it reaches the chain before it can read anything the keyring protects,
    /// and the only way it had to get there was two provider round trips per
    /// membership change — a listing and a read per head, then a read per entry
    /// — back to the Store's founding entry. On a live Store that was about
    /// eighty percent of the whole join.
    ///
    /// Nothing here is believed on the rollup's word. Every head is put through
    /// the identical check the walk runs on a head it read itself
    /// (`verify_head_at_slot`: the coordinate the slot claims, the author's
    /// registration, the head's own signature), and a head that fails it fails
    /// the whole admission, so the rollup is either a faithful carrier of this
    /// Store's own objects or it is not used at all. What is left after that —
    /// predecessor linkage, grant authority, Store-commit activation of
    /// authority changes, conflict-resolution layering, the cursor the
    /// admission floor names — is decided afterwards by the same anchored walk,
    /// over the same code, as it is for bytes read off the provider.
    ///
    /// The last head of each stream is deliberately *not* held, and that is
    /// what makes this exactly the walk it replaces rather than nearly it. A
    /// membership head lives at a create-once slot, so which of an author's
    /// heads sits at sequence N is settled by the provider and not by whoever
    /// describes it — and an author with two devices can sign two valid heads
    /// at one coordinate, which is the fork the conflict machinery exists for.
    /// A rollup that could stand in for every covered slot could therefore hand
    /// a reader the branch the provider does not hold. Leaving the last covered
    /// head to be read closes it whole: the walk checks that head's signed
    /// predecessor against the reference it built from the held bytes, that
    /// head's entry names its predecessor's hash, and so on down to sequence
    /// one, whose slot the signed Store root names. One read per stream pins
    /// every head under it.
    ///
    /// So what the rollup changes is which round trips happen: the walk finds
    /// the covered heads and entries in memory and reads the newest covered
    /// head, the tail published since the snapshot, and the absent slot that
    /// tells it where the tail ends.
    pub(crate) async fn admit_membership_rollup(
        &self,
        rollup: &coven_protocol::store_commit::MembershipRollup,
    ) -> Result<(), crate::sync::store::membership::AnchoredChainError> {
        use crate::sync::store::commit_verification::commit::ReadProtocolSlot;

        let mut streams = Vec::with_capacity(rollup.streams.len());
        let mut entries = Vec::new();
        for stream in &rollup.streams {
            let mut reads = BTreeMap::new();
            let held = stream.heads.len().saturating_sub(1);
            for (index, carried) in stream.heads.iter().enumerate() {
                let sequence = (index as u64).saturating_add(1);
                let bytes = canonical_rollup_bytes(&carried.head_value)?;
                self.commit_verifier
                    .membership_objects()
                    .verify_head_at_slot(
                        &bytes,
                        &carried.head.object,
                        &stream.author_pubkey,
                        &stream.author_owner_grant,
                        stream.stream_id,
                        sequence,
                    )
                    .await
                    .map_err(membership::map_membership_object_error)?;
                if index < held {
                    reads.insert(
                        carried.head.object.slot().clone(),
                        ReadProtocolSlot {
                            bytes,
                            object: carried.head.object.clone(),
                        },
                    );
                }
                // Every entry, including the one the newest covered head
                // selects: an entry is asked for by content address, and the
                // address comes out of a head the reader has already checked,
                // so holding it can only save the read and never choose it.
                entries.push((
                    carried.entry.object.clone(),
                    canonical_rollup_bytes(&carried.entry_value)?,
                ));
            }
            streams.push((
                coven_protocol::store_commit::membership_head_stream_prefix(
                    &stream.author_pubkey,
                    &stream.author_owner_grant,
                    stream.stream_id,
                ),
                std::sync::Arc::new(reads),
            ));
        }
        for carried in &rollup.resolutions {
            self.commit_verifier.remember_exact_object(
                &carried.resolution.object,
                &canonical_rollup_bytes(&carried.resolution_value)?,
            );
        }
        for (object, bytes) in entries {
            self.commit_verifier.remember_exact_object(&object, &bytes);
        }
        for (prefix, reads) in streams {
            self.commit_verifier.remember_slot_stream(&prefix, reads);
        }
        Ok(())
    }
}

/// The canonical bytes of one object a rollup carries.
///
/// A rollup is refused before it is admitted unless every carried value hashes
/// to the reference beside it, and that check is made over exactly this
/// serialization — so re-serializing here reproduces the bytes the provider
/// holds rather than merely equivalent ones.
fn canonical_rollup_bytes<T: serde::Serialize>(
    value: &T,
) -> Result<Vec<u8>, crate::sync::store::membership::AnchoredChainError> {
    serde_json::to_vec(value).map_err(|error| {
        crate::sync::store::membership::AnchoredChainError::LoadFailed(format!(
            "membership rollup object does not re-serialize: {error}"
        ))
    })
}
