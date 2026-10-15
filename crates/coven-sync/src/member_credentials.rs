//! Which sealing credentials key copies route to, and which copies expose
//! their audience key, after member-key replacement (§11,
//! `KeySelection.MemberKeys`).

use crate::key_introductions::belongs;
use crate::SyncError;
use coven_crypto::SealingKey;
use coven_database::{DropReason, EntryOutcome, StoreLog, StoreLogCheck, StoreLogState};
use coven_format::sealed_key::KeyCopyPrefix;
use coven_foundation::id_source::{KeyId, MemberId};
use coven_merge::Audience;
use std::collections::BTreeSet;

/// Appendix C12 `belongs` as the members of an audience: the store's current
/// members, or a live circle's.
fn audience_members<'a>(
    state: &'a StoreLogState,
    audience: &'a Audience,
) -> impl Iterator<Item = &'a MemberId> + 'a {
    state
        .members
        .keys()
        .filter(move |member| belongs(state, audience, member))
}

/// The sealing credentials one replay allows copies to (§11). A kept
/// replacement retires the pair its author's recorded view bound to its
/// target, whatever replaced it later, and even though the member stays.
pub(crate) struct Credentials<'a> {
    pub(crate) state: &'a StoreLogState,
    /// Every sealing key a kept replacement retired.
    pub(crate) retired: BTreeSet<SealingKey>,
}

impl<'a> Credentials<'a> {
    /// `MemberKeys.retired`, from the checks retained with kept replacements.
    pub(crate) fn new(log: &'a StoreLog) -> Self {
        let retired = log
            .entries
            .iter()
            .filter(|applied| {
                log.replay.entries.get(&applied.entry.position) == Some(&EntryOutcome::Kept)
            })
            .filter_map(|applied| match &applied.check {
                StoreLogCheck::KeyReplacement(previous) => Some(previous.sealing),
                _ => None,
            })
            .collect();
        Self {
            state: &log.replay.state,
            retired,
        }
    }

    /// `MemberKeys.route`: the member's current sealing key, refused when a
    /// kept replacement retired it. Audience membership is checked apart.
    pub(crate) fn route(&self, member: &MemberId) -> Option<SealingKey> {
        let sealing = self.state.members.get(member)?.keys.sealing;
        (!self.retired.contains(&sealing)).then_some(sealing)
    }

    /// `MemberKeys.currentCredential`: the named member's current sealing key.
    pub(crate) fn current(&self, member: &MemberId, sealing: &SealingKey) -> bool {
        self.state
            .members
            .get(member)
            .is_some_and(|bound| bound.keys.sealing == *sealing)
    }

    /// Whether this copy exposes its audience key: it names a member outside
    /// the audience, a retired sealing key, or a credential that is not its
    /// member's current one, such as a losing or never-admitted candidate's
    /// (§11). Such a box cannot be opened to check it, so a forged one counts
    /// too; at worst it costs a rotation.
    pub(crate) fn exposes(&self, copy: &KeyCopyPrefix) -> bool {
        !belongs(self.state, &copy.audience, &copy.member)
            || self.retired.contains(&copy.sealing)
            || !self.current(&copy.member, &copy.sealing)
    }

    /// `KeyPhases.MemberKeys.rotationCopies`: a rotated key's copy for every
    /// member of its audience at their current non-retired credential, the
    /// rotating member's own included. An unroutable member blocks the
    /// rotation; no audience member silently loses their copy.
    pub(crate) fn rotation_copies(
        &self,
        audience: &Audience,
        key: KeyId,
    ) -> Result<Vec<KeyCopyPrefix>, SyncError> {
        audience_members(self.state, audience)
            .map(|member| {
                let sealing = self
                    .route(member)
                    .ok_or(SyncError::Rejected(DropReason::NotAllowed))?;
                Ok(KeyCopyPrefix {
                    audience: audience.clone(),
                    key,
                    member: *member,
                    sealing,
                })
            })
            .collect()
    }

    /// `MemberKeys.share` for one held key: a copy for each current audience
    /// member at their current non-retired credential, unless one is observed
    /// there. A copy to the member's older credential does not stand in, and
    /// a member whose current key is retired gets none (§11).
    pub(crate) fn shares(
        &self,
        observed: &[KeyCopyPrefix],
        audience: &Audience,
        key: KeyId,
    ) -> Vec<KeyCopyPrefix> {
        audience_members(self.state, audience)
            .filter_map(|member| {
                let Some(sealing) = self.route(member) else {
                    tracing::debug!(?audience, ?member, "member has no routable credential");
                    return None;
                };
                let copy = KeyCopyPrefix {
                    audience: audience.clone(),
                    key,
                    member: *member,
                    sealing,
                };
                (!observed.contains(&copy)).then_some(copy)
            })
            .collect()
    }
}
