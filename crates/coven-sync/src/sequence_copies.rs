//! Key copies sent beside an operation's entry and on their own (§12.1).

use crate::{
    history::History,
    sequence_sync::sequence_operations::Request,
    sequence_sync::{SequenceSync, Session},
    SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_format::{admin::StoreChange, sealed_key::KeyCopyPrefix};
use coven_merge::Audience;
use serde::{Deserialize, Serialize};

/// The copies `change` sends before its entry, when `author`'s device
/// holding `ring` makes it.
pub(super) fn required_copies(
    history: &History,
    ring: &StoreKeyring,
    change: &StoreChange,
    author: coven_foundation::id_source::MemberId,
) -> Vec<KeyCopyPrefix> {
    match change {
        StoreChange::RotateKey { audience, key, .. } => history.routes(audience, *key),
        StoreChange::CreateCircle { circle, key, .. } => vec![KeyCopyPrefix {
            audience: Audience::Circle(*circle),
            key: *key,
            member: author,
            sealing: history.state.members[&author].keys.sealing,
        }],
        StoreChange::AddCircleMember { circle, member } => history
            .introductions
            .iter()
            .filter(|intro| intro.audience == Audience::Circle(*circle))
            .map(|intro| KeyCopyPrefix {
                audience: intro.audience.clone(),
                key: intro.key,
                member: *member,
                sealing: history.state.members[member].keys.sealing,
            })
            .collect(),
        StoreChange::AddMember { member, keys, .. }
        | StoreChange::ReplaceMemberKeys { member, keys, .. } => history
            .introductions
            .iter()
            .filter(|intro| {
                intro.audience == Audience::Store
                    || (crate::effects::belongs(&history.state, &intro.audience, member)
                        && history.accepted(ring, &intro.audience, intro.key))
            })
            .map(|intro| KeyCopyPrefix {
                audience: intro.audience.clone(),
                key: intro.key,
                member: *member,
                sealing: keys.sealing,
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) mod route {
    use super::*;
    pub(crate) fn serialize<S: serde::Serializer>(
        value: &KeyCopyPrefix,
        output: S,
    ) -> Result<S::Ok, S::Error> {
        (&value.audience.to_string(), value.key, value.member, value.sealing.as_bytes())
            .serialize(output)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        input: D,
    ) -> Result<KeyCopyPrefix, D::Error> {
        let (audience, key, member, sealing): (String, _, _, [u8; 32]) =
            Deserialize::deserialize(input)?;
        let audience: Audience = audience.parse().map_err(serde::de::Error::custom)?;
        Ok(KeyCopyPrefix {
            audience,
            key,
            member,
            sealing: coven_crypto::SealingKey::from_bytes(sealing),
        })
    }
}

impl SequenceSync {
    /// Send the older keys this device's member holds to the current members
    /// of those keys' audiences whose current sealing key has no copy of
    /// them, each as an operation of its own (§12.1). Another member's device
    /// may send the same copy meanwhile; two copies of one key to one member
    /// are harmless, so no device waits for another.
    pub(crate) async fn send_owed_copies(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for route in session.history.owed_copies(ring, member.member()) {
            let sends =
                |request: &Request| matches!(request, Request::Copy(owed) if *owed == route);
            // A journaled copy resumes under its own backoff.
            if self.journaled(&sends).await? {
                continue;
            }
            let result = self
                .start_request(session, ring, member, Request::Copy(route.clone()), "key copy")
                .await
                .map(drop);
            self.own_work(result, &sends).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_copies_tests.rs"]
mod tests;
