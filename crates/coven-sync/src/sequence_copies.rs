//! Key copies sent beside an operation's entry, in an admission's slot and on
//! their own (§12.1, §13.2).

use crate::{
    history::History,
    sequence_sync::sequence_operations::Request,
    sequence_sync::{SequenceSync, Session},
    OperationRejection, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_format::{
    admin::StoreChange,
    sealed_key::{CopyBox, KeyCopyPrefix},
    sealed_slot::{BoxPlace, SlotKind, SlotObjectPrefix},
};
use coven_merge::Audience;
use serde::{Deserialize, Serialize};

/// An approved join request's admission, as its operation journals it: the
/// entry, and the member's own copy of the circle key it introduces, exactly
/// as the request carried it, so the one claim, and every retry of it, sends
/// the bytes the request held (§13.2). The approving device never holds that
/// key.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ApprovedAdmission {
    #[serde(with = "change")]
    pub(crate) change: StoreChange,
    #[serde(with = "route")]
    own_routing: KeyCopyPrefix,
    own_sealed: Vec<u8>,
}

impl ApprovedAdmission {
    pub(crate) fn new(change: StoreChange, own: CopyBox) -> Self {
        Self { change, own_routing: own.routing, own_sealed: own.sealed }
    }

    /// The member's own circle box.
    fn own(&self) -> CopyBox {
        CopyBox { routing: self.own_routing.clone(), sealed: self.own_sealed.clone() }
    }
}

/// The copies `change` sends before its entry, when `author`'s device
/// holding `ring` makes it. An admission carries its own in its slot, each
/// key once, in `(audience, key)` order (format.md D9).
pub(crate) fn required_copies(
    history: &History,
    ring: &StoreKeyring,
    change: &StoreChange,
    author: coven_foundation::id_source::MemberId,
) -> Vec<KeyCopyPrefix> {
    match change {
        StoreChange::RotateKey { audience, key, .. } => history.routes(audience, *key),
        // The key replaces the one the removed member holds: it goes to the
        // members who stay, never to the one removed (§15.8).
        StoreChange::RemoveCircleMember { circle, member, key, .. } => history
            .routes(&Audience::Circle(*circle), *key)
            .into_iter()
            .filter(|route| route.member != *member)
            .collect(),
        // Store creation carries the creator's copy of the first store key
        // and of each circle's first key (§10.3).
        StoreChange::CreateStore { key, admin, circles, .. } => {
            let mut copies = vec![KeyCopyPrefix {
                audience: Audience::Store,
                key: *key,
                member: author,
                sealing: admin.sealing,
            }];
            copies.extend(circles.iter().map(|circle| KeyCopyPrefix {
                audience: Audience::Circle(circle.circle),
                key: circle.key,
                member: author,
                sealing: admin.sealing,
            }));
            copies.sort_by(|a, b| (&a.audience, a.key).cmp(&(&b.audience, b.key)));
            copies
        }
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
        StoreChange::AdmitMember { member, keys, .. }
        | StoreChange::RecoverMember { member, keys, .. }
        | StoreChange::ReplaceMemberKeys { member, keys, .. } => history
            .introductions
            .iter()
            .filter(|intro| {
                intro.audience == Audience::Store
                    || (crate::effects::belongs(&history.state, &intro.audience, member)
                        && history.accepted(ring, &intro.audience, intro.key))
            })
            .map(|intro| ((intro.audience.clone(), intro.key), keys.sealing))
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_iter()
            .map(|((audience, key), sealing)| KeyCopyPrefix {
                audience,
                key,
                member: *member,
                sealing,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// An admission's clear prefix: each of `copies`, in order, sealed for its
/// box at its index in the slot at `path` whose body `store_key` seals, then
/// an admitted member's own box as the request carried it (a creation has
/// none). A claim that loses its slot
/// seals the copies again (D11). A store key `ring` lacks
/// fails here, before anything is claimed: the approval waits for it (§13.2).
/// An admission whose slot, with `plaintext` bytes of body, would exceed a
/// slot's size is refused with `AdmissionTooLarge`, also before anything is
/// claimed (format.md D2).
pub(crate) fn admission_prefix(
    ring: &StoreKeyring,
    path: &coven_storage::ObjectPath,
    store_key: coven_foundation::id_source::KeyId,
    copies: Vec<KeyCopyPrefix>,
    admission: Option<&ApprovedAdmission>,
    plaintext: usize,
) -> Result<Result<SlotObjectPrefix, OperationRejection>, SyncError> {
    let boxes = copies
        .into_iter()
        .enumerate()
        .map(|(index, route)| {
            seal_copy(ring, path, store_key, BoxPlace::Admission(index as u32), route)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let prefix = SlotObjectPrefix {
        format: coven_format::FormatVersion::CURRENT,
        key: store_key,
        kind: SlotKind::Admission { copies: boxes, own: admission.map(ApprovedAdmission::own) },
    };
    Ok(if prefix.fits(plaintext)? {
        Ok(prefix)
    } else {
        Err(OperationRejection::AdmissionTooLarge)
    })
}

/// `route`'s key from `ring`, sealed to its recipient for the box at `place`
/// in the slot at `path` whose body `store_key` seals (D11).
pub(super) fn seal_copy(
    ring: &StoreKeyring,
    path: &coven_storage::ObjectPath,
    store_key: coven_foundation::id_source::KeyId,
    place: BoxPlace,
    route: KeyCopyPrefix,
) -> Result<CopyBox, SyncError> {
    let context = SlotObjectPrefix::copy_context(
        coven_format::FormatVersion::CURRENT,
        store_key,
        place,
        &route,
    )?;
    let sealed = match route.audience {
        Audience::Store => coven_crypto::seal_store_key(
            ring.store_key(route.key)?,
            &route.sealing,
            path.as_str(),
            &context,
        )?,
        Audience::Circle(circle) => coven_crypto::seal_circle_key(
            ring.circle_key(circle, route.key)?,
            &route.sealing,
            path.as_str(),
            &context,
        )?,
    };
    Ok(CopyBox { routing: route, sealed })
}

pub(super) mod change {
    use super::*;
    pub(crate) fn serialize<S: serde::Serializer>(
        value: &StoreChange,
        output: S,
    ) -> Result<S::Ok, S::Error> {
        value.encode().map_err(serde::ser::Error::custom)?.serialize(output)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        input: D,
    ) -> Result<StoreChange, D::Error> {
        StoreChange::decode(&Vec::<u8>::deserialize(input)?).map_err(serde::de::Error::custom)
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
    /// Admit a member as an approved join request does, with the member's
    /// own copy of the circle key the entry introduces, for a test that
    /// admits without a join: the pair `admitting` returns.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn administer_admission(
        self: &std::sync::Arc<Self>,
        (change, own): (StoreChange, CopyBox),
    ) -> Result<(), SyncError> {
        self.submit(crate::sequence_sync::sequence_operations::Request::Admission(
            ApprovedAdmission::new(change, own),
        ))
        .await
        .map(drop)
    }

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
            if self.journaled(&sends).await?.is_some() {
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
