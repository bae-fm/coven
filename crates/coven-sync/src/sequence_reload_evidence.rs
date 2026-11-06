//! What a reload reads is checked against history and announcements before
//! it is trusted: the floor, and a snapshot's signed prefix.

use super::*;

impl SequenceSync {
    /// Authenticate `floor` against `history`, which holds the slots
    /// through `through`. The device that wrote the floor may have applied
    /// slots past it, such as the announcement that won the slot a claim
    /// attempted: a copy of the history, with a copy of the keyring, reads
    /// the kept slots through the floor's `at` first, and both copies are
    /// dropped once the floor is checked (§5.1, §16.4).
    pub(crate) async fn authenticate_floor_from(
        &self,
        floor: &FloorObject,
        history: &History,
        through: u64,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        if floor.header.at <= through {
            return self.authenticate_floor(floor, history, ring, member).await;
        }
        let mut later = history.clone();
        let mut tentative = ring.clone();
        self.advance_history(&mut later, through, floor.header.at, &mut tentative, member).await?;
        self.authenticate_floor(floor, &later, &mut tentative, member).await
    }

    pub(crate) async fn authenticate_floor(
        &self,
        floor: &FloorObject,
        history: &History,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for copy in &floor.copies {
            if copy.slot.0 > floor.header.at {
                return Err(crate::write_object::wrong_identity(&ObjectPath::floor(self.store())));
            }
            snapshot_object::open_routing(copy, self.store(), ring, member)?;
            slot_object::open_bounded(
                copy.object.clone(),
                &self.path(copy.slot.0)?,
                ring,
                history,
            )?;
        }
        let required = history.introduced_through(Slot(floor.header.at));
        let mut included = std::collections::BTreeSet::new();
        let mut previous = None;
        for original in &floor.announcements {
            let received = slot_object::open_bounded(
                original.object.clone(),
                &self.path(original.slot.0)?,
                ring,
                history,
            )?;
            let SlotBody::Bounded { frame: SlotFrame::Announcement(announcement), .. } =
                received.into_parts().1
            else {
                unreachable!("floor format requires announcements");
            };
            let audience = &announcement.snapshot.audience;
            if previous.as_ref().is_some_and(|prior| prior >= audience)
                || (previous.is_none() && *audience != Audience::Store)
                || announcement.covers < floor.header.first.0 - 1
                || history.announcements.get(&original.slot) != Some(&announcement)
                || !history.usable_at(&announcement, floor.header.at)
            {
                return Err(crate::write_object::wrong_identity(&ObjectPath::floor(self.store())));
            }
            included.insert(audience.clone());
            previous = Some(audience.clone());
        }
        if included != required {
            return Err(crate::write_object::wrong_identity(&ObjectPath::floor(self.store())));
        }
        floor.authenticate(history)
    }

    /// A snapshot's signed prefix, checked against its announcement.
    pub(crate) async fn snapshot_prefix(
        &self,
        announcement: &SnapshotAnnouncement,
    ) -> Result<SnapshotObject, SyncError> {
        let path = snapshot_object::path(self.store(), &announcement.snapshot)?;
        let object =
            SnapshotObject::read_prefix(&self.database, self.storage.as_ref(), &path).await?;
        object.check_announced(self.storage.as_ref(), announcement).await?;
        Ok(object)
    }
}

#[cfg(test)]
#[path = "sequence_reload_evidence_tests.rs"]
mod tests;
