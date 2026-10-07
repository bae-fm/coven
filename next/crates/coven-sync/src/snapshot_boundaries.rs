//! Replay-selected boundaries constrain both snapshot choice and late writes.

use super::{catalog::inconsistent, io, snapshot_path, StoreLogSync};
use crate::{snapshot_data::SavedBoundary, SyncError};
use coven_database::{EntryOutcome, StoreLog, WriteBoundary};
use coven_format::{
    chunks::FrameDecoder,
    sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix},
    snapshot::SnapshotDecoder,
    store_log::{SnapshotId, StoreChange, StoreLogEntry},
    Object,
};
use coven_merge::Audience;

pub(super) fn entries(log: &StoreLog) -> impl Iterator<Item = (&StoreLogEntry, &SnapshotId)> {
    let mut state = coven_database::StoreLogState::default();
    log.entries.iter().filter_map(move |entry| {
        if log.replay.entries[&entry.entry.position] != EntryOutcome::Kept {
            return None;
        }
        // A kept entry whose effect is already in place brings no new boundary.
        // Use the replay's effect functions so repeated resets and lower raises
        // do not invalidate snapshots following the original boundary.
        if crate::effects::already_in_place(&state, &entry.entry) {
            return None;
        }
        crate::effects::apply_effect(&mut state, &entry.entry);
        match &entry.entry.change {
            StoreChange::RaiseSchema { snapshot, .. }
            | StoreChange::RaiseFormat { snapshot, .. }
            | StoreChange::Reset { snapshot } => Some((&entry.entry, snapshot)),
            _ => None,
        }
    })
}

pub(super) fn allows(log: &StoreLog, id: &SnapshotId, prefix: &SnapshotObjectPrefix) -> bool {
    let selected = entries(log)
        .filter(|(_, s)| s.audience == id.audience)
        .last();
    selected
        .is_none_or(|(entry, snapshot)| snapshot == id || prefix.store_log.covers(entry.position))
}

pub(super) fn decode(saved: &SavedBoundary) -> Result<WriteBoundary, SyncError> {
    let Object::StoreLog(entry) = Object::decode(&saved.entry)? else {
        return Err(inconsistent("recorded boundary is not a store-log entry"));
    };
    let prefix = SnapshotObjectPrefix::decode(&saved.prefix)?;
    match entry.change {
        StoreChange::Reset { snapshot } => Ok(WriteBoundary::Reset {
            entry: entry.position,
            audience: snapshot.audience,
            included: prefix.writes,
        }),
        StoreChange::RaiseSchema { version, snapshot } => Ok(WriteBoundary::SchemaChange {
            version,
            audience: snapshot.audience,
            included: prefix.writes,
        }),
        _ => Err(inconsistent("recorded boundary does not exclude writes")),
    }
}

impl StoreLogSync {
    pub(super) async fn snapshot_boundaries(
        &self,
        log: &StoreLog,
    ) -> Result<Vec<SavedBoundary>, SyncError> {
        let readable = self.snapshot_audiences(log)?;
        let mut saved = Vec::new();
        for (entry, id) in entries(log) {
            if !readable.contains_key(&id.audience)
                || matches!(entry.change, StoreChange::RaiseFormat { .. })
            {
                continue;
            }
            let path = snapshot_path(id)?;
            let prefix = self.snapshot_prefix(&path).await?;
            if prefix.audience != id.audience {
                return Err(inconsistent("boundary audience differs from its snapshot"));
            }
            // Its prefix is authenticated by each chunk. Open the complete
            // header frame without requiring an obsolete app schema to load.
            let ring = self
                .store_keys
                .unlock()?
                .ok_or(SyncError::KeyUnavailable(prefix.key))?;
            let key = io::key(&ring, &id.audience, prefix.key)?;
            let clear = prefix.encode()?;
            let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
            let mut offset = clear.len() as u64;
            let mut layout = SnapshotObjectLayout::new();
            let mut frames = FrameDecoder::new(None);
            loop {
                let mut piece = io::range(storage, &path, offset, 4).await?;
                let length = layout.chunk_length(&piece)?;
                piece.extend(io::range(storage, &path, offset + 4, length - 4).await?);
                let index = layout.index();
                let sealed = layout.decode_chunk(&piece)?;
                let plain = key.open_object_chunk(path.as_str(), &clear, 0, index, sealed)?;
                if let Some(frame) = frames.next(&mut plain.as_slice())? {
                    let decoder = SnapshotDecoder::start(&frame)?;
                    let header = decoder.header();
                    if header.id != *id
                        || header.writes != prefix.writes
                        || header.store_log != prefix.store_log
                    {
                        return Err(inconsistent(
                            "boundary header and authenticated prefix disagree",
                        ));
                    }
                    break;
                }
                offset += length as u64;
            }
            saved.push(SavedBoundary {
                entry: Object::StoreLog(entry.clone()).encode()?,
                prefix: clear,
            });
        }
        Ok(saved)
    }

    pub(super) fn has_snapshot_boundary(&self, log: &StoreLog, audience: &Audience) -> bool {
        entries(log).any(|(_, snapshot)| snapshot.audience == *audience)
    }
}
