//! Claiming the slot an operation's step needs, settling the claim, and
//! undoing a step's artifacts (§11.4, §6.3).

use super::{OperationProgress, OperationStep, Request, SequenceOperation};
use crate::sequence_sync::sequence_copies::{admission_prefix, required_copies, seal_copy};
use crate::sequence_sync::OperationState;
use crate::{
    sequence_sync::{next_slot, SequenceSync, Session},
    slot_object, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{ClaimAttempt, ClaimSettlement, ClaimWork, OperationId, OperationRecord};
use coven_format::{
    admin::StoreChange,
    sealed_slot::{BoxPlace, SlotKind, SlotObjectPrefix},
    sequence::{Purpose, SlotFrame, SlotHeader},
};
use std::io::Cursor;

impl SequenceSync {
    /// The settlement of the open `claim`: an attempt not yet read is read,
    /// and one whose slot is still empty is sent again with its exact bytes.
    pub(crate) async fn settle_claim(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        claim: &coven_database::Claim,
    ) -> Result<ClaimSettlement, SyncError> {
        let settlement = match claim.settlement {
            ClaimSettlement::Unknown => {
                self.settle_open(session, ring, member).await?.expect("open claim")
            }
            value => value,
        };
        if settlement == ClaimSettlement::Retry {
            return self.publish_current(session, ring, member).await;
        }
        Ok(settlement)
    }

    pub(super) async fn claim_step(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        record: &OperationRecord,
        request: &Request,
        step: &OperationStep,
    ) -> Result<(), SyncError> {
        // The store's creation is sealed once, when the store is created
        // (`creation`); an operation of it with no attempt to send has lost
        // its slot.
        if matches!(request, Request::Change(StoreChange::CreateStore { .. })) {
            return Err(coven_database::DbError::Invariant {
                what: "a store's creation journals the attempt at its slot 1",
            }
            .into());
        }
        let path = self.path(next_slot(session.through)?);
        let store_key = session.history.metadata_key(ring)?;
        let (frame, kind) = match step {
            OperationStep::Copy(route) => {
                let route = route.clone();
                let copy = seal_copy(ring, &path, store_key, BoxPlace::KeyCopy, route)?;
                (SlotFrame::KeyCopy, SlotKind::KeyCopy(copy))
            }
            OperationStep::Claiming => {
                if let Request::Change(change) = request {
                    (SlotFrame::Admin(change.clone()), SlotKind::Admin)
                } else {
                    let Request::Admission(admission) = request else {
                        unreachable!("an admin claim is a change or an admission")
                    };
                    let change = &admission.change;
                    let copies = required_copies(&session.history, ring, change, member.member());
                    let frame = SlotFrame::Admin(change.clone());
                    let header = self.operation_header(session, member, 1);
                    let plaintext = frame.encode(&header, &path)?.len();
                    match admission_prefix(
                        ring,
                        &path,
                        store_key,
                        copies,
                        Some(admission),
                        plaintext,
                    )? {
                        Ok(prefix) => (frame, prefix.kind),
                        Err(reason) => {
                            self.database
                                .advance_operation(
                                    SequenceOperation {
                                        request: request.clone(),
                                        progress: OperationProgress::Undo {
                                            step: OperationStep::Claiming,
                                            reason,
                                        },
                                    }
                                    .update(record)?,
                                )
                                .await?;
                            return Ok(());
                        }
                    }
                }
            }
            OperationStep::Announcing(artifact) => {
                (SlotFrame::Announcement(artifact.announcement()), SlotKind::Announcement)
            }
            OperationStep::Activating { artifact, announcement } => {
                let change = match artifact.capture.purpose {
                    Purpose::Raise(version) => {
                        StoreChange::RaiseSchema { version, announcement: *announcement }
                    }
                    Purpose::Reset => StoreChange::Reset { announcement: *announcement },
                    Purpose::Ordinary => unreachable!("ordinary snapshots need no activation"),
                };
                session.history.validate(
                    &self.operation_header(session, member, 1),
                    next_slot(session.through)?,
                    &change,
                )?;
                (SlotFrame::Admin(change), SlotKind::Admin)
            }
            _ => unreachable!("a publication step"),
        };
        let held = ring.clone();
        let author = member.clone();
        let device = self.device;
        let after = session.through;
        let seal = move |claim| {
            let bytes = slot_object::seal_bounded(
                &path,
                &SlotHeader { device, claim, author: author.member(), after },
                &frame,
                SlotObjectPrefix {
                    format: coven_format::FormatVersion::CURRENT,
                    key: store_key,
                    kind,
                },
                &held,
                &author,
            )?;
            Ok::<_, SyncError>(ClaimAttempt { path, bytes: Cursor::new(bytes) })
        };
        if let Some(claim) = self.database.current_claim().await? {
            self.database.retry_claim(seal(claim.number)?).await?;
        } else {
            self.database.begin_claim(ClaimWork::Operation(record.id), seal).await?;
        }
        self.publish_current(session, ring, member).await?;
        Ok(())
    }

    pub(super) async fn undo_step(&self, step: &OperationStep) -> Result<(), SyncError> {
        let artifact = match step {
            OperationStep::Capturing(capture) => {
                self.artifact_file(&capture.name)?.remove()?;
                return Ok(());
            }
            OperationStep::Uploading(artifact)
            | OperationStep::Announcing(artifact)
            | OperationStep::Activating { artifact, .. } => artifact,
            _ => return Ok(()),
        };
        if let Some(session) = &artifact.session {
            self.storage.abort_upload(session).await?;
        }
        let path = crate::snapshot_object::path(self.store(), &artifact.capture.id)?;
        self.storage.delete(&path).await?;
        if self.storage.status(&path).await?.is_some() {
            return Err(coven_storage::StorageFailure::Protocol
                .with_source("snapshot remains after deletion")
                .into());
        }
        self.artifact_file(&artifact.capture.name)?.remove()?;
        Ok(())
    }

    pub(super) async fn abandon_settled_claim(&self, id: OperationId) -> Result<(), SyncError> {
        if let Some(claim) = self.database.current_claim().await? {
            if claim.work == ClaimWork::Operation(id) {
                self.database.finish_claim(None).await?;
            }
        }
        Ok(())
    }
}
