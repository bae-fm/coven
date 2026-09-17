use super::*;

/// The exact accepted Circle activation an inherited entry or an observed
/// predecessor control names: the control it activated and the signed object
/// graph that control published.
pub(super) struct AcceptedCircleActivation {
    pub(super) control: CircleControlCoord,
    pub(super) objects: CircleActivationObjects,
}

/// One Circle entry whose place in accepted history is being proved: its
/// coordinate, the signed entry itself, and the reference the control under
/// verification carries for it.
pub(super) enum CircleEntryProvenance<'entry> {
    Roster {
        coord: &'entry CircleRosterCoord,
        entry: &'entry coven_protocol::circle::CircleRosterEntry,
        reference: &'entry CircleRosterEntryRef,
    },
    Metadata {
        coord: &'entry CircleMetadataCoord,
        entry: &'entry coven_protocol::circle::CircleMetadata,
        reference: &'entry CircleMetadataObjectRef,
    },
}

impl CircleEntryProvenance<'_> {
    fn kind(&self) -> &'static str {
        match self {
            Self::Roster { .. } => "roster",
            Self::Metadata { .. } => "metadata",
        }
    }

    fn author(&self) -> (&str, &str) {
        match self {
            Self::Roster { entry, .. } => (&entry.author_pubkey, &entry.device_id),
            Self::Metadata { entry, .. } => (&entry.author_pubkey, &entry.device_id),
        }
    }

    fn origin(&self) -> &CircleEntryOrigin {
        match self {
            Self::Roster { reference, .. } => &reference.origin,
            Self::Metadata { reference, .. } => &reference.origin,
        }
    }

    /// Whether `objects` introduces this exact entry at this exact object.
    fn introduced_by(&self, objects: &CircleActivationObjects) -> bool {
        match self {
            Self::Roster {
                coord, reference, ..
            } => objects.roster_entries.get(coord).is_some_and(|introduced| {
                introduced.object == reference.object && introduced.origin.is_introduced()
            }),
            Self::Metadata {
                coord, reference, ..
            } => objects
                .metadata_entries
                .get(coord)
                .is_some_and(|introduced| {
                    introduced.object == reference.object
                        && introduced.key_fingerprint == reference.key_fingerprint
                        && introduced.origin.is_introduced()
                }),
        }
    }
}

impl<'operation, 'storage> CircleActivationVerifier<'operation, 'storage> {
    /// Resolve the exact accepted Circle activation `activating_commit` carries
    /// for `circle_id`.
    ///
    /// Three sources, in order: activations verified earlier in this same batch
    /// but not yet installed; this device's own retained accepted activations;
    /// and the candidate commit's verified predecessor history. A commit that
    /// resolves to none of them, or that carries no control for this Circle, is
    /// a hard failure — there is no repair pass behind this.
    ///
    /// The third source walks predecessors by reading commits from cloud
    /// storage, so it needs no separate answer for a commit a snapshot baseline
    /// already covers. Store commits are never reclaim targets: reclamation
    /// retires object bodies, and `snapshot_required_retained_refs` keeps
    /// every commit an `Inherited` origin names for exactly as long as a
    /// retained control names it. So a named commit that the walk does not
    /// reach is not a retired body — it is a commit outside this candidate's
    /// accepted history, which is one failure with one meaning.
    pub(super) async fn accepted_circle_activation(
        &mut self,
        prefix: &VerifiedCircleActivationPrefix,
        candidate: &StoreBatchCommit,
        circle_id: CircleId,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<AcceptedCircleActivation, CircleOperationError> {
        if let Some(staged) = prefix.activation(activating_commit, circle_id) {
            return Ok(AcceptedCircleActivation {
                control: staged.reference.control().clone(),
                objects: staged.reference.objects().clone(),
            });
        }
        let root = self.root().clone();
        if let Some(retained) = self
            .database
            .retained_circle_activation(root, circle_id, activating_commit.clone())
            .await?
        {
            return Ok(AcceptedCircleActivation {
                control: retained.reference.control().clone(),
                objects: retained.reference.objects().clone(),
            });
        }
        let expected = activating_commit.clone();
        let reached = self
            .history
            .predecessor_commit_matching(
                &candidate.order,
                Box::new(move |predecessor| predecessor.reference() == &expected),
            )
            .await
            .map_err(crate::sync::store::StorePullError::from)
            .map_err(CircleOperationError::from)?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} names activating commit {} outside its accepted \
                     predecessor history",
                    activating_commit.commit_hash
                ))
            })?;
        let reference = reached
            .value()
            .circle_controls()
            .iter()
            .find(|reference| reference.circle_id() == circle_id)
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} names activating commit {} which activates no control \
                     for it",
                    activating_commit.commit_hash
                ))
            })?;
        Ok(AcceptedCircleActivation {
            control: reference.control().clone(),
            objects: reference.objects().clone(),
        })
    }

    /// Prove where one Circle roster or metadata entry entered accepted history.
    ///
    /// An `Introduced` entry is published by the commit under verification,
    /// whose signing device must be the entry's own author device: the
    /// device-signed Store commit is the entry's device-authorship proof. An
    /// `Inherited` entry names the exact earlier accepted activation whose
    /// signed object graph introduced this exact entry object, which preserves
    /// the original author and device across every later control that carries
    /// the entry forward.
    pub(super) async fn verify_circle_entry_origin(
        &mut self,
        prefix: &VerifiedCircleActivationPrefix,
        candidate: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
        circle_id: CircleId,
        entry: CircleEntryProvenance<'_>,
    ) -> Result<(), CircleOperationError> {
        let kind = entry.kind();
        match entry.origin() {
            CircleEntryOrigin::Introduced => {
                let (author_pubkey, device_id) = entry.author();
                if author_pubkey != author.author_pubkey
                    || device_id != author.device_id.to_string()
                {
                    return Err(CircleOperationError::InvalidState(format!(
                        "introduced Circle {kind} entry was not authored by the device that \
                         signed its activating commit"
                    )));
                }
                Ok(())
            }
            CircleEntryOrigin::Inherited { activating_commit } => {
                let accepted = self
                    .accepted_circle_activation(prefix, candidate, circle_id, activating_commit)
                    .await?;
                if !entry.introduced_by(&accepted.objects) {
                    return Err(CircleOperationError::InvalidState(format!(
                        "inherited Circle {kind} entry was not introduced by the accepted \
                         activation it names"
                    )));
                }
                Ok(())
            }
        }
    }

    /// Verify every observed predecessor control this control names, and that
    /// this control carries every entry its predecessors published.
    ///
    /// Each edge names the exact accepted Store commit that activated the
    /// predecessor, so a Store member outside the Circle verifies the whole
    /// predecessor spine — and the inheritance of the entry inventory — without
    /// holding any Circle key.
    ///
    /// Inheritance is monotone: a successor may add entries, never drop or
    /// replace one. That is what stops an Owner from re-publishing a different
    /// entry at an author-stream position an earlier accepted control already
    /// filled — the earlier entry stays in the inventory, both land in the
    /// successor's reduction, and the reduction refuses two entries at one
    /// sequence.
    ///
    /// Two *concurrent* controls that each fill one position differently both
    /// verify alone, so they surface as a control conflict — and that conflict
    /// is terminal for the Circle. An entry position belongs to one author
    /// device, so both controls also sit at one position of that device's
    /// control stream, and `covered_controls` is a frontier of one control per
    /// stream: no successor can name both. A resolution is refused when it is
    /// authored, because its inventory would carry two entries its frontier
    /// cannot both reach; a deletion is accepted but covers only the branch it
    /// authors from, and the other branch outlives it.
    ///
    /// So the position stays held and the failure stays surfaced, on every
    /// device, forever. Only an Owner device that authors from a Circle state
    /// it has already moved past reaches it — which is what its own durable
    /// operation journal exists to prevent — and nothing here repairs it.
    pub(super) async fn verify_covered_controls(
        &mut self,
        prefix: &VerifiedCircleActivationPrefix,
        candidate: &StoreBatchCommit,
        control: &CircleControl,
        objects: &CircleActivationObjects,
    ) -> Result<(), CircleOperationError> {
        for covered in control.covered_controls() {
            let accepted = self
                .accepted_circle_activation(
                    prefix,
                    candidate,
                    control.circle_id,
                    &covered.activating_commit,
                )
                .await?;
            if accepted.control != covered.coord {
                return Err(CircleOperationError::InvalidState(
                    "covered Circle control differs from the control its named activation \
                     accepted"
                        .to_string(),
                ));
            }
            let inherits_roster =
                accepted
                    .objects
                    .roster_entries
                    .iter()
                    .all(|(coord, inherited)| {
                        objects.roster_entries.get(coord).is_some_and(|carried| {
                            carried.object == inherited.object
                                && introduces_the_same_activation(
                                    &covered.activating_commit,
                                    &inherited.origin,
                                    &carried.origin,
                                )
                        })
                    });
            let inherits_metadata =
                accepted
                    .objects
                    .metadata_entries
                    .iter()
                    .all(|(coord, inherited)| {
                        objects.metadata_entries.get(coord).is_some_and(|carried| {
                            carried.object == inherited.object
                                && carried.key_fingerprint == inherited.key_fingerprint
                                && introduces_the_same_activation(
                                    &covered.activating_commit,
                                    &inherited.origin,
                                    &carried.origin,
                                )
                        })
                    });
            if !inherits_roster || !inherits_metadata {
                return Err(CircleOperationError::InvalidState(
                    "Circle control drops or replaces an entry a covered predecessor published"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Whether a successor's origin for one entry names the same introduction the
/// predecessor's origin did. A predecessor that introduced the entry itself is
/// the activation the successor must now inherit from.
fn introduces_the_same_activation(
    predecessor_commit: &StoreBatchCommitRef,
    predecessor: &CircleEntryOrigin,
    successor: &CircleEntryOrigin,
) -> bool {
    match (predecessor, successor) {
        (CircleEntryOrigin::Introduced, CircleEntryOrigin::Inherited { activating_commit }) => {
            activating_commit == predecessor_commit
        }
        (
            CircleEntryOrigin::Inherited {
                activating_commit: introduced,
            },
            CircleEntryOrigin::Inherited {
                activating_commit: carried,
            },
        ) => introduced == carried,
        (_, CircleEntryOrigin::Introduced) => false,
    }
}
