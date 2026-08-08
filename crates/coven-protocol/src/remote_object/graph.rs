use super::identity::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateObjectMaterial {
    pub object: ExactObjectRef,
    pub canonical_semantic_bytes: Vec<u8>,
    /// The ciphertext this object is uploaded as. Carried here because the
    /// transaction that writes the record's row is what installs it in the
    /// payload spool, and a record cannot be persisted without it.
    pub stored_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateObjectGraph {
    family: CandidateFamilyId,
    objects: Vec<CandidateExclusiveObjectDomain>,
}

impl CandidateObjectGraph {
    pub fn from_commit(
        commit: &crate::store_commit::StoreBatchCommit,
    ) -> Result<Self, RemoteObjectRecordError> {
        let manifest = commit
            .verified_candidate_objects()
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        let mut objects = Vec::new();
        for candidate in &manifest.objects {
            match candidate {
                crate::store_commit::CandidateExclusiveObjectRef::StorePackage(reference) => {
                    objects.push(CandidateExclusiveObjectDomain::StorePackage {
                        reference: reference.clone(),
                    });
                }
                crate::store_commit::CandidateExclusiveObjectRef::CirclePackage(reference) => {
                    objects.push(CandidateExclusiveObjectDomain::CirclePackage {
                        reference: reference.clone(),
                    });
                }
                crate::store_commit::CandidateExclusiveObjectRef::CircleAccess {
                    circle_id,
                    access,
                } => {
                    objects.push(CandidateExclusiveObjectDomain::CircleAccessLeaf {
                        family: manifest.family,
                        circle_id: *circle_id,
                        reference: access.leaf.clone(),
                    });
                    objects.push(CandidateExclusiveObjectDomain::CircleAccessEnvelope {
                        family: manifest.family,
                        circle_id: *circle_id,
                        reference: access.envelope.clone(),
                    });
                    if let Some(bootstrap) = &access.bootstrap {
                        objects.push(CandidateExclusiveObjectDomain::CircleBootstrapImage {
                            family: manifest.family,
                            circle_id: *circle_id,
                            owner_pubkey: access.leaf.owner_pubkey.clone(),
                            epoch_id: access.leaf.epoch_id,
                            recipient_slot: access.leaf.recipient_slot.clone(),
                            reference: bootstrap.clone(),
                        });
                    }
                }
                crate::store_commit::CandidateExclusiveObjectRef::CircleEpochCloseIntent {
                    circle_id,
                    reference,
                } => {
                    objects.push(CandidateExclusiveObjectDomain::CircleEpochCloseIntent {
                        family: manifest.family,
                        circle_id: *circle_id,
                        reference: reference.clone(),
                    });
                }
                crate::store_commit::CandidateExclusiveObjectRef::CircleEpochCloseOutcome {
                    circle_id,
                    reference,
                } => {
                    objects.push(CandidateExclusiveObjectDomain::CircleEpochCloseOutcome {
                        family: manifest.family,
                        circle_id: *circle_id,
                        reference: reference.clone(),
                    });
                }
                crate::store_commit::CandidateExclusiveObjectRef::CircleEpochCloseCancellation {
                    circle_id,
                    reference,
                } => {
                    objects.push(CandidateExclusiveObjectDomain::CircleEpochCloseCancellation {
                        family: manifest.family,
                        circle_id: *circle_id,
                        reference: reference.clone(),
                    });
                }
            }
        }
        Ok(Self {
            family: manifest.family,
            objects,
        })
    }

    pub fn exact_objects(&self) -> impl Iterator<Item = &ExactObjectRef> {
        self.objects
            .iter()
            .map(CandidateExclusiveObjectDomain::object)
    }

    pub fn close(
        self,
        commit: &crate::store_commit::StoreBatchCommit,
        owner: &StoreBatchCommitRef,
        materials: Vec<CandidateObjectMaterial>,
    ) -> Result<Vec<ClosedRemoteObject>, RemoteObjectRecordError> {
        owner
            .verify_commit(commit)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        if self.family != commit.candidate_family() {
            return Err(RemoteObjectRecordError::DomainMismatch);
        }
        let mut exact = std::collections::BTreeMap::new();
        for material in materials {
            if exact.insert(material.object.clone(), material).is_some() {
                return Err(RemoteObjectRecordError::DuplicateCandidateObject);
            }
        }
        validate_access_pairs(&self.objects, &exact)?;
        let mut records = Vec::with_capacity(self.objects.len());
        for domain in self.objects {
            let object = domain.object().clone();
            let material = exact
                .remove(&object)
                .ok_or(RemoteObjectRecordError::CandidateObjectMissing)?;
            let (semantic_hash, payloads) = match &domain {
                CandidateExclusiveObjectDomain::CircleBootstrapImage { reference, .. } => {
                    (reference.image_hash, RemoteObjectPayloads::SpooledExternal)
                }
                _ => (
                    ObjectHash::digest(&material.canonical_semantic_bytes),
                    RemoteObjectPayloads::SpooledInline,
                ),
            };
            let record = RemoteObjectRecord::CandidateExclusive(CandidateObjectRecord {
                identity: CandidateExclusiveTarget {
                    family: domain.family(),
                    domain,
                    semantic_hash,
                    object,
                },
                payloads,
                state: CandidateObjectState::Prepared {
                    ownership: PendingCandidateOwnership {
                        pending: BTreeSet::from([owner.clone()]),
                        nonactivated: Vec::new(),
                    },
                },
            });
            record.validate_payload(&material.canonical_semantic_bytes)?;
            records.push(ClosedRemoteObject::with_spooled_payloads(
                record,
                &material.canonical_semantic_bytes,
                &material.stored_bytes,
            )?);
        }
        if !exact.is_empty() {
            return Err(RemoteObjectRecordError::CandidateObjectInvented);
        }
        Ok(records)
    }
}
