use super::*;

impl DeviceJoinBootstrapPlan {
    pub fn from_verified_commits(
        founder: coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
        genesis: ResolvedStoreDeviceState,
        membership: InitialStoreMembershipAuthority,
        installed: &coven_protocol::store_commit::CommitFrontier,
        publication: AcceptedStorePublicationInterval,
        mut commits: std::collections::BTreeMap<StoreBatchCommitRef, DeviceJoinBootstrapCommit>,
    ) -> Result<Self, DbError> {
        let mut ordered = Vec::new();
        let mut emitted = std::collections::BTreeSet::new();
        for entry in publication.interval().entries() {
            let coven_protocol::store_commit::StorePublicationPayload::Commit(reference) =
                &entry.entry().payload
            else {
                continue;
            };
            let carried = commits.remove(reference).ok_or_else(|| {
                DbError::Message(format!(
                    "device join history omits accepted commit {reference:?}"
                ))
            })?;
            if carried.reference != *reference || carried.commit.reference() != reference {
                return Err(DbError::Message(
                    "device join history contains another exact commit".into(),
                ));
            }
            if carried
                .commit
                .order
                .predecessor
                .iter()
                .chain(carried.commit.order.dependencies.values())
                .any(|dependency| {
                    !installed.covers_commit(dependency) && !emitted.contains(dependency)
                })
            {
                return Err(DbError::Message(
                    "device join publication precedes a required history input".into(),
                ));
            }
            publication.accepted_commit(&carried.commit)?;
            emitted.insert(reference.clone());
            ordered.push(carried);
        }
        Ok(Self {
            founder_reference: founder.reference().clone(),
            founder: founder.value().clone(),
            founder_bytes: founder.value().to_bytes(),
            genesis,
            membership,
            publication,
            commits: ordered,
        })
    }

    pub fn verified_commit(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<&VerifiedStoreBatchCommit> {
        self.commits
            .iter()
            .find(|commit| &commit.reference == reference)
            .map(|commit| &commit.commit)
    }

    pub fn into_closure(
        self,
        root: &StoreRootRef,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceJoinBootstrapClosure,
        DbError,
    > {
        if self.founder.store_root != *root || self.founder.to_bytes() != self.founder_bytes {
            return Err(DbError::Message(
                "device join bootstrap founder differs from its canonical bytes".to_string(),
            ));
        }
        let founder = coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
            self.founder_reference,
            self.founder,
        )
        .map_err(DbError::from)?;
        let publication = coven_protocol::store_commit::device_join_exchange::DeviceJoinBootstrapPublicationInterval {
            previous: self.publication.interval().previous().clone(),
            current: self.publication.interval().current().clone(),
            entries: self
                .publication
                .interval()
                .entries()
                .iter()
                .map(|accepted| {
                    Ok(coven_protocol::store_commit::device_join_exchange::DeviceJoinBootstrapPublicationEntry {
                        entry: PreparedExactObject::new(
                            accepted.reference().object.clone(),
                            accepted.entry().to_bytes(),
                        )
                        .map_err(DbError::from)?,
                        author: accepted.author().clone(),
                    })
                })
                .collect::<Result<Vec<_>, DbError>>()?,
        };
        let commits = self
            .commits
            .into_iter()
            .map(|commit| {
                let value = commit.commit.value();
                if commit.commit.reference() != &commit.reference
                    || commit.commit.store_root_hash() != root.store_root_hash
                {
                    return Err(DbError::Message(
                        "device join bootstrap commit differs from its exact reference"
                            .to_string(),
                    ));
                }
                let author =
                    coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
                        value.author_registration.clone(),
                        commit.commit.author().clone(),
                    )
                    .map_err(DbError::from)?;
                let registrations = RetainedStoreDeviceRegistrationActivations::from_verified(
                    root,
                    value,
                    &commit.registrations,
                )
                .map_err(DbError::from)?;
                Ok(coven_protocol::store_commit::device_join_exchange::DeviceJoinBootstrapCommitClosure {
                    reference: commit.reference,
                    canonical_commit: value.to_bytes(),
                    author,
                    registrations,
                    device_operations: commit.device_operations.to_retained(),
                    history_evidence: commit.history_evidence,
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        Ok(
            coven_protocol::store_commit::device_join_exchange::DeviceJoinBootstrapClosure {
                founder,
                genesis: self.genesis,
                membership: coven_protocol::membership::MembershipFloor(self.membership.head_refs),
                publication,
                commits,
            },
        )
    }

    pub fn from_closure(
        root: &StoreRootRef,
        previous: coven_protocol::store_commit::StoreCurrentPublicationRecord,
        closure: coven_protocol::store_commit::device_join_exchange::DeviceJoinBootstrapClosure,
    ) -> Result<Self, DbError> {
        if *previous != closure.publication.previous {
            return Err(DbError::Message(
                "device join history starts from another snapshot boundary".into(),
            ));
        }
        let founder_reference = closure.founder.reference().clone();
        let founder = closure.founder.value().clone();
        let founder_bytes = founder.to_bytes();
        if founder.store_root != *root {
            return Err(DbError::Message(
                "device join bootstrap founder belongs to another Store".to_string(),
            ));
        }
        founder_reference
            .object
            .verify(&founder_bytes)
            .map_err(DbError::from)?;

        let publication = closure.publication.verify().map_err(DbError::from)?;
        let publication = AcceptedStorePublicationInterval::from_verified(publication, None);

        let commits = closure
            .commits
            .into_iter()
            .map(|carried| {
                let commit = VerifiedStoreBatchCommit::parse(
                    &carried.canonical_commit,
                    root.store_root_hash,
                    &carried.reference,
                    carried.author.value(),
                )
                .map_err(DbError::from)?;
                if commit.author_registration != *carried.author.reference() {
                    return Err(DbError::Message(
                        "device join bootstrap commit differs from its carried author".to_string(),
                    ));
                }
                publication
                    .accepted_commit(&commit)
                    .map_err(|error| DbError::context("device join accepted publication", error))?;
                let registrations = carried
                    .registrations
                    .verify_for(root, commit.value())
                    .map_err(DbError::from)?;
                let device_operations = carried
                    .device_operations
                    .verify_for(root, commit.value())
                    .map_err(DbError::from)?;
                Ok(DeviceJoinBootstrapCommit {
                    reference: carried.reference,
                    commit,
                    registrations,
                    device_operations,
                    history_evidence: carried.history_evidence,
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?;

        Ok(Self {
            founder_reference,
            founder,
            founder_bytes,
            genesis: closure.genesis,
            membership: InitialStoreMembershipAuthority {
                head_refs: closure.membership.0,
            },
            publication,
            commits,
        })
    }
}
