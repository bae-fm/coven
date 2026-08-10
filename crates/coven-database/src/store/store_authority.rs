use crate::*;
use crate::{RetainedReplayAuthority, RetainedReplayGenesisAuthority, GENERATION_ZERO};
use coven_protocol::store_commit::{
    ResolvedStoreDeviceState, StoreAckRef, StoreDeviceRegistration, StoreDeviceRegistrationRef,
};
use rusqlite::OptionalExtension;

use super::*;

impl StoreSession<'_> {
    fn membership_head_cursors(&mut self) -> Result<InitialStoreMembershipAuthority, DbError> {
        InitialStoreMembershipAuthority::load_on(self.conn)
    }

    fn persist_membership_head_cursors(
        &mut self,
        head_refs: Vec<coven_protocol::membership::MembershipHeadRef>,
    ) -> Result<(), DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        InitialStoreMembershipAuthority { head_refs }.install_on(&transaction)?;
        transaction.commit().map_err(DbError::from)
    }

    fn validated_store_owner(
        &mut self,
        expected_root: coven_protocol::store_commit::StoreRootRef,
    ) -> Result<String, DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let (root, protocol_root) = self
            .verified_store_authority
            .root_authority_on(records)?
            .ok_or(DbError::StoreRootHashMissing)?;
        if root != expected_root {
            return Err(DbError::Message(
                "local Store root differs from the operation authority".to_string(),
            ));
        }
        let owner = get_protocol_state_on(
            records.conn,
            coven_protocol::membership::OWNER_PUBKEY_STATE_KEY,
        )?
        .ok_or_else(|| DbError::Message("Store owner anchor is absent".to_string()))?;
        if owner != protocol_root.descriptor.founder_pubkey {
            return Err(DbError::Message(
                "Store owner anchor differs from its signed root".to_string(),
            ));
        }
        let baseline = self
            .verified_store_authority
            .retained_replay_baseline_on(records)?;
        let (baseline_root, founder_reference) = match &baseline.authority {
            RetainedReplayAuthority::Genesis(authority) => (
                authority.store_root.clone(),
                authority.founder_registration.clone(),
            ),
            RetainedReplayAuthority::StableSnapshot(authority) => (
                authority.store_root.clone(),
                authority.founder_registration.clone(),
            ),
        };
        if baseline_root != root {
            return Err(DbError::Message(
                "retained replay baseline belongs to another Store root".to_string(),
            ));
        }
        let founder = self.verified_store_authority.activated_registration_on(
            records,
            &root,
            &founder_reference,
        )?;
        let expected_genesis = ResolvedStoreDeviceState::founder(
            &root,
            founder_reference,
            &protocol_root.descriptor.founder_pubkey,
            protocol_root.descriptor.founder_grant.clone(),
            &protocol_root.descriptor.founder_recovery,
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        let stored_genesis: ResolvedStoreDeviceState = serde_json::from_str(
            &required_protocol_state_on(records.conn, STORE_DEVICE_GENESIS_STATE_KEY)?,
        )
        .map_err(|error| DbError::context("Store device genesis state", error))?;
        if founder.author_pubkey != owner || stored_genesis != expected_genesis {
            return Err(DbError::Message(
                "Store device genesis differs from installed founder authority".to_string(),
            ));
        }
        Ok(owner)
    }

    #[allow(clippy::too_many_arguments)]
    fn install_store_owner_anchor(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        root_bytes: Vec<u8>,
        founder_reference: StoreDeviceRegistrationRef,
        founder: StoreDeviceRegistration,
        founder_bytes: Vec<u8>,
        genesis: ResolvedStoreDeviceState,
        owner: String,
        membership: InitialStoreMembershipAuthority,
    ) -> Result<(), DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let root_value = install_store_root_authority_on(&tx, &root, &root_bytes)?;
        install_store_founder_state_on(
            &tx,
            &root,
            &founder_reference,
            &founder,
            &founder_bytes,
            &genesis,
        )?;
        set_protocol_state_on(
            &tx,
            coven_protocol::membership::OWNER_PUBKEY_STATE_KEY,
            &owner,
        )?;
        membership.install_on(&tx)?;
        ensure_founder_replay_baseline_on(
            crate::store::StoreRecords::new(&tx, records.store_dir),
            self.schema_version,
            self.sync_routing_hash,
            RetainedReplayGenesisAuthority {
                store_root: root.clone(),
                founder_registration: founder_reference.clone(),
            },
        )?;
        tx.commit().map_err(DbError::from)?;
        self.verified_store_authority
            .commit_installed_root(root, root_value);
        self.verified_store_authority
            .commit_installed_registration(founder_reference, founder);
        Ok(())
    }

    fn local_store_founder_graph(&mut self) -> Result<Option<Box<DurableFounderGraph>>, DbError> {
        load_local_store_founder_graph_on(self.conn)
    }

    fn reset_store_founder_graph_publication(
        &mut self,
        expected_identity: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let durable = load_local_store_founder_graph_on(&tx)?
            .ok_or_else(|| DbError::Message("local Store founder graph is absent".to_string()))?;
        if founder_graph_identity(&durable) != expected_identity {
            return Err(DbError::Message(
                "local Store founder graph changed before publication rollback".to_string(),
            ));
        }
        match durable.registration_state {
            LocalDeviceRegistrationState::Prepared => {}
            LocalDeviceRegistrationState::Created => {
                let created = serde_json::to_string(&LocalDeviceRegistrationState::Created)
                    .map_err(|error| DbError::context("serialize created journal state", error))?;
                let prepared = serde_json::to_string(&LocalDeviceRegistrationState::Prepared)
                    .map_err(|error| DbError::context("serialize prepared journal state", error))?;
                let updated = tx
                    .execute(
                        "UPDATE local_store_device_registration SET state = ?1 \
                         WHERE singleton = 1 AND state = ?2",
                        (prepared, created),
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "created founder journal did not reset after exact rollback".to_string(),
                    ));
                }
            }
            LocalDeviceRegistrationState::Activated { .. } => {
                return Err(DbError::Message(
                    "activated founder graph cannot be rolled back".to_string(),
                ));
            }
        }
        tx.commit().map_err(DbError::from)
    }
    fn stage_store_founder_graph(
        &mut self,
        graph: Box<DurableFounderGraph>,
    ) -> Result<(), DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        if let Some(existing) = load_local_store_founder_graph_on(&tx)? {
            existing.validate()?;
            if founder_graph_identity(&existing) == founder_graph_identity(&graph) {
                return Ok(());
            }
            return Err(DbError::Message(
                "local Store founder graph already owns different exact objects".to_string(),
            ));
        }
        use coven_protocol::provider::{ExactProbeProgress, ProviderProbeJournalRecord};
        use coven_protocol::store_creation::{
            StoreCreationAttempt, STORE_CREATION_ATTEMPT_STATE_KEY,
        };

        let attempt_json =
            crate::required_protocol_state_on(&tx, STORE_CREATION_ATTEMPT_STATE_KEY)?;
        let attempt: StoreCreationAttempt = serde_json::from_str(&attempt_json)
            .map_err(|error| DbError::context("parse Store creation attempt", error))?;
        let StoreCreationAttempt::FounderGraphReserved(graph_reservation) = attempt else {
            return Err(DbError::Message(
                "Store creation attempt has not reserved the complete founder graph".to_string(),
            ));
        };
        let reservation = &graph_reservation.descriptor;
        let descriptor = &graph.root.value.descriptor;
        let founder = &reservation.membership.founder;
        let authority = &founder.root.authority;
        if authority.creation_id != descriptor.creation_id
            || authority.founder_grant != descriptor.founder_grant
            || authority.provider_admin_grant != descriptor.founder_provider_admin.grant_id
            || authority.binding.store != descriptor.provider
            || authority.binding.device != descriptor.founder_provider_admin.provider
            || authority.founder_pubkey != descriptor.founder_pubkey
            || authority.schema_version != descriptor.schema_version
            || authority.sync_routing_hash != descriptor.sync_routing_hash
            || founder.root.root_slot != descriptor.root_slot
            || founder.registration_slot != descriptor.founder_registration
            || &reservation.recovery_slot != descriptor.founder_recovery.first_slot()
            || descriptor.founder_membership.first_slot() != &reservation.membership.first_slot
        {
            return Err(DbError::Message(
                "signed Store descriptor differs from its durable creation attempt".to_string(),
            ));
        }
        if graph.registration.value.store_commits != graph_reservation.store_commits
            || graph.registration.value.acknowledgements != graph_reservation.acknowledgements
            || graph.registration.value.snapshots != graph_reservation.snapshots
            || graph.initial_ack.value.last_sync != authority.founder_timestamp
            || graph.initial_ack.value.successor.next_slot != graph_reservation.next_ack_slot
            || graph.membership.entry.value.created_at != authority.founder_timestamp
            || graph.membership.head.value.body.successor.next_slot
                != graph_reservation.membership.next_head_slot
        {
            return Err(DbError::Message(
                "signed founder graph differs from its durable slot reservation".to_string(),
            ));
        }

        let exact_key = format!(
            "provider_probe/{}",
            hex::encode(authority.probes.exact_slots().as_bytes())
        );
        let exact_json = crate::required_protocol_state_on(&tx, &exact_key)?;
        let exact: ProviderProbeJournalRecord = serde_json::from_str(&exact_json)
            .map_err(|error| DbError::context("parse provider probe journal", error))?;
        let ProviderProbeJournalRecord::Exact(exact) = exact else {
            return Err(DbError::Message(
                "Store creation exact probe id names another probe kind".to_string(),
            ));
        };
        let ExactProbeProgress::ReceiptReady { receipt } = exact.progress else {
            return Err(DbError::Message(
                "Store creation exact probe has no terminal receipt".to_string(),
            ));
        };
        if receipt != descriptor.founder_provider_admin.capability.exact_slots {
            return Err(DbError::Message(
                "signed Store descriptor differs from its terminal exact probe".to_string(),
            ));
        }
        for key in [exact_key, STORE_CREATION_ATTEMPT_STATE_KEY.to_string()] {
            let deleted = crate::delete_protocol_state_on(&tx, &key)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "Store creation journal disappeared during typed consumption".to_string(),
                ));
            }
        }
        tx.execute(
            "INSERT INTO local_store_protocol_root \
         (singleton, store_root_hash, store_protocol_root_bytes, prepared_object) \
         VALUES (1, ?1, ?2, ?3)",
            rusqlite::params![
                graph.root.value.object_hash().to_string(),
                graph.root.bytes,
                serde_json::to_string(&graph.root.prepared).map_err(|error| {
                    DbError::context("serialize prepared Store root", error)
                })?,
            ],
        )
        .map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO local_store_device_registration \
         (singleton, device_id, registration_hash, registration_bytes, prepared_object, \
          initial_ack_ref, initial_ack_bytes, initial_ack_prepared, state) \
         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                graph.registration.value.device_id.to_string(),
                graph.registration.value.registration_hash().to_string(),
                graph.registration.bytes,
                serde_json::to_string(&graph.registration.prepared).map_err(|error| {
                    DbError::context("serialize prepared founder registration", error)
                })?,
                serde_json::to_string(&graph.initial_ack_ref).map_err(|error| {
                    DbError::context("serialize founder initial ack ref", error)
                })?,
                graph.initial_ack.bytes,
                serde_json::to_string(&graph.initial_ack.prepared).map_err(|error| {
                    DbError::context("serialize founder initial ack object", error)
                })?,
                serde_json::to_string(&LocalDeviceRegistrationState::Prepared).map_err(
                    |error| DbError::context("serialize registration journal state", error)
                )?,
            ],
        )
        .map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO local_store_founder_graph \
         (singleton, membership_graph) VALUES (1, ?1)",
            rusqlite::params![
                serde_json::to_string(&DurableFounderMembershipJournal::from_graph(
                    &graph.membership,
                ))
                .map_err(|error| DbError::context("serialize founder membership graph", error))?,
            ],
        )
        .map_err(DbError::from)?;
        tx.commit().map_err(DbError::from)
    }

    fn complete_store_founder_graph(
        &mut self,
        expected_root: coven_protocol::store_commit::StoreRootRef,
        expected_registration: StoreDeviceRegistrationRef,
        expected_initial_ack: StoreAckRef,
        expected_membership: FounderMembershipRefs,
    ) -> Result<(), DbError> {
        let schema_version = self.schema_version;
        let routing_hash = self.sync_routing_hash;
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let verified_authority = &mut *self.verified_store_authority;
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let store_dir = records.store_dir;
        let graph = load_local_store_founder_graph_on(&tx)?
            .ok_or_else(|| DbError::Message("local Store founder graph is absent".to_string()))?;
        let root = coven_protocol::store_commit::StoreRootRef {
            store_root_id: graph.root.value.descriptor.store_root_id(),
            store_root_hash: graph.root.value.object_hash(),
            object: graph.root.prepared.reference().clone(),
        };
        let registration = StoreDeviceRegistrationRef::from_registration(
            &graph.registration.value,
            graph.registration.prepared.reference().clone(),
        );
        if root != expected_root
            || registration != expected_registration
            || graph.initial_ack_ref != expected_initial_ack
            || graph.membership.entry_ref != expected_membership.entry
            || graph.membership.head_ref != expected_membership.head
        {
            return Err(DbError::Message(
                "verified founder graph differs from its durable exact references".to_string(),
            ));
        }
        let founder_authority =
            coven_protocol::store_commit::StoreDeviceRegistrationActivation::Founder {
                root: root.clone(),
            };
        let device_genesis = ResolvedStoreDeviceState::founder(
            &root,
            registration.clone(),
            &graph.root.value.descriptor.founder_pubkey,
            graph.root.value.descriptor.founder_grant.clone(),
            &graph.root.value.descriptor.founder_recovery,
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        let device_genesis_json = serde_json::to_string(&device_genesis)
            .map_err(|error| DbError::context("serialize Store device genesis state", error))?;
        let device_id = registration.device_id.to_string();
        let registration_hash = registration.registration_hash.to_string();
        match &graph.registration_state {
            LocalDeviceRegistrationState::Prepared => {
                return Err(DbError::Message(
                    "founder registration and initial acknowledgement are not exact-created"
                        .to_string(),
                ));
            }
            LocalDeviceRegistrationState::Created => {}
            LocalDeviceRegistrationState::Activated { authority } => {
                if authority != &founder_authority {
                    return Err(DbError::Message(
                        "founder registration journal carries another activation authority"
                            .to_string(),
                    ));
                }
                let installed = verified_authority
                    .root_authority_on(crate::store::StoreRecords::new(&tx, store_dir))?;
                let installed_registration = verified_authority.activated_registration_on(
                    crate::store::StoreRecords::new(&tx, store_dir),
                    &root,
                    &registration,
                )?;
                let stored: Option<(String, String, String)> = tx
                    .query_row(
                        "SELECT registration_object, activation_authority, registration_hash \
                         FROM store_device_registration_activations WHERE device_id = ?1",
                        [&device_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()
                    .map_err(DbError::from)?;
                let ack: Option<String> = tx
                    .query_row(
                        "SELECT ack_ref FROM published_store_acks WHERE singleton = 1",
                        [],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(DbError::from)?;
                let stored_device_genesis =
                    crate::get_protocol_state_on(&tx, STORE_DEVICE_GENESIS_STATE_KEY)?;
                if installed
                    .as_ref()
                    .map(|(reference, value)| (reference, value))
                    != Some((&root, &graph.root.value))
                    || installed_registration != graph.registration.value
                    || stored
                        != Some((
                            serde_json::to_string(&registration).map_err(|error| {
                                DbError::context("serialize founder registration ref", error)
                            })?,
                            serde_json::to_string(&founder_authority).map_err(|error| {
                                DbError::context("serialize founder authority", error)
                            })?,
                            registration.registration_hash.to_string(),
                        ))
                    || ack
                        != Some(
                            serde_json::to_string(&graph.initial_ack_ref).map_err(|error| {
                                DbError::context("serialize founder ack ref", error)
                            })?,
                        )
                    || stored_device_genesis.as_deref() != Some(&device_genesis_json)
                {
                    return Err(DbError::Message(
                        "activated founder journal differs from installed exact authority"
                            .to_string(),
                    ));
                }
                let baseline = load_generation_zero_replay_baseline_on(
                    crate::store::StoreRecords::new(&tx, store_dir),
                )?
                .ok_or_else(|| {
                    DbError::Message(
                        "activated founder state has no generation-zero replay baseline"
                            .to_string(),
                    )
                })?;
                if baseline.generation != GENERATION_ZERO
                    || baseline.schema_version != schema_version
                    || baseline.routing_hash != routing_hash
                    || baseline.authority
                        != RetainedReplayAuthority::Genesis(RetainedReplayGenesisAuthority {
                            store_root: root.clone(),
                            founder_registration: registration.clone(),
                        })
                {
                    return Err(DbError::Message(
                        "activated founder state differs from its generation-zero replay baseline"
                            .to_string(),
                    ));
                }
                return Ok(());
            }
        }
        let root_value = install_store_root_authority_on(&tx, &root, &graph.root.bytes)?;
        let activation = serde_json::to_string(
            &coven_protocol::store_commit::StoreDeviceRegistrationActivation::Founder {
                root: root.clone(),
            },
        )
        .map_err(|error| DbError::context("serialize founder registration activation", error))?;
        let journal_state = serde_json::to_string(&LocalDeviceRegistrationState::Activated {
            authority: founder_authority,
        })
        .map_err(|error| DbError::context("serialize founder registration journal", error))?;
        let updated = tx
            .execute(
                "UPDATE local_store_device_registration SET state = ?1 \
                 WHERE singleton = 1 AND device_id = ?2 AND registration_hash = ?3 \
                   AND initial_ack_ref = ?4 AND state = ?5",
                rusqlite::params![
                    journal_state,
                    &device_id,
                    &registration_hash,
                    serde_json::to_string(&graph.initial_ack_ref).map_err(|error| {
                        DbError::context("serialize founder initial ack ref", error)
                    })?,
                    serde_json::to_string(&LocalDeviceRegistrationState::Created).map_err(
                        |error| DbError::context("serialize created journal state", error)
                    )?,
                ],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(
                "founder registration journal did not activate".to_string(),
            ));
        }
        tx.execute(
            "INSERT INTO store_device_registration_activations \
             (device_id, registration_hash, author_pubkey, device_signing_pubkey, \
              registration_bytes, registration_object, activation_authority) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                &device_id,
                &registration_hash,
                graph.registration.value.author_pubkey,
                graph.registration.value.device_signing_pubkey,
                graph.registration.bytes,
                serde_json::to_string(&registration).map_err(|error| {
                    DbError::context("serialize founder registration ref", error)
                })?,
                activation,
            ],
        )
        .map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO published_store_acks \
             (singleton, ack_ref, successor_slot) VALUES (1, ?1, ?2)",
            rusqlite::params![
                serde_json::to_string(&graph.initial_ack_ref).map_err(|error| {
                    DbError::context("serialize founder initial ack ref", error)
                })?,
                serde_json::to_string(&graph.initial_ack.value.successor.next_slot)
                    .map_err(|error| DbError::context("serialize founder ack successor", error))?,
            ],
        )
        .map_err(DbError::from)?;
        for (key, value) in [
            (LOCAL_DEVICE_ID_STATE_KEY, device_id),
            (STORE_DEVICE_GENESIS_STATE_KEY, device_genesis_json),
        ] {
            crate::set_protocol_state_on(&tx, key, &value)?;
        }
        crate::set_protocol_state_on(
            &tx,
            coven_protocol::membership::OWNER_PUBKEY_STATE_KEY,
            &graph.root.value.descriptor.founder_pubkey,
        )?;
        crate::InitialStoreMembershipAuthority {
            head_refs: vec![graph.membership.head_ref.clone()],
        }
        .install_on(&tx)?;
        install_generation_zero_replay_baseline_on(
            crate::store::StoreRecords::new(&tx, store_dir),
            schema_version,
            routing_hash,
            RetainedReplayGenesisAuthority {
                store_root: root.clone(),
                founder_registration: registration.clone(),
            },
        )?;
        tx.commit().map_err(DbError::from)?;
        verified_authority.commit_installed_root(root, root_value);
        verified_authority.commit_installed_registration(registration, graph.registration.value);
        Ok(())
    }
}

impl StoreDatabase {
    pub async fn membership_head_cursors(
        &self,
    ) -> Result<crate::InitialStoreMembershipAuthority, DbError> {
        self.connection
            .call_store(|session| session.membership_head_cursors())
            .await
    }

    pub async fn persist_membership_head_cursors(
        &self,
        head_refs: Vec<coven_protocol::membership::MembershipHeadRef>,
    ) -> Result<(), DbError> {
        self.connection
            .call_store(move |session| session.persist_membership_head_cursors(head_refs))
            .await
    }

    pub async fn local_store_root_ref(
        &self,
    ) -> Result<Option<coven_protocol::store_commit::StoreRootRef>, DbError> {
        self.connection
            .call_store(|session| {
                session
                    .root_authority()
                    .map(|authority| authority.map(|(reference, _)| reference))
            })
            .await
    }

    pub async fn validated_store_owner(
        &self,
        expected_root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<String, DbError> {
        let expected_root = expected_root.clone();
        self.connection
            .call_store(move |session| session.validated_store_owner(expected_root))
            .await
    }

    pub async fn install_store_owner_anchor(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        root_bytes: Vec<u8>,
        founder_reference: StoreDeviceRegistrationRef,
        founder: StoreDeviceRegistration,
        founder_bytes: Vec<u8>,
        genesis: ResolvedStoreDeviceState,
        owner: String,
        membership: InitialStoreMembershipAuthority,
    ) -> Result<(), DbError> {
        if founder.author_pubkey != owner {
            return Err(DbError::Message(
                "Store founder registration author differs from the owner anchor".to_string(),
            ));
        }
        self.connection
            .call_store(move |session| {
                session.install_store_owner_anchor(
                    root,
                    root_bytes,
                    founder_reference,
                    founder,
                    founder_bytes,
                    genesis,
                    owner,
                    membership,
                )
            })
            .await
    }

    pub async fn local_store_founder_graph(
        &self,
    ) -> Result<Option<Box<DurableFounderGraph>>, DbError> {
        self.connection
            .call_store(|session| session.local_store_founder_graph())
            .await
    }

    pub async fn stage_store_founder_graph(
        &self,
        graph: Box<DurableFounderGraph>,
    ) -> Result<(), DbError> {
        graph.validate()?;
        self.connection
            .call_store(move |session| session.stage_store_founder_graph(graph))
            .await
    }

    pub async fn complete_store_founder_graph(
        &self,
        expected_root: coven_protocol::store_commit::StoreRootRef,
        expected_registration: StoreDeviceRegistrationRef,
        expected_initial_ack: StoreAckRef,
        expected_membership: FounderMembershipRefs,
    ) -> Result<(), DbError> {
        self.connection
            .call_store(move |session| {
                session.complete_store_founder_graph(
                    expected_root,
                    expected_registration,
                    expected_initial_ack,
                    expected_membership,
                )
            })
            .await
    }

    pub async fn reset_store_founder_graph_publication(
        &self,
        expected: &DurableFounderGraph,
    ) -> Result<(), DbError> {
        expected.validate()?;
        let expected_identity = founder_graph_identity(expected);
        self.connection
            .call_store(move |session| {
                session.reset_store_founder_graph_publication(expected_identity)
            })
            .await
    }
}
