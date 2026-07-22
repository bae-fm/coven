use super::*;

impl Database {
    pub async fn serial_membership_state(&self) -> Result<Option<SerialMembershipState>, DbError> {
        let Some(raw) = self.get_protocol_state(SERIAL_MEMBERSHIP_STATE_KEY).await? else {
            return Ok(None);
        };
        serde_json::from_str(&raw)
            .map(Some)
            .map_err(|error| DbError::Message(format!("parse Serial membership state: {error}")))
    }

    pub async fn serial_authorization_state(
        &self,
    ) -> Result<Option<SerialAuthorizationState>, DbError> {
        let membership = self.get_protocol_state(SERIAL_MEMBERSHIP_STATE_KEY).await?;
        let provider_admin = self
            .get_protocol_state(SERIAL_PROVIDER_ADMIN_STATE_KEY)
            .await?;
        let key_generation = self
            .get_protocol_state(SERIAL_KEY_GENERATION_STATE_KEY)
            .await?;
        let wrapped_keys = self
            .get_protocol_state(SERIAL_WRAPPED_KEYS_STATE_KEY)
            .await?;
        match (membership, provider_admin, key_generation, wrapped_keys) {
            (None, None, None, None) => Ok(None),
            (Some(membership), Some(provider_admin), Some(key_generation), Some(wrapped_keys)) => {
                let membership = serde_json::from_str(&membership).map_err(|error| {
                    DbError::Message(format!("parse Serial membership state: {error}"))
                })?;
                let provider_admin: ProviderAdminState = serde_json::from_str(&provider_admin)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "parse Serial provider administrator state: {error}"
                        ))
                    })?;
                let key_generation = key_generation.parse::<u64>().map_err(|error| {
                    DbError::Message(format!("parse Serial key generation: {error}"))
                })?;
                let active_wrapped_keys = serde_json::from_str(&wrapped_keys).map_err(|error| {
                    DbError::Message(format!("parse Serial wrapped keys: {error}"))
                })?;
                Ok(Some(SerialAuthorizationState {
                    membership,
                    provider_admin,
                    key_generation,
                    active_wrapped_keys,
                }))
            }
            _ => Err(DbError::Message(
                "Serial authorization state is only partially durable".to_string(),
            )),
        }
    }

    pub async fn serial_key_generation(&self) -> Result<Option<u64>, DbError> {
        let Some(raw) = self
            .get_protocol_state(SERIAL_KEY_GENERATION_STATE_KEY)
            .await?
        else {
            return Ok(None);
        };
        raw.parse::<u64>()
            .map(Some)
            .map_err(|error| DbError::Message(format!("parse Serial key generation: {error}")))
    }

    pub(super) fn install_serial_root_authorization_on(
        conn: &Connection,
        authorization: &SerialAuthorizationState,
    ) -> Result<(), DbError> {
        if Self::latest_position_for_device_on(conn, SERIAL_STREAM_ID)?.is_some() {
            return Err(DbError::Message(
                "cannot install founder-only Serial authorization after a materialized commit"
                    .to_string(),
            ));
        }
        let existing_state: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM protocol_state WHERE key IN (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    SERIAL_MEMBERSHIP_STATE_KEY,
                    SERIAL_KEY_GENERATION_STATE_KEY,
                    SERIAL_PROVIDER_ADMIN_STATE_KEY,
                    SERIAL_WRAPPED_KEYS_STATE_KEY,
                ],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if existing_state != 0 {
            return Err(DbError::Message(
                "cannot install founder-only Serial authorization over existing state".to_string(),
            ));
        }
        let membership = serde_json::to_string(&authorization.membership).map_err(|error| {
            DbError::Message(format!(
                "serialize Serial founder membership state: {error}"
            ))
        })?;
        let provider_admin =
            serde_json::to_string(&authorization.provider_admin).map_err(|error| {
                DbError::Message(format!(
                    "serialize Serial founder provider administrator state: {error}"
                ))
            })?;
        let wrapped_keys =
            serde_json::to_string(&authorization.active_wrapped_keys).map_err(|error| {
                DbError::Message(format!("serialize Serial founder wrapped keys: {error}"))
            })?;
        for (key, value) in [
            (SERIAL_MEMBERSHIP_STATE_KEY, membership),
            (SERIAL_PROVIDER_ADMIN_STATE_KEY, provider_admin),
            (
                SERIAL_KEY_GENERATION_STATE_KEY,
                authorization.key_generation.to_string(),
            ),
            (SERIAL_WRAPPED_KEYS_STATE_KEY, wrapped_keys),
        ] {
            conn.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    pub(crate) async fn install_serial_authorization_at_position(
        &self,
        expected: StoreBatchCommitRef,
        authorization: SerialAuthorizationState,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let actual = Self::latest_position_for_device_on(&tx, SERIAL_STREAM_ID)?;
            if actual.as_ref() != Some(&expected) {
                return Err(DbError::Message(format!(
                    "cannot install Serial authorization at {expected:?}; durable position is {actual:?}"
                )));
            }
            let membership = serde_json::to_string(&authorization.membership).map_err(|error| {
                DbError::Message(format!("serialize Serial membership state: {error}"))
            })?;
            let provider_admin = serde_json::to_string(&authorization.provider_admin).map_err(
                |error| {
                    DbError::Message(format!(
                        "serialize Serial provider administrator state: {error}"
                    ))
                },
            )?;
            let wrapped_keys = serde_json::to_string(&authorization.active_wrapped_keys)
                .map_err(|error| {
                    DbError::Message(format!("serialize Serial wrapped keys: {error}"))
                })?;
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (SERIAL_MEMBERSHIP_STATE_KEY, membership),
            )
            .map_err(DbError::from)?;
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (SERIAL_WRAPPED_KEYS_STATE_KEY, wrapped_keys),
            )
            .map_err(DbError::from)?;
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (SERIAL_PROVIDER_ADMIN_STATE_KEY, provider_admin),
            )
            .map_err(DbError::from)?;
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (
                    SERIAL_KEY_GENERATION_STATE_KEY,
                    authorization.key_generation.to_string(),
                ),
            )
            .map_err(DbError::from)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn install_device_join_bootstrap(
        &self,
        root: crate::sync::store_commit::StoreRootRef,
        plan: crate::sync::store_pull::DeviceJoinBootstrapPlan,
    ) -> Result<(), DbError> {
        if plan.coverage.policy() != self.write_policy() {
            return Err(DbError::Message(
                "device join bootstrap cut differs from the database write policy".to_string(),
            ));
        }
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let installed_root = required_store_root_authority_on(&tx)?;
            if installed_root != root || plan.founder.store_root != root {
                return Err(DbError::Message(
                    "device join bootstrap root differs from the installed exact root".to_string(),
                ));
            }
            install_store_founder_state_on(
                &tx,
                &root,
                &plan.founder_reference,
                &plan.founder,
                &plan.founder_bytes,
                &plan.genesis,
            )?;

            let frontier = Self::materialized_frontier_on(&tx, None)?;
            let mut represented = BTreeSet::new();
            for prepared in &plan.commits {
                let stream_id = match &prepared.reference.coord {
                    StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
                    StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
                };
                let sequence = prepared.reference.coord.sequence();
                if let Some(existing) =
                    Self::materialized_commit_ref_on(&tx, &stream_id, sequence)?
                {
                    if existing != prepared.reference {
                        return Err(DbError::Message(format!(
                            "device join bootstrap conflicts at {stream_id}/{sequence}"
                        )));
                    }
                    represented.insert(prepared.reference.clone());
                    continue;
                }
                let encoded = serde_json::to_string(&prepared.reference).map_err(|error| {
                    DbError::Message(format!(
                        "serialize device join bootstrap commit ref: {error}"
                    ))
                })?;
                let has_snapshot_state = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM store_device_state_snapshots
                         WHERE commit_ref = ?1)",
                        [&encoded],
                        |row| row.get::<_, bool>(0),
                    )
                    .map_err(DbError::from)?;
                let covered = frontier.get(&stream_id).is_some_and(|tip| {
                    sequence <= tip.coord.sequence()
                });
                if has_snapshot_state && covered {
                    represented.insert(prepared.reference.clone());
                }
            }

            for prepared in &plan.commits {
                let stream_id = match &prepared.reference.coord {
                    StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
                    StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
                };
                let already_represented = represented.contains(&prepared.reference);
                if !already_represented
                    && (prepared.commit.store_package().is_some()
                        || !prepared.commit.circle_packages().is_empty())
                {
                    return Err(DbError::Message(format!(
                        "device join bootstrap cannot advance over unmaterialized row data at {stream_id}/{}",
                        prepared.reference.coord.sequence()
                    )));
                }
            }

            for prepared in plan.commits {
                let stream_id = match &prepared.reference.coord {
                    StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
                    StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
                };
                if represented.contains(&prepared.reference) {
                    continue;
                }
                if let Some(existing) = Self::materialized_commit_ref_on(
                    &tx,
                    &stream_id,
                    prepared.reference.coord.sequence(),
                )? {
                    if existing != prepared.reference {
                        return Err(DbError::Message(format!(
                            "device join bootstrap conflicts at {stream_id}/{}",
                            prepared.reference.coord.sequence()
                        )));
                    }
                    continue;
                }
                Self::record_activated_store_device_registrations_on(
                    &tx,
                    &prepared.commit,
                    &prepared.registrations,
                )?;
                let circle_activations =
                    VerifiedCircleActivations::none(&prepared.commit, &prepared.reference)
                        .map_err(|error| DbError::Message(error.to_string()))?;
                match (&prepared.reference.coord, &prepared.activation) {
                    (
                        StoreCommitCoord::MergeConcurrent { .. },
                        crate::sync::store_pull::DeviceJoinBootstrapActivation::MergeConcurrent {
                            head,
                            object,
                            history_summary,
                        },
                    ) => {
                        let materialization = VerifiedMergeMaterialization::verify(
                            &root,
                            &prepared.commit,
                            &prepared.reference,
                            &prepared.registrations,
                            &prepared.device_operations,
                            &circle_activations,
                            head,
                            object,
                            history_summary,
                            None,
                            &[],
                            None,
                        )?;
                        Self::record_verified_merge_materialization_on(&tx, materialization)?;
                    }
                    (
                        StoreCommitCoord::Serial { .. },
                        crate::sync::store_pull::DeviceJoinBootstrapActivation::Serial,
                    ) => Self::record_materialized_commit_with_device_operations_on(
                        &tx,
                        &prepared.commit,
                        &prepared.reference,
                        &prepared.device_operations,
                        circle_activations.stream_activations(),
                        MaterializedCommitRetention::Serial,
                        &ReclaimCommitActivation::serial(prepared.reference.clone())
                            .map_err(store_reclaim_journal_error)?,
                    )?,
                    _ => {
                        return Err(DbError::Message(
                            "device join bootstrap activation evidence differs from commit policy"
                                .to_string(),
                        ));
                    }
                }
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }
}
