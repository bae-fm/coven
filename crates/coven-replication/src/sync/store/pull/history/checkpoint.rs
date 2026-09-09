use super::*;

impl PullHistory<'_, '_> {
    pub(crate) async fn prepare_checkpoint(
        &mut self,
        expected: coven_database::StorePublicationBoundary,
        selected: SelectedStoreSnapshot,
        membership: &MembershipChain,
        identity: Option<&UserKeypair>,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<(), StorePullError> {
        let PullDatabase::Installed(receiver) = &self.database else {
            return Err(StorePullError::InvalidState(
                "pull already owns a received checkpoint preparation".into(),
            ));
        };
        let snapshot = &selected.snapshot;
        let plaintext = self.history.load_store_snapshot_image(snapshot).await?;
        let founder = self.history.load_founder_registration().await?;
        let install = coven_database::VerifiedSnapshotBootstrapInstall::new(
            snapshot.clone(),
            self.history.verified_root().object().clone(),
            founder,
            selected.verified,
            coven_database::InitialStoreMembershipAuthority {
                head_refs: snapshot.meta.state.membership.heads.clone(),
            },
            routing_encryption,
        )?;
        let receiver = receiver.clone();
        let database = receiver
            .prepare_snapshot_database(plaintext, install)
            .await
            .map_err(|error| {
                StorePullError::SnapshotRestoration(Box::new(
                    crate::sync::store::snapshots::SnapshotError::DatabaseOpen(error),
                ))
            })?;
        let receiver_wall_ms = receiver.receive_wall_ms();
        self.database = PullDatabase::Checkpoint {
            receiver,
            database,
            expected,
        };
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;
        let local = LocalStoreMembership::from_membership(membership, identity)?;
        let circles = if local.allows_circle_access() {
            let identity = identity.ok_or_else(|| {
                StorePullError::InvalidState(
                    "recipient Circle restoration has no receiving identity".into(),
                )
            })?;
            crate::sync::store::snapshots::CircleSnapshotReader::new(
                database,
                self.storage,
                self.history,
            )
            .select_staged_installs(&snapshot.meta.coverage, identity, routing_key, local)
            .await
            .map_err(|error| StorePullError::SnapshotRestoration(Box::new(error)))?
        } else {
            coven_database::StagedCircleRestore {
                access: Vec::new(),
                bases: Vec::new(),
                packages: None,
            }
        };
        database
            .prepare_received_snapshot_circles(circles, receiver_wall_ms)
            .await?;
        Ok(())
    }

    pub(crate) async fn finish_checkpoint_preparation<T>(
        &mut self,
        outcome: Result<T, StorePullError>,
    ) -> Result<T, StorePullError> {
        let receiver = match &self.database {
            PullDatabase::Installed(database)
            | PullDatabase::Checkpoint {
                receiver: database, ..
            } => database.clone(),
        };
        let database = std::mem::replace(&mut self.database, PullDatabase::Installed(receiver));
        let cleanup = match database {
            PullDatabase::Installed(_) => Ok(()),
            PullDatabase::Checkpoint { database, .. } => {
                database.discard_snapshot_preparation().await
            }
        };
        match (outcome, cleanup) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error.into()),
            (Err(operation), Err(cleanup)) => Err(StorePullError::SnapshotPreparationCleanup {
                operation: Box::new(operation),
                cleanup,
            }),
        }
    }
}
