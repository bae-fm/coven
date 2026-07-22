use super::*;

pub(super) async fn apply_serial_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    application: &SerialApplicationCandidate,
    root: &StoreRootRef,
    identity: Option<&crate::keys::UserKeypair>,
) -> Result<Vec<RowChange>, StorePullError> {
    let candidate = &application.candidate;
    let device_operations = application.device_operations.clone();
    let verified_prefix = VerifiedStreamActivationPrefix::empty();
    let verified_circle_activations = match Box::pin(load_circle_payload_activations(
        db,
        storage,
        root,
        &candidate.commit_ref,
        &candidate.commit,
        &candidate.author,
        identity,
        &CircleMembershipAuthority::Serial(application.membership_authority.clone()),
        &verified_prefix,
    ))
    .await
    {
        Ok(activations) => activations,
        Err(PullCircleActivationError::Database(error)) => return Err(error.into()),
        Err(PullCircleActivationError::Invalid(error)) => {
            return Err(StorePullError::Serial(error));
        }
    };
    let no_prior_circle_accesses = CirclePackageAccesses::new();
    let prepared = prepare_serial_candidate(
        db,
        storage,
        store_dir,
        schema.clone(),
        candidate,
        verified_circle_activations.circles(),
        &no_prior_circle_accesses,
    )
    .await?;
    let resolution = SerialResolutionCommit {
        commit: candidate.commit.clone(),
        commit_ref: candidate.commit_ref.clone(),
        packages: prepared.packages,
        changesets: prepared.changesets,
        registrations: candidate.registrations.clone(),
        verified_circle_activations,
        device_operations,
        authorization_after: application.authorization_after.clone(),
    };
    let blob_decls = db.blob_decls();
    let gates = db.gates();
    let synced_tables = db.synced_tables().to_vec();
    let apply_schema = schema.clone();
    let returned_changes = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let changes = apply_prepared_serial_commit_on(
                &tx,
                apply_schema,
                &gates,
                &synced_tables,
                &blob_decls,
                &resolution,
            )?;
            tx.commit().map_err(DbError::from)?;
            Ok(changes)
        })
        .await?;
    let mut changeset_max = None;
    advance_max_updated_at(
        &mut changeset_max,
        &returned_changes,
        &schema,
        db.receive_wall_ms(),
    );
    if let Some(max_applied) = changeset_max.as_ref() {
        db.hlc().advance_past(max_applied);
    }
    Ok(returned_changes)
}

pub(crate) fn apply_prepared_serial_commit_on(
    conn: &rusqlite::Connection,
    schema: Arc<TableSchema>,
    gates: &super::gate::Gates,
    synced_tables: &[SyncedTable],
    blob_decls: &BlobDecls,
    resolution: &SerialResolutionCommit,
) -> Result<Vec<RowChange>, DbError> {
    let deletes = ValidatedChangeset::new(resolution.changesets.deletes.as_slice(), schema.clone())
        .map_err(|error| DbError::Message(format!("invalid Serial deletes: {error}")))?;
    let writes = ValidatedChangeset::new(resolution.changesets.writes.as_slice(), schema.clone())
        .map_err(|error| DbError::Message(format!("invalid Serial writes: {error}")))?;
    let mut materialization_session =
        rusqlite::session::Session::new(conn).map_err(DbError::from)?;
    for table in synced_tables {
        materialization_session
            .attach(Some(table.name()))
            .map_err(DbError::from)?;
    }
    for package in &resolution.packages {
        super::gate::validate_serial_visibility_deletes(
            conn,
            gates,
            package.changeset(),
            &package_audience(package.audience()),
        )
        .map_err(|error| {
            DbError::Message(format!("validate Serial visibility removal: {error}"))
        })?;
    }
    apply_serial_visibility_deletes_on(conn, deletes).map_err(|error| {
        DbError::Message(format!(
            "apply Serial commit {} visibility removals: {error}",
            resolution.commit_ref.coord.sequence()
        ))
    })?;
    if !writes.bytes().is_empty() {
        apply_changeset_strict_on(conn, writes).map_err(|error| {
            DbError::Message(format!(
                "Serial commit {} did not apply exactly: {error}",
                resolution.commit_ref.coord.sequence()
            ))
        })?;
    }
    Database::record_activated_store_device_registrations_on(
        conn,
        &resolution.commit,
        &resolution.registrations,
    )
    .map_err(|error| DbError::Message(format!("record Serial registrations: {error}")))?;
    Database::record_verified_circle_activations_on(
        conn,
        &resolution.commit,
        &resolution.commit_ref,
        resolution.verified_circle_activations.circles(),
    )
    .map_err(|error| DbError::Message(format!("record Serial Circle controls: {error}")))?;
    for package in &resolution.packages {
        let expected_audience = package_audience(package.audience());
        let winning_rows = crate::sync::apply::current_winning_rows_with_schema(
            conn,
            &schema,
            package.changeset(),
        )?;
        for winner in winning_rows
            .iter()
            .filter(|winner| winner.row_stamp.is_some())
        {
            let live = super::gate::live_row_audience(conn, gates, &winner.table, &winner.row_id)
                .map_err(|error| {
                DbError::Message(format!(
                    "resolve Serial package row audience for {}.{}: {error}",
                    winner.table, winner.row_id
                ))
            })?;
            if live != expected_audience {
                return Err(DbError::Message(format!(
                    "Serial {:?} package cannot write {}.{} into {:?}",
                    expected_audience, winner.table, winner.row_id, live
                )));
            }
        }
    }
    let inactive_circles = resolution
        .verified_circle_activations
        .circles()
        .iter()
        .filter_map(|activation| {
            activation
                .local_access
                .as_ref()
                .filter(|access| access.active.is_none())
                .map(|_| activation.circle_id)
        })
        .collect::<BTreeSet<_>>();
    super::gate::prune_inactive_serial_circles(conn, gates, &inactive_circles)
        .map_err(|error| DbError::Message(format!("prune inactive Serial Circles: {error}")))?;
    let mut materialized_changeset = Vec::new();
    materialization_session
        .changeset_strm(&mut materialized_changeset)
        .map_err(DbError::from)?;
    drop(materialization_session);
    let old_changes =
        crate::changeset::walk_old(&materialized_changeset).map_err(DbError::Message)?;
    let changes = crate::changeset::walk(&materialized_changeset).map_err(DbError::Message)?;
    for intent in local_blob_cleanup_intents(blob_decls, &old_changes, &changes)
        .map_err(|error| DbError::Message(error.to_string()))?
    {
        local_cleanup::record_obsolete_copy_intents_on(conn, blob_decls, &intent)?;
    }
    for package in &resolution.packages {
        let winning_rows = crate::sync::apply::current_winning_rows_with_schema(
            conn,
            &schema,
            package.changeset(),
        )?;
        Database::install_pulled_package_activation_on(
            conn,
            &resolution.commit,
            &resolution.commit_ref,
            package,
        )
        .map_err(|error| DbError::Message(format!("record Serial package activation: {error}")))?;
        Database::install_pulled_blob_activations_on(conn, package, &resolution.commit_ref)
            .map_err(|error| {
                DbError::Message(format!("record Serial blob activations: {error}"))
            })?;
        Database::install_winning_blob_bindings_on(
            conn,
            gates,
            synced_tables,
            package,
            &BlobActivation {
                coord: resolution.commit_ref.coord.clone(),
            },
            &winning_rows,
        )
        .map_err(|error| DbError::Message(format!("record Serial blob bindings: {error}")))?;
    }
    Database::record_materialized_serial_commit_with_device_operations_on(
        conn,
        &resolution.commit,
        &resolution.commit_ref,
        &resolution.authorization_after,
        &resolution.device_operations,
        resolution.verified_circle_activations.stream_activations(),
    )
    .map_err(|error| DbError::Message(format!("record Serial commit position: {error}")))?;
    Ok(changes)
}

fn package_audience(audience: &PackageAudience) -> super::circle::Audience {
    match audience {
        PackageAudience::Store => super::circle::Audience::Store,
        PackageAudience::Circle { circle_id, .. } => super::circle::Audience::Circle(*circle_id),
    }
}

fn apply_serial_visibility_deletes_on<B: AsRef<[u8]>>(
    conn: &rusqlite::Connection,
    changeset: ValidatedChangeset<B>,
) -> Result<(), DbError> {
    if changeset.bytes().is_empty() {
        return Ok(());
    }
    if crate::changeset::walk(changeset.bytes())
        .map_err(DbError::Message)?
        .iter()
        .any(|change| change.op != crate::changeset::ChangeOp::Delete)
    {
        return Err(DbError::Message(
            "Serial visibility removal contains a non-delete operation".to_string(),
        ));
    }
    let bytes = changeset.bytes();
    conn.apply_strm(
        &mut &bytes[..],
        None::<fn(&str) -> bool>,
        |conflict, _item| match conflict {
            ConflictType::SQLITE_CHANGESET_DATA => ConflictAction::SQLITE_CHANGESET_REPLACE,
            ConflictType::SQLITE_CHANGESET_NOTFOUND => ConflictAction::SQLITE_CHANGESET_OMIT,
            _ => ConflictAction::SQLITE_CHANGESET_ABORT,
        },
    )
    .map_err(DbError::from)
}

pub(crate) struct PreparedSerialCandidate {
    pub(crate) packages: Vec<AudiencePackage>,
    pub(crate) changesets: super::gate::SerialInboundChangesets,
}

pub(crate) async fn prepare_serial_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    candidate: &Candidate,
    circle_activations: &[super::circle_ops::VerifiedCircleReference],
    prior_circle_accesses: &CirclePackageAccesses,
) -> Result<PreparedSerialCandidate, StorePullError> {
    let mut packages = Vec::<(AudiencePackage, BlobSpoolProtection)>::new();
    if let Some(package_bytes) = candidate.package.as_ref() {
        let package = parse_candidate_store_package(candidate, package_bytes)
            .map_err(StorePullError::Serial)?;
        packages.push((package, storage.store_blob_protection()?));
    }
    let circle_packages = load_applicable_circle_packages_with_prior_accesses(
        db,
        storage,
        &candidate.commit_ref,
        &candidate.commit,
        circle_activations,
        &candidate.author,
        prior_circle_accesses,
    )
    .await
    .map_err(|error| match error {
        PullCircleActivationError::Database(error) => StorePullError::Database(error.to_string()),
        PullCircleActivationError::Invalid(error) => StorePullError::Serial(error),
    })?;
    for loaded in circle_packages {
        let package =
            parse_candidate_circle_package(candidate, &loaded).map_err(StorePullError::Serial)?;
        packages.push((package, loaded.blob_protection));
    }

    let blob_decls = db.blob_decls();
    for (package, protection) in &packages {
        let validated = ValidatedChangeset::new(package.changeset(), schema.clone())
            .map_err(|error| StorePullError::Serial(format!("invalid changeset: {error}")))?;
        let changes = crate::changeset::walk(validated.bytes())
            .map_err(|error| StorePullError::Serial(format!("invalid changeset: {error}")))?;
        let eager = cache_eager_blobs(&blob_decls, &changes, package)
            .map_err(|error| StorePullError::Serial(format!("invalid blob changes: {error}")))?;
        verify_package_blobs(
            db,
            storage,
            store_dir,
            package.blob_bindings(),
            protection.clone(),
            &eager,
        )
        .await
        .map_err(StorePullError::BlobDownloads)?;
    }
    let package_changesets = packages
        .iter()
        .map(|(package, _)| package.changeset().to_vec())
        .collect::<Vec<_>>();
    let changesets = db
        .call(move |conn| {
            let changesets = package_changesets
                .iter()
                .map(Vec::as_slice)
                .collect::<Vec<_>>();
            super::gate::combine_serial_inbound_changesets(conn, &changesets)
                .map_err(|error| DbError::Message(error.to_string()))
        })
        .await?;
    ValidatedChangeset::new(changesets.deletes.as_slice(), schema.clone())
        .map_err(|error| StorePullError::Serial(format!("invalid Serial deletes: {error}")))?;
    ValidatedChangeset::new(changesets.writes.as_slice(), schema)
        .map_err(|error| StorePullError::Serial(format!("invalid Serial writes: {error}")))?;
    Ok(PreparedSerialCandidate {
        packages: packages.into_iter().map(|(package, _)| package).collect(),
        changesets,
    })
}
