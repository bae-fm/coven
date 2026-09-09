use super::*;

/// The whole admission runs through the transport: neither driver is handed an
/// artifact, and the joining device ends with a saved member config.
#[test]
fn transport_carries_a_whole_join_between_two_drivers() {
    on_a_deep_stack(run_transport_carries_a_whole_join_between_two_drivers);
}

async fn run_transport_carries_a_whole_join_between_two_drivers() {
    let fixture = TransportFixture::build("device-join-transport-happy-path").await;
    let bundle = fixture.begin().await;
    // An object under the attempt's namespace at a name this build does not
    // know: a future protocol's artifact, or anything else a writer with
    // provider access can put there. Tearing the namespace down means this goes
    // too, and a teardown that asks for the names it was compiled with cannot
    // see it.
    let stray = fixture.home.insert_exact_object(
        &format!(
            "{}/from-another-version.json",
            bundle.transport.attempt_namespace
        ),
        b"an artifact this build has no kind for".to_vec(),
    );
    assert!(fixture.home.get(stray.logical_key()).is_some());
    fixture.home.clear_exact_creates();

    let joiner = fixture.client();
    let cancel = never_cancelled();
    let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_progress = Arc::clone(&progress);
    let observe_progress: coven_replication::sync::JoiningDeviceJoinProgressObserver =
        Arc::new(move |phase| observed_progress.lock().expect("progress lock").push(phase));
    let owner_progress = std::sync::Mutex::new(Vec::new());
    let observe_owner_progress = |phase| {
        owner_progress
            .lock()
            .expect("owner progress lock")
            .push(phase)
    };
    let (config, activation) = tokio::join!(
        Box::pin(joiner.join_via_transport(&bundle, timing(), observe_progress, &cancel,)),
        Box::pin(fixture.drive_owner_observing(&bundle, timing(), &observe_owner_progress,)),
    );
    let activation = activated(activation);
    let config = joined(config);

    let registration_prefix =
        coven_protocol::store_commit::registration_semantic_prefix(&config.device_id);
    assert_eq!(
        fixture
            .home
            .exact_creates()
            .iter()
            .filter(|slot| slot.logical_key().starts_with(&registration_prefix))
            .count(),
        1,
        "the owner publishes the joining registration once; the joiner publishes only its acknowledgement",
    );

    let transport_creates = fixture
        .home
        .exact_creates()
        .into_iter()
        .filter(|slot| slot.logical_key().contains("device-join-transport"))
        .collect::<Vec<_>>();
    // A join publishes no proof of itself. The attempt and the outcome used to
    // be signed files whose every field was checked against the commit that
    // named them — the same device signing the same facts twice.
    let written = fixture
        .home
        .exact_creates()
        .into_iter()
        .map(|slot| slot.logical_key().to_string())
        .collect::<Vec<_>>();
    for class in [
        "store-v1/device-join-attempts/",
        "store-v1/device-join-outcomes/",
    ] {
        assert!(
            written.iter().all(|key| !key.starts_with(class)),
            "the join wrote a {class} object, which nothing reads: {written:?}",
        );
    }

    // One device admits, so the exchange has no handoff to carry between an
    // owner machine and a storage-administrator machine: the attempt's
    // namespace holds exactly what crosses between the two devices.
    let created_kinds = transport_creates
        .iter()
        .map(|slot| {
            slot.logical_key()
                .rsplit('/')
                .next()
                .expect("a transport slot key ends in its kind")
                .to_string()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        created_kinds,
        std::collections::BTreeSet::from([
            "provider-access-request.json".to_string(),
            "same-principal-join.json".to_string(),
        ]),
        "a same-provider join creates only the artifacts that cross between the two devices",
    );
    for kind in ["provider-access-request", "same-principal-join"] {
        assert_eq!(
            transport_creates
                .iter()
                .filter(|slot| { slot.logical_key().ends_with(&format!("/{kind}.json")) })
                .count(),
            1,
            "the uninterrupted join attempts one create for {kind}",
        );
    }
    {
        let progress = progress.lock().expect("progress lock");
        assert!(progress.contains(
            &coven_replication::sync::JoiningDeviceJoinProgress::RequestingProviderAccess
        ));
        assert!(progress
            .contains(&coven_replication::sync::JoiningDeviceJoinProgress::WaitingForLibrary));
        assert!(progress.iter().any(|phase| matches!(
            phase,
            coven_replication::sync::JoiningDeviceJoinProgress::DownloadingSnapshot {
                bytes_done: 0,
                bytes_total
            } if *bytes_total > 0
        )));
        assert!(progress.iter().any(|phase| matches!(
            phase,
            coven_replication::sync::JoiningDeviceJoinProgress::DownloadingSnapshot {
                bytes_done,
                bytes_total
            } if bytes_done == bytes_total && *bytes_total > 0
        )));
        assert!(!progress
            .contains(&coven_replication::sync::JoiningDeviceJoinProgress::WaitingForActivation));
        assert!(!progress.contains(&coven_replication::sync::JoiningDeviceJoinProgress::CatchingUp));
        assert!(
            progress.contains(&coven_replication::sync::JoiningDeviceJoinProgress::SavingLibrary)
        );
    }
    let owner_progress = owner_progress.into_inner().expect("owner progress lock");
    assert!(owner_progress.contains(
        &coven_replication::sync::AdmittingDeviceJoinProgress::WaitingForProviderAccessRequest
    ));
    assert!(owner_progress
        .contains(&coven_replication::sync::AdmittingDeviceJoinProgress::RegisteringDevice));
    assert!(!owner_progress
        .contains(&coven_replication::sync::AdmittingDeviceJoinProgress::GrantingProviderAccess));
    assert!(!owner_progress
        .contains(&coven_replication::sync::AdmittingDeviceJoinProgress::PreparingLibrary));
    assert!(!owner_progress
        .contains(&coven_replication::sync::AdmittingDeviceJoinProgress::ActivatingDevice));

    assert!(fixture
        .layout
        .store_dir(&config.store_id)
        .config_path()
        .exists());
    // A finished join leaves no journal row on either device: the library's
    // config file is what says it finished, and the rows were working notes on
    // an exchange that is over.
    assert!(
        fixture
            .pending_journal_records()
            .iter()
            .all(|record| record.attempt_id != bundle.offer.attempt_id),
        "the joining device kept a journal row for a join it finished",
    );
    assert_eq!(activation.attempt_id, bundle.offer.attempt_id,);
    assert!(fixture
        .client()
        .resume_device_joins()
        .expect("enumerate completed joins")
        .is_empty());
    // The joiner's completion is the point every artifact has been consumed,
    // so the attempt's namespace is empty again — all of it, not only the names
    // this version of the protocol happens to know.
    assert!(
        fixture.attempt_namespace_keys(&bundle).is_empty(),
        "the completed join left objects in its attempt namespace: {:?}",
        fixture.attempt_namespace_keys(&bundle),
    );
}

/// The bundle is what the host encodes as its join code, so it has to survive
/// the trip out and back: the joining device rebuilds it from bytes alone and
/// reads the same slots and seal key the owner allocated.
#[tokio::test]
async fn the_offer_bundle_round_trips_through_its_encoded_form() {
    tokio::spawn(async {
        let fixture = TransportFixture::build("device-join-transport-bundle").await;
        fixture
            .home
            .delay_exact_slot_allocations(Duration::from_millis(10));
        let bundle = fixture.begin().await;
        assert!(
            fixture.home.exact_slot_allocation_max_inflight() > 1,
            "independent transport slots are allocated concurrently",
        );

        let decoded = DeviceJoinOfferBundle::from_bytes(&bundle.to_bytes())
            .expect("a bundle the owner minted decodes");
        assert_eq!(decoded.offer, bundle.offer);
        assert_eq!(
            decoded.transport.attempt_namespace,
            bundle.transport.attempt_namespace
        );
        assert_eq!(decoded.transport.slots, bundle.transport.slots);

        // The decoded seal key opens what the original sealed, which is the only
        // property the joining device needs from it.
        let joiner = fixture.client();
        let request = joiner
            .prepare_provider_access_request(bundle.offer.clone())
            .await
            .expect("prepare provider access request");
        let joiner_storage = joiner
            .transport_storage()
            .await
            .expect("joining device transport storage");
        DeviceJoinTransport::open(&joiner_storage, &bundle, DeviceJoinRole::Joiner)
            .expect("open transport")
            .publish(&DeviceJoinAction::TransferProviderAccessRequest(
                request.clone(),
            ))
            .await
            .expect("publish the access request");
        let read_back =
            DeviceJoinTransport::open(&joiner_storage, &decoded, DeviceJoinRole::Joiner)
                .expect("open the decoded transport")
                .read(DeviceJoinTransportKind::ProviderAccessRequest)
                .await
                .expect("read through the decoded bundle");
        assert_eq!(
            read_back,
            Some(DeviceJoinAction::TransferProviderAccessRequest(request)),
        );

        // Bytes that are not a bundle are refused rather than half-decoded.
        assert!(DeviceJoinOfferBundle::from_bytes(b"{}").is_err());
    })
    .await
    .expect("offer bundle task");
}

#[tokio::test]
async fn the_scanned_invitation_exposes_provider_credentials_only_to_its_requesting_device() {
    let fixture = TransportFixture::build("sealed-device-invitation").await;
    let bundle = fixture.begin().await;
    let mut admission = fixture.admission.clone();
    admission.join_info = coven_storage::CloudHomeJoinInfo::S3 {
        bucket: "sealed-bucket".to_string(),
        region: "sealed-region".to_string(),
        endpoint: Some("https://sealed.example".to_string()),
        access_key: "ACCESS-KEY-MUST-STAY-SEALED".to_string(),
        secret_key: "SECRET-KEY-MUST-STAY-SEALED".to_string(),
        key_prefix: Some("sealed-prefix".to_string()),
    };
    let invite = crate::joining::DeviceJoinInvite::new(admission, bundle)
        .expect("seal the invitation for its requesting device");

    let wire = invite.to_bytes();
    let visible = String::from_utf8(wire.clone()).expect("device invitation is JSON");
    for secret in [
        "ACCESS-KEY-MUST-STAY-SEALED",
        "SECRET-KEY-MUST-STAY-SEALED",
        "sealed-bucket",
        "sealed-region",
        "sealed-prefix",
    ] {
        assert!(!visible.contains(secret), "wire exposed {secret}");
    }

    let decoded = crate::joining::DeviceJoinInvite::from_bytes(&wire)
        .expect("decode the sealed invitation wire");
    assert_eq!(
        decoded
            .open_admission(&fixture.member_pubkey)
            .expect("requesting device opens the invitation")
            .store_id,
        "sealed-device-invitation",
    );
    let other_identity =
        coven_keys::keys::mint_pending_identity().expect("mint unrelated pending identity");
    let other_pubkey = coven_keys::keys::public_key_hex(&other_identity);
    assert!(matches!(
        decoded.open_admission(&other_pubkey),
        Err(crate::joining::DeviceInviteError::RecipientMismatch)
    ));
}

/// The same admission when the joining device is on a different provider
/// account than the owner: the protocol adds its cross-principal probe, and the
/// transport carries the larger artifacts without knowing they grew.
#[test]
fn transport_carries_a_cross_principal_join() {
    on_a_deep_stack(run_transport_carries_a_cross_principal_join);
}

async fn run_transport_carries_a_cross_principal_join() {
    let fixture = TransportFixture::build_cross_principal("device-join-transport-cross").await;
    let bundle = fixture.begin().await;

    let joiner = fixture.client();
    let cancel = never_cancelled();

    // Advance far enough to read the approval off its slot, so the test proves
    // this really took the probe path rather than the same-principal one.
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel)).await,
        DeviceJoinTransportKind::ProviderAdmissionApproval,
    );
    assert_owner_waited_for(
        fixture.drive_owner_with(&bundle, one_shot()).await,
        DeviceJoinTransportKind::RegistrationRequest,
    );
    match fixture
        .transport(&bundle)
        .read(DeviceJoinTransportKind::ProviderAdmissionApproval)
        .await
        .expect("read the approval off its slot")
    {
        Some(DeviceJoinAction::TransferProviderAdmissionApproval(approval)) => assert!(
            matches!(
                approval.admission,
                coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission::CrossPrincipal { .. }
            ),
            "separate provider accounts must admit through the cross-principal probe",
        ),
        other => panic!("the approval slot holds the approval, got {other:?}"),
    }

    let finishing_joiner = fixture.client();
    let (config, activation) = tokio::join!(
        Box::pin(finishing_joiner.join_via_transport(
            &bundle,
            timing(),
            no_join_progress(),
            &cancel,
        )),
        Box::pin(fixture.drive_owner(&bundle)),
    );
    let config = joined(config);
    let activation = activated(activation);

    assert!(fixture
        .layout
        .store_dir(&config.store_id)
        .config_path()
        .exists());
    // A finished join leaves no journal row on either device: the library's
    // config file is what says it finished, and the rows were working notes on
    // an exchange that is over.
    assert!(
        fixture
            .pending_journal_records()
            .iter()
            .all(|record| record.attempt_id != bundle.offer.attempt_id),
        "the joining device kept a journal row for a join it finished",
    );
    assert_eq!(activation.attempt_id, bundle.offer.attempt_id,);
    for kind in DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "{kind:?} slot outlived the completed join",
        );
    }
}

/// Run the same-provider join one side at a time. The joining device dies after
/// publishing its request, the owner completes admission without another
/// joining-device round trip, and a fresh joining process consumes the exact
/// durable response and activation.
#[test]
fn each_side_resumes_from_every_artifact_boundary() {
    on_a_deep_stack(run_each_side_resumes_from_every_artifact_boundary);
}

async fn run_each_side_resumes_from_every_artifact_boundary() {
    let fixture = TransportFixture::build("device-join-transport-resume").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let join_once = |timing| {
        let client = fixture.client();
        let bundle = &bundle;
        let cancel = &cancel;
        async move {
            client
                .join_via_transport(bundle, timing, no_join_progress(), cancel)
                .await
        }
    };

    // The joiner publishes its access request, then dies waiting for an owner
    // that is not running.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    let access_request = fixture
        .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
        .await
        .expect("the access request survived the joiner's death");

    // The request already carries the exact registration. A same-principal
    // owner can publish both the library bootstrap and activation without
    // another response from the joining device.
    let activation = activated(fixture.drive_owner_with(&bundle, one_shot()).await);
    assert_eq!(activation.attempt_id, bundle.offer.attempt_id);
    assert!(fixture
        .slot_bytes(&bundle, DeviceJoinTransportKind::SamePrincipalJoin)
        .await
        .is_some());
    assert_eq!(
        fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
            .await
            .as_ref(),
        Some(&access_request),
        "the owner left the joining device's exact request in place",
    );
    // The joiner's restart republishes its identical request, installs the
    // library, consumes the activation, and saves the store.
    let config = joined(Box::pin(join_once(timing())).await);
    assert!(fixture
        .layout
        .store_dir(&config.store_id)
        .config_path()
        .exists());
    // A finished join leaves no journal row on either device: the library's
    // config file is what says it finished, and the rows were working notes on
    // an exchange that is over.
    assert!(
        fixture
            .pending_journal_records()
            .iter()
            .all(|record| record.attempt_id != bundle.offer.attempt_id),
        "the joining device kept a journal row for a join it finished",
    );
    for kind in DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "{kind:?} slot outlived the completed join",
        );
    }
}

/// Saving local configuration is a separate durable step after the Store
/// activation. If that filesystem write fails, a fresh process resumes from
/// the activated join journal and saves the same library without downloading
/// or installing the snapshot again.
#[test]
fn config_write_failure_after_snapshot_installation_resumes_without_another_pairing() {
    on_a_deep_stack(
        run_config_write_failure_after_snapshot_installation_resumes_without_another_pairing,
    );
}

async fn run_config_write_failure_after_snapshot_installation_resumes_without_another_pairing() {
    let fixture = TransportFixture::build("device-join-config-resume").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let first_joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(first_joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel))
            .await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    let config_path = fixture
        .layout
        .store_dir("device-join-config-resume")
        .config_path();
    let block_config_path = config_path.clone();
    let block_config_write: coven_replication::sync::JoiningDeviceJoinProgressObserver =
        Arc::new(move |progress| {
            if matches!(
                progress,
                coven_replication::sync::JoiningDeviceJoinProgress::SavingLibrary
            ) {
                std::fs::create_dir(&block_config_path)
                    .expect("occupy the config path before its atomic write");
            }
        });
    let failed = fixture
        .client()
        .join_via_transport(&bundle, timing(), block_config_write, &cancel)
        .await;
    assert!(
        matches!(failed, Err(crate::joining::BootstrapError::Config(_))),
        "the injected config write failure must reach the caller: {failed:?}",
    );
    std::fs::remove_dir(&config_path).expect("remove the config-path blocker");

    let config = joined(
        fixture
            .client()
            .join_via_transport(&bundle, timing(), no_join_progress(), &cancel)
            .await,
    );
    assert_eq!(config.store_id, "device-join-config-resume");
    assert!(config_path.is_file());
}

/// Cancelling while the snapshot bytes are arriving stops that transfer,
/// removes its staged database, and leaves the durable pairing attempt ready
/// for a fresh process to retry from the beginning of library installation.
#[test]
fn snapshot_download_cancellation_is_prompt_and_the_same_pairing_retries() {
    on_a_deep_stack(run_snapshot_download_cancellation_is_prompt_and_the_same_pairing_retries);
}

async fn run_snapshot_download_cancellation_is_prompt_and_the_same_pairing_retries() {
    let fixture = TransportFixture::build("device-join-snapshot-cancel").await;
    let bundle = fixture.begin().await;
    let initial_cancel = never_cancelled();

    assert_joiner_waited_for(
        Box::pin(fixture.client().join_via_transport(
            &bundle,
            one_shot(),
            no_join_progress(),
            &initial_cancel,
        ))
        .await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    fixture
        .joiner_home
        .stream_exact_reads_in_chunks(128, Duration::from_millis(25));
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let cancel_on_download: coven_replication::sync::JoiningDeviceJoinProgressObserver =
        Arc::new(move |progress| {
            if matches!(
                progress,
                coven_replication::sync::JoiningDeviceJoinProgress::DownloadingSnapshot {
                    bytes_done,
                    ..
                } if bytes_done > 0
            ) {
                cancel_tx
                    .send(true)
                    .expect("snapshot cancellation receiver remains alive");
            }
        });
    let cancelled = tokio::time::timeout(
        Duration::from_secs(2),
        fixture
            .client()
            .join_via_transport(&bundle, timing(), cancel_on_download, &cancel_rx),
    )
    .await
    .expect("snapshot cancellation must interrupt the active storage read");
    assert!(
        matches!(cancelled, Err(crate::joining::BootstrapError::Cancelled)),
        "the caller receives cancellation, got {cancelled:?}",
    );
    let store_dir = fixture.layout.store_dir("device-join-snapshot-cancel");
    assert!(
        !store_dir.db_path().exists(),
        "a cancelled snapshot transfer must not leave a database image"
    );

    fixture
        .joiner_home
        .stream_exact_reads_in_chunks(usize::MAX, Duration::ZERO);
    let config = joined(
        fixture
            .client()
            .join_via_transport(&bundle, timing(), no_join_progress(), &never_cancelled())
            .await,
    );
    assert_eq!(config.store_id, "device-join-snapshot-cancel");
    assert!(store_dir.config_path().is_file());
}

/// The owner keeps writing after it activates the joining device but before
/// that device installs its library. Enrollment opens the exact offered
/// snapshot without waiting for unrelated newer history; the ordinary sync
/// loop owns that later commit after the library is usable.
#[test]
fn a_join_opens_before_later_owner_commits_are_synced() {
    on_a_deep_stack(run_a_join_opens_before_later_owner_commits_are_synced);
}

async fn run_a_join_opens_before_later_owner_commits_are_synced() {
    let fixture = TransportFixture::build("device-join-across-owner-commits").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let join_once = |timing| {
        let client = fixture.client();
        let bundle = &bundle;
        let cancel = &cancel;
        async move {
            client
                .join_via_transport(bundle, timing, no_join_progress(), cancel)
                .await
        }
    };

    // The joining device publishes the exact registration request. The owner
    // enrolls it and publishes the library bootstrap and activation.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    let activation = activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    // The owner's sync loop commits a row before the joining device resumes.
    // That row is newer than the enrollment activation and therefore is not in
    // the offered snapshot.
    let intervening = fixture.publish_owner_row("owner-writes-mid-join").await;
    assert_eq!(
        intervening.coord.stream_id,
        activation.outcome_activation.coord.stream_id,
    );
    assert!(
        intervening.coord.sequence > activation.outcome_activation.coord.sequence,
        "the owner's row must be newer than the enrollment activation",
    );

    let config = joined(Box::pin(join_once(timing())).await);
    let joined_store_dir = fixture.layout.store_dir(&config.store_id);
    assert!(joined_store_dir.config_path().exists());
    // Enrollment is not a disguised sync cycle. The later row is absent from
    // the installed snapshot and will arrive through the opened library's
    // ordinary sync loop.
    let joined_db = coven_database::DatabaseImageTest::open(&joined_store_dir.db_path())
        .expect("open the joined device's database");
    assert_eq!(
        joined_db
            .query_row(
                "SELECT COUNT(*) FROM notes WHERE id = 'owner-writes-mid-join'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count the owner's intervening row"),
        0,
        "enrollment waited for a row outside its signed snapshot cut",
    );
}

/// An owner that gives up before the attempt exists publishes its abandonment,
/// and the joining device — sitting in its wait for the library — reads that
/// instead and converges on the same terminal, clearing the namespace behind it.
#[tokio::test]
async fn an_abandoned_attempt_reaches_the_joining_device() {
    tokio::spawn(run_an_abandoned_attempt_reaches_the_joining_device())
        .await
        .expect("abandonment task");
}

async fn run_an_abandoned_attempt_reaches_the_joining_device() {
    let fixture = TransportFixture::build("device-join-transport-abandon").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    // The joining device publishes its access request and waits.
    let joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel)).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );

    let abandonment = fixture
        .owner_store
        .device_join_transport()
        .abandon(&bundle)
        .await
        .expect("the owner abandons the attempt");
    assert!(
        fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::Abandonment)
            .await
            .is_some(),
        "the abandonment reached its slot",
    );

    // The joining device's next run finds the abandonment where it would have
    // found the library bootstrap.
    match Box::pin(fixture.client().join_via_transport(
        &bundle,
        timing(),
        no_join_progress(),
        &cancel,
    ))
    .await
    .expect("the joining device accepts the abandonment")
    {
        crate::joining::DeviceJoinTransportOutcome::Abandoned(observed) => {
            assert_eq!(observed, abandonment)
        }
        crate::joining::DeviceJoinTransportOutcome::Joined(_) => {
            panic!("an abandoned attempt must not produce a member config")
        }
    }
    for kind in DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "{kind:?} slot outlived the abandoned attempt",
        );
    }

    // Publishing the abandonment is the admitting side's last step, and the
    // row that anchored getting it there goes with it. Nothing is left to
    // re-offer the same transfer on every later pass.
    assert_eq!(
        fixture.owner_status(&bundle).await,
        None,
        "the admitting device kept a journal row for an attempt it abandoned",
    );
    // Asking to give it up again has nothing to give up: the attempt finished,
    // and its absence is the record of that.
    fixture
        .owner_store
        .device_join_transport()
        .abort(&bundle)
        .await
        .expect("aborting a finished attempt is a no-op");
}

/// The admitting side published its abandonment commit and died before the
/// artifact reached its slot. The row it left is the resume anchor, and a
/// driver that finds it owes both halves of that step: deliver the artifact,
/// then drop the row.
#[tokio::test]
async fn an_interrupted_abandonment_is_finished_by_the_next_drive() {
    tokio::spawn(run_an_interrupted_abandonment_is_finished_by_the_next_drive())
        .await
        .expect("interrupted abandonment task");
}

async fn run_an_interrupted_abandonment_is_finished_by_the_next_drive() {
    let fixture = TransportFixture::build("device-join-transport-abandon-resume").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel)).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );

    // The Store-side abandonment alone: the commit lands and the row records
    // it, which is where a crash before the transfer leaves things.
    let abandonment = fixture
        .owner_store
        .abandon_device_join(bundle.offer.clone())
        .await
        .expect("the owner commits the abandonment");
    assert!(
        fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::Abandonment)
            .await
            .is_none(),
        "the interrupted abandonment must not have reached its slot yet",
    );

    match fixture
        .drive_owner_with(&bundle, one_shot())
        .await
        .expect("the admitting driver finishes the abandonment")
    {
        coven_replication::sync::DeviceJoinDriveOutcome::Abandoned(observed) => {
            assert_eq!(observed, abandonment)
        }
        coven_replication::sync::DeviceJoinDriveOutcome::Activated(_) => {
            panic!("an abandoned attempt has no activation")
        }
    }
    assert!(
        fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::Abandonment)
            .await
            .is_some(),
        "the resumed drive owed the abandonment its slot",
    );
    assert_eq!(
        fixture.owner_status(&bundle).await,
        None,
        "the resumed drive delivered the abandonment but kept its row",
    );
}

/// `AutoApproveSelfIssued` admits only attempts this device issued. A device
/// with no owner journal for the attempt is a device that never made the offer,
/// and it refuses rather than admitting on a stranger's say-so.
#[tokio::test]
async fn auto_approval_refuses_an_attempt_this_device_did_not_issue() {
    tokio::spawn(run_auto_approval_refuses_an_attempt_this_device_did_not_issue())
        .await
        .expect("auto approval task");
}

async fn run_auto_approval_refuses_an_attempt_this_device_did_not_issue() {
    let fixture = TransportFixture::build("device-join-transport-not-self-issued").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel)).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );

    // Drop this device's owner journal for the attempt: what is left is exactly
    // what a device that never issued the offer holds.
    fixture.forget_owner_journal(&bundle).await;

    let refused = fixture.drive_owner_with(&bundle, one_shot()).await;
    assert!(
        matches!(
            refused,
            Err(DeviceJoinTransportError::DeviceJoin(
                coven_replication::sync::DeviceJoinError::OfferMismatch
            ))
        ),
        "an attempt this device did not issue must be refused, got {refused:?}",
    );
    for kind in [DeviceJoinTransportKind::SamePrincipalJoin] {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "a refused attempt produces no {kind:?}",
        );
    }
}

/// The `Ask` policy hands the request to the host and abides by the answer: a
/// refusal stops the join before a library bootstrap or activation is published.
#[tokio::test]
async fn the_ask_policy_consults_the_host_and_a_refusal_stops_the_join() {
    tokio::spawn(run_the_ask_policy_consults_the_host_and_a_refusal_stops_the_join())
        .await
        .expect("ask policy task");
}

async fn run_the_ask_policy_consults_the_host_and_a_refusal_stops_the_join() {
    let fixture = TransportFixture::build("device-join-transport-ask").await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel)).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );

    let asked = std::sync::atomic::AtomicUsize::new(0);
    let refuse = |request: &coven_replication::sync::DeviceProviderAccessRequest| {
        assert_eq!(request.offer.attempt_id, bundle.offer.attempt_id);
        asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        coven_replication::sync::DeviceJoinApproval::Refuse
    };
    let refused = fixture
        .owner_store
        .device_join_transport()
        .drive(
            &bundle,
            coven_replication::sync::DeviceJoinApprovalPolicy::Ask(&refuse),
            None,
            &|_| {},
            one_shot(),
        )
        .await;
    assert_eq!(
        asked.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the host was asked exactly once",
    );
    assert!(
        matches!(
            refused,
            Err(DeviceJoinTransportError::DeviceJoin(
                coven_replication::sync::DeviceJoinError::OfferMismatch
            ))
        ),
        "a refused request stops the join, got {refused:?}",
    );
    for kind in [DeviceJoinTransportKind::SamePrincipalJoin] {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_none(),
            "a refused request produces no {kind:?}",
        );
    }

    // The same request approved by the host produces the library bootstrap and
    // activation without another joining-device round trip.
    let approve = |_request: &coven_replication::sync::DeviceProviderAccessRequest| {
        asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        coven_replication::sync::DeviceJoinApproval::Approve
    };
    activated(
        fixture
            .owner_store
            .device_join_transport()
            .drive(
                &bundle,
                coven_replication::sync::DeviceJoinApprovalPolicy::Ask(&approve),
                None,
                &|_| {},
                one_shot(),
            )
            .await,
    );
    assert_eq!(
        asked.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the host was asked again on the next run",
    );
    for kind in [DeviceJoinTransportKind::SamePrincipalJoin] {
        assert!(
            fixture.slot_bytes(&bundle, kind).await.is_some(),
            "an approved request produces {kind:?}",
        );
    }
}

/// Republishing an artifact already at its slot is the same transfer, not a
/// second one: it succeeds and leaves the first write's bytes untouched, which
/// is what a crash between the journal advance and the create resumes into.
/// A *different* artifact at that slot is refused — a counterpart may already
/// have read what is there.
#[tokio::test]
async fn republishing_is_idempotent_and_a_different_artifact_is_refused() {
    tokio::spawn(async {
        let fixture = TransportFixture::build("device-join-transport-duplicate").await;
        let bundle = fixture.begin().await;
        let joiner = fixture.client();

        let request = joiner
            .prepare_provider_access_request(bundle.offer.clone())
            .await
            .expect("prepare provider access request");
        let joiner_storage = joiner
            .transport_storage()
            .await
            .expect("joining device transport storage");
        let transport = DeviceJoinTransport::open(&joiner_storage, &bundle, DeviceJoinRole::Joiner)
            .expect("open transport");

        let mut conflicting_request = request.clone();
        conflicting_request
            .body_mut()
            .offer
            .body_mut()
            .member_pubkey
            .push('0');
        let action = DeviceJoinAction::TransferProviderAccessRequest(request);
        let creates_before = fixture.home.exact_create_count();
        let reads_before = fixture.home.exact_full_read_count();
        transport.publish(&action).await.expect("first publish");
        assert_eq!(
            fixture.home.exact_create_count(),
            creates_before + 1,
            "a first publish creates exactly one object",
        );
        assert_eq!(
            fixture.home.exact_full_read_count(),
            reads_before,
            "a first publish does not read an empty slot before creating it",
        );
        let first_bytes = fixture
            .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
            .await
            .expect("the access request is stored");

        transport
            .publish(&action)
            .await
            .expect("republishing the same artifact is the same transfer");
        assert_eq!(
            fixture
                .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
                .await
                .expect("the access request is still stored"),
            first_bytes,
            "an idempotent republish leaves the first write's exact bytes",
        );

        // A different artifact of the same kind cannot replace the first one.
        // Its signature is deliberately stale because transport conflict
        // detection compares bytes; protocol acceptance is the later verifier's
        // responsibility.
        let conflict = transport
            .publish(&DeviceJoinAction::TransferProviderAccessRequest(
                conflicting_request,
            ))
            .await;
        assert!(
            matches!(
                conflict,
                Err(DeviceJoinTransportError::ArtifactConflict {
                    kind: DeviceJoinTransportKind::ProviderAccessRequest
                })
            ),
            "a different artifact at an occupied slot is refused, got {conflict:?}",
        );

        // A second object in the namespace, so the teardown has two independent
        // deletions to overlap. It reads what the listing names and nothing
        // else, so what it overlaps is objects that are really there.
        fixture.home.insert_exact_object(
            &format!(
                "{}/from-another-version.json",
                bundle.transport.attempt_namespace
            ),
            b"an artifact this build has no kind for".to_vec(),
        );
        fixture
            .home
            .delay_exact_full_reads(Duration::from_millis(10));
        transport
            .delete_attempt_slots()
            .await
            .expect("delete the attempt transport");
        assert!(
            fixture.home.exact_full_read_max_inflight() > 1,
            "attempt cleanup reads the objects it found concurrently",
        );
        assert!(
            fixture
                .home
                .keys()
                .iter()
                .all(|key| !key.starts_with(&bundle.transport.attempt_namespace)),
            "the teardown removed the namespace, including what it has no kind for",
        );
    })
    .await
    .expect("duplicate publish task");
}

/// Each artifact kind has one producing role, and a transport opened for other
/// roles will not write it — the slot a counterpart reads only ever holds bytes
/// the role that owns that step put there.
#[tokio::test]
async fn a_role_cannot_publish_another_roles_artifact() {
    tokio::spawn(async {
        let fixture = TransportFixture::build("device-join-transport-producer").await;
        let bundle = fixture.begin().await;
        let joiner = fixture.client();

        let request = joiner
            .prepare_provider_access_request(bundle.offer.clone())
            .await
            .expect("prepare provider access request");
        let refused = fixture
            .transport(&bundle)
            .publish(&DeviceJoinAction::TransferProviderAccessRequest(request))
            .await;
        assert!(
            matches!(
                refused,
                Err(DeviceJoinTransportError::WrongProducer {
                    kind: DeviceJoinTransportKind::ProviderAccessRequest,
                    role: coven_replication::sync::DeviceJoinRole::Joiner,
                })
            ),
            "the admitting side must not write the joiner's artifact, got {refused:?}",
        );
        assert!(
            fixture
                .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAccessRequest)
                .await
                .is_none(),
            "a refused publish never reaches storage",
        );
    })
    .await
    .expect("producer role task");
}

/// With no counterpart running, awaiting an artifact fails at its deadline and
/// names the role that never published — what a host renders as "the owner's
/// app must be open".
#[tokio::test]
async fn awaiting_an_absent_counterpart_times_out_naming_its_role() {
    tokio::spawn(async {
        let fixture = TransportFixture::build("device-join-transport-timeout").await;
        let bundle = fixture.begin().await;
        let transport = fixture.transport(&bundle);

        let expired = DeviceJoinTransportTiming {
            poll: Duration::from_millis(1),
            deadline: Duration::from_millis(20),
        };
        let timed_out = transport
            .await_artifact::<coven_replication::sync::DeviceProviderAccessRequest>(expired)
            .await;
        assert!(
            matches!(
                timed_out,
                Err(DeviceJoinTransportError::Timeout {
                    kind: DeviceJoinTransportKind::ProviderAccessRequest,
                    producer: coven_replication::sync::DeviceJoinRole::Joiner,
                })
            ),
            "an absent joiner surfaces as a timeout naming it, got {timed_out:?}",
        );
    })
    .await
    .expect("timeout task");
}

/// Bytes swapped in the slot behind the transport's back do not advance the
/// join: the seal refuses them, and the awaiting driver surfaces that rather
/// than feeding anything to the protocol.
#[tokio::test]
async fn tampered_slot_bytes_refuse_to_open() {
    tokio::spawn(async {
        let fixture = TransportFixture::build("device-join-transport-sabotage").await;
        let bundle = fixture.begin().await;
        let joiner = fixture.client();

        let request = joiner
            .prepare_provider_access_request(bundle.offer.clone())
            .await
            .expect("prepare provider access request");
        let joiner_storage = joiner
            .transport_storage()
            .await
            .expect("joining device transport storage");
        DeviceJoinTransport::open(&joiner_storage, &bundle, DeviceJoinRole::Joiner)
            .expect("open transport")
            .publish(&DeviceJoinAction::TransferProviderAccessRequest(request))
            .await
            .expect("publish the access request");

        let target = slot(&bundle, DeviceJoinTransportKind::ProviderAccessRequest);
        let mut sealed = fixture
            .home
            .read_at(target)
            .await
            .expect("the access request is stored");
        let last = sealed.len() - 1;
        sealed[last] ^= 0xff;
        fixture
            .home
            .delete_at(target)
            .await
            .expect("clear the slot for the tampered bytes");
        let tampered_object = coven_protocol::objects::ExactObjectRef::new(
            target.clone(),
            sealed.len() as u64,
            coven_protocol::store_commit::ObjectHash::digest(&sealed),
        );
        let tampered_upload = ExactUpload::from_bytes(&tampered_object, &sealed)
            .expect("tampered bytes match their replacement exact reference");
        fixture
            .home
            .create_at(&tampered_upload, &UploadControl::running(no_progress()))
            .await
            .expect("plant the tampered bytes");

        let opened = fixture
            .transport(&bundle)
            .read(DeviceJoinTransportKind::ProviderAccessRequest)
            .await;
        assert!(
            matches!(opened, Err(DeviceJoinTransportError::Unsealable(_))),
            "tampered bytes must refuse to open, got {opened:?}",
        );

        let driven = fixture.drive_owner(&bundle).await;
        assert!(
            matches!(driven, Err(DeviceJoinTransportError::Unsealable(_))),
            "a driver never advances past bytes it could not open, got {driven:?}",
        );
        assert!(
            fixture
                .slot_bytes(&bundle, DeviceJoinTransportKind::ProviderAdmissionApproval)
                .await
                .is_none(),
            "no approval was produced from unopenable bytes",
        );
    })
    .await
    .expect("sabotage task");
}

/// Two attempts against one store never touch each other's slots: each is
/// namespaced by its own attempt id, and each carries its own seal key.
#[tokio::test]
async fn concurrent_attempts_keep_separate_namespaces() {
    tokio::spawn(async {
        let (fixture, second_member_pubkey) =
            TransportFixture::build_two_joiners("device-join-transport-concurrent").await;
        let first = fixture.begin().await;
        let second = fixture.begin_for(&second_member_pubkey).await;

        assert_ne!(first.offer.attempt_id, second.offer.attempt_id);
        assert_ne!(
            first.transport.attempt_namespace,
            second.transport.attempt_namespace
        );
        for kind in DeviceJoinTransportKind::ALL {
            assert_ne!(
                slot(&first, kind).logical_key(),
                slot(&second, kind).logical_key(),
                "{kind:?} slots collide across attempts",
            );
        }

        let joiner = fixture.client();
        let request = joiner
            .prepare_provider_access_request(first.offer.clone())
            .await
            .expect("prepare provider access request");
        let joiner_storage = joiner
            .transport_storage()
            .await
            .expect("joining device transport storage");
        DeviceJoinTransport::open(&joiner_storage, &first, DeviceJoinRole::Joiner)
            .expect("open the first attempt's transport")
            .publish(&DeviceJoinAction::TransferProviderAccessRequest(request))
            .await
            .expect("publish into the first attempt");

        assert!(
            fixture
                .slot_bytes(&first, DeviceJoinTransportKind::ProviderAccessRequest)
                .await
                .is_some(),
            "the first attempt holds its access request",
        );
        assert!(
            fixture
                .slot_bytes(&second, DeviceJoinTransportKind::ProviderAccessRequest)
                .await
                .is_none(),
            "the second attempt's slot is untouched",
        );
        assert!(fixture
            .transport(&second)
            .read(DeviceJoinTransportKind::ProviderAccessRequest)
            .await
            .expect("read the second attempt's empty slot")
            .is_none(),);
    })
    .await
    .expect("concurrent attempts task");
}
