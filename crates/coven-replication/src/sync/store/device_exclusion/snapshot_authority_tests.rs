use super::*;
use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_keys::{encryption::EncryptionService, keys::UserKeypair};
use coven_protocol::store_commit::*;

#[tokio::test]
async fn excluded_owner_device_cannot_restore_its_own_snapshot_authority() {
    assert_device_snapshot_claim_rejected(DeviceSnapshotClaim::OmittedExclusion).await;
}

#[tokio::test]
async fn accepted_exclusion_heads_cannot_authenticate_a_false_active_device_image() {
    assert_device_snapshot_claim_rejected(DeviceSnapshotClaim::ChangedExclusionEffect).await;
}

#[tokio::test]
async fn an_unaccepted_device_registration_cannot_authorize_its_own_snapshot() {
    assert_device_snapshot_claim_rejected(DeviceSnapshotClaim::UnacceptedRegistration).await;
}

#[tokio::test]
async fn an_unaccepted_recovery_registration_cannot_authorize_its_own_snapshot() {
    assert_device_snapshot_claim_rejected(DeviceSnapshotClaim::UnacceptedRecovery).await;
}

#[tokio::test]
async fn an_accepted_device_registration_cannot_disappear_from_snapshot_state() {
    assert_device_snapshot_claim_rejected(DeviceSnapshotClaim::OmittedRegistration).await;
}

#[tokio::test]
async fn a_snapshot_cannot_activate_an_unaccepted_device_exclusion() {
    assert_device_snapshot_claim_rejected(DeviceSnapshotClaim::UnacceptedExclusion).await;
}

#[tokio::test]
async fn snapshot_membership_requires_the_exact_rooted_head_not_only_its_entry() {
    assert_device_snapshot_claim_rejected(DeviceSnapshotClaim::ChangedFounderHead).await;
}

enum DeviceSnapshotClaim {
    OmittedExclusion,
    ChangedExclusionEffect,
    UnacceptedRegistration,
    UnacceptedRecovery,
    OmittedRegistration,
    UnacceptedExclusion,
    ChangedFounderHead,
}

async fn assert_device_snapshot_claim_rejected(claim: DeviceSnapshotClaim) {
    let include_exclusion_authority = matches!(claim, DeviceSnapshotClaim::ChangedExclusionEffect);
    let omitted_registration = matches!(claim, DeviceSnapshotClaim::OmittedRegistration);
    let unaccepted_exclusion = matches!(claim, DeviceSnapshotClaim::UnacceptedExclusion);
    let changed_founder_head = matches!(claim, DeviceSnapshotClaim::ChangedFounderHead);
    let unaccepted_join = matches!(claim, DeviceSnapshotClaim::UnacceptedRegistration);
    let unaccepted_recovery = matches!(claim, DeviceSnapshotClaim::UnacceptedRecovery);
    let unaccepted_registration = unaccepted_join || unaccepted_recovery;
    let directory = test_store_dir();
    let database = open_test_db(directory.clone());
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &database,
        directory.clone(),
        "snapshot-excluded-owner-device",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&database, directory.clone(), &signer)
        .await
        .expect("bind owner");
    let peer_directory = test_store_dir();
    let joined_database = open_test_db(peer_directory.clone());
    let peer_database = if changed_founder_head {
        &database
    } else {
        &joined_database
    };
    let peer = if changed_founder_head {
        owner.clone()
    } else {
        store
            .activate_joined_device(
                &database,
                directory.clone(),
                peer_database,
                peer_directory,
                &signer,
                "2026-09-08T00:00:00Z",
            )
            .await
            .expect("activate another device of the same Owner")
    };
    let peer_registration = peer
        .latest_local_store_device_registration()
        .await
        .expect("read peer registration")
        .expect("peer registered");
    let mut peer_registration: StoreDeviceRegistration =
        serde_json::from_slice(&peer_registration.registration_bytes).expect("registration bytes");
    let mut peer_ref = StoreDatabase::new(peer_database)
        .activated_store_device_registration_for_device(peer_registration.device_id)
        .await
        .expect("read activated peer")
        .expect("peer is active")
        .reference()
        .clone();
    let mut peer_signer = peer_registration
        .device_signer(&signer)
        .expect("peer device signer");
    let root = store.root();
    let database_owner = StoreDatabase::new(&database);
    let mut template = {
        let mut writer = owner
            .authorize_writer()
            .await
            .expect("authorize predecessor snapshot");
        let mut snapshots = writer.snapshots();
        let cut = snapshots
            .capture_snapshot_cut(Some(&EncryptionService::from_key([42; 32])))
            .await
            .expect("capture active peer");
        snapshots
            .push_snapshot_cut(cut, "2026-09-08T00:00:01Z".to_string())
            .await
            .expect("publish active-peer snapshot")
    };
    let (_, mut prior_devices) = database_owner
        .store_device_state_for_history_cut(&StoreHistoryCut(template.coverage.0.clone()))
        .await
        .expect("read the actual template device state");
    assert!(matches!(
        prior_devices.devices[&peer_ref.device_id].status,
        StoreDeviceStatus::Active
    ));
    let mut prior_membership = owner
        .membership_for_test()
        .await
        .expect("read accepted membership before preparing an unaccepted registration");
    if unaccepted_exclusion {
        let mut writer = owner
            .authorize_writer()
            .await
            .expect("authorize actual proposal");
        let StoreDeviceExclusionResult::ProposalActivated { proposal, .. } = writer
            .device_exclusion()
            .propose(&peer_ref)
            .await
            .expect("accept actual exclusion proposal")
        else {
            panic!("proposal was not accepted");
        };
        {
            let mut snapshots = writer.snapshots();
            let cut = snapshots
                .capture_snapshot_cut(Some(&EncryptionService::from_key([42; 32])))
                .await
                .expect("capture actual pending proposal");
            template = snapshots
                .push_snapshot_cut(cut, "2026-09-08T00:00:02Z".into())
                .await
                .expect("publish actual pending proposal snapshot");
        }
        let before_outcome = database_owner
            .store_current_publication()
            .await
            .expect("accepted proposal boundary");
        let durable = writer
            .device_exclusion()
            .prepare_outcome(&proposal, OutcomeIntent::Exclude)
            .await
            .expect("prepare actual exclusion without accepting it");
        let DurableStoreDeviceExclusionObject::Outcome {
            reference: StoreDeviceExclusionOutcomeRef::Excluded(exclusion),
            prepared,
            ..
        } = durable.object()
        else {
            panic!("prepared operation is not exclusion");
        };
        storage
            .create_protocol_object(prepared)
            .await
            .expect("expose the valid outcome bytes without its activating head");
        assert_eq!(
            database_owner
                .store_current_publication()
                .await
                .expect("unchanged accepted boundary"),
            before_outcome
        );
        prior_devices = template
            .state
            .devices
            .exclude(exclusion.clone())
            .expect("claim the unaccepted exclusion effect");
        drop(writer);
        prior_membership = owner
            .membership_for_test()
            .await
            .expect("actual accepted proposal authority");
    }
    if unaccepted_join {
        let pending_dir = tempfile::tempdir().expect("pending join directory");
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .expect("open pending join journal");
        let offer = owner
            .begin_device_join(&coven_keys::keys::public_key_hex(&signer))
            .await
            .expect("offer another device of the same Owner");
        let mut joining = owner
            .open_pending_device_join_for_test(&pending, &signer, offer)
            .await
            .expect("open the actual joining device");
        let request = joining
            .prepare_provider_access_request()
            .await
            .expect("prepare actual provider access request");
        let approval = owner
            .authorize_device_provider_access(request, None)
            .await
            .expect("approve the actual provider binding");
        let request = joining
            .prepare_registration_request(approval)
            .await
            .expect("prepare the actual registration without activating it");
        peer_registration = request.expected_registration().clone();
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let prefix = registration_semantic_prefix(&peer_registration.device_id.to_string());
        let prepared = storage
            .prepare_protocol_object(
                &context,
                request.registration_slot().clone(),
                &prefix,
                peer_registration.to_bytes(),
            )
            .expect("prepare the pending request's exact registration object");
        storage
            .create_verified_protocol_object(
                &context,
                &prepared,
                &prefix,
                &peer_registration.to_bytes(),
            )
            .await
            .expect("upload the valid registration without its activating publication");
        peer_ref = StoreDeviceRegistrationRef::from_registration(
            &peer_registration,
            prepared.reference().clone(),
        );
        peer_signer = peer_registration
            .device_signer(&signer)
            .expect("derive the pending registration's actual device key");
        assert!(database_owner
            .activated_store_device_registration_for_device(peer_ref.device_id)
            .await
            .expect("query actual activation state")
            .is_none());
        prior_devices = prior_devices
            .activate_registration(peer_ref.clone(), None)
            .expect("construct the candidate's false active-device claim");
    } else if unaccepted_recovery {
        let authority = store.founder_recovery_authority().await;
        let mut recovery = owner
            .owner_recovery_for_test()
            .await
            .expect("authorize the actual founder recovery");
        let before = database_owner
            .store_current_publication()
            .await
            .expect("read boundary before recovery preparation");
        home.fail_exact_create_before_call(4);
        recovery
            .recover_owner_device(&authority, Some(&EncryptionService::from_key([42; 32])))
            .await
            .expect_err("interrupt before uploading the recovery activation commit");
        let staged = database_owner
            .owner_recovery_publication()
            .await
            .expect("read interrupted recovery publication")
            .expect("the actual recovery candidate is durable");
        let registration = database_owner
            .latest_local_store_device_registration()
            .await
            .expect("read actual recovery registration")
            .expect("recovery registration was staged");
        peer_registration = StoreDeviceRegistration::parse_at(
            &registration.registration_bytes,
            &root,
            registration.device_id,
        )
        .expect("authenticate the actual prepared recovery registration");
        peer_ref = staged.commit.value.author_registration.clone();
        peer_ref
            .verify_registration(&peer_registration)
            .expect("candidate activates this exact recovery device");
        peer_signer = peer_registration
            .device_signer(&signer)
            .expect("derive the actual recovery device key");
        let [activation] = staged.commit.value.device_registrations() else {
            panic!("recovery must activate exactly one device");
        };
        let StoreDeviceRegistrationActivationRef::Recovery { node, .. } = &activation.authority
        else {
            panic!("actual recovery has another activation origin");
        };
        assert_eq!(
            database_owner
                .store_current_publication()
                .await
                .expect("read boundary after interrupted recovery"),
            before,
            "prepared recovery has not changed accepted Store history"
        );
        assert!(database_owner
            .activated_store_device_registration_for_device(peer_ref.device_id)
            .await
            .expect("query recovery activation")
            .is_none());
        prior_devices = prior_devices
            .activate_registration(
                peer_ref.clone(),
                Some(OwnerRecoveryCursor {
                    owner_grant: authority.owner_grant,
                    position: OwnerRecoveryPosition::At { node: node.clone() },
                }),
            )
            .expect("construct the false accepted-recovery state");
    }
    let image_context = ProtocolObjectContext::store_encrypted(
        root.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotImage,
    );

    let membership = if !unaccepted_registration
        && !omitted_registration
        && !unaccepted_exclusion
        && !changed_founder_head
    {
        owner.finalize_peer_exclusion(&peer_ref).await;
        owner
            .membership_for_test()
            .await
            .expect("membership after exclusion")
    } else {
        prior_membership
    };
    assert!(
        membership.is_owner_now(&coven_keys::keys::public_key_hex(&signer)),
        "excluding a device does not revoke the principal's Owner grant"
    );
    let coverage = CommitFrontier::from_refs(
        database_owner
            .materialized_frontier()
            .await
            .expect("accepted exclusion frontier"),
    )
    .expect("exact frontier");
    let (_, current_devices) = database_owner
        .store_device_state_for_history_cut(&StoreHistoryCut(coverage.0.clone()))
        .await
        .expect("accepted post-exclusion device state");
    if omitted_registration || unaccepted_exclusion || changed_founder_head {
        assert!(matches!(
            current_devices.devices[&peer_ref.device_id].status,
            StoreDeviceStatus::Active
        ));
        if omitted_registration {
            prior_devices
                .devices
                .remove(&peer_ref.device_id)
                .expect("remove the accepted peer from the false claim");
            prior_devices = ResolvedStoreDeviceState::merge([prior_devices])
                .expect("recompute the false state hash");
        }
        let founder = database_owner
            .activated_store_device_registration_for_device(owner.typed_device_id())
            .await
            .expect("load the actual snapshot author")
            .expect("founder is active");
        peer_ref = founder.reference().clone();
        peer_registration = founder.value().clone();
        peer_signer = peer_registration
            .device_signer(&signer)
            .expect("founder device signer");
    } else if unaccepted_registration {
        assert!(
            !current_devices.devices.contains_key(&peer_ref.device_id),
            "the claimed device has never entered accepted Store state"
        );
    } else {
        assert!(matches!(
            current_devices.devices[&peer_ref.device_id].status,
            StoreDeviceStatus::Inactive { .. }
        ));
    }
    let template = if include_exclusion_authority {
        let mut writer = owner
            .authorize_writer()
            .await
            .expect("authorize post-exclusion snapshot");
        let mut snapshots = writer.snapshots();
        let cut = snapshots
            .capture_snapshot_cut(Some(&EncryptionService::from_key([42; 32])))
            .await
            .expect("capture genuine excluded-device state");
        snapshots
            .push_snapshot_cut(cut, "2026-09-08T00:00:02Z".to_string())
            .await
            .expect("publish genuine exclusion heads and proofs")
    } else {
        template
    };
    let image_bytes = storage
        .read_protocol_object(
            &image_context,
            &template.image.object,
            &semantic_prefix_from_exact_object(&template.image.object, ".db")
                .expect("validated image path"),
        )
        .await
        .expect("read the genuine template image");
    let before = database_owner
        .store_current_publication()
        .await
        .expect("accepted boundary");
    let mut summary = template.history_summary.clone();
    if unaccepted_registration {
        summary.registrations.insert(
            peer_ref.device_id,
            ReferencedStoreDeviceRegistration::verified(
                peer_ref.clone(),
                peer_registration.clone(),
            )
            .expect("the unaccepted registration has valid exact signed bytes"),
        );
    }
    for reference in coverage.0.values() {
        summary
            .causal_cut
            .insert(reference.coord.clone(), reference.clone());
    }
    let claimed_state = StoreDeviceStateRef::from_resolved(coverage.clone(), &prior_devices)
        .expect("claim the candidate device is active");
    summary.post_state = claimed_state.clone();
    summary
        .reclaim
        .include_previous_snapshot(
            before
                .record()
                .latest_snapshot()
                .expect("predecessor snapshot"),
            &template,
        )
        .expect("retain predecessor snapshot payloads");
    summary
        .validate_snapshot_baseline()
        .expect("internally consistent adversarial summary");
    let image = coven_database::DatabaseImageTest::from_bytes(&image_bytes)
        .expect("open genuine active-peer image");
    for reference in coverage.0.values() {
        image
            .replace_store_device_snapshot(reference, &prior_devices)
            .expect("claim old device state at the current accepted cut");
    }
    let image_bytes = image.into_bytes().expect("serialize adversarial image");
    let rollup_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipRollup,
    );
    let original_rollup = storage
        .read_protocol_object(
            &rollup_context,
            &template.membership_rollup.object,
            &semantic_prefix_from_exact_object(&template.membership_rollup.object, ".json")
                .expect("validated rollup path"),
        )
        .await
        .expect("read genuine membership rollup");
    let original_rollup: MembershipRollup =
        coven_protocol::objects::decode_protocol_object(&original_rollup)
            .expect("decode genuine membership rollup");
    let mut rollup_streams = original_rollup.streams.clone();
    let mut claimed_membership = template.state.membership.clone();
    if changed_founder_head {
        let [stream] = rollup_streams.as_mut_slice() else {
            panic!("Founder-only fixture has one authority stream");
        };
        let [carried] = stream.heads.as_mut_slice() else {
            panic!("Founder-only fixture has one authority head");
        };
        assert!(matches!(
            carried.head_value.activation,
            coven_protocol::membership::MembershipHeadActivation::Direct
        ));
        let original = carried.head.clone();
        let mut body = carried.head_value.body.clone();
        body.successor.next_slot = coven_protocol::objects::ObjectSlot::logical(format!(
            "{}-alternate.json",
            body.successor
                .next_slot
                .logical_key()
                .strip_suffix(".json")
                .expect("head successor JSON slot")
        ))
        .expect("alternate exact successor slot");
        let alternate = coven_protocol::membership::AuthorHead::signed(
            carried.head_value.store_id.clone(),
            body,
            coven_protocol::membership::MembershipHeadActivation::Direct,
            &peer_signer,
        );
        let bytes = alternate.to_bytes();
        carried.head = coven_protocol::membership::MembershipHeadRef {
            coord: original.coord.clone(),
            head_hash: alternate.head_hash(),
            object: coven_protocol::objects::ExactObjectRef::new(
                original.object.slot().clone(),
                bytes.len() as u64,
                ObjectHash::digest(&bytes),
            ),
        };
        assert_eq!(carried.head.coord, original.coord);
        assert_ne!(carried.head, original);
        carried.head_value = alternate;
        claimed_membership.heads = vec![carried.head.clone()];
        let head_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let prefix = semantic_prefix_from_exact_object(&original.object, ".json")
            .expect("original head path");
        let provider = storage
            .read_protocol_object(&head_context, &original.object, &prefix)
            .await
            .expect("provider still owns the actual Founder head");
        assert_ne!(
            provider, bytes,
            "the alternate head only exists in the candidate rollup"
        );
    }
    let rollup = MembershipRollup::signed(
        root.store_root_hash,
        peer_ref.clone(),
        rollup_streams,
        &peer_signer,
    )
    .expect("sign the unchanged membership rollup with the candidate author");
    let meta_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotMeta,
    );
    let meta_prefix = snapshot_candidate_semantic_prefix(
        &peer_ref.device_id.to_string(),
        "device-authority-claim",
    );
    let meta_slot = storage
        .allocate_protocol_slot(&meta_context, &meta_prefix, ".json")
        .await
        .expect("reserve candidate metadata slot");
    let rollup_bytes = rollup.to_bytes();
    let rollup_hash = ObjectHash::digest(&rollup_bytes);
    let rollup_prefix = membership_rollup_semantic_prefix(&meta_slot, rollup_hash);
    let rollup_object = store
        .create_exact_protocol_object(&rollup_context, &rollup_prefix, ".json", &rollup_bytes)
        .await
        .expect("upload correctly signed membership rollup");
    let image_hash = ObjectHash::digest(&image_bytes);
    let image_prefix = snapshot_image_semantic_prefix(&meta_slot, image_hash);
    let image_object = store
        .create_exact_protocol_object(&image_context, &image_prefix, ".db", &image_bytes)
        .await
        .expect("upload adversarial image");
    if unaccepted_recovery {
        claimed_membership =
            coven_protocol::circle_control::StoreMembershipStateRef::from_membership(
                &membership,
                prior_devices.recovery.clone(),
            )
            .expect("bind the claimed recovery cursor to the genuine Owner membership");
    }
    let meta = SnapshotMeta::signed(
        root.store_root_hash,
        peer_ref.clone(),
        before.record().clone(),
        SnapshotImageRef {
            image_hash,
            object: image_object,
        },
        MembershipRollupRef {
            rollup_hash,
            object: rollup_object,
        },
        coverage,
        StoreSnapshotState {
            devices: prior_devices,
            membership: claimed_membership,
        },
        summary,
        template.schema_version,
        "2026-09-08T00:00:02Z".to_string(),
        &peer_signer,
    )
    .expect("sign candidate using its claimed device key");
    let prepared = storage
        .prepare_protocol_object(&meta_context, meta_slot, &meta_prefix, meta.to_bytes())
        .expect("prepare exact candidate metadata");
    storage
        .create_verified_protocol_object(&meta_context, &prepared, &meta_prefix, &meta.to_bytes())
        .await
        .expect("upload candidate snapshot metadata");
    let object = prepared.reference().clone();
    let reference = StoreSnapshotRef {
        snapshot_hash: meta.snapshot_hash(),
        object,
    };
    SnapshotMeta::parse_at(
        &meta.to_bytes(),
        root.store_root_hash,
        &reference,
        &peer_registration,
    )
    .expect("candidate has a valid device signature and shape");
    let entry =
        StorePublicationEntry::signed_snapshot(before.record(), peer_ref, reference, &peer_signer)
            .expect("sign candidate publication");
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StorePublicationEntry,
    );
    let prefix = store_publication_entry_semantic_prefix(&entry);
    let object = store
        .create_exact_protocol_object(&context, &prefix, ".json", &entry.to_bytes())
        .await
        .expect("upload publication entry");
    let publication_ref =
        StorePublicationRef::from_entry(&entry, object).expect("exact publication");
    let replacement = StoreCurrentPublicationRecord::advance_snapshot(
        before.record(),
        &entry,
        publication_ref,
        &peer_signer,
    )
    .expect("sign conditional successor");
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCurrentPublication,
    );
    let outcome = storage
        .replace_protocol_record_if_version(
            &context,
            &owner
                .protocol_root_for_test()
                .descriptor
                .current_publication_slot,
            store_current_publication_semantic_prefix(),
            before
                .require_observed()
                .expect("fixture replaced a provider-observed boundary")
                .version(),
            replacement.to_bytes(),
        )
        .await
        .expect("replace the exact provider record");
    assert!(matches!(
        outcome,
        coven_storage::cloud::ConditionalWriteOutcome::Replaced(_)
    ));
    let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &root)
        .await
        .expect("open cold verifier");
    if changed_founder_head {
        assert!(
            history.adopt_published_membership_rollup().await,
            "the real rollup reader accepts the signed candidate bytes before rooted verification"
        );
    }
    let result = history.load_current_accepted_snapshot().await;
    assert_eq!(
        database_owner
            .store_current_publication()
            .await
            .expect("unchanged local boundary"),
        before
    );
    let error = result
        .map(|_| ())
        .expect_err("snapshot state must match independently accepted device authority");
    if include_exclusion_authority {
        assert!(
            error.to_string().contains("device state omits or changes"),
            "the genuine authority heads must reach the altered device effect: {error}"
        );
    }
    if omitted_registration {
        assert!(
            error
                .to_string()
                .contains("omits an independently accepted registration"),
            "the snapshot must preserve the accepted peer registration: {error}"
        );
    }
    if unaccepted_exclusion {
        assert!(
            error.to_string().contains("unaccepted authority effect"),
            "the snapshot must reject the prepared outcome's claimed effect: {error}"
        );
    }
    if unaccepted_join {
        assert!(
            error
                .to_string()
                .contains("no independently accepted registration activation"),
            "the pending device must fail its activation authority check: {error}"
        );
    }
    if changed_founder_head {
        assert!(
            error.to_string().contains("exact rooted head"),
            "reject the candidate head identity itself: {error}"
        );
    }
    assert!(
        !error.to_string().contains("NotFound"),
        "authority rejection must not depend on absent fixture objects: {error}"
    );
}
