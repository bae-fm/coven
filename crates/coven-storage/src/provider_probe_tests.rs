use super::*;
use crate::cloud::*;
use async_trait::async_trait;
use coven_protocol::objects::{
    ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
};
use coven_protocol::store_commit::{
    DeviceJoinAttemptId, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceRegistrationRef,
    StoreRootRef,
};
use std::sync::Mutex;

type Replacement = (
    ProviderDeviceBinding,
    ObjectSlot,
    CloudObjectVersion,
    ConditionalWriteOutcome,
);

struct ProbeHome {
    inner: Arc<dyn ExactCloudHome>,
    replacements: Arc<Mutex<Vec<Replacement>>>,
    accept_stale_revisions: bool,
}

#[async_trait]
impl ExactSlotStorage for ProbeHome {
    async fn provider_binding(
        &self,
    ) -> Result<coven_protocol::objects::ResolvedProviderBinding, CloudHomeError> {
        self.inner.as_ref().provider_binding().await
    }

    async fn cross_principal_evidence(
        &self,
    ) -> Result<coven_protocol::provider::CrossPrincipalProviderEvidence, CloudHomeError> {
        self.inner.as_ref().cross_principal_evidence().await
    }

    async fn allocate_slot(&self, logical_key: &str) -> Result<ObjectSlot, CloudHomeError> {
        self.inner.as_ref().allocate_slot(logical_key).await
    }

    async fn list_slots(&self, prefix: &str) -> Result<Vec<ObjectSlot>, CloudHomeError> {
        self.inner.as_ref().list_slots(prefix).await
    }

    async fn create_at(
        &self,
        upload: &ExactUpload<'_>,
        control: &UploadControl,
    ) -> Result<ExactCreateOutcome, CloudHomeError> {
        self.inner.as_ref().create_at(upload, control).await
    }

    async fn create_versioned_at(
        &self,
        upload: &ExactUpload<'_>,
        control: &UploadControl,
    ) -> Result<ExactCreateOutcome, CloudHomeError> {
        self.inner
            .as_ref()
            .create_versioned_at(upload, control)
            .await
    }

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.as_ref().read_at(slot).await
    }

    async fn read_versioned_at(
        &self,
        slot: &ObjectSlot,
    ) -> Result<CloudVersionedObject, CloudHomeError> {
        self.inner.as_ref().read_versioned_at(slot).await
    }

    async fn replace_at_if_version(
        &self,
        slot: &ObjectSlot,
        expected: &CloudObjectVersion,
        bytes: Vec<u8>,
    ) -> Result<ConditionalWriteOutcome, CloudHomeError> {
        let principal = self.inner.provider_binding().await?.device;
        let revision = if self.accept_stale_revisions {
            self.inner.read_versioned_at(slot).await?.version
        } else {
            expected.clone()
        };
        let result = self
            .inner
            .replace_at_if_version(slot, &revision, bytes)
            .await?;
        self.replacements.lock().unwrap().push((
            principal,
            slot.clone(),
            expected.clone(),
            result.clone(),
        ));
        Ok(result)
    }

    async fn observe_at(
        &self,
        slot: &ObjectSlot,
    ) -> Result<Option<coven_protocol::objects::ExactObjectRef>, CloudHomeError> {
        self.inner.as_ref().observe_at(slot).await
    }

    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.as_ref().read_range_at(slot, start, end).await
    }

    async fn open_stream_at(&self, slot: &ObjectSlot) -> Result<CloudObjectStream, CloudHomeError> {
        self.inner.as_ref().open_stream_at(slot).await
    }

    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        self.inner.as_ref().delete_at(slot).await
    }

    async fn delete_versioned_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        self.inner.as_ref().delete_versioned_at(slot).await
    }

    async fn delete_and_verify_absent(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        self.inner.as_ref().delete_and_verify_absent(slot).await
    }
}

#[async_trait]
impl CloudHome for ProbeHome {
    fn multipart_threshold(&self) -> u64 {
        self.inner.multipart_threshold()
    }

    async fn probe(&self) -> Result<(), CloudHomeError> {
        self.inner.as_ref().probe().await
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.inner.as_ref().put_object(key, data).await
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        self.inner.as_ref().open_multipart(key, total_len).await
    }

    async fn write(
        &self,
        key: &str,
        body: BlobBody,
        progress: &UploadProgress,
    ) -> Result<(), CloudHomeError> {
        self.inner.as_ref().write(key, body, progress).await
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.as_ref().read(key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.as_ref().read_range(key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        self.inner.as_ref().list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.inner.as_ref().delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        self.inner.as_ref().exists(key).await
    }

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        self.inner.as_ref().set_access(desired).await
    }
}

#[derive(Default)]
struct ProbeJournal(Mutex<Option<ProviderProbeJournalRecord>>);

#[async_trait]
impl ProviderProbeJournal for ProbeJournal {
    async fn load(
        &self,
        probe_id: ProviderProbeId,
    ) -> Result<Option<ProviderProbeJournalRecord>, StorageError> {
        let value = self.0.lock().unwrap().clone();
        if let Some(record) = &value {
            assert_eq!(record.probe_id(), probe_id);
        }
        Ok(value)
    }

    async fn begin(
        &self,
        prepared: ProviderProbeJournalRecord,
    ) -> Result<ProviderProbeJournalRecord, StorageError> {
        prepared.validate_begin()?;
        let mut state = self.0.lock().unwrap();
        match state.as_ref() {
            Some(existing) => {
                assert_eq!(existing, &prepared);
                Ok(existing.clone())
            }
            None => {
                *state = Some(prepared.clone());
                Ok(prepared)
            }
        }
    }

    async fn advance(
        &self,
        previous: &ProviderProbeJournalRecord,
        next: ProviderProbeJournalRecord,
    ) -> Result<(), StorageError> {
        previous.validate_transition(&next)?;
        let mut state = self.0.lock().unwrap();
        assert_eq!(state.as_ref(), Some(previous));
        *state = Some(next);
        Ok(())
    }
}

#[derive(Default)]
struct ChallengeJournal(Mutex<Option<DeviceJoinChallengePublicationRecord>>);

#[async_trait]
impl DeviceJoinChallengePublicationJournal for ChallengeJournal {
    async fn prepare(
        &self,
        challenge: &CrossPrincipalProbeChallenge,
    ) -> Result<DeviceJoinChallengePublicationRecord, StorageError> {
        let mut state = self.0.lock().unwrap();
        match state.as_ref() {
            Some(existing) => {
                assert_eq!(&existing.challenge, challenge);
                Ok(existing.clone())
            }
            None => {
                let record = DeviceJoinChallengePublicationRecord {
                    challenge: challenge.clone(),
                    progress: DeviceJoinChallengePublicationProgress::Prepared,
                };
                *state = Some(record.clone());
                Ok(record)
            }
        }
    }

    async fn claim_published(
        &self,
        authorization: &DeviceJoinChallengePublicationAuthorization,
        challenge: &CrossPrincipalProbeChallenge,
    ) -> Result<(), StorageError> {
        let mut state = self.0.lock().unwrap();
        let record = state
            .as_mut()
            .expect("challenge prepared before accepted activation");
        assert_eq!(&record.challenge, challenge);
        record.progress = DeviceJoinChallengePublicationProgress::Published {
            authorization: authorization.clone(),
        };
        Ok(())
    }
}

fn object(label: &str) -> ExactObjectRef {
    ExactObjectRef::new(
        ObjectSlot::logical(label.into()).unwrap(),
        label.len() as u64,
        ObjectHash::digest(label.as_bytes()),
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProbeScenario {
    Uninterrupted,
    PeerResponseLost,
    CompletionCleanupInterrupted,
    ProviderAcceptsStaleRevision,
}

#[tokio::test]
async fn cross_principal_probe_requires_both_accounts_to_compete_on_one_revision() {
    exercise_cross_principal_probe(ProbeScenario::Uninterrupted).await;
}

#[tokio::test]
async fn cross_principal_probe_reuses_the_original_revision_after_a_lost_peer_response() {
    exercise_cross_principal_probe(ProbeScenario::PeerResponseLost).await;
}

#[tokio::test]
async fn cross_principal_probe_reopens_verified_evidence_after_interrupted_cleanup() {
    exercise_cross_principal_probe(ProbeScenario::CompletionCleanupInterrupted).await;
}

#[tokio::test]
async fn cross_principal_probe_refuses_a_provider_that_accepts_a_stale_revision() {
    exercise_cross_principal_probe(ProbeScenario::ProviderAcceptsStaleRevision).await;
}

async fn exercise_cross_principal_probe(scenario: ProbeScenario) {
    let administrator = UserKeypair::generate();
    let peer = UserKeypair::generate();
    let store = StoreProviderBinding::Dropbox {
        namespace_id: "cross-conditional-namespace".into(),
    };
    let binding = |name: &str| ResolvedProviderBinding {
        store: store.clone(),
        device: ProviderDeviceBinding {
            principal: ProviderPrincipalId::Dropbox {
                account_id: name.into(),
            },
        },
    };
    let administrator_binding = binding("administrator");
    let peer_binding = binding("peer");
    let home = crate::InMemoryCloudHome::new();
    let replacements = Arc::new(Mutex::new(Vec::new()));
    let administrator_provider = Arc::new(ProbeHome {
        inner: Arc::new(
            home.clone()
                .with_provider_binding(administrator_binding.clone()),
        ),
        replacements: replacements.clone(),
        accept_stale_revisions: scenario == ProbeScenario::ProviderAcceptsStaleRevision,
    });
    let administrator_storage = ProviderProbeStorage::new(administrator_provider.clone());
    let peer_provider = Arc::new(ProbeHome {
        inner: Arc::new(home.clone().with_provider_binding(peer_binding.clone())),
        replacements: replacements.clone(),
        accept_stale_revisions: false,
    });
    let peer_storage = ProviderProbeStorage::new(peer_provider.clone());
    let probe_id = ProviderProbeId::from_bytes([49; 32]);
    let root = StoreRootRef {
        store_root_id: ObjectHash::digest(b"probe root id"),
        store_root_hash: ObjectHash::digest(b"probe root"),
        object: object("roots/probe.json"),
    };
    let registration = StoreDeviceRegistrationRef {
        device_id: ObjectHash::digest(b"probe administrator device")
            .to_string()
            .parse()
            .unwrap(),
        registration_hash: ObjectHash::digest(b"probe registration"),
        object: object("registrations/probe.json"),
    };
    let context = CrossPrincipalChallengeContext {
        root,
        attempt_id: DeviceJoinAttemptId::from_hash(ObjectHash::digest(b"probe attempt")),
        access_request_hash: ObjectHash::digest(b"probe access"),
        provider_admin_grant: ProviderAdminGrantId(ObjectHash::digest(b"probe admin grant")),
        owner_registration: registration,
        member_pubkey: coven_keys::keys::public_key_hex(&peer),
        administrator_binding: administrator_binding.device.clone(),
        peer_binding: peer_binding.device.clone(),
    };
    let publication = ChallengeJournal::default();
    let challenge = administrator_storage
        .prepare_cross_principal_challenge(&publication, probe_id, &store, &context, &administrator)
        .await
        .unwrap();
    let authorization = DeviceJoinChallengePublicationAuthorization {
        attempt_id: context.attempt_id,
        attempt_activation: StoreBatchCommitRef {
            coord: StoreCommitCoord {
                stream_id: coven_protocol::causal_grants::AuthorStreamId::from_digest(
                    ObjectHash::digest(b"probe stream"),
                ),
                sequence: 1,
            },
            commit_hash: ObjectHash::digest(b"accepted probe activation"),
            object: object("commits/probe-activation.json"),
        },
    };
    administrator_storage
        .settle_cross_principal_challenge(
            &publication,
            &authorization,
            &challenge,
            &context,
            &store,
        )
        .await
        .unwrap();
    let response_context = CrossPrincipalResponseContext {
        challenge: context,
        expected_registration_hash: ObjectHash::digest(b"peer registration"),
        response_slot: peer_storage
            .reserve_cross_principal_response_slot(probe_id)
            .await
            .unwrap(),
    };
    let initial = home
        .read_versioned_at(&challenge.conditional_slot)
        .await
        .unwrap();
    if scenario == ProbeScenario::PeerResponseLost {
        home.lose_next_conditional_replace_response();
        let error = peer_storage
            .create_cross_principal_response(
                &challenge,
                &response_context,
                &store,
                &coven_keys::keys::public_key_hex(&administrator),
                &peer,
            )
            .await
            .expect_err("lost response stays explicit to the initiator");
        assert!(matches!(error, ProviderProbeError::Storage(_)));
        let retained: CrossPrincipalConditionalStart =
            serde_json::from_slice(&home.read_at(&response_context.response_slot).await.unwrap())
                .unwrap();
        assert_eq!(retained.version, initial.version);
        assert_ne!(
            home.read_versioned_at(&challenge.conditional_slot)
                .await
                .unwrap()
                .version,
            initial.version
        );
    }
    let peer_storage = ProviderProbeStorage::new(peer_provider);
    let response = peer_storage
        .create_cross_principal_response(
            &challenge,
            &response_context,
            &store,
            &coven_keys::keys::public_key_hex(&administrator),
            &peer,
        )
        .await
        .unwrap();
    assert_eq!(response.conditional_start.version, initial.version);
    let mut journal = ProbeJournal::default();
    if scenario == ProbeScenario::CompletionCleanupInterrupted {
        home.fail_nth_exact_delete_of(&[&response.peer_object.slot], 1);
        administrator_storage
            .complete_cross_principal_probe(
                &journal,
                &challenge,
                &response,
                &response_context,
                &store,
                &administrator,
                &coven_keys::keys::public_key_hex(&peer),
            )
            .await
            .expect_err("response deletion interrupts completion after verified CAS");
        assert!(matches!(
            home.read_versioned_at(&challenge.conditional_slot).await,
            Err(CloudHomeError::NotFound(_))
        ));
        assert_eq!(
            home.read_at(&response.peer_object.slot).await.unwrap(),
            response.conditional_start.canonical_bytes()
        );
        let persisted = journal.load(probe_id).await.unwrap().unwrap();
        let ProviderProbeJournalRecord::CrossPrincipal(ref state) = persisted else {
            panic!("cross-principal journal expected")
        };
        assert!(matches!(
            state.progress,
            CrossPrincipalCompletionProgress::ReadsVerified { .. }
        ));
        journal = ProbeJournal(Mutex::new(Some(
            serde_json::from_slice(&serde_json::to_vec(&persisted).unwrap()).unwrap(),
        )));
    }
    let administrator_storage = ProviderProbeStorage::new(administrator_provider);
    let result = administrator_storage
        .complete_cross_principal_probe(
            &journal,
            &challenge,
            &response,
            &response_context,
            &store,
            &administrator,
            &coven_keys::keys::public_key_hex(&peer),
        )
        .await;
    if scenario == ProbeScenario::ProviderAcceptsStaleRevision {
        assert!(matches!(result, Err(ProviderProbeError::InvalidReceipt(_))));
        let ProviderProbeJournalRecord::CrossPrincipal(state) =
            journal.load(probe_id).await.unwrap().unwrap()
        else {
            panic!("cross-principal journal expected")
        };
        assert!(matches!(
            state.progress,
            CrossPrincipalCompletionProgress::Prepared
        ));
        assert_eq!(
            home.read_at(&response.peer_object.slot).await.unwrap(),
            response.conditional_start.canonical_bytes()
        );
        return;
    }
    let receipt = result.unwrap();
    receipt
        .verify(
            &response_context,
            &store,
            &coven_keys::keys::public_key_hex(&administrator),
            &coven_keys::keys::public_key_hex(&peer),
        )
        .unwrap();
    assert_eq!(
        receipt.transcript.conditional.slot,
        challenge.conditional_slot
    );
    assert_eq!(
        receipt.transcript.conditional.contenders[0].outcome,
        ProbeConditionalOutcome::Replaced
    );
    assert_eq!(
        receipt.transcript.conditional.contenders[1].outcome,
        ProbeConditionalOutcome::RejectedRevision
    );
    assert!(matches!(
        home.read_versioned_at(&challenge.conditional_slot).await,
        Err(CloudHomeError::NotFound(_))
    ));
    assert!(matches!(
        home.read_at(&response.peer_object.slot).await,
        Err(CloudHomeError::NotFound(_))
    ));
    assert!(matches!(
        home.read_at(&challenge.administrator_object.slot).await,
        Err(CloudHomeError::NotFound(_))
    ));
    let settled = administrator_storage
        .complete_cross_principal_probe(
            &journal,
            &challenge,
            &response,
            &response_context,
            &store,
            &administrator,
            &coven_keys::keys::public_key_hex(&peer),
        )
        .await
        .unwrap();
    assert_eq!(
        settled, receipt,
        "completed receipt retries do not recreate deleted probes"
    );
    let mut invalid = receipt.transcript.clone();
    invalid.conditional.contenders[1].outcome = ProbeConditionalOutcome::Replaced;
    assert!(
        CrossPrincipalProbeReceipt::signed(invalid, &response_context, &store, &administrator)
            .is_err()
    );
    let attempts = replacements.lock().unwrap();
    assert_eq!(
        attempts.len(),
        2,
        "admission requires actual competing updates by both provider principals"
    );
    assert_eq!(attempts[0].0, peer_binding.device);
    assert_eq!(attempts[1].0, administrator_binding.device);
    assert_eq!(
        attempts[0].1, attempts[1].1,
        "both accounts must target the same provider object"
    );
    assert_eq!(
        attempts[0].2, attempts[1].2,
        "both contenders must use the same observed revision"
    );
    if scenario == ProbeScenario::PeerResponseLost {
        assert_eq!(attempts[0].3, ConditionalWriteOutcome::VersionChanged);
    } else {
        assert!(matches!(
            attempts[0].3,
            ConditionalWriteOutcome::Replaced(_)
        ));
    }
    assert_eq!(attempts[1].3, ConditionalWriteOutcome::VersionChanged);
}
