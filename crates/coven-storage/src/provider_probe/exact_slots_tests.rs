use super::*;
use crate::provider_probe::tests::ProbeJournal;
use std::collections::BTreeSet;

fn exact_key(probe_id: ProviderProbeId) -> String {
    format!("__coven_probe__/exact/{}", hex::encode(probe_id.as_bytes()))
}

fn conditional_key(probe_id: ProviderProbeId) -> String {
    format!(
        "__coven_probe__/conditional/{}",
        hex::encode(probe_id.as_bytes())
    )
}

fn durable_exact(record: ProviderProbeJournalRecord) -> ExactProbeJournal {
    let ProviderProbeJournalRecord::Exact(record) = record else {
        panic!("exact probe journal expected")
    };
    record
}

/// The reserved slot settles a create whose outcome the probe never recorded:
/// the winner's bytes stand, and the rerun recognises the winner by reading
/// them back rather than by rehearsing the loss against a slot of its own.
#[tokio::test]
async fn exact_probe_settles_a_lost_create_response() {
    let home = crate::InMemoryCloudHome::new();
    let binding = home.provider_binding().await.unwrap();
    let storage = ProviderProbeStorage::new(Arc::new(home.clone()));
    let journal = ProbeJournal::default();
    let probe_id = ProviderProbeId::from_bytes([17; 32]);
    let winner = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateFirst);

    // The second contender's create never reaches the provider, so the run ends
    // with the first contender's object standing and no durable record of who
    // won. The create response is lost with the run that issued it.
    home.fail_exact_create_before_call(2);
    let interrupted = storage
        .probe_exact_slots(&journal, probe_id, &binding)
        .await;
    assert!(
        interrupted.is_err(),
        "an unresolved create race fails its initiator"
    );
    let record = durable_exact(journal.load(probe_id).await.unwrap().unwrap());
    assert!(matches!(record.progress, ExactProbeProgress::Prepared));
    assert_eq!(
        home.stored_exact_bytes(&record.slot).as_deref(),
        Some(winner.as_slice()),
        "the winner's bytes outlive the run that wrote them"
    );

    let receipt = storage
        .probe_exact_slots(&journal, probe_id, &binding)
        .await
        .expect("the reserved slot settles the unknown create result");
    assert_eq!(
        receipt
            .transcript
            .contenders
            .iter()
            .map(|attempt| attempt.outcome)
            .collect::<Vec<_>>(),
        vec![
            ProbeCreateOutcome::Created,
            ProbeCreateOutcome::RejectedOccupied
        ],
    );
    assert_eq!(
        receipt.transcript.accepted,
        ExactObjectRef::new(
            record.slot.clone(),
            winner.len() as u64,
            ObjectHash::digest(&winner),
        ),
        "the accepted reference names the object the interrupted race left behind"
    );
    let reserved = exact_key(probe_id);
    assert_eq!(
        home.deletes_seen()
            .iter()
            .filter(|key| **key == reserved)
            .count(),
        1,
        "one object occupied the slot across both runs, and cleanup removed it once"
    );
    receipt.verify(&binding.store, &binding.device).unwrap();
}

/// Cleanup that fails part-way keeps the conditional evidence in the journal,
/// so the rerun finishes deleting and the run after that answers from the
/// receipt alone.
#[tokio::test]
async fn exact_probe_resumes_interrupted_cleanup() {
    let home = crate::InMemoryCloudHome::new();
    let binding = home.provider_binding().await.unwrap();
    let storage = ProviderProbeStorage::new(Arc::new(home.clone()));
    let journal = ProbeJournal::default();
    let probe_id = ProviderProbeId::from_bytes([23; 32]);

    home.fail_exact_delete_on_call(1);
    let interrupted = storage
        .probe_exact_slots(&journal, probe_id, &binding)
        .await;
    assert!(
        matches!(interrupted, Err(ProviderProbeError::Storage(_))),
        "a refused delete reaches the initiator as a storage failure"
    );
    let record = durable_exact(journal.load(probe_id).await.unwrap().unwrap());
    assert!(matches!(
        record.progress,
        ExactProbeProgress::ConditionalVerified { .. }
    ));

    let creates = home.exact_create_count();
    let receipt = storage
        .probe_exact_slots(&journal, probe_id, &binding)
        .await
        .expect("the rerun finishes the cleanup the failed delete interrupted");
    assert_eq!(
        home.exact_create_count(),
        creates,
        "resumed cleanup recreates neither slot"
    );
    assert_eq!(home.stored_exact_bytes(&record.slot), None);
    assert_eq!(home.stored_exact_bytes(&record.conditional_slot), None);
    assert!(matches!(
        durable_exact(journal.load(probe_id).await.unwrap().unwrap()).progress,
        ExactProbeProgress::ReceiptReady { .. }
    ));

    let deletes = home.exact_delete_count();
    let settled = storage
        .probe_exact_slots(&journal, probe_id, &binding)
        .await
        .expect("a terminal receipt answers without touching the provider");
    assert_eq!(settled, receipt);
    assert_eq!(home.exact_create_count(), creates);
    assert_eq!(home.exact_delete_count(), deletes);
}

/// The probe reserves the exact and conditional slots and nothing else: no
/// third object is ever created, and none is left behind.
#[tokio::test]
async fn exact_probe_receipt_has_no_third_slot() {
    let home = crate::InMemoryCloudHome::new();
    let binding = home.provider_binding().await.unwrap();
    let storage = ProviderProbeStorage::new(Arc::new(home.clone()));
    let journal = ProbeJournal::default();
    let probe_id = ProviderProbeId::from_bytes([31; 32]);

    let receipt = storage
        .probe_exact_slots(&journal, probe_id, &binding)
        .await
        .unwrap();
    assert_eq!(receipt.transcript.logical_key, exact_key(probe_id));
    assert_eq!(
        receipt.transcript.conditional.logical_key,
        conditional_key(probe_id)
    );
    assert_eq!(
        home.exact_creates()
            .iter()
            .map(|slot| slot.logical_key().to_string())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([exact_key(probe_id), conditional_key(probe_id)]),
        "the probe only ever creates the two slots its receipt names"
    );
    assert!(
        home.keys().is_empty(),
        "a completed probe leaves the provider as it found it: {:?}",
        home.keys()
    );
}
