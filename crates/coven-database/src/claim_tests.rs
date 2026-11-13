use crate::test_utils::tests::{run_lean, Generator};
use crate::tests::TestStore;
use crate::{ClaimAttempt, ClaimRead, ClaimSettlement, ClaimWork, DatabaseBuilder, Migration};
use coven_crypto::{ObjectDigest, ObjectHasher};
use coven_foundation::files::StoreDirAccess;

fn digest(bytes: &[u8]) -> ObjectDigest {
    let mut hash = ObjectHasher::new();
    hash.update(bytes);
    hash.finish()
}
use coven_format::{
    path::ObjectPath, sealed_snapshot::SnapshotObjectPrefix, snapshot::SnapshotRecord,
    snapshot_stream::SnapshotChunkDecoder, test_utils::fixture_bytes as hex,
};
use coven_foundation::{
    files::StoreDirectories,
    id_source::{DeviceId, IdSource, SequentialIds},
};
use serde_json::json;
use std::io::Cursor;

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_settlement_matches_lean() {
    let runner = std::env::var_os("COVEN_SEQUENCE_LEAN").expect("sequenceRunner");
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE local(value TEXT)").await.unwrap();
    // kept observations have already passed sync's authenticated own-id check;
    // its copy detection is exercised in the sync settlement differential.
    let mut cases = Vec::new();
    for slot in 1..=3 {
        for first in 1..=4 {
            for relative in 0..4 {
                for same_digest in [false, true] {
                    for current in 0..3 {
                        cases.push((slot, first, relative, same_digest, current));
                    }
                }
            }
        }
    }
    let exhaustive = cases.len();
    let mut rng = Generator::new(0x5e771e);
    for _ in 0..128 {
        cases.push((
            1 + rng.pick(100) as u64,
            1 + rng.pick(101) as u64,
            rng.pick(4),
            rng.pick(2) == 0,
            rng.pick(3),
        ));
    }
    for (slot, first, relative, same_digest, current) in &cases {
        let bytes = format!("attempt at {slot}").into_bytes();
        let path = ObjectPath::slot(store.id(), (*slot).try_into().unwrap());
        let saved = bytes.clone();
        let claim = db
            .begin_claim(ClaimWork::Writes(0), move |_| {
                Ok::<_, crate::DbError>(ClaimAttempt { path, bytes: Cursor::new(saved) })
            })
            .await
            .unwrap();
        let n = claim.number;
        let last = match relative {
            0 => 0,
            1 => n - 1,
            2 => n,
            3 => n + 1,
            _ => unreachable!(),
        };
        let other = digest(b"different complete bytes");
        let digest = digest(&bytes);
        let covered = if *same_digest { digest.as_bytes() } else { other.as_bytes() };
        let digest_token = format!("{:?}", digest.as_bytes());
        let current_json = match current {
            0 => serde_json::Value::Null,
            1 => {
                json!({"device":store.device().0,"number":n,"bytes":String::from_utf8(bytes.clone()).unwrap()})
            }
            2 => json!({"device":store.device().0+1,"number":n,"bytes":"different"}),
            _ => unreachable!(),
        };
        let input = json!({"kind":"settle",
            "recorded":{"device":store.device().0,"number":n,"slot":slot,"bytes":String::from_utf8(bytes.clone()).unwrap(),"digest":digest_token},
            "current":current_json,"floor":{"first_kept":first,
                "last_claims":if last==0 {vec![]} else {vec![json!([store.device().0,last,format!("{covered:?}")])]}}});
        let read = if first > slot {
            ClaimRead::Compacted {
                first: *first,
                covers: first - 1,
                last_claim: last,
                digest: (last > 0).then_some(*covered),
            }
        } else {
            ClaimRead::Kept(match current {
                0 => None,
                1 => Some(Box::new(Cursor::new(bytes))),
                2 => Some(Box::new(Cursor::new(b"different".to_vec()))),
                _ => unreachable!(),
            })
        };
        let result = db.settle_claim(read).await.unwrap();
        let rust = match result {
            ClaimSettlement::Landed => "landed",
            ClaimSettlement::NotLanded => "notLanded",
            ClaimSettlement::Retry => "retry",
            ClaimSettlement::DeviceIdInUse => "copy",
            ClaimSettlement::Unknown => panic!("unsettled"),
        };
        assert_eq!(json!(rust), run_lean(&runner, &input), "{input}");
        assert_eq!(db.current_claim().await.unwrap().unwrap().settlement, result);
        // End the retry fixture with an authoritative competing occupant.
        if result == ClaimSettlement::Retry {
            db.settle_claim(ClaimRead::Kept(Some(Box::new(Cursor::new(b"competitor".to_vec())))))
                .await
                .unwrap();
        }
        db.finish_claim(None).await.unwrap();
    }
    eprintln!(
        "sequence settle: {exhaustive} exhaustive + {} seeded database cases",
        cases.len() - exhaustive
    );
    db.close().await.unwrap();
}

struct FixtureDevice(SequentialIds);
impl IdSource for FixtureDevice {
    fn new_id(&self) -> uuid::Uuid {
        self.0.new_id()
    }
    fn new_device_id(&self) -> DeviceId {
        DeviceId(1)
    }
}

#[tokio::test]
async fn pinned_last_claims_settle_lost_replies_for_every_slot_kind() {
    let snapshots = include_str!("../../coven-format/fixtures/snapshot-settlement-claims.hex");
    let slots = [
        (1, 1, include_str!("../../coven-format/fixtures/sealed-creation.hex")),
        (2, 2, include_str!("../../coven-format/fixtures/sealed-store-key.hex")),
        (6, 5, include_str!("../../coven-format/fixtures/sealed-writes-slot.hex")),
        (9, 8, include_str!("../../coven-format/fixtures/sealed-circle-announcement.hex")),
    ];
    assert_eq!(snapshots.lines().count(), slots.len());
    let mut cases: Vec<_> = slots
        .into_iter()
        .zip(snapshots.lines())
        .map(|((slot, number, object), snapshot)| (slot, number, object, hex(snapshot), None))
        .collect();
    let refused = include_str!("../../coven-format/fixtures/snapshot-claims.hex");
    for ((slot, number, object), snapshot) in [
        (9, 8, include_str!("../../coven-format/fixtures/sealed-refused-writes-slot.hex")),
        (10, 9, include_str!("../../coven-format/fixtures/sealed-reset-ignored-writes-slot.hex")),
    ]
    .into_iter()
    .zip(refused.lines())
    {
        let mut prefix =
            coven_format::test_utils::snapshot_prefix(&coven_format::test_utils::snapshot_header());
        prefix.covers = 10;
        cases.push((slot, number, object, hex(snapshot), Some(prefix)));
    }
    for (slot, number, object, snapshot, prefix) in cases {
        let temporary = tempfile::tempdir().unwrap();
        let directory = StoreDirectories::new(
            temporary.path().to_owned(),
            coven_foundation::task::HeardBugs::shared(),
        )
        .create_store_dir(coven_format::test_utils::STORE_ID, &FixtureDevice(SequentialIds::new()))
        .unwrap();
        let device = directory.settings().unwrap().device_id;
        let database = DatabaseBuilder::new(directory)
            .synced_tables(vec![])
            .migrations(vec![Migration::addition(1, "local", "CREATE TABLE local(value TEXT)")])
            .open()
            .await
            .unwrap();
        let bytes = hex(object);
        let path = coven_format::path::ObjectPath::slot(
            coven_format::test_utils::STORE_ID,
            slot.try_into().unwrap(),
        );
        for reserved in 1..=number {
            let saved = bytes.clone();
            let path = path.clone();
            let claim = database
                .begin_claim(ClaimWork::Writes(0), move |_| {
                    Ok::<_, crate::DbError>(ClaimAttempt { path, bytes: Cursor::new(saved) })
                })
                .await
                .unwrap();
            assert_eq!(claim.number, reserved);
            if reserved < number {
                database
                    .settle_claim(ClaimRead::Kept(Some(Box::new(Cursor::new(vec![])))))
                    .await
                    .unwrap();
                database.finish_claim(None).await.unwrap();
            }
        }
        let (prefix, mut input) = match prefix {
            Some(prefix) => (prefix, snapshot.as_slice()),
            None => {
                let length = SnapshotObjectPrefix::length(&snapshot).unwrap();
                (SnapshotObjectPrefix::decode(&snapshot[..length]).unwrap(), &snapshot[length..])
            }
        };
        let covers = prefix.covers;
        assert!(covers >= slot);
        let mut decoder = SnapshotChunkDecoder::new(prefix);
        let mut included = None;
        while let Some(record) =
            decoder.next_record(&mut input, &coven_format::test_utils::oracle()).unwrap()
        {
            if let SnapshotRecord::Claim(claim) = record {
                if claim.device == device {
                    assert!(included.replace(claim).is_none());
                }
            }
        }
        decoder.finish().unwrap();
        let included = included.unwrap();
        assert_eq!(included.device, device);
        assert_eq!(included.number, number);
        assert_eq!(
            database
                .settle_claim(ClaimRead::Compacted {
                    first: covers + 1,
                    covers,
                    last_claim: included.number,
                    digest: included.digest
                })
                .await
                .unwrap(),
            ClaimSettlement::Landed
        );
        database.finish_claim(None).await.unwrap();
        database.close().await.unwrap();
    }
}

/// Fill the claims' tables, thousands of rows each but one journal entry,
/// from the rows of the plan audit's `fixture` table, inside its transaction
/// (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_upload_claims SELECT device,n,counter(1) FROM fixture;
         INSERT INTO _coven_claim_counters SELECT n,n FROM fixture;
         INSERT INTO _coven_claim_journal SELECT device,n,NULL,i,'landed',1 FROM fixture WHERE i=1;
         INSERT INTO _coven_claim_attempts SELECT counter(1),counter(1),i,'store/seq/' || i,key,counter(32),NULL FROM fixture;
         INSERT INTO _coven_claim_chunks SELECT counter(1),counter(1),i,0,key FROM fixture;",
    )
    .unwrap();
}

/// §19.3: a stored claim whose settlement or work doesn't decode is
/// damage, not a query error.
#[tokio::test]
async fn a_stored_claim_that_does_not_decode_is_damaged_database() {
    for broken in [
        "UPDATE _coven_claim_journal SET settlement='lost'",
        "UPDATE _coven_claim_journal SET through=NULL",
    ] {
        let store = TestStore::new();
        let database = store.builder(vec![], vec![]).open().await.unwrap();
        let path = ObjectPath::slot(coven_format::test_utils::STORE_ID, 1.try_into().unwrap());
        database
            .begin_claim(ClaimWork::Writes(0), move |_| {
                Ok::<_, crate::DbError>(ClaimAttempt { path, bytes: Cursor::new(b"slot".to_vec()) })
            })
            .await
            .unwrap();
        database.inspect_writer(|db| {
            db.batch(&format!(
                "PRAGMA ignore_check_constraints=ON; {broken}; PRAGMA ignore_check_constraints=OFF;"
            ))
            .unwrap()
        });
        let read = database.current_claim().await;
        assert!(matches!(read, Err(crate::DbError::DamagedDatabase)), "{broken}: {read:?}");
        database.close().await.unwrap();
    }
}
