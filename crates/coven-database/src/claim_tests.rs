use crate::test_utils::tests::{run_lean, Generator};
use crate::tests::TestStore;
use crate::{ClaimAttempt, ClaimRead, ClaimSettlement, ClaimWork};
use coven_crypto::{ObjectDigest, ObjectHasher};

fn digest(bytes: &[u8]) -> ObjectDigest {
    let mut hash = ObjectHasher::new();
    hash.update(bytes);
    hash.finish()
}
use coven_format::path::ObjectPath;
use serde_json::json;
use std::io::Cursor;

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_settlement_matches_lean() {
    let runner = std::env::var_os("COVEN_SEQUENCE_LEAN").expect("sequenceRunner");
    let store = TestStore::new();
    let db = store
        .schema(vec![], "CREATE TABLE local(value TEXT)")
        .await
        .unwrap();
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
                Ok(ClaimAttempt {
                    path,
                    bytes: Cursor::new(saved),
                })
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
        let covered = if *same_digest {
            digest.as_bytes()
        } else {
            other.as_bytes()
        };
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
        assert_eq!(
            db.current_claim().await.unwrap().unwrap().settlement,
            result
        );
        // End the retry fixture with an authoritative competing occupant.
        if result == ClaimSettlement::Retry {
            db.settle_claim(ClaimRead::Kept(Some(Box::new(Cursor::new(
                b"competitor".to_vec(),
            )))))
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
