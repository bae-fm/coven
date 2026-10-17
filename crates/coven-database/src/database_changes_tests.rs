use crate::tests::TestStore;
use crate::upload::tests::attempt;
use crate::write::tests::{notes, NOTES};
use serde_json::{json, Value};
use std::collections::VecDeque;

/// Evaluate cases with the Lean IO runner that scripts/check.sh supplies.
pub(crate) fn io_runner(cases: &[Value]) -> Vec<Value> {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let runner = std::env::var("COVEN_IO_RUNNER").expect("Lean IO runner");
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(cases).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}

/// IO `Wakes`: at each pass start the sync loop's write-queue observation
/// reports whether an app write committed since the previous start; the
/// pass's own attempts and confirmations never count (SP, Waiting without repeated work).
#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER"]
async fn io_differential_wakes() {
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut pick = |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for _ in 0..24 {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        let mut changes = db.sync_changes();
        // Queued writes, oldest first, with the id of an attempted head.
        let mut queued: VecDeque<Option<crate::WriteId>> = VecDeque::new();
        let mut written = 0;
        let mut actions = Vec::new();
        let mut observed = Vec::new();
        for _ in 0..24 {
            let choice = pick(4);
            if choice == 0 || queued.is_empty() && choice != 3 {
                written += 1;
                let id = written.to_string();
                db.write(move |sql| {
                    sql.execute("INSERT INTO notes VALUES(?1,'title','body')", [&id])?;
                    Ok(())
                })
                .await
                .unwrap();
                queued.push_back(None);
                actions.push(json!({"op": "write"}));
            } else if choice == 3 {
                observed.push(json!(changes.take_changed().unwrap()));
                actions.push(json!({"op": "take"}));
            } else if let Some(Some(head)) = queued.front().copied() {
                if choice == 1 {
                    // Retrying the attempt keeps its upload session.
                    assert_eq!(attempt(&db, written as u8).await.unwrap(), Some(head));
                    actions.push(json!({"op": "update"}));
                } else {
                    crate::tests::publish_through(&db, head.number)
                        .await
                        .unwrap();
                    queued.pop_front();
                    actions.push(json!({"op": "remove"}));
                }
            } else {
                let head = attempt(&db, written as u8).await.unwrap();
                *queued.front_mut().unwrap() = head;
                actions.push(json!({"op": "update"}));
            }
        }
        cases.push(json!({"check": "wakes", "actions": actions}));
        actual.push(json!(observed));
        db.close().await.unwrap();
    }
    // The histories exercise both outcomes and both kinds of bookkeeping.
    let text = serde_json::to_string(&(&cases, &actual)).unwrap();
    for needed in ["\"remove\"", "\"update\"", "true", "false"] {
        assert!(text.contains(needed), "no {needed} in the histories");
    }
    assert_eq!(actual, io_runner(&cases));
}
