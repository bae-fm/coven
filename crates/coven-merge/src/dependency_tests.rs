use super::*;

fn provenance(generation: Option<u8>, setters: &[u8]) -> Provenance<u8> {
    Provenance {
        generation,
        setters: setters.to_vec(),
        execution: Vec::new(),
        causes: Vec::new(),
    }
}

fn verdicts(changes: &[(bool, Provenance<u8>)]) -> Vec<DataVerdict<u8>> {
    let mut verdicts: Vec<DataVerdict<u8>> = Vec::new();
    for (admitted, provenance) in changes {
        let verdict = classify(*admitted, provenance, |source: &u8| {
            Ok::<_, ()>(verdicts[usize::from(*source)].retained())
        })
        .unwrap();
        verdicts.push(verdict);
    }
    verdicts
}

/// A dropped creation freezes the edit that advanced its generation and the
/// delete replacing its cells, but not an unrelated change; chains follow.
#[test]
fn a_failed_source_freezes_its_dependents_and_their_dependents() {
    let result = verdicts(&[
        (false, provenance(None, &[])),
        (true, provenance(Some(0), &[0])),
        (true, provenance(None, &[])),
        (true, provenance(Some(1), &[1, 0])),
        (true, provenance(Some(2), &[2])),
    ]);
    assert_eq!(
        result,
        [
            DataVerdict::Dropped,
            DataVerdict::Dependent(vec![0, 0]),
            DataVerdict::Retained,
            DataVerdict::Dependent(vec![1, 1, 0]),
            DataVerdict::Retained,
        ]
    );
}

/// Only failed sources are named, in generation-then-setter order with
/// repeats; a dropped change's own policy decides its loss.
#[test]
fn failed_sources_keep_role_order_and_repeats() {
    let result = verdicts(&[
        (true, provenance(None, &[])),
        (false, provenance(None, &[])),
        (true, provenance(Some(1), &[0, 1, 1])),
        (false, provenance(Some(0), &[])),
    ]);
    assert_eq!(result[2], DataVerdict::Dependent(vec![1, 1, 1]));
    assert_eq!(result[3], DataVerdict::Dropped);
}

/// A source lookup's failure is the caller's, never a verdict.
#[test]
fn an_unavailable_source_is_returned_not_judged() {
    assert_eq!(
        classify(true, &provenance(Some(7), &[]), |_: &u8| Err::<bool, _>(
            "missing"
        )),
        Err("missing")
    );
}

/// Lean `DataDependency.retained` and `droppedSources` against [`classify`]
/// over generated rows: each change has its own admission, an optional
/// earlier generation source and earlier replaced setters, with repeats.
#[test]
#[ignore = "requires COVEN_RECEIVE_LEAN"]
fn mg_differential_data_dependency() {
    use serde_json::{json, Value};
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut next = |bound: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % bound
    };
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for _ in 0..400 {
        let length = 1 + next(6) as usize;
        let mut changes = Vec::new();
        for index in 0..length {
            let admitted = next(4) != 0;
            let earlier = |next: &mut dyn FnMut(u64) -> u64| next(index as u64) as u8;
            let generation = (index > 0 && next(3) != 0).then(|| earlier(&mut next));
            let setters: Vec<u8> = if index == 0 {
                Vec::new()
            } else {
                (0..next(4)).map(|_| earlier(&mut next)).collect()
            };
            changes.push((admitted, provenance(generation, &setters)));
        }
        let verdicts = verdicts(&changes);
        cases.push(json!({
            "check": "dependency",
            "changes": changes.iter().map(|(admitted, p)| json!({
                "base": admitted, "generation": p.generation, "setters": p.setters,
            })).collect::<Vec<_>>(),
        }));
        actual.push(Value::Array(
            verdicts
                .iter()
                .zip(&changes)
                .map(|(verdict, (_, p))| {
                    let dropped: Vec<u8> = match verdict {
                        DataVerdict::Dependent(sources) => sources.clone(),
                        // A dropped change's own failed sources, as Lean lists them.
                        DataVerdict::Dropped => p
                            .sources()
                            .filter(|s| !verdicts[usize::from(**s)].retained())
                            .copied()
                            .collect(),
                        DataVerdict::Retained => Vec::new(),
                    };
                    json!({"retained": verdict.retained(), "dropped": dropped})
                })
                .collect(),
        ));
    }
    let runner = std::env::var("COVEN_RECEIVE_LEAN").expect("Lean receive runner");
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&cases).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let expected: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(Value::Array(actual), expected);
}
