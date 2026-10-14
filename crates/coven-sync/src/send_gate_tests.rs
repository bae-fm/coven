use super::*;
use coven_foundation::{
    clock::{Clock, FixedClock},
    id_source::{DeviceId, StoreId},
};
use coven_storage::{test_utils::MemoryStorage, Storage};
use std::{sync::Arc, time::UNIX_EPOCH};

fn path() -> ObjectPath {
    ObjectPath::positions(StoreId(uuid::Uuid::from_u128(1)), DeviceId(1))
}
fn complete(gate: &SendGate, binding: Binding) {
    gate.complete(binding, Vec::new()).unwrap();
}

#[test]
fn freshness_counts_from_start_including_suspension_and_excludes_equality() {
    for duration in [
        Duration::from_millis(299_999),
        Duration::from_secs(300),
        Duration::from_secs(86_400),
    ] {
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
        let gate = SendGate::new(clock.clone());
        assert!(gate.check(&path()).is_err());
        gate.begin();
        complete(&gate, Binding::Device);
        clock.set(UNIX_EPOCH + duration);
        assert_eq!(
            gate.check(&path()).is_ok(),
            duration < Duration::from_secs(300)
        );
    }
}

#[test]
fn incomplete_and_slow_observations_cannot_refresh_permission() {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let gate = SendGate::new(clock.clone());
    gate.begin();
    complete(&gate, Binding::Device);
    clock.set(UNIX_EPOCH + Duration::from_secs(299));
    gate.begin();
    assert!(
        gate.check(&path()).is_ok(),
        "beginning does not discard the prior completed observation"
    );
    clock.set(UNIX_EPOCH + Duration::from_secs(300));
    assert!(
        gate.check(&path()).is_err(),
        "an unfinished phase cannot renew it"
    );
    clock.set(UNIX_EPOCH + Duration::from_secs(599));
    complete(&gate, Binding::Device);
    assert!(
        gate.check(&path()).is_err(),
        "a phase taking five minutes is already expired"
    );
    gate.begin();
    complete(&gate, Binding::Device);
    assert!(gate.check(&path()).is_ok());
}

#[test]
fn reconnect_reset_and_retirement_close_every_ordinary_path() {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let gate = SendGate::new(clock.clone());
    for binding in [
        Binding::Member,
        Binding::Absent,
        Binding::Retired,
        Binding::Device,
    ] {
        gate.begin();
        complete(&gate, binding);
        assert_eq!(gate.check(&path()).is_ok(), binding == Binding::Device);
    }
    gate.invalidate();
    assert!(gate.check(&path()).is_err());
    assert!(gate.complete(Binding::Device, Vec::new()).is_err());
    assert!(SendGate::new(clock).check(&path()).is_err());
}

#[test]
fn bootstrap_authorizes_only_its_paths_and_registration_requires_current_membership() {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let gate = SendGate::new(clock.clone());
    let object = ObjectPath::store_log(path().store(), DeviceId(1), std::num::NonZeroU64::MIN);
    {
        let _scope = gate.bootstrap([object.clone()].into(), false);
        assert!(gate.check(&object).is_ok());
        assert!(gate.check(&path()).is_err());
    }
    assert!(gate.check(&object).is_err());
    let _scope = gate.bootstrap([object.clone()].into(), true);
    assert!(gate.check(&object).is_err());
    gate.begin();
    complete(&gate, Binding::Member);
    assert!(gate.check(&object).is_ok());
    assert!(gate.check(&path()).is_err());
    clock.set(UNIX_EPOCH + Duration::from_secs(300));
    assert!(gate.check(&object).is_err());
}

#[tokio::test]
async fn multipart_expiry_keeps_confirmed_progress_until_a_new_catch_up() {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let gate = Arc::new(SendGate::new(clock.clone()));
    let storage = MemoryStorage::builder()
        .transfer_limits(4, 4)
        .build()
        .unwrap()
        .with_publication_gate(gate.clone());
    gate.begin();
    complete(&gate, Binding::Device);
    let object = ObjectPath::file(
        path().store(),
        DeviceId(1),
        coven_foundation::id_source::FileId(uuid::Uuid::from_u128(2)),
    );
    let mut session = storage.begin_upload(&object, 8).await.unwrap();
    storage.upload_part(&mut session, b"abcd").await.unwrap();
    let before = session.encode().unwrap();
    let requests = storage.request_count();
    clock.set(clock.now() + Duration::from_secs(300));
    assert_eq!(
        storage
            .upload_part(&mut session, b"efgh")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::CatchUpRequired
    );
    assert_eq!(session.encode().unwrap().as_bytes(), before.as_bytes());
    assert_eq!(storage.request_count(), requests);
    gate.begin();
    complete(&gate, Binding::Device);
    storage.resume_upload(&mut session).await.unwrap();
    storage.upload_part(&mut session, b"efgh").await.unwrap();
    clock.set(clock.now() + Duration::from_secs(300));
    let requests = storage.request_count();
    assert_eq!(
        storage
            .finish_upload(&mut session)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::CatchUpRequired
    );
    assert_eq!(storage.request_count(), requests);
    gate.begin();
    complete(&gate, Binding::Device);
    storage.finish_upload(&mut session).await.unwrap();
    assert_eq!(storage.read(&object).await.unwrap(), b"abcdefgh");
}

#[test]
#[ignore = "requires COVEN_IO_RUNNER"]
fn io_differential_send_gate() {
    use serde_json::json;
    let mut cases = Vec::new();
    let mut results = Vec::new();
    for binding in [Binding::Device, Binding::Retired, Binding::Absent] {
        for finish in [0, 299, 300, 301] {
            let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
            let gate = SendGate::new(clock.clone());
            let mut actions = Vec::new();
            let mut allowed = Vec::new();
            gate.begin();
            actions.push(json!({"op":"begin","time":0}));
            allowed.push(true);
            clock.set(UNIX_EPOCH + Duration::from_secs(finish));
            complete(&gate, binding);
            actions.push(json!({"op":"checked","time":finish,"send":binding == Binding::Device,"retired":binding == Binding::Retired}));
            allowed.push(true);
            for now in [finish, finish.max(299), finish.max(300), finish.max(600)] {
                clock.set(UNIX_EPOCH + Duration::from_secs(now));
                actions.push(json!({"op":"request","time":now}));
                allowed.push(gate.check(&path()).is_ok());
            }
            gate.invalidate();
            actions.push(json!({"op":"invalidate"}));
            allowed.push(true);
            actions.push(json!({"op":"request","time":600}));
            allowed.push(gate.check(&path()).is_ok());
            cases.push(json!({"check":"send_gate","actions":actions}));
            results.push(json!(allowed));
        }
    }
    assert_eq!(results, crate::discovery::tests::io_runner(&cases));
}

#[test]
#[ignore = "requires COVEN_RECEIVE_LEAN"]
fn mg_differential_send_gate_publication_bound() {
    use serde_json::json;
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let mut cases = Vec::new();
    let mut results = Vec::new();
    for final_at in [1, 301, 86_400] {
        for start in [final_at - 1, final_at, final_at + 1] {
            for elapsed in [0, 299, 300] {
                for duration in [0, 86_400] {
                    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(start)));
                    let gate = SendGate::new(clock.clone());
                    gate.begin();
                    complete(
                        &gate,
                        if start >= final_at {
                            Binding::Retired
                        } else {
                            Binding::Device
                        },
                    );
                    clock.set(clock.now() + Duration::from_secs(elapsed));
                    let allowed = gate.check(&path()).is_ok();
                    let stored = start + elapsed + duration;
                    if allowed {
                        assert!(stored < final_at + 300 + 86_400);
                    }
                    cases.push(json!({"check":"publication","final":final_at,"start":start,"sent":start+elapsed,"stored":stored}));
                    results.push(json!(allowed));
                }
            }
        }
    }
    let runner = std::env::var("COVEN_RECEIVE_LEAN").unwrap();
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
    assert_eq!(
        results,
        serde_json::from_slice::<Vec<serde_json::Value>>(&output.stdout).unwrap()
    );
}

#[test]
fn overlapping_provider_checks_keep_independent_scopes_and_invalidation_revokes_both() {
    let gate = SendGate::new(Arc::new(FixedClock::new(UNIX_EPOCH)));
    let first = path();
    let second = ObjectPath::clock(first.store(), DeviceId(2));
    let one = gate.provider_check(first.clone());
    let two = gate.provider_check(second.clone());
    assert!(gate.check(&first).is_ok());
    assert!(gate.check(&second).is_ok());
    drop(one);
    assert!(gate.check(&first).is_err());
    assert!(gate.check(&second).is_ok());
    gate.invalidate();
    assert!(gate.check(&second).is_err());
    let three = gate.provider_check(first.clone());
    drop(two);
    assert!(gate.check(&first).is_ok());
    drop(three);
    assert!(gate.check(&first).is_err());
}
