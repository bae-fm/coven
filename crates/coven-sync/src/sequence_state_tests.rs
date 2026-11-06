use super::*;
use coven_foundation::id_source::{DeviceId, ObjectId, StoreId};
use std::{future::Future, task::Poll};

fn check_object() -> ObjectPath {
    ObjectPath::file(
        StoreId(uuid::Uuid::from_u128(1)),
        DeviceId(2),
        ObjectId(uuid::Uuid::from_u128(3)),
    )
}

/// A storage setup cancelled while its provider check runs drops the scope
/// future; the check's object is permitted only while the scope runs.
#[tokio::test]
async fn a_dropped_provider_check_scope_stops_permitting_its_object() {
    let state = SequenceState::new();
    let path = check_object();
    assert!(PublicationGate::check(&state, &path).is_err());

    let held = tokio::sync::Notify::new();
    let mut scope = Box::pin(async {
        let _permit = state.permit_provider_check(&path);
        held.notified().await;
    });
    let polled = std::future::poll_fn(|cx| Poll::Ready(scope.as_mut().poll(cx))).await;
    assert!(polled.is_pending());
    assert!(PublicationGate::check(&state, &path).is_ok());

    drop(scope);
    assert!(PublicationGate::check(&state, &path).is_err());
}

#[tokio::test]
async fn a_dropped_provider_check_permit_stops_permitting_its_object() {
    let state = SequenceState::new();
    let path = check_object();
    drop(state.permit_provider_check(&path));
    assert!(PublicationGate::check(&state, &path).is_err());
}
