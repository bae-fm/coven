use super::*;

#[test]
fn interactive_pairing_uses_coven_owned_polling_and_deadline() {
    assert_eq!(
        DeviceJoinTransportTiming::interactive(),
        DeviceJoinTransportTiming {
            poll: Duration::from_millis(100),
            deadline: Duration::from_secs(180),
        }
    );
}
use std::cell::Cell;

fn conflict() -> DeviceJoinTransportError {
    DeviceJoinError::Outbound(crate::sync::store::StoreError::ActivationConflict).into()
}

/// Losing the activation slot is not a failure of the join — the operation
/// persisted nothing, so the driver re-derives and goes again.
#[tokio::test]
async fn a_lost_activation_slot_is_re_entered_until_it_lands() {
    let attempts = Cell::new(0usize);
    let settled = retrying_activation_conflicts(|| {
        let attempts = &attempts;
        async move {
            attempts.set(attempts.get() + 1);
            if attempts.get() < 3 {
                Err(conflict())
            } else {
                Ok("landed")
            }
        }
    })
    .await;

    assert_eq!(settled.expect("the retry converges"), "landed");
    assert_eq!(attempts.get(), 3, "it re-entered until the slot was free");
}

/// A store that keeps refusing is not a race. The budget runs out and the
/// failure surfaces rather than looping forever.
#[tokio::test]
async fn a_store_that_never_yields_surfaces_the_conflict() {
    let attempts = Cell::new(0usize);
    let settled: Result<(), _> = retrying_activation_conflicts(|| {
        let attempts = &attempts;
        async move {
            attempts.set(attempts.get() + 1);
            Err(conflict())
        }
    })
    .await;

    assert!(
        is_activation_conflict(&settled.expect_err("an unyielding store fails")),
        "the conflict propagates unchanged once the budget is spent",
    );
    assert_eq!(
        attempts.get(),
        ACTIVATION_CONFLICT_RETRIES + 1,
        "bounded: the retries plus the final attempt whose failure is returned",
    );
}

/// Anything that is not a lost slot is the caller's to see immediately —
/// retrying a real failure would only delay it.
#[tokio::test]
async fn any_other_failure_is_not_retried() {
    let attempts = Cell::new(0usize);
    let settled: Result<(), _> = retrying_activation_conflicts(|| {
        let attempts = &attempts;
        async move {
            attempts.set(attempts.get() + 1);
            Err(DeviceJoinError::OfferMismatch.into())
        }
    })
    .await;

    assert!(settled.is_err());
    assert_eq!(attempts.get(), 1, "it ran once and propagated");
}
