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
/// A wait on a counterpart looks immediately, then backs off.
///
/// The counterpart is a person reading an approval prompt, or a device whose
/// next sync cycle is tens of seconds away. Looking every hundred milliseconds
/// for all of it is hundreds of provider reads that answer "not yet"; the first
/// look still happens at the asked-for cadence, so an answer already waiting is
/// still seen at once.
#[test]
fn a_wait_looks_at_once_and_then_backs_off_to_the_ceiling() {
    let mut polls = DeviceJoinTransportTiming::interactive().polls();
    let cadence = std::iter::from_fn(|| Some(polls.next()))
        .take(7)
        .collect::<Vec<_>>();

    assert_eq!(
        cadence,
        vec![
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(400),
            Duration::from_millis(800),
            Duration::from_millis(1600),
            JOIN_POLL_CEILING,
            JOIN_POLL_CEILING,
        ],
    );
    let looks_in_a_minute = {
        let mut polls = DeviceJoinTransportTiming::interactive().polls();
        let mut elapsed = Duration::ZERO;
        let mut looks = 0;
        while elapsed < Duration::from_secs(60) {
            elapsed += polls.next();
            looks += 1;
        }
        looks
    };
    assert!(
        looks_in_a_minute < 40,
        "a minute of waiting still cost {looks_in_a_minute} provider reads",
    );
}

/// A caller that asks for a slower cadence than the ceiling keeps its own: the
/// ceiling exists to stop fast polling, never to speed a caller up.
#[test]
fn a_wait_slower_than_the_ceiling_keeps_its_own_cadence() {
    let mut polls = DeviceJoinTransportTiming {
        poll: Duration::from_secs(5),
        deadline: Duration::from_secs(180),
    }
    .polls();

    assert_eq!(polls.next(), Duration::from_secs(5));
    assert_eq!(polls.next(), Duration::from_secs(5));
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
