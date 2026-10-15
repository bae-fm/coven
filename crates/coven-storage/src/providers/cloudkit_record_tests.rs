//! Real NSError conversion: the failure, whether CloudKit's server answered,
//! and its requested wait (E5, SP waiting).

use super::*;
use crate::RetryAfter;
use objc2::{runtime::AnyObject, AnyThread};

fn native(code: CKErrorCode, info: &[(&NSString, &AnyObject)]) -> Retained<NSError> {
    let (keys, values): (Vec<_>, Vec<_>) = info.iter().copied().unzip();
    let info = NSDictionary::<NSString, AnyObject>::from_slices(&keys, &values);
    unsafe {
        NSError::initWithDomain_code_userInfo(NSError::alloc(), CKErrorDomain, code.0, Some(&info))
    }
}

fn waiting(code: CKErrorCode, seconds: f64) -> Retained<NSError> {
    let seconds = NSNumber::new_f64(seconds);
    native(code, &[(unsafe { CKErrorRetryAfterKey }, &seconds)])
}

fn partial(items: &[Retained<NSError>], seconds: Option<f64>) -> Retained<NSError> {
    let keys: Vec<_> = (0..items.len())
        .map(|index| NSString::from_str(&index.to_string()))
        .collect();
    let items = NSDictionary::from_retained_objects(
        &keys.iter().map(|key| &**key).collect::<Vec<_>>(),
        items,
    );
    let seconds = seconds.map(NSNumber::new_f64);
    let mut info: Vec<(&NSString, &AnyObject)> =
        vec![(unsafe { CKPartialErrorsByItemIDKey }, &items)];
    if let Some(seconds) = &seconds {
        info.push((unsafe { CKErrorRetryAfterKey }, seconds));
    }
    native(CKErrorCode::PartialFailure, &info)
}

fn facts(error: &NSError) -> (StorageFailure, bool, Option<RetryAfter>) {
    let error = native_error(error);
    (error.failure(), error.answered(), error.retry_after())
}

fn wait(seconds: f64) -> Option<RetryAfter> {
    Some(RetryAfter::Wait(Duration::from_secs_f64(seconds)))
}

#[test]
fn native_failures_carry_the_servers_answer_and_requested_wait() {
    use StorageFailure::*;
    for (error, expected) in [
        // A temporary service failure and throttling state their wait.
        (
            waiting(CKErrorCode::ServiceUnavailable, 30.5),
            (RateLimited, true, wait(30.5)),
        ),
        (
            waiting(CKErrorCode::RequestRateLimited, 12.0),
            (RateLimited, true, wait(12.0)),
        ),
        (
            native(CKErrorCode::ZoneBusy, &[]),
            (RateLimited, true, None),
        ),
        (
            native(CKErrorCode::NotAuthenticated, &[]),
            (Authentication, false, None),
        ),
        (
            native(CKErrorCode::QuotaExceeded, &[]),
            (QuotaExceeded, true, None),
        ),
        (
            native(CKErrorCode::PermissionFailure, &[]),
            (PermissionDenied, true, None),
        ),
        (
            native(CKErrorCode::UnknownItem, &[]),
            (NotFound, true, None),
        ),
        // No response arrived: the device, not the server, failed.
        (
            native(CKErrorCode::NetworkUnavailable, &[]),
            (Network, false, None),
        ),
        (
            waiting(CKErrorCode::NetworkFailure, 4.0),
            (Network, false, wait(4.0)),
        ),
        (
            native(CKErrorCode::ServerResponseLost, &[]),
            (Network, false, None),
        ),
        (
            native(CKErrorCode::OperationCancelled, &[]),
            (Refused, false, None),
        ),
    ] {
        assert_eq!(facts(&error), expected, "{error:?}");
    }
    // Another domain's error is no CloudKit answer, whatever its code.
    let foreign = NSError::new(
        CKErrorCode::ServiceUnavailable.0,
        &NSString::from_str("other"),
    );
    assert_eq!(facts(&foreign), (Refused, false, None));
}

#[test]
fn partial_failures_answer_through_their_items_and_wait_for_the_longest() {
    use StorageFailure::*;
    let batch = || native(CKErrorCode::BatchRequestFailed, &[]);
    for (error, expected) in [
        (
            partial(
                &[waiting(CKErrorCode::RequestRateLimited, 5.0), batch()],
                None,
            ),
            (RateLimited, true, wait(5.0)),
        ),
        (
            partial(
                &[
                    waiting(CKErrorCode::RequestRateLimited, 5.0),
                    waiting(CKErrorCode::ZoneBusy, 20.0),
                ],
                Some(10.0),
            ),
            (RateLimited, true, wait(20.0)),
        ),
        (
            partial(&[native(CKErrorCode::QuotaExceeded, &[]), batch()], None),
            (QuotaExceeded, true, None),
        ),
        (
            partial(&[native(CKErrorCode::NetworkFailure, &[])], None),
            (Network, false, None),
        ),
    ] {
        assert_eq!(facts(&error), expected, "{error:?}");
    }
    // A partial failure without its items is malformed and states nothing.
    assert_eq!(
        facts(&native(CKErrorCode::PartialFailure, &[])),
        (Protocol, false, None)
    );
    // A destination conflict keeps the answer while becoming a refusal.
    let conflict = save_error(&native(CKErrorCode::ServerRecordChanged, &[]), None);
    assert_eq!(conflict.failure(), Refused);
    assert!(conflict.answered());
}

#[test]
fn an_empty_or_missing_account_is_authentication() {
    for name in [None, Some(String::new())] {
        let error = account_name(name).unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Authentication);
        assert!(!error.answered());
    }
    assert_eq!(account_name(Some("_owner".into())).unwrap(), "_owner");
}
