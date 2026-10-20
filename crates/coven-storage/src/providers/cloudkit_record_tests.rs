//! Real NSError conversion: the failure, whether CloudKit's server answered,
//! and its requested wait (E5, §6.7).

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

/// A CloudKit failure prints the domain and code of its error, of the error
/// beneath it, and of each partial-failure item: never a description, a
/// record or an item id.
#[test]
fn native_failures_name_their_underlying_and_item_codes() {
    let description = NSString::from_str("secret description");
    let offline = unsafe {
        NSError::initWithDomain_code_userInfo(
            NSError::alloc(),
            &NSString::from_str("NSURLErrorDomain"),
            -1009,
            Some(&NSDictionary::<NSString, AnyObject>::from_slices(
                &[objc2_foundation::NSLocalizedDescriptionKey],
                &[&*description],
            )),
        )
    };
    let failed = native(
        CKErrorCode::NetworkFailure,
        &[(unsafe { NSUnderlyingErrorKey }, &*offline)],
    );
    let error = native_error(&failed);
    let domain = unsafe { CKErrorDomain }.to_string();
    let expected = format!("{domain} (4): NSURLErrorDomain (-1009)");
    assert_eq!(
        std::error::Error::source(&error).unwrap().to_string(),
        expected
    );
    assert!(error.to_string().ends_with(&expected), "{error}");

    let batch = partial(
        &[
            native(CKErrorCode::QuotaExceeded, &[]),
            native(CKErrorCode::QuotaExceeded, &[]),
            failed.clone(),
        ],
        None,
    );
    let text = native_error(&batch).to_string();
    assert!(
        text.ends_with(&format!(
            "{domain} (2) [items: {domain} (25) ×2, {expected}]"
        )),
        "{text}"
    );
    for text in [text, format!("{:?}", native_error(&failed))] {
        assert!(!text.contains("secret"), "{text}");
    }
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

#[test]
fn listing_skips_non_d10_paths_after_checking_native_metadata() {
    let store = coven_foundation::id_source::StoreId(uuid::Uuid::from_u128(1));
    let prefix = ObjectPrefix::all(store);
    let path = format!("{store}/unrelated");
    let fields = NativeMetadata {
        name: path.clone(),
        path,
        size: "64".into(),
        created: Some(20.0),
        modified: Some(30.0),
        revision: Some("tag".into()),
    };
    for suffix in ["unrelated", "/unrelated", "./unrelated", "unrelated/"] {
        let path = format!("{store}/{suffix}");
        let unrelated = NativeMetadata {
            name: path.clone(),
            path,
            size: fields.size.clone(),
            created: fields.created,
            modified: fields.modified,
            revision: fields.revision.clone(),
        };
        assert!(unrelated.listed(&prefix).unwrap().is_none());
    }
    for index in 0..10 {
        let mut bad = NativeMetadata {
            name: fields.name.clone(),
            path: fields.path.clone(),
            size: fields.size.clone(),
            created: fields.created,
            modified: fields.modified,
            revision: fields.revision.clone(),
        };
        match index {
            0 => bad.name.push('x'),
            1 => bad.size = "-1".into(),
            2 => bad.created = None,
            3 => bad.created = Some(f64::NAN),
            4 => bad.modified = Some(-1.0),
            5 => bad.revision = None,
            6 => bad.revision = Some(String::new()),
            7 => {
                bad.path = format!("{}/unrelated", uuid::Uuid::from_u128(2));
                bad.name = bad.path.clone();
            }
            8 => {
                bad.path = format!("{store}/../unrelated");
                bad.name = bad.path.clone();
            }
            9 => {
                bad.path = format!("{store}/files/other");
                bad.name = bad.path.clone();
            }
            _ => unreachable!(),
        }
        let requested = if index == 9 {
            ObjectPrefix::device_files(store, coven_foundation::id_source::DeviceId(1))
        } else {
            prefix.clone()
        };
        assert_eq!(
            bad.listed(&requested).unwrap_err().failure(),
            StorageFailure::Protocol,
            "case {index}"
        );
    }
    let path = ObjectPath::floor(store);
    let valid = NativeMetadata {
        name: path.as_str().into(),
        path: path.as_str().into(),
        size: "64".into(),
        created: None,
        modified: Some(30.0),
        revision: Some("tag".into()),
    };
    assert_eq!(valid.listed(&prefix).unwrap().unwrap().path, path);
}
