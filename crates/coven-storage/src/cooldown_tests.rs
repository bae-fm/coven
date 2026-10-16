use super::*;
use crate::{providers::ProviderResponse, CloudProvider};
use reqwest::header::{HeaderMap, HeaderValue, DATE, RETRY_AFTER};

#[test]
fn ordinary_delays_double_from_one_second_to_five_minutes() {
    let delays: Vec<_> = (0..12).map(|n| retry_delay(n).as_secs()).collect();
    assert_eq!(delays, [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300, 300]);
    assert_eq!(retry_delay(u32::MAX), LONGEST_RETRY_DELAY);
}

fn response(headers: &[(reqwest::header::HeaderName, &str)], body: &str) -> ProviderResponse {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        map.insert(name.clone(), HeaderValue::from_str(value).unwrap());
    }
    ProviderResponse::new(429, map, body.as_bytes().to_vec())
}

#[test]
fn retry_after_seconds_and_dates_use_the_response_clock() {
    let provider = CloudProvider::OneDrive;
    assert_eq!(
        response(&[(RETRY_AFTER, "600")], "").retry_after(provider),
        Some(RetryAfter::Wait(Duration::from_secs(600)))
    );
    // An HTTP-date counts from the response's own Date, whatever the device
    // clock reads, so a skewed device cannot shorten the provider's wait.
    assert_eq!(
        response(
            &[
                (RETRY_AFTER, "Wed, 21 Oct 2026 07:38:00 GMT"),
                (DATE, "Wed, 21 Oct 2026 07:28:00 GMT"),
            ],
            ""
        )
        .retry_after(provider),
        Some(RetryAfter::Wait(Duration::from_secs(600)))
    );
    assert_eq!(
        response(
            &[
                (RETRY_AFTER, "Wed, 21 Oct 2026 07:28:00 GMT"),
                (DATE, "Wed, 21 Oct 2026 07:38:00 GMT"),
            ],
            ""
        )
        .retry_after(provider),
        Some(RetryAfter::Wait(Duration::ZERO))
    );
    // Without a basis the date cannot be converted; that is reported and
    // waits the longest ordinary delay, never zero.
    for headers in [
        vec![(RETRY_AFTER, "Wed, 21 Oct 2026 07:38:00 GMT")],
        vec![
            (RETRY_AFTER, "soon"),
            (DATE, "Wed, 21 Oct 2026 07:28:00 GMT"),
        ],
    ] {
        let after = response(&headers, "").retry_after(provider);
        assert_eq!(after, Some(RetryAfter::NoTimeBasis));
        assert_eq!(after.unwrap().wait(), LONGEST_RETRY_DELAY);
    }
    assert_eq!(response(&[], "").retry_after(provider), None);
}

#[test]
fn dropbox_states_its_wait_in_the_error_body_too() {
    let body = r#"{"error_summary":"too_many_requests/..","error":{"reason":{".tag":"too_many_requests"},"retry_after":300}}"#;
    assert_eq!(
        response(&[], body).retry_after(CloudProvider::Dropbox),
        Some(RetryAfter::Wait(Duration::from_secs(300)))
    );
    assert_eq!(
        response(&[], body).retry_after(CloudProvider::OneDrive),
        None
    );
    // The header, when present, decides.
    assert_eq!(
        response(&[(RETRY_AFTER, "5")], body).retry_after(CloudProvider::Dropbox),
        Some(RetryAfter::Wait(Duration::from_secs(5)))
    );
}

/// Records each requested cooldown and keeps it live, failing persistence
/// when asked.
#[derive(Default)]
pub(crate) struct RecordingCooldown {
    pub(crate) recorded: std::sync::Mutex<Vec<Duration>>,
    pub(crate) fail: bool,
}

#[async_trait::async_trait]
impl Cooldown for RecordingCooldown {
    fn remaining(&self) -> Option<Duration> {
        self.recorded.lock().unwrap().last().copied()
    }
    async fn record(&self, wait: Duration) -> Result<(), StorageError> {
        self.recorded.lock().unwrap().push(wait);
        if self.fail {
            return Err(crate::StorageFailure::File.with_source("disk full"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_requested_cooldown_is_recorded_before_the_response_returns_and_holds_every_request() {
    use crate::{test_utils::MemoryStorage, ObjectPrefix, Storage, StorageFailure};
    let raw = MemoryStorage::builder().build().unwrap();
    let cooldown = std::sync::Arc::new(RecordingCooldown::default());
    let storage = raw.with_cooldown(cooldown.clone());
    let mut faults = crate::test_utils::Faults::none();
    faults.fail_next = 1;
    faults.failure = StorageFailure::RateLimited;
    faults.retry_after = Some(600);
    raw.set_faults(faults).await;
    let requests = || raw.request_trace().len();
    let error = storage
        .list(&ObjectPrefix::all(crate::test_utils::STORE_ID))
        .await
        .unwrap_err();
    assert_eq!(error.failure(), StorageFailure::RateLimited);
    assert_eq!(
        *cooldown.recorded.lock().unwrap(),
        [Duration::from_secs(600)]
    );
    assert_eq!(requests(), 1);
    // Every consumer of the connection waits: nothing is sent while it lasts.
    let fresh = raw.with_cooldown(cooldown.clone());
    let refused = fresh
        .list(&ObjectPrefix::all(crate::test_utils::STORE_ID))
        .await
        .unwrap_err();
    assert_eq!(refused.failure(), StorageFailure::RateLimited);
    let crate::StorageError::Caused { source, .. } = &refused else {
        panic!("cooldown refusal lost its cause: {refused:?}");
    };
    assert_eq!(
        source.downcast_ref::<CoolingDown>().unwrap().remaining,
        Duration::from_secs(600)
    );
    assert_eq!(requests(), 1);
    assert!(!refused.answered(), "a local refusal is no provider answer");
}

#[tokio::test]
async fn an_unpersisted_cooldown_fails_the_work_instead_of_permitting_retry() {
    use crate::{test_utils::MemoryStorage, ObjectPrefix, Storage, StorageFailure};
    let raw = MemoryStorage::builder().build().unwrap();
    let cooldown = std::sync::Arc::new(RecordingCooldown {
        fail: true,
        ..Default::default()
    });
    let storage = raw.with_cooldown(cooldown.clone());
    let mut faults = crate::test_utils::Faults::none();
    faults.fail_next = 1;
    faults.failure = StorageFailure::RateLimited;
    faults.retry_after = Some(30);
    raw.set_faults(faults).await;
    let error = storage
        .list(&ObjectPrefix::all(crate::test_utils::STORE_ID))
        .await
        .unwrap_err();
    assert_eq!(error.failure(), StorageFailure::File);
    assert!(!error.retryable());
    let crate::StorageError::Caused { source, .. } = &error else {
        panic!("persistence failure lost its causes: {error:?}");
    };
    let unrecorded = source.downcast_ref::<CooldownNotRecorded>().unwrap();
    assert_eq!(unrecorded.wait, Duration::from_secs(30));
    assert_eq!(unrecorded.response.failure(), StorageFailure::RateLimited);
    assert_eq!(unrecorded.cause.failure(), StorageFailure::File);
}
