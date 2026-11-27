use super::*;
use crate::providers::tests::{
    full_access, json as reply, response, session_with_clock, RecordingClock, TestServer,
};
use crate::{CloudProvider, NoCooldown};
use axum::{
    body::Body as ReplyBody,
    http::{Response, Uri},
    Router,
};
use std::{collections::VecDeque, sync::Mutex, time::Duration};

/// Follow a sharing job that answers its status checks with `checks` in
/// turn, against a recording clock: how it ended or its failure, the
/// requests made, and the waits between them.
async fn follow(
    checks: Vec<Response<ReplyBody>>,
) -> (Result<DropboxShareJob, StorageError>, Vec<String>, Vec<Duration>) {
    let checks = Arc::new(Mutex::new(VecDeque::from(checks)));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (queued, made) = (checks.clone(), requests.clone());
    let server = TestServer::new(Router::new().fallback(move |uri: Uri| {
        let (queued, made) = (queued.clone(), made.clone());
        async move {
            made.lock().unwrap().push(uri.path().to_owned());
            assert_eq!(uri.path(), "/2/sharing/check_share_job_status");
            queued.lock().unwrap().pop_front().expect("no check after the job ended")
        }
    }))
    .await;
    let clock = Arc::new(RecordingClock::default());
    let folders = OAuthFolders::new(
        session_with_clock(CloudProvider::Dropbox, clock.clone()),
        full_access(CloudProvider::Dropbox),
        format!("{}/2", server.url),
        Arc::new(NoCooldown),
    );
    let ended = folders.follow_dropbox_share("share").await;
    assert!(checks.lock().unwrap().is_empty());
    let requests = requests.lock().unwrap().clone();
    (ended, requests, clock.waits())
}

fn running() -> Response<ReplyBody> {
    reply(json!({".tag":"in_progress"}))
}

fn shared() -> Response<ReplyBody> {
    reply(json!({".tag":"complete","name":"Household","shared_folder_id":"789"}))
}

fn named(ended: Result<DropboxShareJob, StorageError>) -> String {
    match ended.unwrap() {
        DropboxShareJob::Shared(folder) => folder.name,
        other => panic!("a completed job, not {other:?}"),
    }
}

/// A job is followed until Dropbox reports its end, with checks spaced by
/// the backoff of §6.7, however long it runs.
#[tokio::test]
async fn a_slow_sharing_job_is_followed_until_it_completes() {
    let mut checks: Vec<_> = (0..11).map(|_| running()).collect();
    checks.push(shared());
    let (ended, requests, waits) = follow(checks).await;
    assert_eq!(named(ended), "Household");
    assert_eq!(requests.len(), 12);
    assert_eq!(waits, [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300].map(Duration::from_secs));
}

/// A check that fails on Dropbox's side, or is asked to come later, says
/// nothing of the job, so it is sent again: after the backoff, or after the
/// longer wait Dropbox asks for.
#[tokio::test]
async fn a_failed_status_check_is_sent_again() {
    let later = Response::builder()
        .status(429)
        .header("retry-after", "700")
        .body(ReplyBody::empty())
        .unwrap();
    let (ended, _, waits) =
        follow(vec![response(503, "{}"), running(), later, response(500, "{}"), shared()]).await;
    assert_eq!(named(ended), "Household");
    // The first check's resend, the job's next check, then the second
    // check's two resends.
    assert_eq!(waits, [1, 1, 700, 2].map(Duration::from_secs));
}

/// A job Dropbox reports failed, and a check Dropbox refuses, end the follow
/// with that failure; a job Dropbox no longer knows is reported so.
#[tokio::test]
async fn a_failed_job_a_refused_check_or_a_forgotten_job_ends_the_follow() {
    let failed = reply(json!({".tag":"failed","failed":{".tag":"no_permission"}}));
    let (ended, _, _) = follow(vec![running(), failed]).await;
    assert!(matches!(
        ended.unwrap(),
        DropboxShareJob::Failed(failure) if failure.failure() == StorageFailure::PermissionDenied
    ));
    let (ended, _, _) = follow(vec![response(403, "{}")]).await;
    assert_eq!(ended.unwrap_err().failure(), StorageFailure::PermissionDenied);
    let forgotten = json!({"error":{".tag":"invalid_async_job_id"}}).to_string();
    let (ended, _, _) = follow(vec![response(409, forgotten)]).await;
    assert!(matches!(ended.unwrap(), DropboxShareJob::Forgotten));
}
