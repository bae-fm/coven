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

/// The requests of a new shared folder whose job answers its status checks
/// with `checks` in turn, run against a recording clock: the folder or its
/// failure, the requests made, and the waits between them.
async fn share_folder(
    checks: Vec<Response<ReplyBody>>,
) -> (Result<StorageFolder, StorageError>, Vec<String>, Vec<Duration>) {
    let checks = Arc::new(Mutex::new(VecDeque::from(checks)));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (queued, made) = (checks.clone(), requests.clone());
    let server = TestServer::new(Router::new().fallback(move |uri: Uri| {
        let (queued, made) = (queued.clone(), made.clone());
        async move {
            made.lock().unwrap().push(uri.path().to_owned());
            match uri.path() {
                "/2/files/get_metadata" => response(
                    409,
                    json!({"error":{".tag":"path","path":{".tag":"not_found"}}}).to_string(),
                ),
                "/2/sharing/share_folder" => {
                    reply(json!({".tag":"async_job_id","async_job_id":"share"}))
                }
                "/2/sharing/check_share_job_status" => {
                    queued.lock().unwrap().pop_front().expect("no check after the job ended")
                }
                other => panic!("unexpected Dropbox request {other}"),
            }
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
    let folder = folders.create_dropbox_shared_folder("Household").await;
    assert!(checks.lock().unwrap().is_empty());
    let requests = requests.lock().unwrap().clone();
    (folder, requests, clock.waits())
}

fn running() -> Response<ReplyBody> {
    reply(json!({".tag":"in_progress"}))
}

fn shared() -> Response<ReplyBody> {
    reply(json!({".tag":"complete","name":"Household","shared_folder_id":"789"}))
}

/// The job's id is held only by the call that launched it, so the call
/// follows the job until Dropbox reports its end, with checks spaced by the
/// backoff of §6.7, however long it runs.
#[tokio::test]
async fn a_slow_sharing_job_is_followed_until_it_completes() {
    let mut checks: Vec<_> = (0..11).map(|_| running()).collect();
    checks.push(shared());
    let (folder, requests, waits) = share_folder(checks).await;
    assert_eq!(folder.unwrap().name, "Household");
    assert_eq!(requests.len(), 2 + 12);
    assert_eq!(waits, [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300].map(Duration::from_secs));
}

/// A check that fails on Dropbox's side, or is asked to come later, says
/// nothing of the job, so it is sent again: after the backoff, or after the
/// longer wait Dropbox asks for. The folder is launched only once.
#[tokio::test]
async fn a_failed_status_check_is_sent_again() {
    let later = Response::builder()
        .status(429)
        .header("retry-after", "700")
        .body(ReplyBody::empty())
        .unwrap();
    let (folder, requests, waits) =
        share_folder(vec![response(503, "{}"), running(), later, response(500, "{}"), shared()])
            .await;
    assert_eq!(folder.unwrap().name, "Household");
    assert_eq!(requests.iter().filter(|path| path.ends_with("/share_folder")).count(), 1);
    // The first check's resend, the job's next check, then the second
    // check's two resends.
    assert_eq!(waits, [1, 1, 700, 2].map(Duration::from_secs));
}

/// A job Dropbox reports failed, and a check Dropbox refuses, end the call
/// with that failure.
#[tokio::test]
async fn a_failed_job_or_a_refused_check_ends_the_call() {
    let failed = reply(json!({".tag":"failed","failed":{".tag":"no_permission"}}));
    let (folder, _, _) = share_folder(vec![running(), failed]).await;
    assert_eq!(folder.unwrap_err().failure(), StorageFailure::PermissionDenied);
    let refused = response(403, "{}");
    let (folder, _, _) = share_folder(vec![refused]).await;
    assert_eq!(folder.unwrap_err().failure(), StorageFailure::PermissionDenied);
}
