//! A new Dropbox shared folder is journaled, followed without the store's
//! call lock, and taken by a retry when it is this creation's own (§20.3).

use super::*;

const NAME: &str = "Household";

fn rpc(path: &str, body: Value, value: Value) -> Reply {
    Reply::new("POST", path, Some(body), value)
}

fn account(id: &str) -> Reply {
    rpc(
        "/users/get_current_account",
        Value::Null,
        json!({"account_id":id,"email":"ana@example.com"}),
    )
}

fn name_is_free() -> Reply {
    rpc(
        "/files/get_metadata",
        json!({"path":"/Household"}),
        json!({"error_summary":"path/not_found/.","error":{".tag":"path","path":{".tag":"not_found"}}}),
    )
    .status(409)
}

fn share(value: Value) -> Reply {
    rpc("/sharing/share_folder", json!({"path":"/Household","force_async":false}), value)
}

fn job_launched() -> Reply {
    share(json!({".tag":"async_job_id","async_job_id":"share"}))
}

/// The reply to the share request is lost on Dropbox's side.
fn share_reply_lost() -> Reply {
    share(json!({})).status(503)
}

fn check(value: Value) -> Reply {
    rpc("/sharing/check_share_job_status", json!({"async_job_id":"share"}), value)
}

fn job_completed() -> Reply {
    check(json!({".tag":"complete","name":NAME,"shared_folder_id":"789"}))
}

fn name_holds_shared_folder() -> Reply {
    rpc(
        "/files/get_metadata",
        json!({"path":"/Household"}),
        json!({".tag":"folder","name":NAME,"id":"id:household",
            "sharing_info":{"shared_folder_id":"789","read_only":false}}),
    )
}

fn folder_access(access: &str) -> Reply {
    rpc(
        "/sharing/get_folder_metadata",
        json!({"shared_folder_id":"789"}),
        json!({"name":NAME,"shared_folder_id":"789","access_type":{".tag":access}}),
    )
}

fn folder_holds(entries: Value) -> Reply {
    rpc(
        "/files/list_folder",
        json!({"path":"/Household"}),
        json!({"entries":entries,"has_more":false}),
    )
}

fn made() -> StorageFolder {
    StorageFolder { name: NAME.into(), storage: dropbox("789") }
}

/// The sign-in kept for following a sharing job, if one is kept.
fn kept_sign_in(f: &Fixture) -> bool {
    f.app.store_keychain(f.handle.store_id()).dropbox_folder_credentials().unwrap().is_some()
}

async fn signed_in() -> Fixture {
    let f = Fixture::new(dropbox("789")).await;
    f.handle.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await.unwrap();
    f
}

/// Start creating the folder on a device that goes offline once Dropbox runs
/// the share as a job: the call follows the job and hasn't returned.
async fn follow_offline(
    f: &Fixture,
) -> tokio::task::JoinHandle<Result<StorageFolder, StorageSetupError>> {
    for reply in [name_is_free(), account("dbid:ana"), job_launched(), check(json!({})).status(503)]
    {
        f.fake.expect(reply);
    }
    let handle = f.handle.clone();
    let creating = tokio::spawn(async move { handle.create_dropbox_shared_folder(NAME).await });
    f.fake.drained().await;
    creating
}

async fn closes(handle: &CovenHandle) {
    coven_foundation::clock::within(handle.close())
        .await
        .expect("closing doesn't wait for the sharing job")
        .unwrap();
}

#[tokio::test]
async fn following_a_job_holds_no_store_lock_and_closing_stops_it() {
    let f = signed_in().await;
    let creating = follow_offline(&f).await;
    // Another storage call runs while the job is followed.
    assert_eq!(f.handle.storage_config().await.unwrap(), None);
    closes(&f.handle).await;
    assert!(matches!(
        creating.await.unwrap(),
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
    f.fake.consumed();
}

#[tokio::test]
async fn the_next_open_resumes_the_journaled_job() {
    let f = signed_in().await;
    let creating = follow_offline(&f).await;
    closes(&f.handle).await;
    creating.await.unwrap().unwrap_err();
    assert!(kept_sign_in(&f));
    // Opening follows the job with the sign-in kept for it, with no call.
    f.fake.expect(account("dbid:ana"));
    f.fake.expect(job_completed());
    let handle = f.reopen().await;
    f.fake.drained().await;
    // Asking again gives the folder that job made, sharing nothing more.
    handle.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await.unwrap();
    f.fake.expect(name_holds_shared_folder().token(1));
    f.fake.expect(account("dbid:ana").token(1));
    assert_eq!(handle.create_dropbox_shared_folder(NAME).await.unwrap(), made());
    assert!(!kept_sign_in(&f));
    f.fake.consumed();
    handle.close().await.unwrap();
}

#[tokio::test]
async fn a_retry_after_a_lost_share_reply_takes_the_folder_it_made() {
    let f = signed_in().await;
    for reply in [name_is_free(), account("dbid:ana"), share_reply_lost()] {
        f.fake.expect(reply);
    }
    let lost = failure(f.handle.create_dropbox_shared_folder(NAME).await.unwrap_err());
    assert_eq!(lost.failure(), StorageFailure::Network);
    for reply in [
        name_holds_shared_folder(),
        account("dbid:ana"),
        folder_access("owner"),
        folder_holds(json!([])),
    ] {
        f.fake.expect(reply);
    }
    assert_eq!(f.handle.create_dropbox_shared_folder(NAME).await.unwrap(), made());
    f.fake.consumed();
    f.handle.close().await.unwrap();
}

/// A name taken by a folder that isn't this creation's is refused: with
/// nothing journaled, any folder; after a lost reply, one that isn't shared,
/// one another account owns, one that holds anything, and one another
/// account asked for.
#[tokio::test]
async fn a_name_taken_by_someone_elses_folder_is_refused() {
    let unshared = || {
        rpc(
            "/files/get_metadata",
            json!({"path":"/Household"}),
            json!({".tag":"folder","name":NAME,"id":"id:household"}),
        )
    };
    let refused = |error: StorageSetupError| {
        assert_eq!(failure(error).failure(), StorageFailure::AlreadyExists)
    };
    let f = signed_in().await;
    f.fake.expect(name_holds_shared_folder());
    f.fake.expect(account("dbid:ana"));
    refused(f.handle.create_dropbox_shared_folder(NAME).await.unwrap_err());
    for reply in [name_is_free(), account("dbid:ana"), share_reply_lost()] {
        f.fake.expect(reply);
    }
    f.handle.create_dropbox_shared_folder(NAME).await.unwrap_err();
    for taken in [
        vec![unshared()],
        vec![name_holds_shared_folder(), folder_access("editor")],
        vec![
            name_holds_shared_folder(),
            folder_access("owner"),
            folder_holds(json!([{".tag":"file","name":"notes.txt"}])),
        ],
    ] {
        let mut taken = taken.into_iter();
        f.fake.expect(taken.next().unwrap());
        f.fake.expect(account("dbid:ana"));
        taken.for_each(|reply| f.fake.expect(reply));
        refused(f.handle.create_dropbox_shared_folder(NAME).await.unwrap_err());
    }
    f.fake.expect(name_holds_shared_folder());
    f.fake.expect(account("dbid:ben"));
    refused(f.handle.create_dropbox_shared_folder(NAME).await.unwrap_err());
    f.fake.consumed();
    f.handle.close().await.unwrap();
}

/// A job Dropbox reports failed made nothing: its journal and kept sign-in
/// end, so a name someone else takes afterwards is refused outright.
#[tokio::test]
async fn a_failed_job_ends_its_journal() {
    let f = signed_in().await;
    for reply in [
        name_is_free(),
        account("dbid:ana"),
        job_launched(),
        check(json!({".tag":"failed","failed":{".tag":"no_permission"}})),
    ] {
        f.fake.expect(reply);
    }
    let failed = failure(f.handle.create_dropbox_shared_folder(NAME).await.unwrap_err());
    assert_eq!(failed.failure(), StorageFailure::PermissionDenied);
    assert!(!kept_sign_in(&f));
    f.fake.expect(name_holds_shared_folder());
    f.fake.expect(account("dbid:ana"));
    let taken = failure(f.handle.create_dropbox_shared_folder(NAME).await.unwrap_err());
    assert_eq!(taken.failure(), StorageFailure::AlreadyExists);
    f.fake.consumed();
    f.handle.close().await.unwrap();
}

/// A storage setup that commits is done with the picker: it stops the job
/// being followed, and leaves no journal and no kept sign-in.
#[tokio::test]
async fn a_committed_setup_ends_the_folders_being_made() {
    let f = signed_in().await;
    let creating = follow_offline(&f).await;
    f.expect_labels(&dropbox("789"), "ana@example.com", NAME, 0).await;
    f.handle.setup_oauth_storage(dropbox("789"), |_| {}).await.unwrap();
    assert!(matches!(creating.await.unwrap(), Err(StorageSetupError::LocationChangeUnsupported)));
    assert!(!kept_sign_in(&f));
    assert!(coven_sync::DropboxFolderJournal::all(f.handle.test_database())
        .await
        .unwrap()
        .is_empty());
    f.fake.consumed();
    f.handle.close().await.unwrap();
}

/// A journaled job is never without the sign-in kept to follow it: opening a
/// store that finds one so reports the bug to the app's sink.
#[tokio::test]
async fn a_job_without_its_kept_sign_in_is_reported_as_a_bug() {
    let f = signed_in().await;
    let creating = follow_offline(&f).await;
    closes(&f.handle).await;
    creating.await.unwrap().unwrap_err();
    f.app.store_keychain(f.handle.store_id()).delete_dropbox_folder_credentials().unwrap();
    let handle = f.reopen().await;
    let bug = coven_foundation::clock::within(f.bugs.next()).await.expect("the sink hears a bug");
    assert_eq!(bug.operation, "follow a Dropbox sharing job");
    assert_eq!(bug.message, "a journaled sharing job has no kept sign-in");
    handle.close().await.unwrap();
}
