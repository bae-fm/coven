use super::*;
use crate::test_utils::DEVICE;
use coven_foundation::id_source::{DeviceId, ObjectId};
use coven_merge::Audience;
use std::num::NonZeroU64;

/// A connection to the same bucket that writes for Ben's laptop.
fn laptop(mut storage: StorageConnection<S3Storage>) -> StorageConnection<S3Storage> {
    storage.set_device(DeviceId(9));
    storage
}

fn slot(number: u64) -> ObjectPath {
    ObjectPath::slot(STORE_ID, NonZeroU64::new(number).unwrap())
}

/// Ben's phone left two uploads on GCS, a copy profile: one whose multipart
/// upload never finished, and one assembled into its temporary whose copy
/// never happened. Ben's laptop ends both, which sit in the phone's own
/// folder, and nothing of its own (§17.10, Appendix E.10).
#[tokio::test]
async fn another_device_ends_a_devices_sessions_and_temporaries_on_a_copy_profile() {
    let state = Arc::new(Mutex::new(Remote { gcs: true, ..Remote::default() }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let phone = gcs_provider(&server.url);
    let mut unfinished = phone.begin_upload(&slot(1), 4).await.unwrap();
    phone.upload_part(&mut unfinished, b"data").await.unwrap();
    let mut assembled = phone.begin_upload(&slot(2), 4).await.unwrap();
    phone.upload_part(&mut assembled, b"data").await.unwrap();
    state.lock().unwrap().fail_completion_reply = true;
    assert!(phone.finish_upload(&mut assembled).await.is_err());
    let laptop = laptop(gcs_provider(&server.url));
    let mut own = laptop.begin_upload(&slot(3), 4).await.unwrap();
    laptop.upload_part(&mut own, b"data").await.unwrap();
    {
        let state = state.lock().unwrap();
        assert_eq!(state.uploads.len(), 2);
        assert_eq!(state.objects.keys().filter(|key| key.contains("/.coven-uploads/")).count(), 1);
    }
    for _ in 0..2 {
        laptop.end_device_uploads(STORE_ID, DEVICE).await.unwrap();
        let state = state.lock().unwrap();
        let laptops = format!("{STORE_ID}/.coven-uploads/9/");
        assert_eq!(state.uploads.len(), 1);
        assert!(state.uploads.values().all(|upload| upload.key.starts_with(&laptops)));
        assert!(state.objects.keys().all(|key| !key.contains("/.coven-uploads/")));
    }
}

/// On R2, which publishes by conditional completion, Ben's phone left
/// multipart uploads at the destinations of a file and a snapshot, whose
/// paths name the phone, and of slot 1, whose path names no device. Ben's
/// laptop ends the first two. The slot's stays: a shared path doesn't show
/// whose upload it is, and aborting another claimant's would break its claim
/// (Appendix E.10).
#[tokio::test]
async fn another_device_ends_a_devices_sessions_at_destinations_naming_it() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let settings = profile::CataloguedProfile::named(S3ProfileName::R2).settings;
    let phone = profile_provider(&server.url, settings);
    let file = |device| ObjectPath::file(STORE_ID, device, ObjectId(uuid::Uuid::from_u128(5)));
    let snapshot = ObjectPath::snapshot(STORE_ID, Audience::Store, DEVICE, NonZeroU64::MIN);
    for path in [file(DEVICE), snapshot, slot(1)] {
        let mut session = phone.begin_upload(&path, 4).await.unwrap();
        phone.upload_part(&mut session, b"data").await.unwrap();
    }
    let laptop = laptop(profile_provider(&server.url, settings));
    let mut own = laptop.begin_upload(&file(DeviceId(9)), 4).await.unwrap();
    laptop.upload_part(&mut own, b"data").await.unwrap();
    laptop.end_device_uploads(STORE_ID, DEVICE).await.unwrap();
    let mut left: Vec<_> =
        state.lock().unwrap().uploads.values().map(|upload| upload.key.clone()).collect();
    left.sort();
    let mut expected = vec![file(DeviceId(9)).as_str().to_owned(), slot(1).as_str().to_owned()];
    expected.sort();
    assert_eq!(left, expected);
}

/// On R2, a claim's multipart upload sits at its slot's path. Once this
/// device has applied slot 2, every session left at slots 1 and 2 can never
/// publish, whoever began it: create-once refuses it there. Those are
/// aborted; the session at slot 3, not yet applied, stays (Appendix E.10).
#[tokio::test]
async fn sessions_at_applied_slots_are_ended_whoever_began_them() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let settings = profile::CataloguedProfile::named(S3ProfileName::R2).settings;
    let storage = profile_provider(&server.url, settings);
    let other = laptop(profile_provider(&server.url, settings));
    for (connection, number) in [(&storage, 1), (&other, 2), (&other, 3)] {
        let mut session = connection.begin_upload(&slot(number), 4).await.unwrap();
        connection.upload_part(&mut session, b"data").await.unwrap();
    }
    for _ in 0..2 {
        storage.end_settled_claims(STORE_ID, 2).await.unwrap();
        let state = state.lock().unwrap();
        let left: Vec<_> = state.uploads.values().map(|upload| upload.key.clone()).collect();
        assert_eq!(left, [slot(3).as_str().to_owned()]);
    }
}

/// On a profile certified to date its uploads' starts, a floor session that
/// began before the current floor was published observed an older revision,
/// so it can never publish: it is aborted. One begun in the floor's second or
/// later stays, and so does every floor session on a profile not certified
/// for it (Appendix E.6, E.10).
#[tokio::test]
async fn floor_sessions_begun_before_the_floor_are_ended_where_certified() {
    for certified in [true, false] {
        let state = Arc::new(Mutex::new(Remote::default()));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let settings = profile::CataloguedProfile::named(S3ProfileName::R2).settings;
        let mut storage = profile_provider(&server.url, settings);
        Arc::get_mut(storage.provider_mut()).unwrap().floor_sessions_dated = certified;
        let floor = ObjectPath::floor(STORE_ID);
        storage.create(&floor, b"floor").await.unwrap();
        let previous = storage.status(&floor).await.unwrap().unwrap();
        for initiated in
            ["2026-10-05T23:59:59.900Z", "2026-10-06T00:00:00.000Z", "2026-10-06T00:00:01.000Z"]
        {
            state.lock().unwrap().next_initiated = Some(initiated.into());
            let mut session = storage.begin_replace(&previous, 4).await.unwrap();
            storage.upload_part(&mut session, b"data").await.unwrap();
        }
        let requests = state.lock().unwrap().requests.len();
        storage.end_superseded_floor_sessions(STORE_ID).await.unwrap();
        let state = state.lock().unwrap();
        let mut left: Vec<_> =
            state.uploads.values().map(|upload| upload.initiated.clone()).collect();
        left.sort();
        if certified {
            assert_eq!(left, ["2026-10-06T00:00:00.000Z", "2026-10-06T00:00:01.000Z"]);
        } else {
            assert_eq!(left.len(), 3);
            assert_eq!(state.requests.len(), requests, "an uncertified profile sends nothing");
        }
    }
}
