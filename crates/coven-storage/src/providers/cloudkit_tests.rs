use super::record::{
    check_prefix, failure, native_error, part_name, publication_metadata, record_name, upload_name,
};
use super::*;
use coven_foundation::id_source::{CircleId, DeviceId, FileId, InviteId, StoreId, UuidIds};
use coven_merge::Audience;
use objc2_cloud_kit::{
    CKErrorCode, CKErrorDomain, CKPartialErrorsByItemIDKey, CKQueryOperation, CKRecord,
    CKRecordChangedErrorClientRecordKey, CKRecordID,
};
use objc2_foundation::{NSError, NSString};
use std::{
    collections::BTreeSet,
    num::NonZeroU64,
    sync::{Arc, Mutex},
    time::Duration,
};

#[path = "cloudkit_live_zone.rs"]
pub(super) mod live_zone;
#[path = "cloudkit_race.rs"]
mod race;

fn path() -> ObjectPath {
    ObjectPath::slot(StoreId(uuid::Uuid::from_u128(1)), NonZeroU64::MIN)
}

#[test]
fn listing_uses_equality_for_directories_longer_than_cloudkits_prefix_limit() {
    let prefix = ObjectPrefix::device_files(path().store(), DeviceId(7));
    assert!(prefix.as_str().len() > 30);
    let predicate = query::Selection(prefix)
        .predicate()
        .predicateFormat()
        .to_string();
    assert!(!predicate.contains("BEGINSWITH"), "{predicate}");
    assert!(predicate.contains(" == "), "{predicate}");
}

#[test]
fn record_names_preserve_canonical_paths_and_reserve_uploads() {
    let path = path();
    assert_eq!(record_name(&path), path.as_str());
    assert!(record_name(&path).is_ascii());
    assert!(record_name(&path).len() <= 255);
    let token = uuid::Uuid::from_u128(9).to_string();
    let upload = upload_name(&path, &token).unwrap();
    let part = part_name(&upload, u64::MAX);
    assert!(ObjectPath::parse(&upload).is_err());
    assert!(ObjectPath::parse(&part).is_err());
    assert!(part.is_ascii() && part.len() <= 255);
    assert!(upload_name(&path, "../escape").is_err());
    assert!(upload_name(&path, "00000000000000000000000000000009").is_err());
}

#[test]
fn prefix_conversion_rejects_another_writer_or_store() {
    let path = ObjectPath::file(
        path().store(),
        DeviceId(7),
        FileId(uuid::Uuid::from_u128(1)),
    );
    let prefix = ObjectPrefix::device_files(path.store(), DeviceId(7));
    check_prefix(&prefix, path.as_str()).unwrap();
    for other in [
        ObjectPath::slot(path.store(), NonZeroU64::MIN),
        ObjectPath::slot(StoreId(uuid::Uuid::from_u128(2)), NonZeroU64::MIN),
        ObjectPath::floor(path.store()),
    ] {
        assert_eq!(
            check_prefix(&prefix, other.as_str()).unwrap_err().failure(),
            StorageFailure::Protocol
        );
    }
}

#[test]
fn metadata_uses_publication_and_replacement_dates() {
    for (path, expected) in [
        (path(), 20),
        (ObjectPath::floor(path().store()), 30),
        (ObjectPath::floor(path().store()), 30),
    ] {
        let object = publication_metadata(
            path.as_str(),
            path.as_str(),
            "64",
            Some(20.0),
            Some(30.0),
            Some("revision"),
        )
        .unwrap();
        assert_eq!(object.path, path);
        assert_eq!(object.size, 64);
        assert_eq!(object.object_id, path.as_str());
        assert_eq!(object.revision, "revision");
        assert_eq!(object.time_resolution, Duration::from_millis(1));
        assert_eq!(object.checksum, None);
        assert_eq!(
            object.stored_at,
            coven_format::value::StorageTime::since_epoch(Duration::from_secs(expected))
        );
    }
}

#[test]
fn replacements_retain_change_tags_even_with_equal_sizes_and_dates() {
    for path in [
        ObjectPath::floor(path().store()),
        ObjectPath::floor(path().store()),
    ] {
        let observed = ["first-change-tag", "second-change-tag"].map(|tag| {
            publication_metadata(
                path.as_str(),
                path.as_str(),
                "64",
                Some(20.0),
                Some(30.125),
                Some(tag),
            )
            .unwrap()
        });
        assert_eq!(observed[0].object_id, observed[1].object_id);
        assert_eq!(observed[0].size, observed[1].size);
        assert_eq!(observed[0].stored_at, observed[1].stored_at);
        assert_eq!(
            observed[0].stored_at,
            coven_format::value::StorageTime::since_epoch(Duration::from_millis(30_125))
        );
        assert_eq!(observed[0].revision, "first-change-tag");
        assert_eq!(observed[1].revision, "second-change-tag");
    }
}

/// `NSDate` seconds rarely hold a millisecond exactly; the time lands on the
/// millisecond tick CloudKit counts, never between ticks (Appendix E.11).
#[test]
fn dates_land_on_whole_milliseconds() {
    use coven_format::value::StorageTime;
    let path = ObjectPath::floor(path().store());
    for (seconds, expected) in [
        (
            1_760_400_000.123,
            StorageTime::new(1_760_400_000, 123_000_000),
        ),
        (0.1 + 0.2, StorageTime::new(0, 300_000_000)),
        (30.0009999999, StorageTime::new(30, 1_000_000)),
        (0.0, StorageTime::new(0, 0)),
    ] {
        let object = publication_metadata(
            path.as_str(),
            path.as_str(),
            "64",
            None,
            Some(seconds),
            Some("tag"),
        )
        .unwrap();
        assert_eq!(object.stored_at, expected.unwrap(), "{seconds}");
        assert_eq!(object.stored_at.nanos() % 1_000_000, 0);
    }
    for seconds in [-0.001, f64::INFINITY, f64::NEG_INFINITY, f64::NAN, 1e300] {
        assert_eq!(
            publication_metadata(
                path.as_str(),
                path.as_str(),
                "64",
                None,
                Some(seconds),
                Some("tag")
            )
            .unwrap_err()
            .failure(),
            StorageFailure::Protocol,
            "{seconds}"
        );
    }
}

#[test]
fn metadata_requires_the_date_for_the_object_kind() {
    for path in [
        path(),
        ObjectPath::floor(path().store()),
        ObjectPath::floor(path().store()),
    ] {
        let (created, modified) = if path.is_replaceable() {
            (Some(20.0), None)
        } else {
            (None, Some(30.0))
        };
        assert_eq!(
            publication_metadata(
                path.as_str(),
                path.as_str(),
                "64",
                created,
                modified,
                Some("tag")
            )
            .unwrap_err()
            .failure(),
            StorageFailure::Protocol,
        );
    }
}

#[test]
fn malformed_native_metadata_is_an_error() {
    let path = path();
    for (name, size, created, revision) in [
        ("other", "64", Some(20.0), Some("tag")),
        (path.as_str(), "not a size", Some(20.0), Some("tag")),
        (path.as_str(), "64", None, Some("tag")),
        (path.as_str(), "64", Some(f64::NAN), Some("tag")),
        (path.as_str(), "64", Some(20.0), None),
        (path.as_str(), "64", Some(20.0), Some("")),
    ] {
        assert_eq!(
            publication_metadata(name, path.as_str(), size, created, Some(30.0), revision)
                .unwrap_err()
                .failure(),
            StorageFailure::Protocol
        );
    }
}

#[test]
fn native_failures_are_classified_without_discarding_their_cause() {
    use std::error::Error;
    for (code, expected) in [
        (
            CKErrorCode::ServerRecordChanged,
            StorageFailure::AlreadyExists,
        ),
        (CKErrorCode::UnknownItem, StorageFailure::NotFound),
        (CKErrorCode::ZoneNotFound, StorageFailure::ContainerNotFound),
        (
            CKErrorCode::NotAuthenticated,
            StorageFailure::Authentication,
        ),
        (
            CKErrorCode::PermissionFailure,
            StorageFailure::PermissionDenied,
        ),
        (CKErrorCode::QuotaExceeded, StorageFailure::QuotaExceeded),
        (CKErrorCode::NetworkFailure, StorageFailure::Network),
        (CKErrorCode::RequestRateLimited, StorageFailure::RateLimited),
        (CKErrorCode::ServiceUnavailable, StorageFailure::RateLimited),
        (
            CKErrorCode::AccountTemporarilyUnavailable,
            StorageFailure::RateLimited,
        ),
        (
            CKErrorCode::MissingEntitlement,
            StorageFailure::InvalidConfiguration,
        ),
        (CKErrorCode::OperationCancelled, StorageFailure::Refused),
    ] {
        assert_eq!(failure(code), expected);
        let error = NSError::new(code.0, unsafe { CKErrorDomain });
        let mapped = native_error(&error);
        assert_eq!(mapped.failure(), expected);
        assert!(mapped.source().is_some());
    }
    let foreign = NSError::new(CKErrorCode::UnknownItem.0, &NSString::from_str("other"));
    assert_eq!(native_error(&foreign).failure(), StorageFailure::Refused);
}

/// Run only from the entitled macOS app described in cloudkit-live.md.
#[tokio::test]
#[ignore = "requires a signed CloudKit host and an iCloud account; waits one listing margin"]
async fn cloudkit_live() {
    let ids: IdSourceRef = Arc::new(UuidIds);
    let store = StoreId(ids.new_id());
    // A failed run keeps its marked zones for inspection and later cleanup.
    let native = Arc::new(
        live_zone::prepare(store, "cloudkit_live", ids.clone())
            .await
            .unwrap(),
    );
    let config = native.config();
    let storage = StorageConnection::from_provider(native.clone());
    let first = ObjectPath::file(store, DeviceId(7), FileId(ids.new_id()));
    let positions = ObjectPath::floor(store);
    assert!(storage.status(&first).await.unwrap().is_none());
    storage.create(&first, b"encrypted bytes").await.unwrap();
    // Exact status must see publication immediately, independently of query indexing.
    let first_metadata = storage.status(&first).await.unwrap().unwrap();
    assert_eq!(first_metadata.object_id, first.as_str());
    assert_eq!(first_metadata.size, 15);
    assert_eq!(first_metadata.time_resolution, Duration::from_millis(1));
    assert!(!first_metadata.revision.is_empty());
    assert_eq!(first_metadata.checksum, None);
    assert_eq!(
        storage
            .create(&first, b"changed")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(storage.read(&first).await.unwrap(), b"encrypted bytes");
    assert_eq!(
        storage.status(&first).await.unwrap().unwrap(),
        first_metadata
    );
    let whole = storage.read_object(&first, None).await.unwrap();
    assert_eq!(whole.metadata, first_metadata);
    assert_eq!(whole.bytes, b"encrypted bytes");
    let ranged = storage
        .read_object(&first, Some(ByteRange::new(2, 7).unwrap()))
        .await
        .unwrap();
    assert_eq!(ranged.metadata, first_metadata);
    assert_eq!(ranged.bytes, b"crypt");
    assert_eq!(
        storage
            .read_range(&first, ByteRange::new(2, 7).unwrap())
            .await
            .unwrap(),
        b"crypt"
    );
    storage.create(&positions, b"one").await.unwrap();
    let prior = storage.status(&positions).await.unwrap().unwrap();
    storage.replace(&prior, b"one").await.unwrap();
    let replaced = storage.status(&positions).await.unwrap().unwrap();
    assert_ne!(replaced.revision, prior.revision);
    assert_eq!(replaced.object_id, prior.object_id);
    assert!(replaced.stored_at >= prior.stored_at);
    storage.replace(&replaced, b"two").await.unwrap();
    let changed = storage.read_object(&positions, None).await.unwrap();
    assert_eq!(changed.bytes, b"two");
    assert_eq!(changed.metadata.size, replaced.size);
    assert_ne!(changed.metadata.revision, replaced.revision);
    assert_eq!(
        storage.status(&positions).await.unwrap().unwrap(),
        changed.metadata
    );
    let large = ObjectPath::file(store, DeviceId(7), FileId(ids.new_id()));
    let mut session = storage
        .begin_upload(&large, PART_SIZE as u64 + 3)
        .await
        .unwrap();
    let before = session.encode().unwrap();
    let part = vec![42; PART_SIZE];
    storage.upload_part(&mut session, &part).await.unwrap();
    assert!(storage.status(&large).await.unwrap().is_none());
    // Discard the acknowledged recording, as after a process died before recording it.
    let mut resumed = UploadSession::decode(before.as_bytes()).unwrap();
    let restarted = CloudKitStorage::new(config, ids).await.unwrap();
    let restarted = StorageConnection::from_provider(Arc::new(restarted));
    restarted.resume_upload(&mut resumed).await.unwrap();
    assert_eq!(resumed.confirmed_bytes(), PART_SIZE as u64);
    assert_eq!(
        storage.read(&large).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    restarted.upload_part(&mut resumed, b"end").await.unwrap();
    let before_publish = resumed.encode().unwrap();
    storage
        .replace(&changed.metadata, b"publication time")
        .await
        .unwrap();
    let positions_metadata = native
        .fetch(positions.as_str(), record::OBJECT_KEYS)
        .wait()
        .await
        .unwrap()
        .metadata()
        .unwrap();
    let before_publication = positions_metadata.stored_at;
    restarted.finish_upload(&mut resumed).await.unwrap();
    let published = storage.status(&large).await.unwrap().unwrap();
    assert_eq!(published.object_id, large.as_str());
    assert_eq!(published.size, PART_SIZE as u64 + 3);
    assert!(published.stored_at >= before_publication);
    let mut recovered = UploadSession::decode(before_publish.as_bytes()).unwrap();
    storage.resume_upload(&mut recovered).await.unwrap();
    assert!(recovered.is_complete());
    storage.abort_upload(&recovered).await.unwrap();
    assert_eq!(
        storage
            .read_range(
                &large,
                ByteRange::new(PART_SIZE as u64 - 1, PART_SIZE as u64 + 3).unwrap()
            )
            .await
            .unwrap(),
        [42, b'e', b'n', b'd']
    );

    let abandoned_path = ObjectPath::file(store, DeviceId(7), FileId(native.ids.new_id()));
    let mut abandoned = storage
        .begin_upload(&abandoned_path, PART_SIZE as u64 + 1)
        .await
        .unwrap();
    storage.upload_part(&mut abandoned, &part).await.unwrap();
    storage.abort_upload(&abandoned).await.unwrap();
    storage.abort_upload(&abandoned).await.unwrap();
    assert_eq!(
        storage.read(&abandoned_path).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );

    let mut pages = Vec::new();
    for number in 1..=native::QUERY_PAGE_SIZE + 1 {
        let path = ObjectPath::slot(store, NonZeroU64::new(number as u64).unwrap());
        storage.create(&path, b"page").await.unwrap();
        pages.push(path);
    }
    let other = ObjectPath::floor(StoreId(native.ids.new_id()));
    storage.create(&other, b"another store").await.unwrap();

    let discovery = Discovery::prepare(&native, store).await;
    // Queries are complete only beyond the common one-hour listing margin.
    tokio::time::sleep(Duration::from_secs(3601)).await;
    clear_pages(&native);
    let listed = storage.list(&ObjectPrefix::all(store)).await.unwrap();
    check_pages(&native, &["path", "size"], listed.len());
    assert_eq!(listed.len(), pages.len() + 3 + discovery.objects().len());
    for object in discovery.objects() {
        assert_eq!(
            listed
                .iter()
                .find(|listed| listed.path == object.path)
                .unwrap(),
            object
        );
    }
    discovery.check(&native, store, &listed).await;
    assert!(pages
        .iter()
        .all(|path| listed.iter().any(|object| &object.path == path)));
    let large_metadata = listed.iter().find(|object| object.path == large).unwrap();
    assert_eq!(large_metadata.size, PART_SIZE as u64 + 3);
    assert!(large_metadata.stored_at >= before_publication);
    assert_eq!(*large_metadata, published);
    assert_eq!(
        listed.iter().find(|object| object.path == first).unwrap(),
        &first_metadata
    );
    assert_eq!(
        listed
            .iter()
            .find(|object| object.path == positions)
            .unwrap(),
        &positions_metadata
    );
    for path in [&first, &positions, &large] {
        storage.delete(path).await.unwrap();
        storage.delete(path).await.unwrap();
        assert!(storage.status(path).await.unwrap().is_none());
        assert_eq!(
            storage.read(path).await.unwrap_err().failure(),
            StorageFailure::NotFound
        );
    }
    discovery.delete_zone().await;
    live_zone::delete(&native).await.unwrap();
}

/// Run only from the entitled macOS app described in cloudkit-live.md. The
/// server's refusals keep the evidence that it answered (E5).
#[tokio::test]
#[ignore = "requires a signed CloudKit host and an iCloud account"]
async fn cloudkit_live_response_evidence() {
    let ids: IdSourceRef = Arc::new(UuidIds);
    let store = StoreId(ids.new_id());
    let native = Arc::new(
        live_zone::prepare(store, "cloudkit_live_response_evidence", ids.clone())
            .await
            .unwrap(),
    );
    let storage = StorageConnection::from_provider(native.clone());
    let path = ObjectPath::file(store, DeviceId(7), FileId(ids.new_id()));
    let absent = storage.read(&path).await.unwrap_err();
    assert_eq!(absent.failure(), StorageFailure::NotFound);
    assert!(absent.answered());
    assert_eq!(absent.retry_after(), None);
    storage.create(&path, b"encrypted bytes").await.unwrap();
    let occupied = storage.create(&path, b"changed").await.unwrap_err();
    assert_eq!(occupied.failure(), StorageFailure::AlreadyExists);
    assert!(occupied.answered());
    live_zone::delete(&native).await.unwrap();
    let deleted = storage.read(&path).await.unwrap_err();
    assert_eq!(deleted.failure(), StorageFailure::ContainerNotFound);
    assert!(deleted.answered());
}

#[test]
fn partial_failures_keep_the_item_cause_and_do_not_hide_session_conflicts() {
    use super::record::save_error;
    use objc2::{runtime::AnyObject, AnyThread};
    use objc2_foundation::NSDictionary;
    for (codes, expected) in [
        (
            vec![
                CKErrorCode::PermissionFailure,
                CKErrorCode::BatchRequestFailed,
            ],
            StorageFailure::PermissionDenied,
        ),
        (
            vec![CKErrorCode::QuotaExceeded, CKErrorCode::PermissionFailure],
            StorageFailure::Refused,
        ),
        (
            vec![CKErrorCode::BatchRequestFailed],
            StorageFailure::Protocol,
        ),
    ] {
        let keys: Vec<_> = (0..codes.len())
            .map(|index| NSString::from_str(&index.to_string()))
            .collect();
        let errors: Vec<_> = codes
            .iter()
            .map(|code| NSError::new(code.0, unsafe { CKErrorDomain }))
            .collect();
        let items = NSDictionary::from_retained_objects(
            &keys.iter().map(|key| &**key).collect::<Vec<_>>(),
            &errors,
        );
        let info = NSDictionary::<NSString, AnyObject>::from_slices(
            &[unsafe { CKPartialErrorsByItemIDKey }],
            &[&items],
        );
        let partial = unsafe {
            NSError::initWithDomain_code_userInfo(
                NSError::alloc(),
                CKErrorDomain,
                CKErrorCode::PartialFailure.0,
                Some(&info),
            )
        };
        assert_eq!(native_error(&partial).failure(), expected);
    }
    let conflict = NSError::new(CKErrorCode::ServerRecordChanged.0, unsafe { CKErrorDomain });
    assert_eq!(
        save_error(&conflict, None).failure(),
        StorageFailure::Refused
    );
    assert_eq!(
        save_error(&conflict, Some(path().as_str())).failure(),
        StorageFailure::Refused
    );
}

#[test]
fn destination_conflict_keeps_already_exists_for_create_once() {
    use objc2::{runtime::AnyObject, AnyThread};
    use objc2_foundation::NSDictionary;
    let path = path();
    let id = unsafe {
        CKRecordID::initWithRecordName(CKRecordID::alloc(), &NSString::from_str(path.as_str()))
    };
    let record = record::Record::new("CovenObject", &id);
    let info = NSDictionary::<NSString, AnyObject>::from_slices(
        &[unsafe { CKRecordChangedErrorClientRecordKey }],
        &[record.raw()],
    );
    let conflict = unsafe {
        NSError::initWithDomain_code_userInfo(
            NSError::alloc(),
            CKErrorDomain,
            CKErrorCode::ServerRecordChanged.0,
            Some(&info),
        )
    };
    assert_eq!(
        super::record::save_error(&conflict, Some(path.as_str())).failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(
        super::record::save_error(&conflict, Some("other")).failure(),
        StorageFailure::Refused
    );
}

#[test]
fn atomic_destination_conflicts_differ_from_session_conflicts() {
    use objc2::{runtime::AnyObject, AnyThread};
    use objc2_foundation::NSDictionary;
    let path = path();
    let root = upload_name(&path, &uuid::Uuid::from_u128(9).to_string()).unwrap();
    let ids: Vec<_> = [path.as_str(), root.as_str()]
        .iter()
        .map(|name| unsafe {
            CKRecordID::initWithRecordName(CKRecordID::alloc(), &NSString::from_str(name))
        })
        .collect();
    for (changed, expected) in [
        (0, StorageFailure::AlreadyExists),
        (1, StorageFailure::Refused),
    ] {
        let errors: Vec<_> = (0..2)
            .map(|index| {
                let code = if index == changed {
                    CKErrorCode::ServerRecordChanged
                } else {
                    CKErrorCode::BatchRequestFailed
                };
                NSError::new(code.0, unsafe { CKErrorDomain })
            })
            .collect();
        let items = NSDictionary::from_retained_objects(
            &ids.iter().map(|id| &**id).collect::<Vec<_>>(),
            &errors,
        );
        let info = NSDictionary::<NSString, AnyObject>::from_slices(
            &[unsafe { CKPartialErrorsByItemIDKey }],
            &[&items],
        );
        let partial = unsafe {
            NSError::initWithDomain_code_userInfo(
                NSError::alloc(),
                CKErrorDomain,
                CKErrorCode::PartialFailure.0,
                Some(&info),
            )
        };
        assert_eq!(
            super::record::save_error(&partial, Some(path.as_str())).failure(),
            expected
        );
    }
}

pub(super) struct Discovery {
    objects: Vec<StoredObject>,
    pending: Vec<UploadSession>,
    other_zone: CloudKitStorage,
}

impl Discovery {
    pub(super) async fn prepare(native: &Arc<CloudKitStorage>, store: StoreId) -> Self {
        let storage = StorageConnection::from_provider(native.clone());
        let mut paths = Vec::new();
        for device in 100..100 + native::QUERY_PAGE_SIZE as u64 {
            let path = ObjectPath::file(store, DeviceId(device), FileId(native.ids.new_id()));
            storage.create(&path, b"file").await.unwrap();
            paths.push(path);
        }
        let circle = Audience::Circle(CircleId(native.ids.new_id()));
        for device in [DeviceId(7), DeviceId(70)] {
            for path in [
                ObjectPath::snapshot(store, Audience::Store, device, NonZeroU64::MIN),
                ObjectPath::snapshot(store, circle.clone(), device, NonZeroU64::MIN),
                ObjectPath::file(store, device, FileId(native.ids.new_id())),
            ] {
                storage.create(&path, b"scoped").await.unwrap();
                paths.push(path);
            }
        }
        let join = ObjectPath::join_request(store, InviteId(native.ids.new_id()));
        storage.create(&join, b"join").await.unwrap();
        paths.push(join);

        // Unpublished slots and file uploads remain outside object listings.
        let mut pending = Vec::new();
        for path in [
            ObjectPath::slot(store, NonZeroU64::new(1000).unwrap()),
            ObjectPath::file(store, DeviceId(71), FileId(native.ids.new_id())),
        ] {
            let mut session = storage
                .begin_upload(&path, PART_SIZE as u64 + 1)
                .await
                .unwrap();
            storage
                .upload_part(&mut session, &vec![42; PART_SIZE])
                .await
                .unwrap();
            assert!(storage.status(&path).await.unwrap().is_none());
            pending.push(session);
        }

        let other_store = StoreId(native.ids.new_id());
        let other_zone = live_zone::prepare(other_store, "cloudkit_live", native.ids.clone())
            .await
            .unwrap();
        // Same store id and an extra object in another zone must stay invisible.
        let entry = ObjectPath::slot(store, NonZeroU64::MIN);
        let positions = ObjectPath::floor(store);
        let fixture_request = |path| {
            PublicationRequest::new(Arc::new(crate::test_utils::UnrestrictedPublications), path)
        };
        other_zone
            .create(&fixture_request(entry.clone()), &entry, b"other zone")
            .await
            .unwrap();
        other_zone
            .create(
                &fixture_request(positions.clone()),
                &positions,
                b"other zone revision",
            )
            .await
            .unwrap();

        let mut objects = Vec::new();
        for path in paths {
            objects.push(storage.status(&path).await.unwrap().unwrap());
        }
        Self {
            objects,
            pending,
            other_zone,
        }
    }

    pub(super) fn objects(&self) -> &[StoredObject] {
        &self.objects
    }

    pub(super) async fn check(
        &self,
        native: &Arc<CloudKitStorage>,
        store: StoreId,
        all: &[StoredObject],
    ) {
        let storage = StorageConnection::from_provider(native.clone());
        for prefix in [
            ObjectPrefix::files(store),
            ObjectPrefix::device_files(store, DeviceId(7)),
            ObjectPrefix::join_requests(store),
        ] {
            clear_pages(native);
            let listed = storage.list(&prefix).await.unwrap();
            let expected: Vec<_> = all
                .iter()
                .filter(|object| prefix.contains(&object.path))
                .cloned()
                .collect();
            assert_eq!(listed, expected, "{}", prefix.as_str());
            check_pages(native, &["path", "size"], listed.len());
        }
        for session in &self.pending {
            assert!(storage.status(session.path()).await.unwrap().is_none());
            storage.abort_upload(session).await.unwrap();
        }
    }

    pub(super) async fn delete_zone(&self) {
        live_zone::delete(&self.other_zone).await.unwrap();
    }
}

pub(super) fn clear_pages(native: &CloudKitStorage) {
    native.query_pages.lock().unwrap().clear();
}

pub(super) fn check_pages(native: &CloudKitStorage, keys: &[&str], records: usize) {
    let pages = native.query_pages.lock().unwrap();
    assert!(!pages.is_empty());
    assert_eq!(
        pages.iter().map(|page| page.records).sum::<usize>(),
        records
    );
    assert!(pages.len() >= records.div_ceil(native::QUERY_PAGE_SIZE));
    let ids: BTreeSet<_> = pages.iter().map(|page| &page.operation_id).collect();
    assert_eq!(ids.len(), pages.len());
    for (index, page) in pages.iter().enumerate() {
        assert_eq!(page.zone, native.zone);
        assert_eq!(page.owner, native.owner);
        assert_eq!(page.keys, keys);
        assert_eq!(page.continuation, index != 0);
        assert!(page.records <= native::QUERY_PAGE_SIZE);
        assert_eq!(
            page.asset_records, 0,
            "metadata queries must not fetch assets"
        );
        assert!(!page.operation_id.is_empty());
        // Correlate native operations with CloudKit Console's server request logs.
        tracing::info!(?page, "CloudKit query page");
    }
}

#[derive(Debug)]
pub(super) struct Page {
    pub(super) operation_id: String,
    pub(super) zone: String,
    pub(super) owner: String,
    pub(super) keys: Vec<String>,
    pub(super) continuation: bool,
    pub(super) records: usize,
    pub(super) asset_records: usize,
}

pub(super) type Pages = Arc<Mutex<Vec<Page>>>;

pub(super) fn query_enqueued(pages: &Pages, operation: &CKQueryOperation) -> usize {
    let mut pages = pages.lock().unwrap();
    let index = pages.len();
    // The caller still owns configuration exclusively, before addOperation.
    unsafe {
        let zone = operation.zoneID().unwrap();
        pages.push(Page {
            operation_id: operation.operationID().to_string(),
            zone: zone.zoneName().to_string(),
            owner: zone.ownerName().to_string(),
            keys: operation
                .desiredKeys()
                .unwrap()
                .iter()
                .map(|key| key.to_string())
                .collect(),
            continuation: operation.cursor().is_some(),
            records: 0,
            asset_records: 0,
        });
    }
    index
}

pub(super) fn query_matched(pages: &Pages, index: usize, record: Option<&CKRecord>) {
    let mut pages = pages.lock().unwrap();
    pages[index].records += 1;
    if record
        .is_some_and(|record| unsafe { record.objectForKey(&NSString::from_str("data")).is_some() })
    {
        pages[index].asset_records += 1;
    }
}
