//! An upload no file waits for any more has its provider session aborted
//! before it retires (§17.9), and `sessions` runs the worker's provider
//! sessions against the storage model.

use super::*;
use coven_foundation::clock::{Clock, FixedClock};
use coven_storage::{test_utils::Faults, CloudProvider};

/// A song too large for one request, staged for row `song`, whose first
/// part fails: its upload keeps the provider session it began.
async fn left_in_parts(provider: CloudProvider, clock: &Arc<FixedClock>) -> (Fixture, FileRef) {
    let f = Fixture::with_provider(Provenance::AppProvided, clock.clone(), CHUNK as u64, provider)
        .await;
    // Only the explicit drains below attempt uploads.
    f.files.stop_background_for_counts().await;
    f.set_uploads_paused(false).await;
    f.storage.set_faults(Faults { drop_part: true, ..Faults::none() }).await;
    let song = f.attach("files", "song", vec![7; CHUNK * 3]).await;
    f.files.inner.drain_uploads().await.unwrap();
    assert!(f.upload_of(&song).await.unwrap().session.is_some(), "{provider:?}");
    assert_eq!(f.storage.unfinished_uploads().await.len(), 1, "{provider:?}");
    (f, song)
}

async fn delete_song(f: &Fixture) {
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id='song'", [])?;
            Ok(())
        })
        .await
        .unwrap();
}

/// Ana deletes the song while its upload holds a provider session. The
/// worker's next run, once the failed part's wait has passed, aborts the
/// session and then retires the upload, on every provider: retiring it
/// alone would leave the parts at the provider for good (§17.9).
#[tokio::test]
async fn retiring_an_upload_aborts_its_session_first() {
    for provider in [
        CloudProvider::S3,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
        CloudProvider::CloudKit,
    ] {
        let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)));
        let (f, _) = left_in_parts(provider, &clock).await;
        delete_song(&f).await;
        clock.set(clock.now() + Duration::from_secs(60));
        f.files.inner.drain_uploads().await.unwrap();
        assert!(f.storage.unfinished_uploads().await.is_empty(), "{provider:?}");
        assert!(f.files.inner.database.all_uploads().await.unwrap().is_empty(), "{provider:?}");
        f.close().await;
    }
}

/// The abort fails. The upload stays queued, its session still recorded,
/// and the failure is the song's pending record. The run after its wait
/// aborts the session and retires the upload, and the record goes with it.
#[tokio::test]
async fn a_failed_abort_keeps_the_upload_until_its_retry_aborts_it() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)));
    let (f, song) = left_in_parts(CloudProvider::Dropbox, &clock).await;
    let [subject, _] = f.subjects(&song).await;
    let reason = |f: &Fixture| {
        let subject = subject.clone();
        let database = f.database.clone();
        async move {
            database
                .pending::<crate::PendingRecord>()
                .await
                .unwrap()
                .into_iter()
                .find(|record| record.subject == subject)
                .map(|record| record.reason)
        }
    };
    delete_song(&f).await;
    clock.set(clock.now() + Duration::from_secs(60));
    f.storage.set_faults(Faults { abort_failures: 1, ..Faults::none() }).await;
    f.files.inner.drain_uploads().await.unwrap();
    assert_eq!(f.storage.unfinished_uploads().await.len(), 1);
    let left = f.files.inner.database.all_uploads().await.unwrap();
    assert!(left.len() == 1 && left[0].session.is_some());
    assert!(matches!(
        reason(&f).await,
        Some(crate::PendingReason::Storage { failure: coven_storage::StorageFailure::Network, .. })
    ));
    let wait = left[0].retry.as_ref().unwrap().wait;
    f.files.inner.drain_uploads().await.unwrap();
    assert_eq!(f.storage.unfinished_uploads().await.len(), 1, "it waits out its retry");
    clock.set(clock.now() + wait);
    f.files.inner.drain_uploads().await.unwrap();
    assert!(f.storage.unfinished_uploads().await.is_empty());
    assert!(f.files.inner.database.all_uploads().await.unwrap().is_empty());
    assert_eq!(reason(&f).await, None);
    f.close().await;
}

/// With no storage connected, an upload whose session needs aborting stays
/// queued, while one without a session retires (§17.9).
#[tokio::test]
async fn without_storage_only_an_upload_with_no_session_retires() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)));
    let (f, _) = left_in_parts(CloudProvider::Dropbox, &clock).await;
    f.storage.set_faults(Faults { fail_next: 1, ..Faults::none() }).await;
    f.attach("files", "photo", b"photo".to_vec()).await;
    f.files.inner.drain_uploads().await.unwrap();
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files", [])?;
            Ok(())
        })
        .await
        .unwrap();
    f.files.set_storage(None, async { Ok(()) }).await.unwrap();
    f.files.inner.drain_uploads().await.unwrap_err();
    let left = f.files.inner.database.all_uploads().await.unwrap();
    assert!(left.len() == 1 && left[0].session.is_some());
    assert_eq!(f.storage.unfinished_uploads().await.len(), 1);
    f.close().await;
}

/// The upload worker's provider sessions against the storage model's
/// `UploadSession` (§17.9): a file in three parts, through runs that meet a
/// lost part reply, a dropped part, a crash, an expired session, a lost
/// completion reply and failures of each retry class, and the abort of a
/// session no file waits for any more.
mod sessions {
    use super::{Fixture, CHUNK};
    use coven_database::{FileLocation, FilePlace, FileRef, Provenance};
    use coven_foundation::clock::{Clock, FixedClock, HANG_GUARD};
    use coven_storage::{test_utils::Faults, Storage, StorageFailure, UploadSession};
    use serde_json::{json, Value};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    /// A photo whose encrypted bytes take three of the fixture's parts.
    fn photo() -> Vec<u8> {
        (0..150 * 1024).map(|i| (i % 251) as u8).collect()
    }

    const PARTS: u64 = 3;

    /// The provider parts these confirmed bytes fill.
    fn parts(bytes: u64) -> u64 {
        bytes.div_ceil(CHUNK as u64)
    }

    /// A storage failure of each class (api.md E5).
    fn failure_of(class: &str) -> StorageFailure {
        match class {
            "automatic" => StorageFailure::Network,
            "appAction" => StorageFailure::PermissionDenied,
            "never" => StorageFailure::InvalidPath,
            other => panic!("unknown class {other}"),
        }
    }

    fn run(retry: bool, fault: Value) -> Value {
        json!({"op": "run", "retry": retry, "fault": fault})
    }
    fn plain() -> Value {
        run(false, json!({"kind": "none"}))
    }
    fn retried() -> Value {
        run(true, json!({"kind": "none"}))
    }
    fn after(parts: u64, then: &str) -> Value {
        run(false, json!({"kind": "parts", "parts": parts, "then": then}))
    }
    fn failing(class: &str) -> Value {
        run(false, json!({"kind": "fail", "class": class}))
    }
    fn expiring() -> Value {
        run(false, json!({"kind": "expire"}))
    }
    fn losing_completion() -> Value {
        run(false, json!({"kind": "lose_completion"}))
    }
    fn event(op: &str) -> Value {
        json!({"op": op})
    }

    /// One device with the photo queued, its upload worker run only by the
    /// test, and the provider's parts held at a gate each run sets.
    struct Device {
        f: Fixture,
        clock: Arc<FixedClock>,
        file: FileRef,
        location: FileLocation,
    }

    impl Device {
        async fn new() -> Self {
            let clock =
                Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)));
            let f = Fixture::with_clock(Provenance::AppProvided, clock.clone()).await;
            f.files.stop_background_for_counts().await;
            f.set_uploads_paused(false).await;
            let file = f.attach("files", "photo", photo()).await;
            let location = f.location(&file).await;
            let item = f.files.inner.database.uploads().await.unwrap().remove(0);
            assert_eq!(parts(item.bytes_total), PARTS);
            Self { f, clock, file, location }
        }

        /// The process stops and the store opens again.
        async fn reopen(&mut self) {
            self.f.reopen().await;
            self.f.files.stop_background_for_counts().await;
            self.f.set_uploads_paused(false).await;
        }

        /// Part requests the provider has been asked for so far.
        fn part_requests(&self) -> usize {
            self.f
                .storage
                .request_trace()
                .iter()
                .filter(|request| {
                    matches!(
                        request,
                        coven_storage::test_utils::MemoryRequest::Other("upload_part")
                    )
                })
                .count()
        }

        /// One run of the upload worker, meeting `fault`. A fault on the part
        /// after `k` confirmed ones acts once that part is asked for; a run that
        /// ends first never meets it. A crash stops the run there, and the store
        /// opens again after it either way.
        async fn run(&mut self, retry: bool, fault: &Value) {
            let kind = fault["kind"].as_str().unwrap();
            let storage = self.f.storage.clone();
            match kind {
                "expire" => {
                    storage.set_faults(Faults { expire_uploads: true, ..Faults::none() }).await
                }
                "fail" => {
                    let failure = failure_of(fault["class"].as_str().unwrap());
                    storage.set_faults(Faults { fail_next: 1, failure, ..Faults::none() }).await;
                }
                "lose_completion" => {
                    storage
                        .set_faults(Faults { lose_completion_reply: true, ..Faults::none() })
                        .await;
                }
                "none" | "parts" => {}
                other => panic!("unknown fault {other}"),
            }
            let allowed = match kind {
                "parts" => fault["parts"].as_u64().unwrap() as usize,
                // As many as any run of three parts can take.
                _ => 1 << 20,
            };
            let gate = Arc::new(tokio::sync::Semaphore::new(allowed));
            storage.gate_parts(gate.clone()).await;
            let before = self.part_requests();
            let mut requests = storage.subscribe_requests();
            let inner = self.f.files.inner.clone();
            let mut task = tokio::spawn(async move {
                if retry {
                    inner.drain_uploads_retrying().await
                } else {
                    inner.drain_uploads().await
                }
            });
            let mut ended = false;
            if kind == "parts" {
                ended = tokio::time::timeout(HANG_GUARD, async {
                    loop {
                        if self.part_requests() > before + allowed {
                            break false;
                        }
                        tokio::select! {
                            result = &mut task => {
                                result.unwrap().expect("the run");
                                break true;
                            }
                            changed = requests.changed() => changed.unwrap(),
                        }
                    }
                })
                .await
                .expect("the run asks for the faulted part or ends");
            }
            if !ended {
                match fault["then"].as_str() {
                    Some("crash") => {
                        task.abort();
                        assert!(task.await.unwrap_err().is_cancelled());
                    }
                    then => {
                        match then {
                            Some("lost") => {
                                storage
                                    .set_faults(Faults { lose_part_reply: true, ..Faults::none() })
                                    .await;
                            }
                            Some("dropped") => {
                                storage
                                    .set_faults(Faults { drop_part: true, ..Faults::none() })
                                    .await;
                            }
                            _ => {}
                        }
                        if kind == "parts" {
                            gate.add_permits(1);
                        }
                        tokio::time::timeout(HANG_GUARD, task)
                            .await
                            .expect("the run ends")
                            .unwrap()
                            .expect("the run");
                    }
                }
            }
            storage.set_faults(Faults::none()).await;
            if fault["then"] == "crash" {
                self.reopen().await;
            }
        }

        async fn apply(&mut self, event: &Value) {
            match event["op"].as_str().unwrap() {
                "run" => self.run(event["retry"].as_bool().unwrap(), &event["fault"]).await,
                "delete" => {
                    self.f
                        .database
                        .write(|sql| {
                            sql.execute("DELETE FROM files WHERE id='photo'", [])?;
                            Ok(())
                        })
                        .await
                        .unwrap();
                }
                "elapse" => self.clock.set(self.clock.now() + Duration::from_secs(600)),
                "connect" => {
                    let storage = self.f.storage.clone();
                    self.f
                        .files
                        .set_storage(Some(storage), std::future::ready(Ok(())))
                        .await
                        .unwrap();
                }
                "reopen" => self.reopen().await,
                other => panic!("unknown event {other}"),
            }
        }

        /// What the model observes: the queue row and the parts its session
        /// records, the provider's live session and the parts it holds, the
        /// published object, the switched file and the class of the pending
        /// failure about the upload.
        async fn observe(&self) -> Value {
            let uploads = self.f.files.inner.database.all_uploads().await.unwrap();
            assert!(uploads.len() <= 1, "{uploads:?}");
            let upload = uploads.first();
            let recorded = upload.and_then(|upload| {
                upload.session.as_ref().map(|_| {
                    parts(
                        upload
                            .progress
                            .expect("a recorded session has its progress")
                            .bytes_confirmed,
                    )
                })
            });
            let unfinished = self.f.storage.unfinished_uploads().await;
            assert!(unfinished.len() <= 1, "{unfinished:?}");
            let held = match unfinished.first() {
                None => None,
                Some(_) => {
                    let bytes = upload
                        .and_then(|upload| upload.session.as_ref())
                        .expect("the provider's session is one the queue records");
                    let mut session = UploadSession::decode(bytes.as_bytes()).unwrap();
                    self.f.storage.resume_upload(&mut session).await.unwrap();
                    Some(parts(session.confirmed_bytes()))
                }
            };
            let path = self.location.path(self.f.directory.id());
            let published = self.f.storage.status(&path).await.unwrap().is_some();
            let stored = match self.f.database.file_ref(self.file.id()).await {
                Ok(current) => match current.place() {
                    FilePlace::Cloud { .. } => {
                        // Every part went once, in order: the file reads back.
                        assert_eq!(self.f.files.read_file(&current).await.unwrap(), photo());
                        true
                    }
                    FilePlace::DeviceOnly { .. } => false,
                },
                Err(coven_database::DbError::FileAbsent { .. }) => false,
                Err(error) => panic!("reading the record failed: {error}"),
            };
            let subject = crate::PendingSubject::Upload {
                device: self.location.uploader(),
                object: self.location.object(),
            };
            let failure = self
                .f
                .database
                .pending::<crate::PendingRecord>()
                .await
                .unwrap()
                .into_iter()
                .find(|record| record.subject == subject)
                .map(|record| match record.retry() {
                    crate::Retry::Automatic => "automatic",
                    crate::Retry::AppAction => "appAction",
                    crate::Retry::Never => "never",
                    crate::Retry::AfterUpdate => panic!("no upload here waits for an update"),
                });
            json!({"queued": upload.is_some(), "recorded": recorded, "held": held,
                   "published": published, "stored": stored, "failure": failure})
        }
    }

    fn cases() -> Vec<Vec<Value>> {
        let elapse = || event("elapse");
        vec![
            vec![plain()],
            // A lost part reply: the provider holds the part, the record doesn't,
            // and the resumed session continues after it once the delay passes.
            vec![after(1, "lost"), plain(), elapse(), plain()],
            vec![after(1, "dropped"), elapse(), plain()],
            // The process stops mid-upload, and the session continues.
            vec![after(1, "crash"), plain()],
            vec![after(0, "lost"), event("reopen"), plain(), elapse(), plain()],
            // An expired session starts over from its first part.
            vec![after(2, "crash"), expiring()],
            vec![after(1, "lost"), expiring(), elapse(), expiring()],
            // The provider published, but the reply was lost: nothing is resent.
            vec![losing_completion(), plain(), elapse(), plain()],
            // An upload no file waits for aborts its session, once due, then
            // retires.
            vec![after(1, "dropped"), event("delete"), plain(), elapse(), plain()],
            vec![
                after(1, "crash"),
                event("delete"),
                failing("automatic"),
                plain(),
                elapse(),
                plain(),
            ],
            vec![
                after(1, "crash"),
                event("delete"),
                failing("appAction"),
                elapse(),
                plain(),
                event("connect"),
                plain(),
            ],
            vec![
                after(1, "crash"),
                event("delete"),
                failing("never"),
                elapse(),
                event("connect"),
                event("reopen"),
                plain(),
                retried(),
            ],
            vec![after(1, "crash"), event("delete"), expiring()],
            vec![failing("automatic"), event("delete"), plain()],
            // Each class retries as §17.9's table says.
            vec![failing("automatic"), plain(), retried()],
            vec![failing("appAction"), elapse(), plain(), event("connect"), plain()],
            vec![failing("appAction"), event("reopen"), plain()],
            vec![failing("never"), elapse(), event("connect"), event("reopen"), plain(), retried()],
            vec![after(1, "crash"), failing("automatic"), elapse(), plain()],
            vec![after(1, "lost"), retried()],
        ]
    }

    fn model(cases: &[Vec<Value>]) -> Vec<Value> {
        use std::{
            io::Write,
            process::{Command, Stdio},
        };
        let runner =
            std::env::var("COVEN_UPLOAD_SESSION_LEAN").expect("Lean upload session runner");
        let inputs: Vec<Value> =
            cases.iter().map(|events| json!({"total": PARTS, "events": events})).collect();
        let mut child =
            Command::new(runner).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
        child.stdin.take().unwrap().write_all(&serde_json::to_vec(&inputs).unwrap()).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        serde_json::from_slice(&output.stdout).unwrap()
    }

    #[tokio::test]
    #[ignore = "requires COVEN_UPLOAD_SESSION_LEAN; scripts/check.sh builds uploadSessionRunner"]
    async fn upload_sessions_match_the_storage_model() {
        let cases = cases();
        let expected = model(&cases);
        for (events, expected) in cases.iter().zip(expected) {
            let mut device = Device::new().await;
            let mut seen = Vec::new();
            for event in events {
                device.apply(event).await;
                seen.push(device.observe().await);
            }
            assert_eq!(Value::Array(seen), expected, "{}", Value::Array(events.clone()));
            device.f.close().await;
        }
    }
}
