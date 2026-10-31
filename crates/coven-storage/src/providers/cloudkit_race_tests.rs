use super::*;
use crate::providers::cloudkit::tests::live_zone;
use coven_foundation::{
    clock::{ClockRef, SystemClock},
    id_source::{StoreId, UuidIds},
};
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};

const TEST: &str = "cloudkit_live_create_once_race";
/// Both macOS users of one Mac can write here; each run uses a fresh directory.
const SHARED: &str = "/Users/Shared";
/// How long the owner waits for the recipient to accept the share and join.
const JOIN_TIMEOUT: Duration = Duration::from_secs(3600);
/// How long either process waits for the other's next step within a run.
const STEP_TIMEOUT: Duration = Duration::from_secs(600);
/// How far ahead of now the release instant is set, longer than either
/// process takes to notice it.
const RELEASE_MARGIN: Duration = Duration::from_millis(250);
const POLL: Duration = Duration::from_millis(2);

#[test]
fn retryable_reply_cannot_hide_multiple_winners_or_unexpected_errors() {
    let outcomes = |second| {
        [
            Verdict::Won,
            second,
            Verdict::Retryable { cause: "interrupted".into(), retry_after_ms: None },
        ]
        .into_iter()
        .enumerate()
        .map(|(writer, verdict)| Outcome {
            writer: format!("owner-{writer}"),
            verdict,
            late_us: 0,
            started_us: writer as u64,
            finished_us: 10,
        })
        .collect()
    };
    for (second, read) in [
        (Verdict::Won, Ok(Some(claim("owner-0", 1)))),
        (Verdict::Other("permission denied".into()), Ok(Some(claim("owner-0", 1)))),
        (Verdict::Occupied("already exists".into()), Ok(Some(claim("owner-2", 1)))),
        (Verdict::Occupied("already exists".into()), Ok(None)),
        (Verdict::Occupied("already exists".into()), Err(StorageFailure::PermissionDenied.into())),
    ] {
        let mut tally = Tally::default();
        assert!(matches!(tally.record(1, outcomes(second), read), Settled::Round));
        assert_eq!(tally.anomalies().len(), 1);
    }
    let mut tally = Tally::default();
    assert!(matches!(
        tally.record(
            1,
            outcomes(Verdict::Occupied("already exists".into())),
            Ok(Some(claim("owner-0", 1)))
        ),
        Settled::Retry(_)
    ));
    assert!(tally.anomalies().is_empty());
    assert_eq!(tally.rounds(), 0);
}

/// Read by the recipient from the private invitation file.
#[derive(Serialize, Deserialize)]
struct Handoff {
    store: StoreId,
    invitation: StorageInvitation,
    rendezvous: PathBuf,
}

/// One attempt, as the owner announces it to the recipient.
#[derive(Serialize, Deserialize)]
struct Go {
    path: ObjectPath,
    writers: usize,
}

/// A directory both processes read and write. Each message is one file with
/// one writer, published by rename so a reader never sees a partial file.
/// Each side watches for the other's stop file while it waits: `end` from the
/// owner (true once the run completed) and `stopped` from a failed recipient.
struct Rendezvous {
    directory: PathBuf,
    stop: &'static str,
}

impl Rendezvous {
    /// Sticky and world-writable, like `/Users/Shared`: either user adds
    /// files, and only a file's author or the directory's owner removes them.
    fn create(directory: PathBuf) -> Self {
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o1777)).unwrap();
        Self { directory, stop: "stopped" }
    }

    fn write<T: Serialize>(&self, name: &str, value: &T) -> std::io::Result<()> {
        let partial = self.directory.join(format!(".{name}.partial"));
        std::fs::write(&partial, serde_json::to_vec(value)?)?;
        std::fs::rename(partial, self.directory.join(name))
    }

    fn put<T: Serialize>(&self, name: &str, value: &T) {
        self.write(name, value).unwrap();
    }

    fn peek<T: DeserializeOwned>(&self, name: &str) -> Option<T> {
        match std::fs::read(self.directory.join(name)) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes).unwrap()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => panic!("reading {name}: {error}"),
        }
    }

    /// The message `name`, or the other side's stop value if that comes first.
    async fn take<T: DeserializeOwned>(&self, name: &str, timeout: Duration) -> Result<T, bool> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(value) = self.peek(name) {
                return Ok(value);
            }
            if let Some(stop) = self.peek(self.stop) {
                return Err(stop);
            }
            assert!(tokio::time::Instant::now() < deadline, "no {name} within {timeout:?}");
            tokio::time::sleep(POLL).await;
        }
    }

    /// Writes `name` from a drop, where a failure can only be reported.
    fn signal(&self, name: &str) {
        if let Err(error) = self.write(name, &false) {
            eprintln!("could not tell the other process to stop: {error}");
        }
    }
}

/// The owner's side of the rendezvous. Stopping before the run completes
/// tells the recipient to stop too.
struct Peer {
    clock: ClockRef,
    rendezvous: Rendezvous,
    complete: bool,
}

impl Peer {
    async fn take<T: DeserializeOwned>(&self, name: &str, timeout: Duration) -> T {
        match self.rendezvous.take(name, timeout).await {
            Ok(value) => value,
            Err(_) => panic!("the recipient stopped before sending {name}"),
        }
    }

    /// Announces the attempt, waits until the recipient is ready, then sets one
    /// release instant for both processes.
    async fn release(&self, attempt: u64, go: &Go) -> u64 {
        self.rendezvous.put(&format!("go-{attempt}"), go);
        self.take::<()>(&format!("ready-{attempt}"), STEP_TIMEOUT).await;
        let release = now(&*self.clock) + RELEASE_MARGIN.as_micros() as u64;
        self.rendezvous.put(&format!("release-{attempt}"), &release);
        release
    }

    async fn outcomes(&self, attempt: u64) -> Vec<Outcome> {
        self.take(&format!("result-{attempt}"), STEP_TIMEOUT).await
    }

    async fn finish(mut self) {
        self.rendezvous.put("end", &true);
        self.take::<()>("left", STEP_TIMEOUT).await;
        std::fs::remove_dir_all(&self.rendezvous.directory).unwrap();
        self.complete = true;
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if !self.complete {
            self.rendezvous.signal("end");
        }
    }
}

/// The recipient's side: a recipient that fails tells the owner it stopped.
struct Recipient {
    rendezvous: Rendezvous,
    left: bool,
}

impl Recipient {
    /// The message `name`, or None once the owner ended the run, completed or not.
    async fn take<T: DeserializeOwned>(&mut self, name: &str) -> Option<T> {
        match self.rendezvous.take(name, STEP_TIMEOUT).await {
            Ok(value) => Some(value),
            Err(complete) => {
                self.rendezvous.put("left", &());
                self.left = true;
                assert!(complete, "the owner stopped before the race completed");
                None
            }
        }
    }
}

impl Drop for Recipient {
    fn drop(&mut self) {
        if !self.left {
            self.rendezvous.signal("stopped");
        }
    }
}

/// Runs `stage` with `per_account` local writers and, with a peer, as many of
/// the recipient's. Attempts are numbered across stages, as the recipient counts them.
async fn run_stage(
    clock: &ClockRef,
    stage: Stage,
    per_account: usize,
    writers: &[Writer],
    peer: Option<&Peer>,
    reader: &StorageConnection<CloudKitStorage>,
    store: StoreId,
    attempt: &mut u64,
) -> Tally {
    let mut tally = Tally::default();
    while tally.rounds() < stage.rounds {
        *attempt += 1;
        let path = ObjectPath::slot(store, (*attempt).try_into().unwrap());
        let release = match peer {
            Some(peer) => {
                let go = Go { path: path.clone(), writers: per_account };
                peer.release(*attempt, &go).await
            }
            None => now(&**clock) + RELEASE_MARGIN.as_micros() as u64,
        };
        let mut outcomes = race(&**clock, &writers[..per_account], &path, *attempt, release).await;
        if let Some(peer) = peer {
            outcomes.extend(peer.outcomes(*attempt).await);
        }
        let settled = tally.settle(&**clock, reader, &path, *attempt, outcomes).await;
        if !tally.anomalies().is_empty() || tally.overlapping != tally.rounds() {
            tally.print(&format!("{} writers (failed)", stage.writers));
            panic!("CloudKit slot race failed");
        }
        if let Settled::Retry(wait) = settled {
            if tally.streak() >= RETRIES {
                tally.print(&format!("{} writers (gave up)", stage.writers));
                panic!("{RETRIES} attempts in a row were inconclusive");
            }
            tokio::time::sleep(wait).await;
        }
    }
    tally
}

/// Prints every stage, then requires that none had an anomaly.
fn check(tallies: &[(Stage, Tally)]) {
    for (stage, tally) in tallies {
        tally.print(&format!("{} writers", stage.writers));
    }
    for (stage, tally) in tallies {
        assert!(
            tally.anomalies().is_empty(),
            "{} writers: {} anomalies",
            stage.writers,
            tally.anomalies().len()
        );
    }
}

/// Run only from the entitled macOS app described in cloudkit-live.md, with
/// `COVEN_CLOUDKIT_TEST_ROLE` set to `single` (every writer on one account,
/// in this process) or `owner`/`recipient` (half the writers on each of two
/// accounts, in two processes run by two macOS users of one Mac).
// Stderr prompts are the interactive test protocol, not diagnostic logs.
#[tokio::test]
#[ignore = "requires a signed CloudKit host and an iCloud account; owner and recipient need a second macOS user's session"]
async fn cloudkit_live_create_once_race() {
    let role = std::env::var("COVEN_CLOUDKIT_TEST_ROLE").expect("single, owner or recipient");
    let ids: IdSourceRef = Arc::new(UuidIds);
    let clock: ClockRef = Arc::new(SystemClock);
    match role.as_str() {
        "single" => {
            let store = StoreId(ids.new_id());
            use crate::providers::tests::sequence_live;
            use futures_util::FutureExt;
            let native = Arc::new(live_zone::prepare(store, TEST, ids.clone()).await.unwrap());
            let reader = StorageConnection::from_provider(native.clone());
            let result = std::panic::AssertUnwindSafe(async {
                let most = STAGES.iter().map(|stage| stage.writers).max().unwrap();
                let writers = Writer::connect("owner", &native.config(), &ids, most).await;
                sequence_live::lost_replies(
                    clock.as_ref(),
                    &reader,
                    writers[0].storage.as_ref(),
                    store,
                )
                .await
                .unwrap();
                let mut attempt = 1;
                let mut tallies = Vec::new();
                for stage in STAGES {
                    let tally = run_stage(
                        &clock,
                        stage,
                        stage.writers,
                        &writers,
                        None,
                        &reader,
                        store,
                        &mut attempt,
                    )
                    .await;
                    tallies.push((stage, tally));
                }
                check(&tallies);
                let writers: Vec<Arc<dyn Storage>> =
                    writers.into_iter().map(|writer| writer.storage as Arc<dyn Storage>).collect();
                sequence_live::floor(
                    clock.as_ref(),
                    &writers,
                    store,
                    sequence_live::AllRefused::Fails,
                )
                .await
                .unwrap();
            })
            .catch_unwind()
            .await;
            println!("race deleted zone marked by {}", live_zone::delete(&native).await.unwrap());
            if let Err(panic) = result {
                std::panic::resume_unwind(panic);
            }
        }
        "owner" => {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let email =
                std::env::var("COVEN_CLOUDKIT_TEST_EMAIL").expect("recipient's iCloud email");
            let handoff = std::env::var("COVEN_CLOUDKIT_TEST_INVITATION")
                .expect("private invitation file path");
            let store = StoreId(ids.new_id());
            let native = Arc::new(live_zone::prepare(store, TEST, ids.clone()).await.unwrap());
            let reader = StorageConnection::from_provider(native.clone());
            let AccessGrant::Granted { invitation } = reader.grant_access(&email).await.unwrap()
            else {
                panic!("CloudKit must grant an account");
            };
            let rendezvous = Rendezvous::create(
                Path::new(SHARED).join(format!("coven-cloudkit-race-{}", ids.new_id())),
            );
            let peer = Peer { clock: clock.clone(), rendezvous, complete: false };
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&handoff)
                .unwrap();
            let data = serde_json::to_vec(&Handoff {
                store,
                invitation,
                rendezvous: peer.rendezvous.directory.clone(),
            })
            .unwrap();
            file.write_all(&data).unwrap();
            file.sync_all().unwrap();
            eprintln!("Transfer the private invitation file to the recipient's macOS user and start its test there. This host waits for it to join.");
            peer.take::<()>("joined", JOIN_TIMEOUT).await;
            let most = STAGES.iter().map(|stage| stage.writers / 2).max().unwrap();
            let writers = Writer::connect("owner", &native.config(), &ids, most).await;
            let mut attempt = 0;
            let mut tallies = Vec::new();
            for stage in STAGES {
                let tally = run_stage(
                    &clock,
                    stage,
                    stage.writers / 2,
                    &writers,
                    Some(&peer),
                    &reader,
                    store,
                    &mut attempt,
                )
                .await;
                tallies.push((stage, tally));
            }
            check(&tallies);
            println!("race deleted zone marked by {}", live_zone::delete(&native).await.unwrap());
            peer.finish().await;
            std::fs::remove_file(handoff).unwrap();
        }
        "recipient" => {
            let email =
                std::env::var("COVEN_CLOUDKIT_TEST_EMAIL").expect("recipient's iCloud email");
            let handoff = std::env::var("COVEN_CLOUDKIT_TEST_INVITATION")
                .expect("private invitation file path");
            let Handoff { store, invitation, rendezvous } =
                serde_json::from_slice(&std::fs::read(&handoff).unwrap()).unwrap();
            let mut recipient = Recipient {
                rendezvous: Rendezvous { directory: rendezvous, stop: "end" },
                left: false,
            };
            let native = Arc::new(
                CloudKitStorage::new(invitation.location().clone(), ids.clone()).await.unwrap(),
            );
            let joined = StorageConnection::from_provider(native.clone());
            joined.join(store, &invitation).await.unwrap();
            println!("recipient current participant: {}", current_participant(&native).await);
            assert_eq!(
                joined.account().await.unwrap().to_ascii_lowercase(),
                email.to_ascii_lowercase()
            );
            let most = STAGES.iter().map(|stage| stage.writers / 2).max().unwrap();
            let writers = Writer::connect("recipient", invitation.location(), &ids, most).await;
            recipient.rendezvous.put("joined", &());
            let mut attempt = 0;
            loop {
                attempt += 1;
                let Some(go) = recipient.take::<Go>(&format!("go-{attempt}")).await else {
                    break;
                };
                recipient.rendezvous.put(&format!("ready-{attempt}"), &());
                let Some(release) = recipient.take::<u64>(&format!("release-{attempt}")).await
                else {
                    break;
                };
                let outcomes =
                    race(&*clock, &writers[..go.writers], &go.path, attempt, release).await;
                recipient.rendezvous.put(&format!("result-{attempt}"), &outcomes);
            }
            std::fs::remove_file(handoff).unwrap();
        }
        _ => panic!("COVEN_CLOUDKIT_TEST_ROLE must be single, owner or recipient"),
    }
}

/// What `account()` reads from the share on a recipient: the current user
/// participant's role and every identity field it could name the account by.
/// The share URL is not read.
async fn current_participant(storage: &CloudKitStorage) -> String {
    use objc2::AnyThread;
    let name = unsafe {
        objc2_cloud_kit::CKShare::initWithRecordZoneID(
            objc2_cloud_kit::CKShare::alloc(),
            &storage.zone_id(),
        )
        .recordID()
        .recordName()
        .to_string()
    };
    let mut share = storage.fetch(&name, &[]).wait().await.unwrap();
    unsafe {
        let Some(person) = share.share().unwrap().currentUserParticipant() else {
            return "none".to_owned();
        };
        let identity = person.userIdentity();
        let lookup = identity.lookupInfo();
        format!(
            "role={:?} acceptance={:?} permission={:?} user record={:?} \
             has iCloud account={} lookup info present={} lookup email={:?} \
             lookup phone={:?} lookup user record={:?}",
            person.role(),
            person.acceptanceStatus(),
            person.permission(),
            identity.userRecordID().map(|id| id.recordName().to_string()),
            identity.hasiCloudAccount(),
            lookup.is_some(),
            lookup.as_ref().and_then(|lookup| lookup.emailAddress()).map(|email| email.to_string()),
            lookup.as_ref().and_then(|lookup| lookup.phoneNumber()).map(|phone| phone.to_string()),
            lookup
                .as_ref()
                .and_then(|lookup| lookup.userRecordID())
                .map(|id| id.recordName().to_string()),
        )
    }
}
