use super::*;
pub(super) use crate::tests::{carol_joins, join_with_response};
use coven_foundation::clock::HANG_GUARD;
use coven_foundation::progress::RecordingClock;

pub(super) fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::SharedKey),
        SyncedTable::new("circle_notes", RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("profiles", RowIdentity::SharedKey),
    ]
}

pub(super) fn migrations() -> Vec<Migration> {
    vec![Migration::addition(
        1,
        "notes",
        "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL, body BLOB NOT NULL); \
         CREATE TABLE circle_notes(id TEXT PRIMARY KEY NOT NULL, body TEXT NOT NULL, audience TEXT NOT NULL); \
         CREATE TABLE profiles(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, circle TEXT NOT NULL); \
         CREATE TABLE joining_runs(member TEXT NOT NULL, person TEXT, circle TEXT NOT NULL)",
    )]
}

/// The test app's joining write: it records each run in a table of the
/// device's own, and writes the person's profile when it has a name and the
/// member has none, so a second run changes nothing synced.
pub(super) fn joining_write(
    sql: &coven_database::JoiningWrite<'_>,
    joining: &coven_database::Joining,
) -> Result<(), DbError> {
    let (member, circle) = (joining.member.to_string(), joining.circle.to_string());
    sql.execute(
        "INSERT INTO joining_runs VALUES(?1,?2,?3)",
        coven_database::params![member, joining.person, circle],
    )?;
    if let Some(person) = &joining.person {
        sql.execute(
            "INSERT INTO profiles SELECT ?1,?2,?3 WHERE NOT EXISTS(SELECT 1 FROM profiles WHERE id=?1)",
            coven_database::params![member, person, circle],
        )?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) struct Owner {
    network: crate::tests::Network,
    pub(super) directory: StoreDir,
    pub(super) handle: CovenHandle,
    pub(super) member: MemberIdentity,
    pub(super) storage: Arc<MemoryStorage>,
    pub(super) clock: Arc<FixedClock>,
    pub(super) code: RestoreCode,
    /// The joins' checks the test asked for, held until Ana goes.
    often: std::sync::Mutex<Vec<coven_sync::SyncOftenGuard>>,
}

impl Owner {
    /// Ana's store with two rows of notes, the first in a snapshot when
    /// `snapshot` says. Her waits are held, as every network's are: a
    /// backoff or cooldown of hers ends only as the test moves the clock or
    /// asks for a pass, so an invite looks for its request only then.
    pub(super) async fn new(provider: CloudProvider, snapshot: bool) -> Self {
        let network = crate::tests::Network::with_schema(provider, tables(), migrations()).await;
        let device = &network.devices[0];
        let handle = device.handle.clone();
        let directory = device.app.store_dir(&device.store);
        let storage = device.storage.clone();
        let clock = network.clock.clone();
        let code = coven_sync::read_restore_code(&handle.restore_code().await.unwrap()).unwrap();
        let member = MemberIdentity::new(code.member, code.member_keys.clone());
        handle
            .write(move |sql| {
                sql.execute(
                    "INSERT INTO notes VALUES('before',?1)",
                    coven_database::params![vec![42_u8; if snapshot { 1_100_000 } else { 37 }]],
                )?;
                Ok::<_, CovenError>(())
            })
            .await
            .unwrap();
        network.sync(0).await;
        if snapshot {
            assert_eq!(crate::tests::snapshots(&storage, directory.id()).await.len(), 1);
        }
        handle
            .write(|sql| {
                sql.execute(
                    "INSERT INTO notes VALUES('after',?1)",
                    coven_database::params![vec![17_u8; 91]],
                )?;
                Ok::<_, CovenError>(())
            })
            .await
            .unwrap();
        network.sync(0).await;
        // Passes read join requests and approve them; the loop keeps running.
        handle.start_sync().await.unwrap();
        Self { network, directory, handle, member, storage, clock, code, often: Default::default() }
    }

    /// Ana's store as `create_store` made it, with `circles` and her
    /// `creating` write, and set up on S3, with no other rows.
    pub(super) async fn created<F>(circles: &[&str], creating: F) -> Self
    where
        F: FnOnce(&MemberWrite<'_>, &[CircleId]) -> CovenResult<()> + Send + 'static,
    {
        let network = crate::tests::Network::with_creation(
            CloudProvider::S3,
            tables(),
            migrations(),
            circles,
            creating,
        )
        .await;
        let device = &network.devices[0];
        let handle = device.handle.clone();
        let directory = device.app.store_dir(&device.store);
        let storage = device.storage.clone();
        let clock = network.clock.clone();
        let code = coven_sync::read_restore_code(&handle.restore_code().await.unwrap()).unwrap();
        let member = MemberIdentity::new(code.member, code.member_keys.clone());
        network.sync(0).await;
        handle.start_sync().await.unwrap();
        Self { network, directory, handle, member, storage, clock, code, often: Default::default() }
    }

    pub(super) async fn sync(&self) {
        self.handle.start_sync().await.unwrap();
        self.network.sync(0).await;
    }

    /// The request a join sent on one of Ana's invites, once her pass has
    /// read it. Her invite looks for the request only in a pass, her waits
    /// being held: once the provider holds a request at a waiting invite's
    /// path, this asks for one. Unlike [`Self::pass`] it leaves the clock
    /// where it is: a move would also pass the time the join next checks
    /// at, and start its check before Ana approves.
    pub(super) async fn next_join_request(&self) -> JoinRequest {
        let invites = self.handle.read(Invite::list).await.unwrap();
        if let Some(request) = invites.iter().find_map(|invite| invite.shown_request().cloned()) {
            return request;
        }
        let waiting: Vec<_> = invites
            .iter()
            .filter(|invite| matches!(invite.stage, InviteStage::Waiting { request: None, .. }))
            .map(crate::tests::request_path)
            .collect();
        tokio::time::timeout(
            HANG_GUARD,
            self.storage.wait_for_object(|path| waiting.contains(path)),
        )
        .await
        .expect("a join sends its request");
        self.pass_now().await;
        crate::tests::next_join_request(&self.handle).await
    }

    /// Ana reads the request a join sent, approves it, and `control` asks
    /// the join to check for that approval at once: its waits are held, so
    /// its next check runs only then or once the test moves the clock. The
    /// ask lasts until Ana goes.
    pub(super) async fn approve_join(&self, control: &JoinControl) -> MemberId {
        let request = self.next_join_request().await;
        crate::tests::approve(&self.handle, &request).await;
        self.check_at_once(control);
        request.member
    }

    pub(super) fn check_at_once(&self, control: &JoinControl) {
        let guard = control.check_often(Duration::ZERO);
        coven_foundation::locks::lock_unpoisoned(&self.often).push(guard);
    }

    /// Run `joining` while Ana reads its request, hands it to `before`, and
    /// approves it as [`Self::approve_join`] does; returns what the join
    /// returned and the approved member.
    pub(super) async fn join_and_approve<T, F: std::future::Future<Output = ()>>(
        &self,
        joining: impl std::future::Future<Output = T>,
        control: &JoinControl,
        before: impl FnOnce(JoinRequest) -> F,
    ) -> (T, MemberId) {
        join_with_response(joining, async {
            let request = self.next_join_request().await;
            before(request.clone()).await;
            crate::tests::approve(&self.handle, &request).await;
            self.check_at_once(control);
            request.member
        })
        .await
    }

    /// One pass of Ana's at the clock's current time, which runs her
    /// operations whatever their waits.
    pub(super) async fn pass_now(&self) {
        match self.handle.retry_now().await.unwrap().await {
            RetryOutcome::Synced { .. } => {}
            outcome => panic!("Ana's pass: {outcome:?}"),
        }
    }

    /// Stop sync and wait for its release: no pass runs until sync starts
    /// again (E5).
    pub(super) async fn stop_sync(&self) {
        self.network.devices[0].stop_sync().await;
    }

    /// One pass, which may leave operations pending.
    pub(super) async fn pass(&self) {
        self.handle.start_sync().await.unwrap();
        self.network.pass(0).await;
    }

    pub(super) async fn rows(&self) -> Vec<(String, Vec<u8>)> {
        self.handle
            .read(|sql| {
                Ok::<_, CovenError>(sql.query(
                    "SELECT id,body FROM notes ORDER BY id",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap()
    }

    /// The last slot the owner applied.
    pub(super) async fn applied(&self) -> u64 {
        self.handle.applied_slot().await
    }

    pub(super) async fn invite(&self) -> Invite {
        let access = if self.storage.config().provider() == CloudProvider::S3 {
            InviteAccess::S3AccessKey {
                access_key_id: "invited-key".into(),
                secret_access_key: SecretText::new("invited-secret".into()),
            }
        } else {
            InviteAccess::ProviderAccount { email: "join@example.com".into() }
        };
        crate::tests::invite(&self.handle, MemberRole::Member, access, InviteLifetime::SevenDays)
            .await
    }

    pub(super) fn s3_credentials(&self) -> Option<S3Credentials> {
        (self.storage.config().provider() == CloudProvider::S3).then(|| S3Credentials {
            access_key_id: "owner-key".into(),
            secret_access_key: SecretText::new("secret".into()),
        })
    }

    pub(super) fn recipient(&self) -> Arc<MemoryStorage> {
        if self.storage.config().provider() == CloudProvider::S3 {
            self.storage.clone()
        } else {
            Arc::new(MemoryStorage::for_recipient(&self.storage, "join@example.com").unwrap())
        }
    }

    pub(super) async fn close(self) {
        self.network.close().await;
    }
}

#[cfg(test)]
pub(super) struct Installation {
    pub(super) root: tempfile::TempDir,
    pub(super) app: Coven,
    pub(super) keychain: Arc<Keychain>,
    /// Whether this installation's app registers [`joining_write`].
    joining: bool,
    /// Set to make that write fail, as an app's bug would.
    pub(super) joining_fails: Arc<std::sync::atomic::AtomicBool>,
}

impl Installation {
    pub(super) fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let keychain = Keychain::in_memory("bootstrap-test").unwrap();
        let app = Coven::with_keychain(root.path().into(), keychain.clone());
        Self { root, app, keychain, joining: false, joining_fails: Default::default() }
    }

    /// An installation whose app registers [`joining_write`].
    pub(super) fn joining() -> Self {
        Self { joining: true, ..Self::new() }
    }

    /// A builder on the test's clock itself: its waits end only as the test
    /// moves the clock or asks a join to check, as Ana's do.
    pub(super) fn builder(&self, owner: &Owner, storage: Arc<MemoryStorage>) -> CovenBuilder {
        let builder = self
            .app
            .builder()
            .synced_tables(tables())
            .migrations(migrations())
            .clock(owner.clock.clone())
            .storage_connector(storage);
        if !self.joining {
            return builder;
        }
        let fails = self.joining_fails.clone();
        builder.joining_write(move |sql, joining| {
            if fails.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(DbError::Invariant { what: "the app's joining write failed" });
            }
            joining_write(sql, joining)
        })
    }

    /// A builder signed in as the owner's provider asks, through
    /// [`sign_in_held`].
    pub(super) async fn authenticated_builder(
        &self,
        owner: &Owner,
        storage: Arc<MemoryStorage>,
    ) -> CovenBuilder {
        let mut builder = self.builder(owner, storage);
        if let Some(access) = owner.storage.config().oauth_access() {
            let sign_in = crate::authentication::SignIn::new().await;
            builder = sign_in.configure(builder);
            sign_in_held(owner, &mut builder, access).await;
        }
        builder
    }

    pub(super) async fn run(
        &self,
        owner: &Owner,
        request: BootstrapRequest,
        storage: Arc<MemoryStorage>,
        cancel: &watch::Receiver<bool>,
        status: impl Fn(BootstrapStatus),
    ) -> Result<Option<CovenHandle>, BootstrapError> {
        // Boxed, as the API's calls box it: its tens of KiB would otherwise
        // ride in the test's own future on the test thread's stack.
        Box::pin(bootstrap_device(
            self.authenticated_builder(owner, storage).await,
            request,
            status,
            cancel,
        ))
        .await
    }

    /// A bootstrap, as [`Self::run`], whose loading reports `recorder`
    /// records at their source.
    pub(super) async fn run_recorded(
        &self,
        owner: &Owner,
        request: BootstrapRequest,
        storage: Arc<MemoryStorage>,
        cancel: &watch::Receiver<bool>,
        status: impl Fn(BootstrapStatus),
        recorder: &Arc<RecordingClock<LoadStep>>,
    ) -> Result<Option<CovenHandle>, BootstrapError> {
        let builder = self.authenticated_builder(owner, storage).await;
        Box::pin(bootstrap_device(
            builder.record_loading(recorder.clone()),
            request,
            status,
            cancel,
        ))
        .await
    }

    pub(super) async fn restore(&self, owner: &Owner) -> CovenHandle {
        let (_, cancel) = watch::channel(false);
        restore_from_code(
            self.authenticated_builder(owner, owner.storage.clone()).await,
            &owner.code.to_text().unwrap(),
            owner.s3_credentials(),
            "Ana’s laptop",
            |_| {},
            &cancel,
        )
        .await
        .unwrap()
    }

    pub(super) fn restore_request(owner: &Owner) -> BootstrapRequest {
        BootstrapRequest::Restore {
            code: coven_sync::read_restore_code(&owner.code.to_text().unwrap()).unwrap(),
            data: ConnectionCredentials {
                location: owner.storage.config(),
                credentials: StorageCredentials::S3(
                    owner.s3_credentials().expect("S3 restore fixture"),
                ),
            },
            sign_in: SignInCustody::None,
            name: "Ana’s laptop".into(),
        }
    }

    /// A join with `invite`'s code, which `control` asks to check for
    /// approval now: its waits are held, so a check after its first runs
    /// only then or once the test moves the clock.
    pub(super) fn join_request(invite: &Invite, control: &JoinControl) -> BootstrapRequest {
        BootstrapRequest::Join {
            code: coven_sync::read_invite_code(invite.shown_code()).unwrap(),
            terms: RequestTerms::Given {
                device_name: "New phone".into(),
                name: crate::tests::carol_joins(invite.shown_code()),
                circle_name: crate::tests::CIRCLE.into(),
            },
            held: control.often.subscribe(),
            parks: control.parks.clone(),
        }
    }

    /// Nothing of the store is listed or in custody.
    pub(super) async fn absent(&self, id: StoreId) {
        assert!(self.only_kept_sign_in(id).await.is_none());
    }

    /// Nothing of the store is listed or in custody but the provider sign-in
    /// a waiting join keeps, which this returns (§13.2).
    pub(super) async fn only_kept_sign_in(
        &self,
        id: StoreId,
    ) -> Option<coven_storage::OAuthTokens> {
        assert!(self.app.stores().await.unwrap().is_empty());
        let scoped = Arc::new(StoreKeychain::new(self.keychain.clone(), id));
        let person = PersonEntries::new(self.keychain.clone(), id);
        assert!(Custody::<StoreKeyring>::unlock(&KeyringCustody::new(scoped.clone()))
            .unwrap()
            .is_none());
        assert!(Custody::<MemberIdentity>::unlock(&KeyringCustody::new(scoped.clone()))
            .unwrap()
            .is_none());
        assert!(scoped.device_id().unwrap().is_none());
        assert!(person.restore_code().unwrap().is_none());
        assert!(person.s3_credentials().unwrap().is_none());
        self.device_tokens(id)
    }

    /// The OAuth sign-in in the store's device credentials.
    pub(super) fn device_tokens(&self, id: StoreId) -> Option<coven_storage::OAuthTokens> {
        let bytes = StoreKeychain::new(self.keychain.clone(), id).storage_credentials().unwrap()?;
        match StorageCredentials::decode(bytes.as_bytes()).unwrap() {
            StorageCredentials::OAuth(tokens) => Some(tokens),
            _ => panic!("the device credentials hold no OAuth sign-in"),
        }
    }

    pub(super) async fn handle(&self, directory: StoreDir, owner: &Owner) -> CovenHandle {
        let handle =
            self.builder(owner, owner.storage.clone()).open(directory.id(), |_| ()).await.unwrap();
        handle.unlock_store_key().await.unwrap();
        handle
    }
}

/// Each run of the test app's joining write on `handle`'s device: the member,
/// the person (none for a recovery) and the circle it was given.
pub(super) async fn joining_runs(handle: &CovenHandle) -> Vec<(String, Option<String>, String)> {
    handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query(
                "SELECT member,person,circle FROM joining_runs ORDER BY rowid",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?)
        })
        .await
        .unwrap()
}

/// The profiles `handle`'s store shows: id, name and circle.
pub(super) async fn profiles(handle: &CovenHandle) -> Vec<(String, String, String)> {
    handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query(
                "SELECT id,name,circle FROM profiles ORDER BY id",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?)
        })
        .await
        .unwrap()
}

pub(super) async fn rows(db: &Database) -> Vec<(String, Vec<u8>)> {
    db.read(|sql| {
        Ok::<_, CovenError>(
            sql.query("SELECT id,body FROM notes ORDER BY id", [], |r| Ok((r.get(0)?, r.get(1)?)))?,
        )
    })
    .await
    .unwrap()
}

/// The statuses after a new installation's open: they begin with the open's
/// steps, which end with its one migration done (E10).
/// What a restore that opened its store reported before it finished: the
/// statuses before `Registering` and `Committing`, which end every such
/// restore, in that order (E10).
pub(super) fn before_finishing(reported: &[BootstrapStatus]) -> &[BootstrapStatus] {
    let [before @ .., BootstrapStatus::Registering, BootstrapStatus::Committing] = reported else {
        panic!("{reported:?} doesn't end registering, then committing");
    };
    before
}

/// What a join that published its store reported around `Committing`: the
/// statuses before it, through the scan that found its admission, and the
/// loading of the store it published after it, every one `Loading`. A join
/// never reports `Registering`: its approval registered its device (E10,
/// §13.2).
pub(super) fn around_committing(
    reported: &[BootstrapStatus],
) -> (&[BootstrapStatus], &[BootstrapStatus]) {
    let committing = reported
        .iter()
        .position(|status| *status == BootstrapStatus::Committing)
        .unwrap_or_else(|| panic!("{reported:?} doesn't commit"));
    let (before, after) = (&reported[..committing], &reported[committing + 1..]);
    assert!(!reported.contains(&BootstrapStatus::Registering), "{reported:?}");
    assert!(
        !after.is_empty()
            && after.iter().all(|status| matches!(status, BootstrapStatus::Loading(_))),
        "{reported:?}"
    );
    (before, after)
}

pub(super) fn after_opening(reported: &[BootstrapStatus]) -> &[BootstrapStatus] {
    let opened =
        reported.iter().take_while(|status| matches!(status, BootstrapStatus::Opening(_))).count();
    assert_eq!(
        reported[..opened].last(),
        Some(&BootstrapStatus::Opening(Progress { step: OpenStep::Migrating, done: 1, total: 1 })),
        "{reported:?}"
    );
    &reported[opened..]
}

/// Where a test cancels a bootstrap.
#[derive(Clone, Copy, Debug)]
pub(super) enum Phase {
    /// Before connecting.
    Connecting,
    /// As loading begins.
    Loading,
    /// Once the last slot is applied, before the keys are kept and the store
    /// published.
    Loaded,
}

impl Phase {
    fn names(self, status: &BootstrapStatus) -> bool {
        match (self, status) {
            (Self::Connecting, BootstrapStatus::Connecting) => true,
            (Self::Loading, BootstrapStatus::Loading(_)) => true,
            (Self::Loaded, BootstrapStatus::Loading(progress)) => {
                progress.step == LoadStep::Applying && progress.done == progress.total
            }
            _ => false,
        }
    }
}

/// A status callback that cancels at the first report `phase` names.
pub(super) fn cancel_at(phase: Phase, stop: &watch::Sender<bool>) -> impl Fn(BootstrapStatus) + '_ {
    move |status| {
        if phase.names(&status) {
            stop.send_replace(true);
        }
    }
}

/// Records every value a bootstrap reports as it loads, keeping time as an
/// installation's builder does; a host reading the loading watch sees only
/// the newest value, and can miss a step that ends between two reads.
pub(super) fn loading_recorder(owner: &Owner) -> Arc<RecordingClock<LoadStep>> {
    Arc::new(RecordingClock::new(owner.clock.clone()))
}

/// Every loading report is honest: steps come in the order loading runs
/// them, from the scan for this member's keys to applying the last slot,
/// and within a step `done` never goes backwards or passes its total, which
/// never shrinks (E10).
pub(super) fn assert_honest(reported: &[BootstrapStatus]) {
    let loading: Vec<_> = reported
        .iter()
        .filter_map(|status| match status {
            BootstrapStatus::Loading(progress) => Some(progress.clone()),
            _ => None,
        })
        .collect();
    assert_load_order(&loading);
}

/// Loading's reports, as [`assert_honest`] checks them.
pub(super) fn assert_load_order(loading: &[Progress<LoadStep>]) {
    let rank = |step: &LoadStep| match step {
        LoadStep::FindingKeys => 0,
        LoadStep::Streaming(Audience::Store) => 1,
        LoadStep::Streaming(Audience::Circle(_)) => 2,
        LoadStep::Installing => 3,
        LoadStep::InstallingSlots => 4,
        LoadStep::Reading => 5,
        LoadStep::Applying => 6,
    };
    for progress in loading {
        assert!(progress.done <= progress.total, "{loading:?}");
    }
    for pair in loading.windows(2) {
        let [before, after] = pair else { unreachable!() };
        assert!(rank(&before.step) <= rank(&after.step), "{loading:?}");
        if before.step == after.step {
            assert!(before.done <= after.done, "{loading:?}");
            assert!(before.total <= after.total, "{loading:?}");
        }
    }
}

/// Start Carol's join with an invite and stop it while it waits, as when
/// the app is closed: once its first check found the request waiting and
/// saved the wait before the next. That wait ends only as the test moves
/// the clock or asks a resumed join to check ([`check_when_waiting`]).
pub(super) async fn waiting_join(owner: &Owner, install: &Installation, invite: &Invite) {
    waiting_join_with(install.authenticated_builder(owner, owner.recipient()).await, invite).await;
}

/// Start Carol's join through `builder` and stop it while it waits, as
/// [`waiting_join`] does.
pub(super) async fn waiting_join_with(builder: CovenBuilder, invite: &Invite) {
    let (waiting, mut waits) = watch::channel(false);
    let control = JoinControl::new();
    let joining = join_with_invite(
        builder,
        invite.shown_code(),
        carol_joins(invite.shown_code()),
        crate::tests::CIRCLE,
        "Carol's phone",
        |status| {
            if matches!(status, BootstrapStatus::WaitingForApproval { .. }) {
                waiting.send_replace(true);
            }
        },
        &control,
    );
    tokio::time::timeout(HANG_GUARD, async {
        tokio::select! {
            result = joining => panic!("the join ended while waiting: {result:?}"),
            changed = waits.wait_for(|waiting| *waiting) => drop(changed.unwrap()),
        }
    })
    .await
    .unwrap();
}

/// A join's status callback that asks it, through `control`, to check at
/// once whenever it waits for approval, for as long as the callback lives:
/// the held clock never ends a resumed join's saved wait.
pub(super) fn check_when_waiting(control: &JoinControl) -> impl Fn(BootstrapStatus) + Clone + '_ {
    let asked = Arc::new(std::sync::Mutex::new(Vec::new()));
    move |status| {
        if matches!(status, BootstrapStatus::WaitingForApproval { .. }) {
            let guard = control.check_often(Duration::ZERO);
            coven_foundation::locks::lock_unpoisoned(&asked).push(guard);
        }
    }
}

/// A join's status callback that tells the returned watch once the join
/// waits for approval, its request made.
pub(super) fn waiting_for_approval() -> (impl Fn(BootstrapStatus), watch::Receiver<bool>) {
    let (waiting, waits) = watch::channel(false);
    let status = move |status| {
        if matches!(status, BootstrapStatus::WaitingForApproval { .. }) {
            waiting.send_replace(true);
        }
    };
    (status, waits)
}

/// Sign `builder` in for `access`, checking that nothing moved the clock
/// meanwhile. The sign-in races its deadline, and its token request a stall
/// window, on that clock; with every wait held, Ana's backoffs between looks
/// for Carol's request can't move it under them.
pub(super) async fn sign_in_held(
    owner: &Owner,
    builder: &mut CovenBuilder,
    access: coven_storage::OAuthAccess,
) {
    let before = owner.clock.now();
    builder.authenticate(access).await.unwrap();
    assert_eq!(owner.clock.now(), before, "a wait moved the clock under the sign-in");
}

pub(super) fn saved_code(install: &Installation, store: StoreId) -> Option<SecretText> {
    StoreKeychain::new(install.keychain.clone(), store).invite_code().unwrap()
}
