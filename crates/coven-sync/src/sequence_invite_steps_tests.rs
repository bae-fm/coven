//! The invite lifecycle model (`InviteLifecycle.lean`) against the real
//! invite: each case drives one invite on Ana's device through the same
//! events as the Lean runner, and after each compares what the app sees
//! (`coven_invites`, `coven_operations`, `coven_pending`), the journal row,
//! the request in storage, and which of the app's steps a write accepts.

use crate::sequence_sync::sequence_invites::expiry;
use crate::sequence_sync::tests::*;
use crate::*;
use coven_database::{DbError, OperationId, SqlContext};
use coven_format::{
    admin::{MemberRole, StoreChange},
    MemberAccess,
};
use coven_foundation::clock::Clock;
use coven_storage::{ObjectPath, Storage};
use serde_json::{json, Value};
use std::time::Duration;

/// A step probed in a write that is then rolled back.
#[derive(Debug)]
struct Probe;
impl std::fmt::Display for Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("probe rolled back")
    }
}
impl std::error::Error for Probe {}

struct Case {
    group: Group,
    ana: Device,
    ben: Device,
    carol: Option<Joiner>,
    invite: Invite,
    operation: OperationId,
    /// The request the app was last shown.
    request: Option<JoinRequest>,
    binding: Option<OperationId>,
    op: Option<OperationId>,
    renames: u32,
    shown: bool,
    sent: bool,
    demoted: bool,
    removed: bool,
}

impl Case {
    async fn new(delivery: InviteDelivery) -> Self {
        let group = Group::shared();
        let ana = group.device(1).await;
        let ben = group.device(2).await;
        group
            .create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into()))
            .await;
        group.admit(&ana, &ben).await;
        ben.sync.pass().await.unwrap();
        let invite = ana
            .sync
            .create_invite(
                InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
                InviteAccess::ProviderAccount { email: "carol@example.org".into() },
                delivery,
            )
            .await
            .unwrap();
        let operation = invite.operation;
        Self {
            group,
            ana,
            ben,
            carol: None,
            invite,
            operation,
            request: None,
            binding: None,
            op: None,
            renames: 0,
            shown: true,
            sent: false,
            demoted: false,
            removed: false,
        }
    }

    async fn write<T: Send + 'static>(
        &self,
        step: impl FnOnce(&SqlContext<'_, '_>) -> Result<T, SyncError> + Send + 'static,
    ) -> Result<T, DbError> {
        self.ana
            .database()
            .write(move |sql| step(&sql).map_err(|error| DbError::SyncStream(Box::new(error))))
            .await
    }

    /// Whether a write accepts the step; the write is rolled back.
    async fn accepts(
        &self,
        step: impl FnOnce(&SqlContext<'_, '_>) -> Result<(), SyncError> + Send + 'static,
    ) -> bool {
        let result: Result<(), DbError> = self
            .ana
            .database()
            .write(move |sql| {
                step(&sql).map_err(|error| DbError::SyncStream(Box::new(error)))?;
                Err(DbError::SyncStream(Box::new(Probe)))
            })
            .await;
        matches!(result, Err(DbError::SyncStream(error)) if error.is::<Probe>())
    }

    fn approve_step(
        &self,
        request: JoinRequest,
    ) -> impl FnOnce(&SqlContext<'_, '_>) -> Result<OperationId, SyncError> + Send + 'static {
        let now = self.group.clock.now();
        let (waits, state) = (self.ana.sync.waits.clone(), self.ana.sync.state.clone());
        move |sql| crate::approve_join_request(sql, now, waits, state, &request)
    }

    fn decline_step(
        &self,
        request: JoinRequest,
    ) -> impl FnOnce(&SqlContext<'_, '_>) -> Result<(), SyncError> + Send + 'static {
        let (now, waits, state) =
            (self.group.clock.now(), self.ana.sync.waits.clone(), self.ana.sync.state.clone());
        move |sql| crate::decline_join_request(sql, now, waits, state, &request)
    }

    fn cancel_step(
        &self,
    ) -> impl FnOnce(&SqlContext<'_, '_>) -> Result<(), SyncError> + Send + 'static {
        let (now, waits, state) =
            (self.group.clock.now(), self.ana.sync.waits.clone(), self.ana.sync.state.clone());
        let invite = self.invite.id;
        move |sql| crate::cancel_invite(sql, now, waits, state, invite)
    }

    fn retry_step(
        &self,
    ) -> impl FnOnce(&SqlContext<'_, '_>) -> Result<(), SyncError> + Send + 'static {
        let (waits, state, id) =
            (self.ana.sync.waits.clone(), self.ana.sync.state.clone(), self.operation);
        move |sql| crate::retry_pending_operation(sql, waits, state, id)
    }

    fn ack_step(
        id: Option<OperationId>,
    ) -> impl FnOnce(&SqlContext<'_, '_>) -> Result<(), SyncError> + Send + 'static {
        move |sql| match id {
            Some(id) => sql.acknowledge_operation(id).map_err(SyncError::from),
            None => Err(SyncError::InvitationChanged),
        }
    }

    async fn listed(&self) -> Option<Invite> {
        listed(&self.ana).await.into_iter().find(|listed| listed.id == self.invite.id)
    }

    /// The journal row still exists.
    async fn journaled(&self) -> bool {
        self.ana.database().operation(self.operation).await.unwrap().is_some()
    }

    async fn pass(&mut self) {
        self.group.clock.set(self.group.clock.now() + Duration::from_secs(301));
        ended(self.ana.sync.pass().await);
    }

    /// A pass with `during` committed while it reads the request, when it
    /// reads one; otherwise after it.
    async fn race(&mut self, during: &'static str) {
        let reads = !self.removed
            && self.group.clock.now() < self.invite.expires_at
            && matches!(
                self.listed().await.map(|invite| invite.stage),
                Some(InviteStage::Waiting { request: None, .. })
            );
        self.group.clock.set(self.group.clock.now() + Duration::from_secs(301));
        if !reads {
            ended(self.ana.sync.pass().await);
            self.during(during).await;
            return;
        }
        let (observed, read) = tokio::sync::oneshot::channel();
        let (resume, held) = tokio::sync::oneshot::channel();
        self.ana
            .storage
            .hold_next_read(
                ObjectPath::join_request(self.group.store, self.invite.id),
                observed,
                held,
            )
            .await;
        let sync = self.ana.sync.clone();
        let pass = tokio::spawn(async move { sync.pass().await });
        read.await.unwrap();
        self.during(during).await;
        resume.send(()).unwrap();
        ended(pass.await.unwrap());
    }

    async fn during(&mut self, step: &str) {
        match step {
            "cancel" => drop(self.write(self.cancel_step()).await),
            "expire" => self.expire().await,
            _ => unreachable!("a race commits a cancel or an expiry"),
        }
    }

    async fn expire(&self) {
        self.group.clock.set(self.group.clock.now().max(self.invite.expires_at));
        let (waits, state) = (&self.ana.sync.waits, &self.ana.sync.state);
        expiry::expire(self.ana.database(), self.group.clock.now(), waits, state).await.unwrap();
    }

    async fn event(&mut self, name: &str) -> &'static str {
        let ok = |accepted: bool| if accepted { "ok" } else { "refused" };
        match name {
            "pass" => {
                self.pass().await;
                "ok"
            }
            "raceCancel" => {
                self.race("cancel").await;
                "ok"
            }
            "raceExpire" => {
                self.race("expire").await;
                "ok"
            }
            "approve" => match self.request.clone() {
                Some(request) => match self.write(self.approve_step(request)).await {
                    Ok(binding) => {
                        self.binding = Some(binding);
                        "ok"
                    }
                    Err(_) => "refused",
                },
                None => "refused",
            },
            "decline" => match self.request.clone() {
                Some(request) => ok(self.write(self.decline_step(request)).await.is_ok()),
                None => "refused",
            },
            "cancel" => ok(self.write(self.cancel_step()).await.is_ok()),
            "retry" => ok(self.write(self.retry_step()).await.is_ok()),
            "ack" => ok(self.write(Self::ack_step(self.binding)).await.is_ok()),
            "ackOp" => {
                let acked = self.write(Self::ack_step(self.op)).await.is_ok();
                if acked {
                    self.op = None;
                }
                ok(acked)
            }
            "administer" => {
                if self.op.is_some() {
                    return "skipped";
                }
                self.renames += 1;
                let name = format!("Household {}", self.renames);
                let (device, held, state) =
                    (self.ana.sync.device, self.ana.identity.clone(), self.ana.sync.state.clone());
                let change = StoreChange::RenameStore { name };
                match self
                    .write(move |sql| crate::administer(sql, device, &held, state, change))
                    .await
                {
                    Ok(op) => {
                        self.op = Some(op.expect("a new name journals a rename"));
                        "ok"
                    }
                    Err(_) => "refused",
                }
            }
            "send" => {
                if !self.shown || self.sent || !self.journaled().await {
                    return "skipped";
                }
                let code = self.invite.shown_code().to_owned();
                self.carol = Some(join(&self.group, &code, "Carol's phone").await);
                self.sent = true;
                "ok"
            }
            "expire" => {
                self.expire().await;
                "ok"
            }
            "demote" => {
                if self.demoted {
                    return "skipped";
                }
                self.demoted = true;
                self.ben.sync.pass().await.unwrap();
                let member = self.ana.identity.member();
                let change = StoreChange::ChangeRole { member, role: MemberRole::Member };
                self.ben.sync.administer(change).await.unwrap();
                "ok"
            }
            "remove" => {
                if self.removed {
                    return "skipped";
                }
                self.removed = true;
                self.ben.sync.pass().await.unwrap();
                let change = StoreChange::RemoveDevice { device: self.ana.sync.device };
                self.ben.sync.administer(change).await.unwrap();
                "ok"
            }
            other => panic!("unknown event {other}"),
        }
    }

    /// An operation as `coven_operations` lists it.
    async fn entry(&self, id: Option<OperationId>) -> Value {
        let Some(id) = id else { return Value::Null };
        let listed = self
            .ana
            .database()
            .read(move |sql| {
                Ok(sql.query(
                    "SELECT outcome FROM coven_operations WHERE operation=?1",
                    [id.0],
                    |row| row.get::<_, Option<OperationOutcome>>(0),
                )?)
            })
            .await
            .unwrap();
        match listed.first() {
            None => Value::Null,
            Some(None) => json!("running"),
            Some(Some(OperationOutcome::Landed(_) | OperationOutcome::Satisfied)) => {
                json!("landed")
            }
            Some(Some(OperationOutcome::Rejected(_))) => json!("rejected"),
        }
    }

    async fn observe(&mut self, result: &str) -> Value {
        let listed = self.listed().await;
        if let Some(listed) = &listed {
            match &listed.stage {
                InviteStage::Waiting { request: Some(request), .. } => {
                    self.request = Some(request.clone());
                }
                InviteStage::Approving { binding, .. } => self.binding = Some(*binding),
                _ => (),
            }
            if !matches!(listed.stage, InviteStage::Granting) {
                self.invite = listed.clone();
                self.shown = true;
            }
        }
        let invite = match listed.map(|invite| invite.stage) {
            None => Value::Null,
            Some(InviteStage::Granting) => json!("granting"),
            Some(InviteStage::Waiting { request: None, .. }) => json!("waiting"),
            Some(InviteStage::Waiting { request: Some(_), .. }) => json!("requested"),
            Some(InviteStage::Approving { .. }) => json!("approving"),
        };
        let pending = match self
            .ana
            .database()
            .operation_pending::<PendingRecord>(self.operation)
            .await
            .unwrap()
            .map(|record| record.retry())
        {
            None => Value::Null,
            Some(Retry::Automatic) => json!("auto"),
            Some(Retry::AppAction) => json!("app"),
            Some(Retry::Never | Retry::AfterUpdate) => json!("never"),
        };
        let path = ObjectPath::join_request(self.group.store, self.invite.id);
        let request = self.group.storage.status(&path).await.unwrap().is_some();
        let mut accepted = Vec::new();
        if let Some(request) = self.request.clone() {
            let step = self.approve_step(request.clone());
            if self.accepts(move |sql| step(sql).map(drop)).await {
                accepted.push("approve");
            }
            if self.accepts(self.decline_step(request)).await {
                accepted.push("decline");
            }
        }
        if self.accepts(self.cancel_step()).await {
            accepted.push("cancel");
        }
        if self.accepts(self.retry_step()).await {
            accepted.push("retry");
        }
        if self.accepts(Self::ack_step(self.binding)).await {
            accepted.push("ack");
        }
        if self.accepts(Self::ack_step(self.op)).await {
            accepted.push("ackOp");
        }
        json!({
            "result": result,
            "invite": invite,
            "binding": self.entry(self.binding).await,
            "op": self.entry(self.op).await,
            "pending": pending,
            "journal": self.journaled().await,
            "request": request,
            "accepted": accepted,
        })
    }

    async fn close(self) {
        for device in [&self.ana, &self.ben] {
            device.database().close().await.unwrap();
        }
        if let Some(carol) = self.carol {
            carol.device.database().close().await.unwrap();
        }
    }
}

/// A pass ends in an error once this device stops, or after an in-person
/// approval it made was rejected; the views say the rest. Any other error
/// fails the case.
fn ended(result: Result<SequenceStatus, SyncError>) {
    match result {
        Ok(_) | Err(SyncError::Removed | SyncError::Rejected(_)) => (),
        Err(error) => panic!("the pass failed: {error}"),
    }
}

/// The event sequences: one per gap and fixed bug the model names, then
/// seeded ones over every event.
fn cases() -> Vec<(&'static str, Vec<&'static str>)> {
    let mut cases = vec![
        ("sent", vec!["send", "pass", "approve", "pass", "ack"]),
        ("sent", vec!["send", "pass", "pass", "retry", "decline", "pass"]),
        ("sent", vec!["pass", "send", "pass", "cancel", "pass", "send"]),
        ("sent", vec!["raceCancel", "pass"]),
        ("sent", vec!["send", "raceCancel", "pass"]),
        ("sent", vec!["send", "raceExpire", "pass"]),
        ("inPerson", vec!["send", "raceExpire", "pass"]),
        ("inPerson", vec!["send", "pass"]),
        ("inPerson", vec!["demote", "send", "pass"]),
        // An approval under way when the device stops.
        ("sent", vec!["send", "pass", "approve", "remove", "pass", "expire", "cancel"]),
        // Waiting invites and the app's operation on a stopped device.
        ("sent", vec!["administer", "send", "pass", "remove", "pass", "cancel", "pass"]),
        // A demoted approver's decisions.
        ("sent", vec!["send", "pass", "demote", "pass", "approve", "decline", "pass"]),
        ("sent", vec!["send", "pass", "approve", "demote", "pass", "ack"]),
        ("sent", vec!["demote", "pass", "pass", "retry", "cancel", "pass"]),
        ("sent", vec!["send", "pass", "expire", "approve", "pass"]),
        ("sent", vec!["administer", "pass", "ackOp", "administer", "demote", "pass", "ackOp"]),
    ];
    const EVENTS: [&str; 15] = [
        "pass",
        "pass",
        "send",
        "approve",
        "decline",
        "cancel",
        "retry",
        "ack",
        "ackOp",
        "administer",
        "expire",
        "demote",
        "remove",
        "raceCancel",
        "raceExpire",
    ];
    let mut seed: u64 = 0x5eed;
    for case in 0..48 {
        let mut events = Vec::new();
        for _ in 0..8 {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            events.push(EVENTS[(seed >> 33) as usize % EVENTS.len()]);
        }
        cases.push((if case % 3 == 0 { "inPerson" } else { "sent" }, events));
    }
    cases
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn invite_lifecycle_matches_lean() {
    for (delivery, events) in cases() {
        let input = json!({
            "kind": "inviteLifecycle",
            "delivery": delivery,
            "records": false,
            "events": events,
        });
        let expected = crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input);
        let mut case = Case::new(match delivery {
            "sent" => InviteDelivery::Sent { lifetime: InviteLifetime::Day },
            _ => InviteDelivery::InPerson,
        })
        .await;
        let mut actual = vec![case.observe("ok").await];
        for event in &events {
            let result = case.event(event).await;
            actual.push(case.observe(result).await);
        }
        assert_eq!(Value::Array(actual), expected, "{delivery} {events:?}");
        case.close().await;
    }
}
