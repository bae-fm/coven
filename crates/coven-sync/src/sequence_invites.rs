//! The inviting device's one-time invitation, its join request and the
//! approval that binds the request's exact pair (§13.2, §13.3).

use super::{SequenceSync, Session};
use crate::{
    Invite, InviteAccess, InviteDelivery, InviteTarget, JoinRequest, OperationKind,
    OperationRejection, PendingReason, PendingRecord, PendingSubject, ProviderAccessAction,
    SyncError,
};
use coven_crypto::{
    ConfirmationNumber, InviteSecret, MemberIdentity, ObjectHasher, SecretText, StoreKeyring,
};
use coven_database::{
    NewOperation, OperationId, OperationRecord, OperationUpdate, ProviderAccessChange,
};
use coven_format::{
    admin::{MemberRole, StoreChange},
    codes::{InviteCode, InviteDeliveryKind},
    sealed_single::SingleChunkObject,
    MemberAccess, Object,
};
use coven_foundation::id_source::{InviteId, MemberId};
use coven_merge::Audience;
use coven_storage::{
    CloudProvider, InviteStorage, ObjectPath, S3Credentials, StorageFailure, StorageInvitation,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub(crate) const INVITE: &str = "invite";

/// How an invited person reaches storage, as the invite's journal keeps it.
#[derive(Serialize, Deserialize)]
pub(crate) enum InviteGrant {
    /// The provider shares the store with this account.
    Account { email: String },
    /// An S3 key the person made in the provider's console for this invite.
    S3Key(S3Credentials),
}

impl InviteGrant {
    fn member_access(&self) -> MemberAccess {
        match self {
            Self::Account { email } => MemberAccess::ProviderAccount(email.clone()),
            Self::S3Key(key) => MemberAccess::S3AccessKey {
                access_key_id: key.access_key_id.clone(),
            },
        }
    }
}

/// What approving the invite's request binds to its member id.
#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) enum InviteBinding {
    /// A fresh id with this role and the invite's access.
    Add { role: MemberRole },
    /// An existing active id keeps its role, circles, devices and recorded
    /// access; only its pair changes.
    Replace {
        #[serde(with = "crate::pending_encoding::public_keys")]
        expected: coven_crypto::MemberPublicKeys,
    },
}

#[derive(Serialize, Deserialize)]
pub(crate) enum InviteState {
    /// Storage access is being granted.
    Grant,
    /// The code is shown; the request, once read and checked, waits for the app.
    Waiting {
        /// The code as first shown, which every later showing repeats: the
        /// joining device binds its saved request to the exact code (§13.2).
        code: SecretText,
        request: Option<Vec<u8>>,
    },
    /// The app approved this checked request.
    Approving { code: SecretText, request: Vec<u8> },
    /// A kept replacement records the invite's new S3 key for its member.
    Recording,
    /// The invite ended: delete the request object and settle its
    /// account's sharing. An S3 key it carried is the person's to delete in
    /// the provider's console; coven records nothing about it (§14.3).
    Settled,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct InviteWork {
    id: InviteId,
    member: MemberId,
    /// The new person's name; `None` for a replacement.
    invitee: Option<String>,
    delivery: InviteDeliveryKind,
    binding: InviteBinding,
    access: InviteGrant,
    secret: SecretText,
    expires_at: std::time::SystemTime,
    state: InviteState,
}

impl InviteWork {
    fn read(record: &OperationRecord) -> Result<Self, SyncError> {
        Ok(serde_json::from_slice(&record.data)?)
    }
    fn update(&self, record: &OperationRecord) -> Result<OperationUpdate, SyncError> {
        Ok(OperationUpdate {
            id: record.id,
            previous: record.last_step,
            last_step: record
                .last_step
                .checked_add(1)
                .ok_or(SyncError::SequenceExhausted)?,
            data: serde_json::to_vec(self)?,
        })
    }
    fn secret(&self) -> Result<InviteSecret, SyncError> {
        Ok(InviteSecret::from_bytes(serde_json::from_str(
            self.secret.as_str(),
        )?))
    }
    fn account(&self) -> Option<&str> {
        match &self.access {
            InviteGrant::Account { email } => Some(email),
            _ => None,
        }
    }
    /// An invite still open: its account stays shared (§14.2).
    fn open(&self) -> bool {
        !matches!(self.state, InviteState::Settled)
    }
    fn expired(&self, now: std::time::SystemTime) -> bool {
        now >= self.expires_at
            && matches!(self.state, InviteState::Grant | InviteState::Waiting { .. })
    }
}

impl SequenceSync {
    /// Accounts that this device's open invites still need shared.
    pub(crate) async fn invited_accounts(&self) -> Result<BTreeSet<String>, SyncError> {
        let mut accounts = BTreeSet::new();
        for record in self.database.operations().await? {
            if record.kind == INVITE {
                let work = InviteWork::read(&record)?;
                if work.open() {
                    accounts.extend(work.account().map(str::to_owned));
                }
            }
        }
        Ok(accounts)
    }

    /// Fix the member id and storage access, grant that access, and return
    /// the code to show (§13.2). The invite expires after the delivery's
    /// lifetime by this device's clock.
    pub async fn create_invite(
        &self,
        target: InviteTarget,
        access: InviteAccess,
        delivery: InviteDelivery,
    ) -> Result<Invite, SyncError> {
        if let InviteTarget::NewMember { name, .. } = &target {
            coven_format::value::name(name)?;
        }
        let mut session = self.session.lock().await;
        let member = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.settle_open(&mut session, &mut ring, &member).await?;
        let me = member.member();
        let state = &session.history.state;
        if !self.registered(&session)
            || !state
                .members
                .get(&me)
                .is_some_and(|value| !value.removed && value.role == MemberRole::Admin)
        {
            return Err(SyncError::Rejected(OperationRejection::NotAllowed));
        }
        if !session.history.owns_storage(me) {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        let provider = self.storage.config().provider();
        let (target_member, binding, invitee) = match target {
            InviteTarget::NewMember { role, name } => (
                MemberId(self.ids.new_id()),
                InviteBinding::Add { role },
                Some(name),
            ),
            InviteTarget::ReplaceMemberKeys { member: target } => {
                let Some(existing) = state.members.get(&target).filter(|value| !value.removed)
                else {
                    return Err(SyncError::Rejected(OperationRejection::TargetGone(
                        crate::EntryTarget::Member(target),
                    )));
                };
                // Replacing this installation's own pair stops the device that
                // must finish the invite; members replace their own keys directly.
                if target == me {
                    return Err(SyncError::Rejected(OperationRejection::NotAllowed));
                }
                if let InviteAccess::ProviderAccount { email } = &access {
                    let account = MemberAccess::ProviderAccount(email.trim().to_ascii_lowercase());
                    if !existing.access.same_credential(&account) {
                        return Err(StorageFailure::InvalidConfiguration
                            .with_source(
                                "a replacement invite shares the member's recorded account",
                            )
                            .into());
                    }
                }
                (
                    target,
                    InviteBinding::Replace {
                        expected: existing.keys,
                    },
                    None,
                )
            }
        };
        let id = InviteId(self.ids.new_id());
        let grant = match access {
            InviteAccess::ProviderAccount { email } => {
                let email = email.trim().to_ascii_lowercase();
                if email.is_empty() {
                    return Err(StorageFailure::InvalidConfiguration
                        .with_source("invite account is empty")
                        .into());
                }
                if provider == CloudProvider::S3 {
                    return Err(StorageFailure::InvalidConfiguration
                        .with_source("an S3 invitation needs its own access key")
                        .into());
                }
                InviteGrant::Account { email }
            }
            InviteAccess::S3AccessKey {
                access_key_id,
                secret_access_key,
            } => {
                if access_key_id.is_empty() || secret_access_key.as_str().is_empty() {
                    return Err(StorageFailure::InvalidConfiguration
                        .with_source("invite access key is empty")
                        .into());
                }
                InviteGrant::S3Key(S3Credentials {
                    access_key_id,
                    secret_access_key,
                })
            }
        };
        if !matches!(grant, InviteGrant::Account { .. }) && provider != CloudProvider::S3 {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("an access-key invitation requires S3")
                .into());
        }
        let secret = InviteSecret::generate()?;
        let work = InviteWork {
            id,
            member: target_member,
            invitee,
            delivery: delivery.kind(),
            binding,
            access: grant,
            secret: SecretText::new(serde_json::to_string(secret.to_secret_bytes().as_bytes())?),
            expires_at: self.clock.now().checked_add(delivery.lifetime()).ok_or(
                StorageFailure::InvalidConfiguration
                    .with_source("the clock cannot represent the invite's expiry"),
            )?,
            state: InviteState::Grant,
        };
        let changes = match work.account() {
            Some(account) => vec![self.account_change(account, true, true)?],
            None => Vec::new(),
        };
        let operation = self
            .database
            .start_operation_with_access::<PendingRecord>(
                NewOperation {
                    kind: INVITE.into(),
                    data: serde_json::to_vec(&work)?,
                    started_by: "create_invite".into(),
                    pauses_writes: false,
                },
                changes,
            )
            .await?;
        if let Some(account) = work.account() {
            // The call answers with its grant's outcome: a failed grant takes
            // back what the invite intended and fails the call.
            let grant = self
                .database
                .operations()
                .await?
                .into_iter()
                .filter(|record| {
                    record.kind == "provider-access"
                        && serde_json::from_slice::<String>(&record.data)
                            .is_ok_and(|recorded| recorded == account)
                })
                .map(|record| record.id)
                .next_back()
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            if let Err(error) = self
                .apply_access(&mut session, &mut ring, &member, grant)
                .await
            {
                let record = self
                    .database
                    .operation(operation)
                    .await?
                    .ok_or(coven_database::DbError::OperationChanged(operation))?;
                let mut work = InviteWork::read(&record)?;
                work.state = InviteState::Settled;
                self.database
                    .advance_operation(work.update(&record)?)
                    .await?;
                self.run_invite(&mut session, &mut ring, &member, operation)
                    .await?;
                return Err(error);
            }
        }
        match self
            .run_invite(&mut session, &mut ring, &member, operation)
            .await?
        {
            InviteProgress::Shown(invite) => {
                self.publish_invites().await?;
                // The next pass polls for the request, then backs off.
                self.state.request_pass();
                Ok(invite)
            }
            InviteProgress::Waiting(reason) => {
                Err(SyncError::Pending(Box::new(PendingRecord::new(
                    PendingSubject::Operation {
                        id: operation,
                        kind: OperationKind::Invite,
                    },
                    reason,
                ))))
            }
            InviteProgress::Finished => Err(SyncError::InvitationChanged),
        }
    }

    /// This device's shown invites that haven't ended, in the order they
    /// were made, each with its code and its checked request once one has
    /// arrived (§13.2). Only this device holds their secrets.
    pub async fn invites(&self) -> Result<Vec<Invite>, SyncError> {
        let now = self.clock.now();
        let mut invites = Vec::new();
        for record in self.database.operations().await? {
            if record.kind != INVITE {
                continue;
            }
            let work = InviteWork::read(&record)?;
            if work.expired(now) {
                continue;
            }
            let (code, request) = match &work.state {
                InviteState::Waiting { code, request } => (code, request.as_deref()),
                InviteState::Approving { code, request } => (code, Some(request.as_slice())),
                InviteState::Grant | InviteState::Recording | InviteState::Settled => continue,
            };
            invites.push(self.invite_value(&work, code, request)?);
        }
        Ok(invites)
    }

    /// Show the app this device's open invites.
    pub(crate) async fn publish_invites(&self) -> Result<(), SyncError> {
        self.state.set_invites(self.invites().await?);
        Ok(())
    }

    /// Approve exactly this checked request: claim copies of every store key
    /// and the held circle keys for its pair, then bind the pair (§13.2).
    pub async fn approve_join_request(&self, request: &JoinRequest) -> Result<(), SyncError> {
        self.decide(request.invite, Some(request), true).await
    }

    /// Decline the request and take back the access its invite granted.
    pub async fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError> {
        self.decide(request.invite, Some(request), false).await
    }

    /// Cancel an invite before anyone joins with it, taking back its access.
    pub async fn cancel_invite(&self, invite: InviteId) -> Result<(), SyncError> {
        self.decide(invite, None, false).await
    }

    async fn decide(
        &self,
        invite: InviteId,
        request: Option<&JoinRequest>,
        approve: bool,
    ) -> Result<(), SyncError> {
        let result = self.decide_locked(invite, request, approve).await;
        self.publish_invites().await?;
        result
    }

    async fn decide_locked(
        &self,
        invite: InviteId,
        request: Option<&JoinRequest>,
        approve: bool,
    ) -> Result<(), SyncError> {
        let mut session = self.session.lock().await;
        let member = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.settle_open(&mut session, &mut ring, &member).await?;
        let record = self
            .database
            .operations()
            .await?
            .into_iter()
            .filter(|record| record.kind == INVITE)
            .find(|record| InviteWork::read(record).is_ok_and(|work| work.id == invite))
            .ok_or(SyncError::InvitationChanged)?;
        let mut work = InviteWork::read(&record)?;
        if work.expired(self.clock.now()) {
            return Err(SyncError::InvitationChanged);
        }
        let (code, stored) = match &work.state {
            InviteState::Waiting { code, request } => (Some(code.clone()), request.clone()),
            InviteState::Grant if !approve && request.is_none() => (None, None),
            _ => return Err(SyncError::InvitationChanged),
        };
        if let Some(request) = request {
            let bytes = stored.as_ref().ok_or(SyncError::InvitationChanged)?;
            if self.present_request(&work, bytes)? != *request {
                return Err(SyncError::InvitationChanged);
            }
        }
        work.state = if approve {
            InviteState::Approving {
                code: code.ok_or(SyncError::InvitationChanged)?,
                request: stored.ok_or(SyncError::InvitationChanged)?,
            }
        } else {
            InviteState::Settled
        };
        self.database
            .advance_operation(work.update(&record)?)
            .await?;
        loop {
            match self
                .run_invite(&mut session, &mut ring, &member, record.id)
                .await?
            {
                InviteProgress::Finished => return Ok(()),
                InviteProgress::Waiting(reason) => {
                    return Err(SyncError::Pending(Box::new(PendingRecord::new(
                        PendingSubject::Operation {
                            id: record.id,
                            kind: OperationKind::Invite,
                        },
                        reason,
                    ))))
                }
                InviteProgress::Shown(_) => (),
            }
        }
    }

    /// Advance this device's invites in a pass: grant, read the request
    /// under backoff, revoke on expiry, and finish decided work.
    pub(crate) async fn resume_invites(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for record in self.database.operations().await? {
            if record.kind != INVITE {
                continue;
            }
            // Expiry takes back the grant even while a request waits for the app.
            let expired = InviteWork::read(&record)?.expired(self.clock.now());
            if !expired && !self.operation_ready(&record, ring).await? {
                continue;
            }
            if let Err(error) = self.run_invite(session, ring, member, record.id).await {
                if matches!(error, SyncError::Rejected(_) | SyncError::Cleanup { .. })
                    || self.database.operation(record.id).await?.is_none()
                {
                    return Err(error);
                }
            }
            self.state.check()?;
        }
        self.publish_invites().await
    }

    async fn advance_invite(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        id: OperationId,
    ) -> Result<InviteProgress, SyncError> {
        loop {
            let record = self
                .database
                .operation(id)
                .await?
                .ok_or(coven_database::DbError::OperationChanged(id))?;
            let mut work = InviteWork::read(&record)?;
            if work.expired(self.clock.now()) {
                work.state = InviteState::Settled;
                self.database
                    .advance_operation(work.update(&record)?)
                    .await?;
                continue;
            }
            match &work.state {
                InviteState::Grant => {
                    let invitation = match &work.access {
                        InviteGrant::Account { email } => {
                            let access = self
                                .database
                                .provider_access(email.clone())
                                .await?
                                .ok_or(coven_database::DbError::DamagedDatabase)?;
                            if access.request.is_some() || access.granted != Some(true) {
                                return Ok(InviteProgress::Waiting(
                                    PendingReason::ProviderPending {
                                        action: ProviderAccessAction::Grant,
                                        account: email.clone(),
                                    },
                                ));
                            }
                            StorageInvitation::decode(
                                access
                                    .invitation
                                    .as_ref()
                                    .ok_or(coven_database::DbError::DamagedDatabase)?
                                    .as_bytes(),
                            )?
                        }
                        InviteGrant::S3Key(_) => {
                            StorageInvitation::for_account(self.storage.config())?
                        }
                    };
                    let code = self.invite_code(session, &work, invitation)?;
                    let invite = self.invite_value(&work, &code, None)?;
                    work.state = InviteState::Waiting {
                        code,
                        request: None,
                    };
                    self.database
                        .advance_operation(work.update(&record)?)
                        .await?;
                    return Ok(InviteProgress::Shown(invite));
                }
                InviteState::Waiting {
                    request: Some(_), ..
                } => {
                    return Ok(InviteProgress::Waiting(PendingReason::Paused));
                }
                InviteState::Waiting {
                    request: None,
                    code,
                } => {
                    let path = ObjectPath::join_request(self.store(), work.id);
                    let bytes = match self.storage.read(&path).await {
                        Ok(bytes) => bytes,
                        Err(error) if error.failure() == StorageFailure::NotFound => {
                            return Ok(InviteProgress::Waiting(PendingReason::Missing { path }));
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let request = self.open_request(&work, &bytes)?;
                    work.state = InviteState::Waiting {
                        code: code.clone(),
                        request: Some(request),
                    };
                    self.database
                        .advance_operation(work.update(&record)?)
                        .await?;
                    return Ok(InviteProgress::Waiting(PendingReason::Paused));
                }
                InviteState::Approving { request, .. } => {
                    let decoded = decode_request(request)?;
                    let change = match work.binding {
                        InviteBinding::Add { role } => StoreChange::AddMember {
                            member: work.member,
                            keys: decoded.keys,
                            role,
                            access: work.access.member_access(),
                        },
                        InviteBinding::Replace { expected } => StoreChange::ReplaceMemberKeys {
                            member: work.member,
                            expected,
                            keys: decoded.keys,
                        },
                    };
                    match self
                        .approve_invite_change(session, ring, member, change)
                        .await
                    {
                        Ok(()) => (),
                        Err(SyncError::Rejected(reason)) => {
                            work.state = InviteState::Settled;
                            let undo: Result<(), SyncError> = async {
                                self.database
                                    .advance_operation(work.update(&record)?)
                                    .await?;
                                Box::pin(self.advance_invite(session, ring, member, id)).await?;
                                Ok(())
                            }
                            .await;
                            let operation = SyncError::Rejected(reason);
                            return Err(match undo {
                                Ok(()) => operation,
                                Err(cleanup) => SyncError::Cleanup {
                                    operation: Box::new(operation),
                                    cleanup: Box::new(cleanup),
                                },
                            });
                        }
                        Err(error) => return Err(error),
                    }
                    work.state = match (&work.binding, &work.access) {
                        (InviteBinding::Replace { .. }, InviteGrant::S3Key(_)) => {
                            InviteState::Recording
                        }
                        _ => InviteState::Settled,
                    };
                    self.database
                        .advance_operation(work.update(&record)?)
                        .await?;
                }
                InviteState::Recording => {
                    let change = StoreChange::SetAccess {
                        member: work.member,
                        access: work.access.member_access(),
                    };
                    self.approve_invite_change(session, ring, member, change)
                        .await?;
                    work.state = InviteState::Settled;
                    self.database
                        .advance_operation(work.update(&record)?)
                        .await?;
                }
                InviteState::Settled => {
                    // Provider deletion is idempotent, including an absent request.
                    match self
                        .storage
                        .delete(&ObjectPath::join_request(self.store(), work.id))
                        .await
                    {
                        Ok(()) => (),
                        Err(error) if error.failure() == StorageFailure::NotFound => (),
                        Err(error) => return Err(error.into()),
                    }
                    let changes = match work.account() {
                        Some(account) => {
                            let others = self.invited_accounts_except(id).await?;
                            let used = session.history.state.members.values().any(|value| {
                                !value.removed
                                    && value.access.same_credential(&MemberAccess::ProviderAccount(
                                        account.to_owned(),
                                    ))
                            });
                            let invited = others.contains(account);
                            vec![self.account_change(account, used || invited, invited)?]
                        }
                        None => Vec::new(),
                    };
                    self.database
                        .finish_operation_with_access::<PendingRecord>(id, changes)
                        .await?;
                    return Ok(InviteProgress::Finished);
                }
            }
        }
    }

    async fn invited_accounts_except(
        &self,
        id: OperationId,
    ) -> Result<BTreeSet<String>, SyncError> {
        let mut accounts = BTreeSet::new();
        for record in self.database.operations().await? {
            if record.kind == INVITE && record.id != id {
                let work = InviteWork::read(&record)?;
                if work.open() {
                    accounts.extend(work.account().map(str::to_owned));
                }
            }
        }
        Ok(accounts)
    }

    fn account_change(
        &self,
        account: &str,
        intended: bool,
        needs_invitation: bool,
    ) -> Result<ProviderAccessChange, SyncError> {
        Ok(ProviderAccessChange {
            operation: NewOperation {
                kind: "provider-access".into(),
                data: serde_json::to_vec(account)?,
                started_by: "coven".into(),
                pauses_writes: false,
            },
            account: account.to_owned(),
            intended,
            initially_granted: false,
            needs_invitation,
            owner: true,
        })
    }

    /// The code to show, fixed once its access is granted: it names the
    /// store as now named and this device's applied slot.
    fn invite_code(
        &self,
        session: &Session,
        work: &InviteWork,
        invitation: StorageInvitation,
    ) -> Result<SecretText, SyncError> {
        let storage = match &work.access {
            InviteGrant::Account { .. } => InviteStorage::Account(invitation),
            InviteGrant::S3Key(credentials) => InviteStorage::S3 {
                invitation,
                credentials: credentials.clone(),
            },
        };
        let store = session
            .history
            .state
            .store
            .as_ref()
            .ok_or(coven_database::DbError::DamagedDatabase)?;
        let initial_key = session
            .history
            .introductions
            .iter()
            .find(|intro| intro.audience == Audience::Store)
            .ok_or(coven_database::DbError::DamagedDatabase)?
            .key;
        let code = InviteCode {
            store: store.id,
            name: store.name.clone(),
            invitee: work.invitee.clone(),
            delivery: work.delivery,
            invite: work.id,
            member: work.member,
            secret: work.secret()?,
            initial_key,
            inviting_writer: self.device,
            read_through: session.through,
            storage: storage.encode()?,
        }
        .to_text()?;
        Ok(SecretText::new(code.to_string()))
    }

    fn invite_value(
        &self,
        work: &InviteWork,
        code: &SecretText,
        request: Option<&[u8]>,
    ) -> Result<Invite, SyncError> {
        Ok(Invite {
            id: work.id,
            member: work.member,
            invitee: work.invitee.clone(),
            delivery: work.delivery,
            access: work.access.member_access(),
            code: code.as_str().to_owned(),
            expires_at: work.expires_at,
            request: request
                .map(|bytes| self.present_request(work, bytes))
                .transpose()?,
        })
    }

    /// Open and check a request: it repeats the invite's id and member id,
    /// and its own new signing key verifies it (§13.2).
    fn open_request(&self, work: &InviteWork, bytes: &[u8]) -> Result<Vec<u8>, SyncError> {
        let envelope = SingleChunkObject::decode(bytes)?;
        let SingleChunkObject::JoinRequest { signature, .. } = &envelope;
        let path = ObjectPath::join_request(self.store(), work.id);
        let plain = work.secret()?.join_request_key().open_object_chunk(
            path.as_str(),
            &envelope.prefix().encode()?,
            0,
            0,
            envelope.chunk(),
        )?;
        let request = decode_request(&plain)?;
        if request.invite != work.id || request.member != work.member {
            return Err(SyncError::InvitationChanged);
        }
        let mut hasher = ObjectHasher::new();
        hasher.update(&envelope.signed_bytes()?);
        request
            .keys
            .signing
            .verify_object(path.as_str(), &hasher.finish(), signature)?;
        Ok(plain.to_vec())
    }

    fn present_request(&self, work: &InviteWork, bytes: &[u8]) -> Result<JoinRequest, SyncError> {
        let request = decode_request(bytes)?;
        Ok(JoinRequest {
            invite: request.invite,
            member: request.member,
            confirmation: ConfirmationNumber::of(request.invite, &request.keys),
            keys: request.keys,
            device_name: request.device_name,
            provider_account_email: work.account().map(str::to_owned),
            record: bytes.to_vec(),
        })
    }
}

/// How far one call advanced an invite.
pub(crate) enum InviteProgress {
    /// Access is granted and the code can be shown.
    Shown(Invite),
    /// The invite waits on this condition.
    Waiting(PendingReason),
    /// The invite's work is done.
    Finished,
}

fn decode_request(bytes: &[u8]) -> Result<coven_format::objects::JoinRequest, SyncError> {
    match Object::decode(bytes)? {
        Object::JoinRequest(request) => Ok(request),
        _ => Err(SyncError::InvitationChanged),
    }
}

#[cfg(test)]
#[path = "sequence_invites_tests.rs"]
mod tests;

#[path = "sequence_invite_approval.rs"]
mod approval;

#[path = "sequence_invite_reset.rs"]
mod reset;
