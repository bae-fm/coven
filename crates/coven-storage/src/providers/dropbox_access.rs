use super::http;
use crate::{RetainedAccess, RetainedAccessReason, StorageError, StorageFailure};
use serde_json::{json, Value};

pub(super) enum MemberId {
    Account(String),
    Invite(String),
}
impl MemberId {
    pub(super) fn as_str(&self) -> &str {
        match self {
            Self::Account(id) | Self::Invite(id) => id,
        }
    }
    pub(super) fn selector(&self) -> Value {
        match self {
            Self::Account(id) => json!({".tag":"dropbox_id","dropbox_id":id}),
            Self::Invite(email) => json!({".tag":"email","email":email}),
        }
    }
}
pub(super) struct AccountMember {
    pub(super) id: MemberId,
    pub(super) role: String,
}
#[derive(Default)]
pub(super) struct FolderMembers {
    pub(super) direct: Vec<AccountMember>,
    pub(super) retained: Vec<RetainedAccess>,
}
impl FolderMembers {
    /// The folder's owner is the account holding the store; inviting it is
    /// refused (§13.2).
    pub(super) fn refuse_owner(&self) -> Result<(), StorageError> {
        if self.retained.iter().any(|share| share.reason == RetainedAccessReason::StoreOwner) {
            return Err(StorageFailure::InviteeIsStoreOwner.into());
        }
        Ok(())
    }
    pub(super) fn append(
        &mut self,
        value: &Value,
        email: &str,
        inherited: bool,
    ) -> Result<(), StorageError> {
        for member in http::array(value, "users")? {
            let user = &member["user"];
            if http::string(user, "email")?.eq_ignore_ascii_case(email) {
                self.account(
                    member,
                    MemberId::Account(http::string(user, "account_id")?.into()),
                    inherited,
                )?;
            }
        }
        for member in http::array(value, "invitees")? {
            let invitee = &member["invitee"];
            if http::string(invitee, ".tag")? != "email" {
                return Err(StorageFailure::Protocol.with_source("unknown Dropbox invitee"));
            }
            let invited_email = http::string(invitee, "email")?;
            let user = member.get("user");
            let matches = invited_email.eq_ignore_ascii_case(email)
                || match user {
                    Some(user) => http::string(user, "email")?.eq_ignore_ascii_case(email),
                    None => false,
                };
            if matches {
                let id = match user {
                    Some(user) => MemberId::Account(http::string(user, "account_id")?.into()),
                    None => MemberId::Invite(invited_email.into()),
                };
                self.account(member, id, inherited)?;
            }
        }
        for group in http::array(value, "groups")? {
            self.retained.push(RetainedAccess {
                provider_id: http::string(&group["group"], "group_id")?.into(),
                reason: RetainedAccessReason::OtherAccounts,
            });
        }
        Ok(())
    }
    fn account(
        &mut self,
        member: &Value,
        id: MemberId,
        inherited: bool,
    ) -> Result<(), StorageError> {
        let role = http::string(&member["access_type"], ".tag")?;
        if !matches!(role, "owner" | "editor" | "viewer" | "viewer_no_comment") {
            return Err(StorageFailure::Protocol.with_source("unexpected Dropbox account access"));
        }
        let reason = if role == "owner" {
            Some(RetainedAccessReason::StoreOwner)
        } else if inherited {
            Some(RetainedAccessReason::Inherited)
        } else {
            None
        };
        match reason {
            Some(reason) => {
                self.retained.push(RetainedAccess { provider_id: id.as_str().into(), reason })
            }
            None => self.direct.push(AccountMember { id, role: role.into() }),
        }
        Ok(())
    }
}

pub(super) fn remaining_parent_access(
    value: &Value,
    member: &MemberId,
) -> Result<Vec<RetainedAccess>, StorageError> {
    let fields = value
        .as_object()
        .ok_or(StorageFailure::Protocol.with_source("invalid Dropbox removal result"))?;
    if fields.keys().any(|field| {
        !matches!(field.as_str(), ".tag" | "access_level" | "warning" | "access_details")
    }) || fields.get("warning").is_some_and(|warning| !warning.is_null() && !warning.is_string())
    {
        return Err(StorageFailure::Protocol.with_source("invalid Dropbox removal result"));
    }
    let has_parent_access = match fields.get("access_level") {
        None | Some(Value::Null) => false,
        // Dropbox's no_access level still permits actions through a shared link;
        // any reported level here describes access retained through a parent.
        Some(level) => match http::string(level, ".tag")? {
            "owner" | "editor" | "viewer" | "viewer_no_comment" | "traverse" | "no_access" => true,
            _ => {
                return Err(
                    StorageFailure::Protocol.with_source("unknown Dropbox parent access level")
                )
            }
        },
    };
    let mut retained = Vec::new();
    match fields.get("access_details") {
        None | Some(Value::Null) => {}
        Some(details) => {
            for parent in details
                .as_array()
                .ok_or(StorageFailure::Protocol.with_source("invalid Dropbox parent access"))?
            {
                retained.push(RetainedAccess {
                    provider_id: http::string(parent, "shared_folder_id")?.into(),
                    reason: RetainedAccessReason::Inherited,
                });
            }
        }
    }
    if retained.is_empty() && has_parent_access {
        retained.push(RetainedAccess {
            provider_id: member.as_str().into(),
            reason: RetainedAccessReason::Inherited,
        });
    }
    Ok(retained)
}
