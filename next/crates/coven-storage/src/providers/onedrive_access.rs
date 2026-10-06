use crate::{RetainedAccessReason, StorageError};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) enum PermissionAccess {
    Unrelated,
    Exclusive,
    Retained(RetainedAccessReason),
}

/// Resolve native account ids from every email-bearing permission before any
/// deletion. A permission id identifies the permission, not its recipient.
pub(super) struct AccountPermissions<'a> {
    email: &'a str,
    ids: BTreeSet<(&'static str, String)>,
}
impl<'a> AccountPermissions<'a> {
    pub(super) fn new(email: &'a str, permissions: &[Value]) -> Result<Self, StorageError> {
        let mut account = Self {
            email,
            ids: BTreeSet::new(),
        };
        for permission in permissions {
            super::http::string(permission, "id")?;
            let roles = super::http::array(permission, "roles")?;
            if roles.is_empty()
                || roles
                    .iter()
                    .any(|role| role.as_str().is_none_or(str::is_empty))
            {
                return Err(StorageError::Protocol("invalid OneDrive permission roles"));
            }
            let identities = identities(permission)?;
            let invited = invitation_email(permission)?
                .is_some_and(|address| address.eq_ignore_ascii_case(email));
            for identity in &identities {
                if identity_email(identity, email) || (invited && identities.len() == 1) {
                    for kind in ["user", "siteUser"] {
                        if let Some(id) = identity[kind]["id"].as_str().filter(|s| !s.is_empty()) {
                            account.ids.insert((kind, id.into()));
                        }
                    }
                }
            }
        }
        Ok(account)
    }
    pub(super) fn is_named(&self, permission: &Value) -> Result<bool, StorageError> {
        Ok(invitation_email(permission)?
            .is_some_and(|email| email.eq_ignore_ascii_case(self.email))
            || identities(permission)?
                .iter()
                .any(|identity| identity_email(identity, self.email)))
    }
    fn identity_matches(&self, identity: &Value) -> bool {
        identity_email(identity, self.email)
            || ["user", "siteUser"].into_iter().any(|kind| {
                identity[kind]["id"]
                    .as_str()
                    .is_some_and(|id| self.ids.contains(&(kind, id.to_owned())))
            })
    }
    pub(super) fn matches(&self, permission: &Value) -> Result<bool, StorageError> {
        Ok(self.is_named(permission)?
            || identities(permission)?
                .iter()
                .any(|identity| self.identity_matches(identity)))
    }
    pub(super) fn classify(&self, permission: &Value) -> Result<PermissionAccess, StorageError> {
        let identities = identities(permission)?;
        let invited = invitation_email(permission)?;
        let mut target = invited.is_some_and(|email| email.eq_ignore_ascii_case(self.email));
        let mut other = false;
        let mut unknown = false;
        for identity in &identities {
            let matches = self.identity_matches(identity);
            target |= matches;
            if !matches {
                if ["user", "siteUser"]
                    .into_iter()
                    .any(|kind| identity[kind]["email"].as_str().is_some())
                {
                    other = true;
                } else {
                    unknown = true;
                }
            }
            if identity
                .as_object()
                .ok_or(StorageError::Protocol("invalid OneDrive recipient"))?
                .keys()
                .any(|key| !matches!(key.as_str(), "user" | "siteUser" | "@odata.type"))
            {
                // Groups and application principals can reach accounts that are
                // not enumerated by the permission's user identities.
                unknown = true;
            }
        }
        let broad = match permission.get("link").filter(|value| !value.is_null()) {
            Some(link) => {
                if !link.is_object() {
                    return Err(StorageError::Protocol("invalid OneDrive sharing link"));
                }
                match link.get("scope") {
                    Some(scope) => match scope.as_str() {
                        Some("existingAccess") => return Ok(PermissionAccess::Unrelated),
                        Some("anonymous" | "organization") => true,
                        Some("users") => false,
                        Some(_) => {
                            return Ok(PermissionAccess::Retained(
                                RetainedAccessReason::UnidentifiedAccount,
                            ))
                        }
                        None => {
                            return Err(StorageError::Protocol("invalid OneDrive sharing scope"))
                        }
                    },
                    None => identities.is_empty(),
                }
            }
            None => false,
        };
        unknown |= identities.is_empty() && invited.is_none() && !broad;
        if !target && !unknown && !broad {
            return Ok(PermissionAccess::Unrelated);
        }
        let reason = if permission["roles"]
            .as_array()
            .is_some_and(|roles| roles.iter().any(|role| role.as_str() == Some("owner")))
        {
            Some(RetainedAccessReason::StoreOwner)
        } else if permission
            .get("inheritedFrom")
            .is_some_and(|value| !value.is_null())
        {
            Some(RetainedAccessReason::Inherited)
        } else if broad || (target && (other || unknown)) {
            Some(RetainedAccessReason::OtherAccounts)
        } else if unknown {
            Some(RetainedAccessReason::UnidentifiedAccount)
        } else {
            None
        };
        Ok(match reason {
            Some(reason) => PermissionAccess::Retained(reason),
            None => PermissionAccess::Exclusive,
        })
    }
}
fn invitation_email(permission: &Value) -> Result<Option<&str>, StorageError> {
    match permission
        .get("invitation")
        .filter(|value| !value.is_null())
    {
        Some(invitation) => Ok(Some(super::http::string(invitation, "email")?)),
        None => Ok(None),
    }
}
fn identity_email(identity: &Value, email: &str) -> bool {
    ["user", "siteUser"].into_iter().any(|kind| {
        identity[kind]["email"]
            .as_str()
            .is_some_and(|value| value.eq_ignore_ascii_case(email))
    })
}
fn identities(permission: &Value) -> Result<Vec<&Value>, StorageError> {
    let mut result = Vec::new();
    // Graph documents both native field generations. V2 is authoritative when
    // present; the earlier field still describes responses without V2.
    for (current, earlier, multiple) in [
        ("grantedToV2", "grantedTo", false),
        ("grantedToIdentitiesV2", "grantedToIdentities", true),
    ] {
        let value = match permission.get(current).filter(|value| !value.is_null()) {
            Some(value) => Some(value),
            None => permission.get(earlier).filter(|value| !value.is_null()),
        };
        if let Some(value) = value {
            if multiple {
                result.extend(
                    value
                        .as_array()
                        .ok_or(StorageError::Protocol("invalid OneDrive recipients"))?,
                );
            } else {
                result.push(value);
            }
        }
    }
    if result.iter().any(|value| !value.is_object()) {
        return Err(StorageError::Protocol("invalid OneDrive recipient"));
    }
    Ok(result)
}
