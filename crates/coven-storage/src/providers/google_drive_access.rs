use super::{access::PermissionAccess, http};
use crate::{RetainedAccessReason, StorageError, StorageFailure};
use serde_json::Value;

fn matches_account(permission: &Value, email: &str) -> bool {
    permission["type"].as_str() == Some("user")
        && permission["emailAddress"]
            .as_str()
            .is_some_and(|address| address.eq_ignore_ascii_case(email))
}
pub(super) fn writable_for(permission: &Value, email: &str) -> bool {
    matches_account(permission, email)
        && matches!(permission["role"].as_str(), Some("writer" | "owner"))
}
pub(super) fn classify(permission: &Value, email: &str) -> Result<PermissionAccess, StorageError> {
    http::string(permission, "id")?;
    let role = http::string(permission, "role")?;
    if permission["deleted"].as_bool() == Some(true) {
        return Ok(PermissionAccess::Unrelated);
    }
    match http::string(permission, "type")? {
        "anyone" | "domain" | "group" => {
            return Ok(PermissionAccess::Retained(
                RetainedAccessReason::OtherAccounts,
            ))
        }
        "user" => {}
        _ => {
            return Ok(PermissionAccess::Retained(
                RetainedAccessReason::UnidentifiedAccount,
            ))
        }
    }
    let Some(address) = permission["emailAddress"].as_str() else {
        return Ok(PermissionAccess::Retained(
            RetainedAccessReason::UnidentifiedAccount,
        ));
    };
    if !address.eq_ignore_ascii_case(email) {
        return Ok(PermissionAccess::Unrelated);
    }
    if role == "owner" {
        return Ok(PermissionAccess::Retained(RetainedAccessReason::StoreOwner));
    }
    let Some(details) = permission.get("permissionDetails") else {
        return Ok(PermissionAccess::Retained(
            RetainedAccessReason::UnidentifiedAccount,
        ));
    };
    let details = details
        .as_array()
        .ok_or(StorageFailure::Protocol.with_source("invalid Drive permission details"))?;
    let mut direct = false;
    let mut inherited = false;
    for detail in details {
        match detail["inherited"]
            .as_bool()
            .ok_or(StorageFailure::Protocol.with_source("Drive omitted permission inheritance"))?
        {
            true => inherited = true,
            false => direct = true,
        }
    }
    if direct {
        Ok(PermissionAccess::Exclusive)
    } else if inherited {
        Ok(PermissionAccess::Retained(RetainedAccessReason::Inherited))
    } else {
        Ok(PermissionAccess::Retained(
            RetainedAccessReason::UnidentifiedAccount,
        ))
    }
}
