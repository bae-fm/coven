use crate::RetainedAccessReason;

pub(super) enum PermissionAccess {
    Unrelated,
    Exclusive,
    Retained(RetainedAccessReason),
}
