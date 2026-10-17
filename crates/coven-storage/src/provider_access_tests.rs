use super::*;

#[test]
fn explicit_onedrive_grants_require_every_requested_scope() {
    let access = OAuthAccess::OneDrive;
    assert_eq!(access.granted(None).unwrap(), access);
    assert_eq!(access.granted(Some(access.scope())).unwrap(), access);
    for scope in [
        "",
        "User.Read",
        "Files.ReadWrite.All User.Read",
        "Files.ReadWrite.All offline_access",
        "offline_access User.Read",
    ] {
        assert_eq!(
            access.granted(Some(scope)).unwrap_err().failure(),
            StorageFailure::AccessNotGranted { required: access },
            "{scope}"
        );
    }
}
