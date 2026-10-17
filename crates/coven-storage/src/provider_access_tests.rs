use super::*;

#[test]
fn onedrive_grants_need_the_graph_permissions_and_a_refresh_token() {
    let access = OAuthAccess::OneDrive;
    let refused = StorageFailure::AccessNotGranted { required: access };
    for scope in [
        None,
        Some("Files.ReadWrite.All User.Read"),
        Some(access.scope()),
        Some(
            "https://graph.microsoft.com/User.Read https://graph.microsoft.com/Files.ReadWrite.All",
        ),
    ] {
        assert_eq!(access.granted(scope, true).unwrap(), access, "{scope:?}");
        assert_eq!(
            access.granted(scope, false).unwrap_err().failure(),
            refused,
            "{scope:?}"
        );
    }
    for scope in [
        "",
        "User.Read",
        "offline_access User.Read",
        "Files.ReadWrite.All offline_access",
        "Files.Read.All User.Read",
        "https://outlook.office.com/Files.ReadWrite.All User.Read",
    ] {
        assert_eq!(
            access.granted(Some(scope), true).unwrap_err().failure(),
            refused,
            "{scope}"
        );
    }
}

#[test]
fn dropbox_grants_need_every_requested_scope() {
    let access = OAuthAccess::Dropbox(DropboxAccess::FullDropbox);
    assert_eq!(access.granted(None, false).unwrap(), access);
    assert_eq!(access.granted(Some(access.scope()), false).unwrap(), access);
    assert_eq!(
        access
            .granted(
                Some(OAuthAccess::Dropbox(DropboxAccess::AppFolder).scope()),
                true
            )
            .unwrap_err()
            .failure(),
        StorageFailure::AccessNotGranted { required: access }
    );
}
