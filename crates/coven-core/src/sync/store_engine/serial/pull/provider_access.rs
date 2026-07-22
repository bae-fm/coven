use super::registration_authority::verify_serial_provider_administrator;
use super::*;

pub(in crate::sync::store_engine) async fn verify_accepted_provider_access_activation(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    access: &crate::sync::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &crate::sync::provider::ProviderAdminGrantRecord,
    administrator: &StoreDeviceRegistration,
) -> Result<(), StorePullError> {
    let activation =
        load_provider_access_activation(storage, root, root_value, access, administrator).await?;
    let (_, authorization) =
        load_device_join_authorization(storage, root, &activation.membership_state).await?;
    if !verify_serial_provider_administrator(
        &authorization,
        &access.grant.administrator_grant,
        &activation.author_registration,
        provider_admin,
    ) {
        return Err(StorePullError::Serial(
            "device provider approval activation lacks exact predecessor provider-administrator authority"
                .to_string(),
        ));
    }
    let head = read_serial_head(storage, coordination, root).await?;
    let accepted = load_authorized_serial_chain(storage, root, &head.head)
        .await?
        .iter()
        .any(|commit| commit.commit_ref == access.activation);
    if !accepted {
        return Err(StorePullError::Serial(
            "device provider approval activation is absent from current accepted Store history"
                .to_string(),
        ));
    }
    Ok(())
}
