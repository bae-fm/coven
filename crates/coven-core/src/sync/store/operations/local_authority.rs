use super::*;

pub(crate) async fn load_local_store_authority(
    db: &Database,
    expected_device_id: &str,
    identity_signer: &UserKeypair,
) -> Result<
    (
        super::store_commit::StoreRootRef,
        StoreDeviceRegistrationRef,
        StoreDeviceRegistration,
        UserKeypair,
    ),
    StoreOutboundError,
> {
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or(StoreOutboundError::MissingState {
            key: STORE_ROOT_AUTHORITY,
        })?;
    let durable = db.latest_local_store_device_registration().await?.ok_or(
        StoreOutboundError::MissingState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        },
    )?;
    if !durable.is_activated() || durable.device_id.to_string() != expected_device_id {
        return Err(StoreOutboundError::InvalidState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            reason: "local Store device registration is not the activated writer".to_string(),
        });
    }
    let registration =
        StoreDeviceRegistration::parse_at(&durable.registration_bytes, &root, durable.device_id)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    if registration.registration_hash() != durable.registration_hash {
        return Err(StoreOutboundError::InvalidOutbound(
            "local Store device registration differs from its durable hash".to_string(),
        ));
    }
    let reference = StoreDeviceRegistrationRef::from_registration(
        &registration,
        durable.prepared.reference().clone(),
    );
    let activated = db
        .activated_store_device_registration(reference.clone())
        .await?;
    if activated != registration {
        return Err(StoreOutboundError::InvalidOutbound(
            "local Store writer differs from its activated exact registration".to_string(),
        ));
    }
    let device_signer = registration
        .device_signer(identity_signer)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    Ok((root, reference, registration, device_signer))
}
