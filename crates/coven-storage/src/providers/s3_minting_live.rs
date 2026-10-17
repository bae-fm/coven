#![cfg(test)]

//! A member key minted live with the service's admin access (§4): setup's
//! member-side check passes, the key is refused outside its store's prefix,
//! and after revocation the provider no longer lists or honors it. Only fixed
//! labels, principal kinds and failure kinds enter output, never a key.
use super::*;
use crate::{AccessKeyAdmin, MintScope};
use coven_format::S3Principal;
use coven_foundation::{
    clock::{Clock, SystemClock},
    id_source::{DeviceId, IdSource, StoreId, UuidIds},
};
use minting::{key_admin, AdminEndpoints};
use std::{num::NonZeroU64, time::Duration};

/// The longest propagation window of the self-hosted minters.
const PROPAGATION: Duration = Duration::from_secs(60);

#[tokio::test]
#[ignore = "COVEN_LIVE_S3_ENV with the service's admin key and COVEN_LIVE_S3_PROFILE; mints, checks and revokes one member key"]
async fn s3_member_key_lifecycle() {
    let live = live::LiveS3::load();
    let admin = key_admin(
        live.config.clone(),
        live.admin().await,
        AdminEndpoints::production(),
        Arc::new(SystemClock),
        Arc::new(UuidIds),
    )
    .expect("the selected profile mints keys");
    let store = StoreId(UuidIds.new_id());
    let other = StoreId(UuidIds.new_id());
    let scope = MintScope::new(store, "live-member", DeviceId(91), UuidIds.new_id());
    let owner = live.connect(None);
    // Another store's object, which the member key must not read.
    let foreign = ObjectPath::slot(other, NonZeroU64::MIN);
    owner
        .create(&foreign, b"another store's object")
        .await
        .expect("admin writes another store's object");
    let result = lifecycle(admin.as_ref(), &live, &scope, other, &foreign).await;
    let rolled_back = admin.roll_back(&scope).await;
    let outside = owner
        .provider
        .client
        .delete_object()
        .bucket(&owner.provider.bucket)
        .key(outside_key(store))
        .send()
        .await
        .map(drop)
        .map_err(s3_error)
        // GCS answers the deletion of an absent object with 404.
        .or_else(|error| match error.failure() {
            StorageFailure::NotFound => Ok(()),
            _ => Err(error),
        });
    let cleanup = crate::transfer::with_cleanup(
        live_cleanup::cleanup(&owner.provider, other).await,
        live_cleanup::cleanup(&owner.provider, store).await,
    );
    let cleanup = crate::transfer::with_cleanup(cleanup, outside);
    match &result {
        Ok(()) => println!("member_key_lifecycle PASS"),
        Err(error) => println!(
            "member_key_lifecycle FAIL failure={:?} cause={error}",
            error.failure()
        ),
    }
    println!(
        "roll_back {} cleanup {}",
        verdict(&rolled_back),
        verdict(&cleanup)
    );
    result.expect("member key lifecycle");
    rolled_back.expect("roll back after revocation");
    cleanup.expect("cleanup");
}

fn verdict<T>(result: &Result<T, StorageError>) -> &'static str {
    if result.is_ok() {
        "PASS"
    } else {
        "FAIL"
    }
}

/// A key at the bucket's root, outside the location's base prefix.
fn outside_key(store: StoreId) -> String {
    format!("coven-live-outside-{store}")
}

fn kind(principal: &S3Principal) -> &'static str {
    match principal {
        S3Principal::IamUser { .. } => "IamUser",
        S3Principal::RustFsServiceAccount { .. } => "RustFsServiceAccount",
        S3Principal::GcsServiceAccount { .. } => "GcsServiceAccount",
        _ => "other",
    }
}

/// A request outside the store's prefix must be refused with permission denied.
fn refused<T>(label: &str, result: Result<T, StorageError>) -> Result<(), StorageError> {
    match result {
        Err(error) if error.failure() == StorageFailure::PermissionDenied => {
            println!("{label} refused=PermissionDenied");
            Ok(())
        }
        Err(error) => {
            println!("{label} failure={:?}", error.failure());
            Err(error)
        }
        Ok(_) => {
            println!("{label} accepted");
            Err(StorageFailure::KeyBeyondStore.with_source(format!("member key: {label} accepted")))
        }
    }
}

async fn lifecycle(
    admin: &dyn AccessKeyAdmin,
    live: &live::LiveS3,
    scope: &MintScope,
    other: StoreId,
    foreign: &ObjectPath,
) -> Result<(), StorageError> {
    let key = admin.mint(scope).await?;
    println!("mint PASS principal={}", kind(&key.principal));
    let id = key.credentials.access_key_id.as_str();
    // GCS may read a key it just made as absent for a while.
    let mut waited = Duration::ZERO;
    let present = loop {
        let present = admin.key_exists(id, &key.principal).await?;
        if present || waited >= PROPAGATION {
            break present;
        }
        SystemClock.sleep(Duration::from_secs(1)).await;
        waited += Duration::from_secs(1);
    };
    println!(
        "key_exists_after_mint={present} after_s={}",
        waited.as_secs()
    );
    if !present {
        return Err(StorageFailure::Protocol.with_source("the minted key is not listed"));
    }
    // The store's listing names the key with its minting device (§13).
    let listed = admin.list_keys(scope.store).await?;
    let found = listed.iter().any(|listed| {
        listed.access_key_id == id
            && listed.device == scope.device
            && listed.principal == key.principal
    });
    println!("store_listing_after_mint={found} listed={}", listed.len());
    if !found || listed.len() != 1 {
        return Err(
            StorageFailure::Protocol.with_source("the store's listing misses the minted key")
        );
    }
    // Setup's member-side check: write, refuse a second create, read, list
    // and delete under the store, and a refused listing of the base prefix.
    admin.probe(scope, &key.credentials).await?;
    println!("member_check PASS");
    let member = S3Storage::new(
        live.config.clone(),
        key.credentials.clone(),
        Arc::new(SystemClock),
        Arc::new(UuidIds),
        std::sync::Arc::new(crate::NoCooldown),
    )?;
    // Setup's versioning check, read with the member key.
    let StorageConfig::S3 {
        endpoint, profile, ..
    } = &live.config
    else {
        unreachable!("live S3 configuration")
    };
    let versioning = member.bucket_versioning().await.map_err(Arc::new);
    match &versioning {
        Ok(state) => println!("member_versioning={state:?}"),
        Err(error) => println!("member_versioning failure={:?}", error.failure()),
    }
    let checked = versioning::check(endpoint.as_ref(), profile.as_ref(), &versioning);
    println!("setup_versioning_check passed={}", checked.is_ok());
    checked.map_err(|error| StorageFailure::PermissionDenied.with_source(error))?;
    let elsewhere = ObjectPath::slot(other, NonZeroU64::MIN);
    refused(
        "write_other_store",
        member
            .client
            .put_object()
            .bucket(&member.bucket)
            .key(elsewhere.under(&member.prefix))
            .body(ByteStream::from_static(b"member write"))
            .send()
            .await
            .map_err(s3_error),
    )?;
    refused(
        "read_other_store",
        member
            .client
            .get_object()
            .bucket(&member.bucket)
            .key(foreign.under(&member.prefix))
            .send()
            .await
            .map_err(s3_error),
    )?;
    refused(
        "list_other_store",
        member
            .list_page(
                member.settings.listing,
                &ObjectPrefix::all(other).under(&member.prefix),
                None,
                None,
                None,
            )
            .await,
    )?;
    refused(
        "write_outside_base_prefix",
        member
            .client
            .put_object()
            .bucket(&member.bucket)
            .key(outside_key(scope.store))
            .body(ByteStream::from_static(b"member write"))
            .send()
            .await
            .map_err(s3_error),
    )?;
    admin.revoke(id, &key.principal).await?;
    println!("revoke PASS");
    let present = admin.key_exists(id, &key.principal).await?;
    println!("key_exists_after_revoke={present}");
    if present {
        return Err(StorageFailure::Protocol.with_source("the revoked key is still listed"));
    }
    admin.revoke(id, &key.principal).await?;
    println!("revoke_again PASS");
    let listed = admin.list_keys(scope.store).await?;
    println!("store_listing_after_revoke={}", listed.len());
    if !listed.is_empty() {
        return Err(StorageFailure::Protocol.with_source("the store's listing keeps a revoked key"));
    }
    // The provider must stop honoring the key, within propagation.
    let own = ObjectPrefix::all(scope.store).under(&member.prefix);
    let mut waited = Duration::ZERO;
    loop {
        match member
            .list_page(member.settings.listing, &own, None, None, None)
            .await
        {
            Err(error)
                if matches!(
                    error.failure(),
                    StorageFailure::Authentication | StorageFailure::PermissionDenied
                ) =>
            {
                println!(
                    "revoked_key refused={:?} after_s={}",
                    error.failure(),
                    waited.as_secs()
                );
                return Ok(());
            }
            Err(error) => return Err(error),
            Ok(_) if waited < PROPAGATION => {
                SystemClock.sleep(Duration::from_secs(1)).await;
                waited += Duration::from_secs(1);
            }
            Ok(_) => {
                println!("revoked_key accepted after_s={}", waited.as_secs());
                return Err(StorageFailure::Protocol.with_source("the revoked key still lists"));
            }
        }
    }
}
