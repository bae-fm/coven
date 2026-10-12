#![cfg(test)]

//! Opt-in S3-compatible listing measurements; credentials never enter output.
use super::*;
use crate::providers::tests::{
    listing_lag::{observe, report, TRIALS},
    live_settings,
};
use coven_foundation::{
    clock::SystemClock,
    id_source::{DeviceId, IdSource, StoreId, UuidIds},
};
use std::{sync::Arc, time::SystemTime};

async fn stored_time(provider: &S3Storage, path: &ObjectPath) -> Result<SystemTime, StorageError> {
    let head = provider
        .client
        .head_object()
        .bucket(&provider.bucket)
        .key(path.under(&provider.prefix))
        .send()
        .await
        .map_err(s3_error)?;
    (*head
        .last_modified()
        .ok_or(StorageFailure::Protocol.with_source("HEAD omitted stored time"))?)
    .try_into()
    .map_err(|error| StorageFailure::Encoding.with_source(error))
}

async fn device_listed(
    provider: &S3Storage,
    store: StoreId,
    device: DeviceId,
) -> Result<bool, StorageError> {
    let mut token = None;
    let mut pages = Pagination::new();
    let prefix = ObjectPrefix::store_logs(store).under(&provider.prefix);
    let target = ObjectPrefix::store_log(store, device).under(&provider.prefix);
    loop {
        let page = provider
            .client
            .list_objects_v2()
            .bucket(&provider.bucket)
            .prefix(&prefix)
            .delimiter("/")
            .set_continuation_token(token)
            .send()
            .await
            .map_err(s3_error)?;
        for folder in page.common_prefixes() {
            let prefix = folder
                .prefix()
                .ok_or(StorageFailure::Protocol.with_source("S3 listing omitted folder prefix"))?;
            if prefix == target {
                return Ok(true);
            }
        }
        if !page.is_truncated().ok_or(StorageFailure::Protocol)? {
            return Ok(false);
        }
        let next = page
            .next_continuation_token()
            .filter(|value| !value.is_empty())
            .ok_or(StorageFailure::Protocol)?
            .to_owned();
        pages.check(&next)?;
        token = Some(next);
    }
}

#[cfg(test)]
#[path = "s3_live_tests.rs"]
mod tests;
