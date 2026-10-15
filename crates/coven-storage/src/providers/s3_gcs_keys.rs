//! Member keys on GCS (§4): a service account per key, granted
//! `roles/storage.objectUser` on the store's managed folder `<prefix>/<store>/`,
//! whose HMAC key is the member key. Managed folders need uniform bucket-level
//! access; a bucket without it is refused. A new HMAC key can take up to 60
//! seconds to work, so the member-side check retries within that bound.
//!
//! Revocation sets the HMAC key inactive and deletes it, removes the folder
//! binding and deletes the service account. The folder is the store's and
//! stays. Absence is read from the service account's HMAC key list.
use super::minting::{AdminRequest, KeyMinter, Pending};
use super::*;
use crate::{access_admin::named_device, ListedKey};
use coven_format::S3Principal;
use coven_foundation::id_source::StoreId;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
use url::Url;

const OBJECT_USER: &str = "roles/storage.objectUser";

fn mint_key<'a>(context: &'a AdminRequest, scope: &'a MintScope) -> Pending<'a, S3MemberKey> {
    Box::pin(mint(context, scope))
}
fn roll_back_key<'a>(context: &'a AdminRequest, scope: &'a MintScope) -> Pending<'a, ()> {
    Box::pin(roll_back(context, scope))
}
fn revoke_key<'a>(
    context: &'a AdminRequest,
    key: &'a str,
    principal: &'a S3Principal,
) -> Pending<'a, ()> {
    Box::pin(revoke(context, key, principal))
}
fn key_present<'a>(
    context: &'a AdminRequest,
    key: &'a str,
    principal: &'a S3Principal,
) -> Pending<'a, bool> {
    Box::pin(key_exists(context, key, principal))
}
fn list_keys<'a>(context: &'a AdminRequest, store: StoreId) -> Pending<'a, Vec<ListedKey>> {
    Box::pin(list(context, store))
}

pub(super) const GCS_KEYS: KeyMinter = KeyMinter {
    name: "GCS service accounts",
    mint: mint_key,
    roll_back: roll_back_key,
    revoke: revoke_key,
    key_exists: key_present,
    list: list_keys,
    // Google documents up to 60 seconds before a new HMAC key works.
    propagation: Duration::from_secs(60),
    separate_iam: false,
};

struct Gcs<'a> {
    context: &'a AdminRequest,
    bucket: String,
    folder: String,
}

impl<'a> Gcs<'a> {
    fn new(context: &'a AdminRequest, store: StoreId) -> Result<Self, StorageError> {
        let StorageConfig::S3 { bucket, prefix, .. } = &context.config else {
            return Err(StorageFailure::InvalidConfiguration.with_source("expected S3"));
        };
        Ok(Self {
            context,
            bucket: bucket.clone(),
            folder: ObjectPrefix::all(store).under(prefix),
        })
    }

    /// One JSON request with the bearer token; None for a tolerated status.
    async fn call(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
        tolerated: &[u16],
    ) -> Result<Option<Value>, StorageError> {
        let AdminCredential::BearerToken(token) = &self.context.admin else {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("Google Cloud admin access is an OAuth access token"));
        };
        let mut request = self
            .context
            .http
            .request(method, url)
            .bearer_auth(token.as_str());
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| crate::providers::http::transport(CloudProvider::S3, error))?;
        let status = response.status().as_u16();
        if tolerated.contains(&status) {
            return Ok(None);
        }
        let response = crate::providers::http::checked(CloudProvider::S3, response).await?;
        let bytes = response
            .bytes()
            .await
            .map_err(|error| crate::providers::http::transport(CloudProvider::S3, error))?;
        if bytes.is_empty() {
            return Ok(Some(Value::Null));
        }
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| StorageFailure::Protocol.with_source(error))
    }

    fn storage(&self, path: &str) -> Url {
        let mut url = self.context.endpoints.gcs_storage.clone();
        url.path_segments_mut()
            .expect("base URL")
            .pop_if_empty()
            .extend(path.split('/'));
        url
    }

    fn iam(&self, path: &str) -> Url {
        let mut url = self.context.endpoints.gcs_iam.clone();
        url.path_segments_mut()
            .expect("base URL")
            .pop_if_empty()
            .extend(path.split('/'));
        url
    }

    /// The bucket's project id, refusing a bucket without uniform access.
    async fn project(&self) -> Result<String, StorageError> {
        let mut url = self.storage(&format!("b/{}", self.bucket));
        url.query_pairs_mut()
            .append_pair("fields", "projectNumber,iamConfiguration");
        let bucket = self
            .call(Method::GET, url, None, &[])
            .await?
            .unwrap_or_default();
        if bucket["iamConfiguration"]["uniformBucketLevelAccess"]["enabled"] != json!(true) {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("GCS member keys need uniform bucket-level access on the bucket"));
        }
        let number = crate::providers::http::string(&bucket, "projectNumber")?;
        let mut url = self.context.endpoints.gcs_projects.clone();
        url.path_segments_mut()
            .expect("base URL")
            .pop_if_empty()
            .extend(["projects", number]);
        let project = self
            .call(Method::GET, url, None, &[])
            .await?
            .unwrap_or_default();
        Ok(crate::providers::http::string(&project, "projectId")?.to_owned())
    }

    fn email(project: &str, name: &str) -> String {
        format!("{name}@{project}.iam.gserviceaccount.com")
    }

    fn folder_url(&self, suffix: &str) -> Url {
        let mut url = self.context.endpoints.gcs_storage.clone();
        url.path_segments_mut()
            .expect("base URL")
            .pop_if_empty()
            .extend(["b", &self.bucket, "managedFolders", &self.folder])
            .extend(suffix.split('/').filter(|part| !part.is_empty()));
        url
    }

    /// Add or remove the member's binding, retrying once on a concurrent change.
    async fn bind(&self, email: &str, present: bool) -> Result<(), StorageError> {
        let member = format!("serviceAccount:{email}");
        for _ in 0..2 {
            let Some(mut policy) = self
                .call(Method::GET, self.folder_url("iam"), None, &[404])
                .await?
            else {
                return if present {
                    Err(StorageFailure::NotFound.with_source("managed folder is absent"))
                } else {
                    Ok(())
                };
            };
            let bindings = policy["bindings"].as_array().cloned().unwrap_or_default();
            let mut changed = false;
            let mut kept = Vec::new();
            let mut granted = false;
            for mut binding in bindings {
                if binding["role"] == OBJECT_USER {
                    let mut members: Vec<Value> =
                        binding["members"].as_array().cloned().unwrap_or_default();
                    let had = members.iter().any(|m| m == &json!(member));
                    if present && !had {
                        members.push(json!(member));
                        changed = true;
                    }
                    if !present && had {
                        members.retain(|m| m != &json!(member));
                        changed = true;
                    }
                    granted = true;
                    if members.is_empty() {
                        continue;
                    }
                    binding["members"] = json!(members);
                }
                kept.push(binding);
            }
            if present && !granted {
                kept.push(json!({"role": OBJECT_USER, "members": [member]}));
                changed = true;
            }
            if !changed {
                return Ok(());
            }
            policy["bindings"] = json!(kept);
            match self
                .call(Method::PUT, self.folder_url("iam"), Some(policy), &[412])
                .await?
            {
                Some(_) => return Ok(()),
                None => continue,
            }
        }
        Err(StorageFailure::Conflict.with_source("managed folder policy kept changing"))
    }

    fn hmac_url(&self, project: &str, access_id: Option<&str>) -> Url {
        let mut path = format!("projects/{project}/hmacKeys");
        if let Some(id) = access_id {
            path.push('/');
            path.push_str(id);
        }
        self.storage(&path)
    }

    /// The service account's HMAC keys that are not deleted.
    async fn hmac_keys(&self, project: &str, email: &str) -> Result<Vec<String>, StorageError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut url = self.hmac_url(project, None);
            url.query_pairs_mut()
                .append_pair("serviceAccountEmail", email)
                .append_pair("showDeletedKeys", "false");
            if let Some(token) = &token {
                url.query_pairs_mut().append_pair("pageToken", token);
            }
            let page = self
                .call(Method::GET, url, None, &[])
                .await?
                .unwrap_or_default();
            for item in page["items"].as_array().cloned().unwrap_or_default() {
                if item["state"] != "DELETED" {
                    keys.push(crate::providers::http::string(&item, "accessId")?.to_owned());
                }
            }
            match page["nextPageToken"].as_str() {
                Some(next) if Some(next) != token.as_deref() => token = Some(next.to_owned()),
                Some(_) => {
                    return Err(
                        StorageFailure::Protocol.with_source("HMAC listing repeated its token")
                    )
                }
                None => return Ok(keys),
            }
        }
    }

    /// An HMAC key must be inactive before it can be deleted.
    async fn delete_hmac(&self, project: &str, access_id: &str) -> Result<(), StorageError> {
        let url = self.hmac_url(project, Some(access_id));
        self.call(
            Method::PUT,
            url.clone(),
            Some(json!({"state": "INACTIVE"})),
            &[404],
        )
        .await?;
        self.call(Method::DELETE, url, None, &[404]).await?;
        Ok(())
    }

    async fn remove(&self, project: &str, email: &str) -> Result<(), StorageError> {
        for key in self.hmac_keys(project, email).await? {
            self.delete_hmac(project, &key).await?;
        }
        self.bind(email, false).await?;
        self.call(
            Method::DELETE,
            self.iam(&format!("projects/{project}/serviceAccounts/{email}")),
            None,
            &[404],
        )
        .await?;
        Ok(())
    }
}

async fn mint(context: &AdminRequest, scope: &MintScope) -> Result<S3MemberKey, StorageError> {
    let gcs = Gcs::new(context, scope.store)?;
    let project = gcs.project().await?;
    let email = Gcs::email(&project, &scope.name);
    gcs.call(
        Method::POST,
        gcs.iam(&format!("projects/{project}/serviceAccounts")),
        Some(json!({
            "accountId": scope.name,
            "serviceAccount": {
                "displayName": "coven member key",
                // Carries the store and the minting device for a store's listing (§13).
                "description": scope.label(),
            },
        })),
        &[409],
    )
    .await?;
    gcs.call(
        Method::POST,
        gcs.storage(&format!("b/{}/managedFolders", gcs.bucket)),
        Some(json!({"name": gcs.folder})),
        &[409],
    )
    .await?;
    gcs.bind(&email, true).await?;
    let mut url = gcs.hmac_url(&project, None);
    url.query_pairs_mut()
        .append_pair("serviceAccountEmail", &email);
    let created = gcs
        .call(Method::POST, url, None, &[])
        .await?
        .unwrap_or_default();
    let access_key_id =
        crate::providers::http::string(&created["metadata"], "accessId")?.to_owned();
    let secret = crate::providers::http::string(&created, "secret")?.to_owned();
    Ok(S3MemberKey {
        credentials: S3Credentials {
            access_key_id,
            secret_access_key: SecretText::new(secret),
        },
        principal: S3Principal::GcsServiceAccount {
            project,
            email,
            managed_folder: gcs.folder.clone(),
        },
    })
}

async fn roll_back(context: &AdminRequest, scope: &MintScope) -> Result<(), StorageError> {
    let gcs = Gcs::new(context, scope.store)?;
    let project = gcs.project().await?;
    gcs.remove(&project, &Gcs::email(&project, &scope.name))
        .await
}

fn account(principal: &S3Principal) -> Result<(&str, &str), StorageError> {
    match principal {
        S3Principal::GcsServiceAccount { project, email, .. } => Ok((project, email)),
        S3Principal::Console => Err(StorageFailure::InvalidConfiguration
            .with_source("a key made in the console is deleted there")),
        _ => Err(StorageFailure::InvalidConfiguration
            .with_source("this key belongs to another service's principal")),
    }
}

async fn revoke(
    context: &AdminRequest,
    access_key_id: &str,
    principal: &S3Principal,
) -> Result<(), StorageError> {
    let (project, email) = account(principal)?;
    let S3Principal::GcsServiceAccount { managed_folder, .. } = principal else {
        unreachable!("checked principal")
    };
    let StorageConfig::S3 { bucket, .. } = &context.config else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3"));
    };
    let gcs = Gcs {
        context,
        bucket: bucket.clone(),
        folder: managed_folder.clone(),
    };
    gcs.delete_hmac(project, access_key_id).await?;
    gcs.remove(project, email).await
}

async fn key_exists(
    context: &AdminRequest,
    access_key_id: &str,
    principal: &S3Principal,
) -> Result<bool, StorageError> {
    let (project, email) = account(principal)?;
    let StorageConfig::S3 { bucket, .. } = &context.config else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3"));
    };
    let gcs = Gcs {
        context,
        bucket: bucket.clone(),
        folder: String::new(),
    };
    Ok(gcs
        .hmac_keys(project, email)
        .await?
        .iter()
        .any(|key| key == access_key_id))
}

/// Every HMAC key of a service account whose description names this store,
/// with the minting device it names (§13). Accounts are read from the bucket's
/// project, every page.
async fn list(context: &AdminRequest, store: StoreId) -> Result<Vec<ListedKey>, StorageError> {
    let gcs = Gcs::new(context, store)?;
    let project = gcs.project().await?;
    let mut listed = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut url = gcs.iam(&format!("projects/{project}/serviceAccounts"));
        url.query_pairs_mut().append_pair("pageSize", "100");
        if let Some(token) = &token {
            url.query_pairs_mut().append_pair("pageToken", token);
        }
        let page = gcs
            .call(Method::GET, url, None, &[])
            .await?
            .unwrap_or_default();
        for account in page["accounts"].as_array().cloned().unwrap_or_default() {
            let Some(device) = account["description"]
                .as_str()
                .and_then(|description| named_device(description, store))
            else {
                continue;
            };
            let email = crate::providers::http::string(&account, "email")?.to_owned();
            for access_key_id in gcs.hmac_keys(&project, &email).await? {
                listed.push(ListedKey {
                    access_key_id,
                    principal: S3Principal::GcsServiceAccount {
                        project: project.clone(),
                        email: email.clone(),
                        managed_folder: gcs.folder.clone(),
                    },
                    device,
                });
            }
        }
        match page["nextPageToken"].as_str() {
            Some("") | None => return Ok(listed),
            Some(next) if Some(next) != token.as_deref() => token = Some(next.to_owned()),
            Some(_) => {
                return Err(StorageFailure::Protocol
                    .with_source("service account listing repeated its token"))
            }
        }
    }
}

#[cfg(test)]
#[path = "s3_gcs_keys_tests.rs"]
mod tests;
