//! Member keys on RustFS (Appendix E.12): service accounts with the member policy inline,
//! made through RustFS's admin API at `/rustfs/admin/v3`, whose requests carry
//! plain JSON bodies. Every member key is a service account of one dedicated
//! parent user, `coven`, granted `readwrite`; a service account's access is
//! its parent's intersected with its own policy. Deleting the parent would
//! delete every member's key, so coven never deletes it. Revocation deletes
//! the service account.
use super::minting::{member_policy, AdminRequest, KeyMinter, Pending};
use super::*;
use crate::providers::s3_admin_query::{amz_date, authorization, SignedRequest};
use crate::{access_admin::named_device, ListedKey, StoreListing};
use coven_format::S3Principal;
use coven_foundation::id_source::StoreId;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
use url::Url;

/// The parent user of every member key coven makes on RustFS.
const PARENT: &str = "coven";

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
fn list_keys<'a>(context: &'a AdminRequest, store: StoreId) -> Pending<'a, StoreListing> {
    Box::pin(async move {
        Ok(StoreListing {
            keys: list(context, store).await?,
            unkeyed: Vec::new(),
        })
    })
}

pub(super) const RUSTFS_ACCOUNTS: KeyMinter = KeyMinter {
    name: "RustFS service accounts",
    mint: mint_key,
    roll_back: roll_back_key,
    revoke: revoke_key,
    key_exists: key_present,
    list: list_keys,
    remove_unkeyed: super::minting::reject_unkeyed_principal,
    // Policies apply on the serving node; peers learn of new accounts quickly.
    propagation: Duration::from_secs(30),
    separate_iam: false,
};

struct Admin<'a> {
    context: &'a AdminRequest,
    endpoint: Url,
    region: String,
}

impl<'a> Admin<'a> {
    fn new(context: &'a AdminRequest) -> Result<Self, StorageError> {
        let StorageConfig::S3 {
            region, endpoint, ..
        } = &context.config
        else {
            return Err(StorageFailure::InvalidConfiguration.with_source("expected S3"));
        };
        Ok(Self {
            context,
            endpoint: endpoint.clone().ok_or_else(|| {
                StorageFailure::InvalidConfiguration.with_source("RustFS needs its endpoint")
            })?,
            region: region.clone(),
        })
    }

    /// One signed admin call; None for a tolerated status.
    async fn call(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<Value>,
        tolerated: &[u16],
    ) -> Result<Option<Value>, StorageError> {
        let AdminCredential::AccessKey {
            access_key_id,
            secret_access_key,
            session_token,
            ..
        } = &self.context.admin
        else {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("RustFS admin access is an access key"));
        };
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .expect("base URL")
            .pop_if_empty()
            .extend(["rustfs", "admin", "v3", path]);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        let body = body.map(|body| body.to_string()).unwrap_or_default();
        let host = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
            None => url.host_str().unwrap_or_default().to_owned(),
        };
        let stamp = amz_date(self.context.now);
        let mut headers = vec![
            ("host", host),
            (
                "x-amz-content-sha256",
                hex::encode(coven_crypto::s3_request_hash(body.as_bytes())),
            ),
            ("x-amz-date", stamp.clone()),
        ];
        if let Some(token) = session_token {
            headers.push(("x-amz-security-token", token.as_str().to_owned()));
        }
        let signature = authorization(
            access_key_id,
            secret_access_key.as_str(),
            &SignedRequest {
                method: method.as_str(),
                url: &url,
                region: &self.region,
                service: "s3",
            },
            &stamp,
            &headers,
            body.as_bytes(),
        );
        let mut request = self.context.http_request(method, url);
        for (name, value) in &headers {
            if *name != "host" {
                request = request.header(*name, value);
            }
        }
        let request = request.header("authorization", signature).body(body);
        let response = self.context.send(request).await?;
        if tolerated.contains(&response.status().as_u16()) {
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

    /// The shared parent survives failed mints; each retry establishes its grant.
    async fn parent(&self) -> Result<(), StorageError> {
        if self
            .call(
                Method::GET,
                "user-info",
                &[("accessKey", PARENT)],
                None,
                &[404],
            )
            .await?
            .is_none()
        {
            // Nobody signs in as the parent; its secret only has to be unguessable.
            let secret = coven_crypto::random_secret()
                .map_err(|error| StorageFailure::InvalidConfiguration.with_source(error))?;
            self.call(
                Method::PUT,
                "add-user",
                &[("accessKey", PARENT)],
                Some(json!({"secretKey": secret.as_str(), "status": "enabled"})),
                &[],
            )
            .await?;
        }
        self.call(
            Method::PUT,
            "set-user-or-group-policy",
            &[
                ("policyName", "readwrite"),
                ("userOrGroup", PARENT),
                ("isGroup", "false"),
            ],
            None,
            &[],
        )
        .await?;
        Ok(())
    }

    /// The parent's service accounts, every one.
    async fn accounts(&self) -> Result<Vec<Value>, StorageError> {
        let Some(listed) = self
            .call(
                Method::GET,
                "list-service-accounts",
                &[("user", PARENT)],
                None,
                &[404],
            )
            .await?
        else {
            return Ok(Vec::new());
        };
        let accounts = crate::providers::http::array(&listed, "accounts")?;
        for account in accounts {
            crate::providers::http::string(account, "accessKey")?;
            crate::providers::http::optional_string(account, "name")?;
            crate::providers::http::optional_string(account, "description")?;
        }
        Ok(accounts.clone())
    }

    /// The parent's service accounts made with this scope's name.
    async fn named(&self, name: &str) -> Result<Vec<String>, StorageError> {
        self.accounts()
            .await?
            .iter()
            .filter(|account| account["name"] == name)
            .map(|account| crate::providers::http::string(account, "accessKey").map(str::to_owned))
            .collect()
    }

    /// Delete a service account; absence succeeds. RustFS answers a delete
    /// of an absent account with HTTP 400 `InvalidRequest`, a status other
    /// refusals share, so a refused delete succeeds only once the account
    /// reads as absent.
    async fn delete(&self, access_key: &str) -> Result<(), StorageError> {
        let deleted = self
            .call(
                Method::DELETE,
                "delete-service-account",
                &[("accessKey", access_key)],
                None,
                &[400, 404],
            )
            .await?;
        if deleted.is_some() || !self.exists(access_key).await? {
            return Ok(());
        }
        Err(StorageFailure::Refused.with_source("RustFS refused to delete a service account"))
    }

    async fn exists(&self, access_key: &str) -> Result<bool, StorageError> {
        Ok(self
            .call(
                Method::GET,
                "info-service-account",
                &[("accessKey", access_key)],
                None,
                &[404],
            )
            .await?
            .is_some())
    }
}

async fn mint(context: &AdminRequest, scope: &MintScope) -> Result<S3MemberKey, StorageError> {
    let admin = Admin::new(context)?;
    admin.parent().await?;
    let policy: Value = serde_json::from_str(&member_policy(&context.config, scope.store)?)
        .map_err(|error| StorageFailure::Protocol.with_source(error))?;
    let created = admin
        .call(
            Method::PUT,
            "add-service-account",
            &[],
            Some(json!({
                "policy": policy,
                "targetUser": PARENT,
                "name": scope.name,
                // Carries the store and the minting device for a store's listing (§14.3).
                "description": scope.label(),
            })),
            &[],
        )
        .await?
        .unwrap_or_default();
    let credentials = &created["credentials"];
    Ok(S3MemberKey {
        credentials: S3Credentials {
            access_key_id: crate::providers::http::string(credentials, "accessKey")?.to_owned(),
            secret_access_key: SecretText::new(
                crate::providers::http::string(credentials, "secretKey")?.to_owned(),
            ),
        },
        principal: S3Principal::RustFsServiceAccount {
            parent: PARENT.into(),
        },
    })
}

async fn roll_back(context: &AdminRequest, scope: &MintScope) -> Result<(), StorageError> {
    let admin = Admin::new(context)?;
    for key in admin.named(&scope.name).await? {
        admin.delete(&key).await?;
    }
    Ok(())
}

fn service_account(principal: &S3Principal) -> Result<(), StorageError> {
    match principal {
        S3Principal::RustFsServiceAccount { .. } => Ok(()),
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
    service_account(principal)?;
    Admin::new(context)?.delete(access_key_id).await
}

async fn key_exists(
    context: &AdminRequest,
    access_key_id: &str,
    principal: &S3Principal,
) -> Result<bool, StorageError> {
    service_account(principal)?;
    Admin::new(context)?.exists(access_key_id).await
}

/// Every service account of coven's parent whose description names this
/// store, with the minting device it names (§14.3).
async fn list(context: &AdminRequest, store: StoreId) -> Result<Vec<ListedKey>, StorageError> {
    let mut listed = Vec::new();
    for account in Admin::new(context)?.accounts().await? {
        let Some(device) = account["description"]
            .as_str()
            .and_then(|description| named_device(description, store))
        else {
            continue;
        };
        listed.push(ListedKey {
            access_key_id: crate::providers::http::string(&account, "accessKey")?.to_owned(),
            principal: S3Principal::RustFsServiceAccount {
                parent: PARENT.into(),
            },
            device,
        });
    }
    Ok(listed)
}

#[cfg(test)]
#[path = "s3_rustfs_keys_tests.rs"]
mod tests;
