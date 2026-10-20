//! Member keys on Cloudflare R2 (Appendix E.12): an account API token limited to the
//! store's own bucket with Object Read & Write. Its S3 key id is the token id
//! and its secret the SHA-256 of the token value. A token reaches its whole
//! bucket, so every store has a bucket of its own (§20.7). Revocation deletes the
//! token; absence is read from the token itself.
use super::minting::{AdminRequest, KeyMinter, Pending};
use super::*;
use crate::{access_admin::named_device, ListedKey, StoreListing};
use coven_format::S3Principal;
use coven_foundation::id_source::StoreId;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
use url::Url;

/// The permission group granting object reads and writes in named buckets.
const OBJECT_WRITE: &str = "Workers R2 Storage Bucket Item Write";

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

pub(super) const R2_TOKENS: KeyMinter = KeyMinter {
    name: "Cloudflare account tokens",
    mint: mint_key,
    roll_back: roll_back_key,
    revoke: revoke_key,
    key_exists: key_present,
    list: list_keys,
    remove_unkeyed: super::minting::reject_unkeyed_principal,
    // Cloudflare states no bound; new tokens usually work within seconds.
    propagation: Duration::from_secs(30),
    separate_iam: false,
};

/// The account and jurisdiction an R2 endpoint names:
/// `<account>.r2.cloudflarestorage.com` or `<account>.<jurisdiction>.r2...`.
pub(super) fn account(config: &StorageConfig) -> Result<(String, String), StorageError> {
    let StorageConfig::S3 { endpoint, .. } = config else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3"));
    };
    let host = endpoint
        .as_ref()
        .and_then(|endpoint| endpoint.host_str())
        .and_then(|host| host.strip_suffix(".r2.cloudflarestorage.com"))
        .ok_or_else(|| {
            StorageFailure::InvalidConfiguration.with_source("an R2 endpoint names its account")
        })?;
    Ok(match host.split_once('.') {
        Some((account, jurisdiction)) => (account.to_owned(), jurisdiction.to_owned()),
        None => (host.to_owned(), "default".to_owned()),
    })
}

struct Cloudflare<'a> {
    context: &'a AdminRequest,
    account: String,
}

impl Cloudflare<'_> {
    fn url(&self, path: &str) -> Url {
        let mut url = self.context.endpoints.cloudflare.clone();
        url.path_segments_mut()
            .expect("base URL")
            .pop_if_empty()
            .extend(["accounts", &self.account])
            .extend(path.split('/'));
        url
    }

    /// One API call; None for a tolerated status. Cloudflare wraps every
    /// result as `{success, errors, result}`.
    async fn call(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
        tolerated: &[u16],
    ) -> Result<Option<Value>, StorageError> {
        let AdminCredential::BearerToken(token) = &self.context.admin else {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("Cloudflare admin access is an API token"));
        };
        let mut request = self
            .context
            .http_request(method, url)
            .bearer_auth(token.as_str());
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = self.context.send(request).await?;
        if tolerated.contains(&response.status().as_u16()) {
            return Ok(None);
        }
        let response = crate::providers::http::checked(CloudProvider::S3, response).await?;
        let value: Value = response
            .json()
            .await
            .map_err(|error| StorageFailure::Protocol.with_source(error))?;
        if value["success"] != json!(true) {
            return Err(StorageFailure::Refused.with_source("Cloudflare reported failure"));
        }
        Ok(Some(value["result"].clone()))
    }

    /// Every token this account holds, as (id, name), every page.
    async fn tokens(&self) -> Result<Vec<(String, String)>, StorageError> {
        let mut found = Vec::new();
        for page in 1.. {
            let mut url = self.url("tokens");
            url.query_pairs_mut()
                .append_pair("page", &page.to_string())
                .append_pair("per_page", "50");
            let tokens = self
                .call(Method::GET, url, None, &[])
                .await?
                .unwrap_or_default();
            let tokens = tokens.as_array().ok_or_else(|| {
                StorageFailure::Protocol.with_source("Cloudflare token listing is not an array")
            })?;
            for token in tokens {
                found.push((
                    crate::providers::http::string(token, "id")?.to_owned(),
                    crate::providers::http::string(token, "name")?.to_owned(),
                ));
            }
            if tokens.len() < 50 {
                break;
            }
        }
        Ok(found)
    }

    /// Every token this account holds with the given name.
    async fn named(&self, name: &str) -> Result<Vec<String>, StorageError> {
        Ok(self
            .tokens()
            .await?
            .into_iter()
            .filter(|(_, held)| held == name)
            .map(|(id, _)| id)
            .collect())
    }

    async fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.call(
            Method::DELETE,
            self.url(&format!("tokens/{id}")),
            None,
            &[404],
        )
        .await?;
        Ok(())
    }

    async fn permission_group(&self) -> Result<String, StorageError> {
        let groups = self
            .call(Method::GET, self.url("tokens/permission_groups"), None, &[])
            .await?
            .unwrap_or_default();
        groups
            .as_array()
            .into_iter()
            .flatten()
            .find(|group| group["name"] == OBJECT_WRITE)
            .map(|group| crate::providers::http::string(group, "id").map(str::to_owned))
            .unwrap_or_else(|| {
                Err(StorageFailure::Protocol.with_source("R2 object permission group is absent"))
            })
    }
}

async fn mint(context: &AdminRequest, scope: &MintScope) -> Result<S3MemberKey, StorageError> {
    let (account, jurisdiction) = account(&context.config)?;
    let StorageConfig::S3 { bucket, .. } = &context.config else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3"));
    };
    let cloudflare = Cloudflare { context, account };
    let group = cloudflare.permission_group().await?;
    let resource = format!(
        "com.cloudflare.edge.r2.bucket.{}_{jurisdiction}_{bucket}",
        cloudflare.account
    );
    let created = cloudflare
        .call(
            Method::POST,
            cloudflare.url("tokens"),
            Some(json!({
                // Carries the store and the minting device for a store's listing (§14.3).
                "name": scope.label(),
                "policies": [{
                    "effect": "allow",
                    "resources": {resource: "*"},
                    "permission_groups": [{"id": group}],
                }],
            })),
            &[],
        )
        .await?
        .unwrap_or_default();
    let id = crate::providers::http::string(&created, "id")?.to_owned();
    let value = crate::providers::http::string(&created, "value")?;
    Ok(S3MemberKey {
        credentials: S3Credentials {
            access_key_id: id,
            secret_access_key: SecretText::new(hex::encode(coven_crypto::s3_request_hash(
                value.as_bytes(),
            ))),
        },
        principal: S3Principal::R2Token {
            account: cloudflare.account,
        },
    })
}

async fn roll_back(context: &AdminRequest, scope: &MintScope) -> Result<(), StorageError> {
    let (account, _) = account(&context.config)?;
    let cloudflare = Cloudflare { context, account };
    for id in cloudflare.named(&scope.label()).await? {
        cloudflare.delete(&id).await?;
    }
    Ok(())
}

fn token_account(principal: &S3Principal) -> Result<&str, StorageError> {
    match principal {
        S3Principal::R2Token { account } => Ok(account),
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
    let cloudflare = Cloudflare {
        context,
        account: token_account(principal)?.to_owned(),
    };
    cloudflare.delete(access_key_id).await
}

async fn key_exists(
    context: &AdminRequest,
    access_key_id: &str,
    principal: &S3Principal,
) -> Result<bool, StorageError> {
    let cloudflare = Cloudflare {
        context,
        account: token_account(principal)?.to_owned(),
    };
    Ok(cloudflare
        .call(
            Method::GET,
            cloudflare.url(&format!("tokens/{access_key_id}")),
            None,
            &[404],
        )
        .await?
        .is_some())
}

/// Every account token whose name coven made for this store, with the minting
/// device it names (§14.3). The token id is the S3 key id.
async fn list(context: &AdminRequest, store: StoreId) -> Result<Vec<ListedKey>, StorageError> {
    let (account, _) = account(&context.config)?;
    let cloudflare = Cloudflare { context, account };
    Ok(cloudflare
        .tokens()
        .await?
        .into_iter()
        .filter_map(|(id, name)| {
            Some(ListedKey {
                access_key_id: id,
                principal: S3Principal::R2Token {
                    account: cloudflare.account.clone(),
                },
                device: named_device(&name, store)?,
            })
        })
        .collect())
}

#[cfg(test)]
#[path = "s3_r2_keys_tests.rs"]
mod tests;
