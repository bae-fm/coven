//! Member keys through AWS-style IAM (§4): AWS IAM and Ceph RGW accounts with
//! an inline user policy, SeaweedFS's embedded IAM with a managed policy
//! attached to the user, because its PutUserPolicy drops conditions.
//!
//! Every step tolerates what an earlier attempt with the same deterministic
//! user name already created. A user holds at most two keys and coven holds
//! the secret of none an earlier attempt made, so minting deletes them first.
use super::minting::{member_policy, AdminRequest, KeyMinter, Pending};
use super::*;
use crate::providers::s3_admin_query::{elements, send, QueryService};
use coven_format::{IamPolicy, S3Principal};
use std::time::Duration;
use url::Url;

const VERSION: &str = "2010-05-08";
/// The inline policy's name on each user coven makes.
const INLINE_POLICY: &str = "coven-member";

/// How a service's IAM gives a user its policy, and where its API is.
#[derive(Clone, Copy)]
struct Flavor {
    /// PutUserPolicy, users under `/coven/` with store and member tags;
    /// otherwise CreatePolicy and AttachUserPolicy on a plain user.
    inline: bool,
    /// AWS IAM's global endpoint, signing for us-east-1; otherwise the
    /// location's S3 endpoint and region. Ceph RGW accounts take the inline
    /// policy at the RGW endpoint once a profile names that service.
    global: bool,
}
const AWS: Flavor = Flavor {
    inline: true,
    global: true,
};
const SEAWEEDFS: Flavor = Flavor {
    inline: false,
    global: false,
};

macro_rules! minter {
    ($name:ident, $label:literal, $flavor:expr, $propagation:expr) => {
        pub(super) const $name: KeyMinter = {
            fn mint_key<'a>(
                context: &'a AdminRequest,
                scope: &'a MintScope,
            ) -> Pending<'a, S3MemberKey> {
                Box::pin(mint(context, scope, $flavor))
            }
            fn roll_back_key<'a>(
                context: &'a AdminRequest,
                scope: &'a MintScope,
            ) -> Pending<'a, ()> {
                Box::pin(roll_back(context, scope, $flavor))
            }
            fn revoke_key<'a>(
                context: &'a AdminRequest,
                key: &'a str,
                principal: &'a S3Principal,
            ) -> Pending<'a, ()> {
                Box::pin(revoke(context, key, principal, $flavor))
            }
            fn key_present<'a>(
                context: &'a AdminRequest,
                key: &'a str,
                principal: &'a S3Principal,
            ) -> Pending<'a, bool> {
                Box::pin(key_exists(context, key, principal, $flavor))
            }
            KeyMinter {
                name: $label,
                mint: mint_key,
                roll_back: roll_back_key,
                revoke: revoke_key,
                key_exists: key_present,
                propagation: $propagation,
            }
        };
    };
}

// IAM documents eventual consistency without a bound; a minute covers the
// propagation commonly observed for new keys.
minter!(AWS_IAM, "AWS IAM", AWS, Duration::from_secs(60));
// SeaweedFS's embedded IAM on the S3 port (`-iam.readOnly=false`). Gateways
// read identity changes from filer metadata asynchronously.
minter!(
    SEAWEEDFS_IAM,
    "SeaweedFS IAM",
    SEAWEEDFS,
    Duration::from_secs(30)
);

struct Iam<'a> {
    context: &'a AdminRequest,
    endpoint: Url,
    region: String,
}

impl<'a> Iam<'a> {
    fn new(context: &'a AdminRequest, flavor: Flavor) -> Result<Self, StorageError> {
        let StorageConfig::S3 {
            region, endpoint, ..
        } = &context.config
        else {
            return Err(StorageFailure::InvalidConfiguration.with_source("expected S3"));
        };
        Ok(match flavor.global {
            true => Self {
                context,
                endpoint: context.endpoints.aws_iam.clone(),
                region: "us-east-1".into(),
            },
            false => Self {
                context,
                endpoint: endpoint.clone().ok_or_else(|| {
                    StorageFailure::InvalidConfiguration.with_source("IAM needs the S3 endpoint")
                })?,
                region: region.clone(),
            },
        })
    }

    async fn call(
        &self,
        action: &str,
        parameters: &[(&str, &str)],
    ) -> Result<String, StorageError> {
        send(
            &self.context.http,
            self.context.now,
            &self.context.admin,
            &QueryService {
                endpoint: &self.endpoint,
                region: &self.region,
                service: "iam",
                version: VERSION,
            },
            action,
            parameters,
        )
        .await
    }

    async fn tolerating(
        &self,
        tolerated: StorageFailure,
        action: &str,
        parameters: &[(&str, &str)],
    ) -> Result<Option<String>, StorageError> {
        match self.call(action, parameters).await {
            Ok(body) => Ok(Some(body)),
            Err(error) if error.failure() == tolerated => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The user's key ids, every page; None when the user is absent.
    async fn keys(&self, user: &str) -> Result<Option<Vec<String>>, StorageError> {
        let mut keys = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let mut parameters = vec![("UserName", user)];
            if let Some(marker) = &marker {
                parameters.push(("Marker", marker));
            }
            let Some(body) = self
                .tolerating(StorageFailure::NotFound, "ListAccessKeys", &parameters)
                .await?
            else {
                return Ok(None);
            };
            keys.extend(elements(&body, "AccessKeyId")?);
            if elements(&body, "IsTruncated")?.first().map(String::as_str) != Some("true") {
                return Ok(Some(keys));
            }
            let next = elements(&body, "Marker")?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    StorageFailure::Protocol.with_source("truncated IAM listing without a marker")
                })?;
            if marker.as_ref() == Some(&next) {
                return Err(StorageFailure::Protocol.with_source("IAM listing repeated its marker"));
            }
            marker = Some(next);
        }
    }

    async fn delete_keys(&self, user: &str) -> Result<(), StorageError> {
        for key in self.keys(user).await?.unwrap_or_default() {
            self.tolerating(
                StorageFailure::NotFound,
                "DeleteAccessKey",
                &[("UserName", user), ("AccessKeyId", &key)],
            )
            .await?;
        }
        Ok(())
    }

    /// A managed policy's ARN by name; None when no such policy exists.
    async fn policy_arn(&self, name: &str) -> Result<Option<String>, StorageError> {
        let mut marker: Option<String> = None;
        loop {
            let mut parameters = vec![("Scope", "Local")];
            if let Some(marker) = &marker {
                parameters.push(("Marker", marker));
            }
            let body = self.call("ListPolicies", &parameters).await?;
            let names = elements(&body, "PolicyName")?;
            let arns = elements(&body, "Arn")?;
            if names.len() != arns.len() {
                return Err(StorageFailure::Protocol.with_source("IAM policy listing is malformed"));
            }
            if let Some(index) = names.iter().position(|found| found == name) {
                return Ok(Some(arns[index].clone()));
            }
            if elements(&body, "IsTruncated")?.first().map(String::as_str) != Some("true") {
                return Ok(None);
            }
            let next = elements(&body, "Marker")?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    StorageFailure::Protocol.with_source("truncated IAM listing without a marker")
                })?;
            if marker.as_ref() == Some(&next) {
                return Err(StorageFailure::Protocol.with_source("IAM listing repeated its marker"));
            }
            marker = Some(next);
        }
    }

    /// Remove the user's keys, its policy and the user, each already absent or not.
    async fn remove(&self, user: &str, policy: Option<&IamPolicy>) -> Result<(), StorageError> {
        self.delete_keys(user).await?;
        match policy {
            Some(IamPolicy::Inline { name }) => {
                self.tolerating(
                    StorageFailure::NotFound,
                    "DeleteUserPolicy",
                    &[("UserName", user), ("PolicyName", name)],
                )
                .await?;
            }
            Some(IamPolicy::Managed { arn }) => {
                self.tolerating(
                    StorageFailure::NotFound,
                    "DetachUserPolicy",
                    &[("UserName", user), ("PolicyArn", arn)],
                )
                .await?;
                self.tolerating(
                    StorageFailure::NotFound,
                    "DeletePolicy",
                    &[("PolicyArn", arn)],
                )
                .await?;
            }
            None => {}
        }
        self.tolerating(
            StorageFailure::NotFound,
            "DeleteUser",
            &[("UserName", user)],
        )
        .await?;
        Ok(())
    }
}

async fn mint(
    context: &AdminRequest,
    scope: &MintScope,
    flavor: Flavor,
) -> Result<S3MemberKey, StorageError> {
    let iam = Iam::new(context, flavor)?;
    let user = scope.name.as_str();
    let store = scope.store.to_string();
    let mut create = vec![("UserName", user)];
    if flavor.inline {
        create.extend([
            ("Path", "/coven/"),
            ("Tags.member.1.Key", "coven-store"),
            ("Tags.member.1.Value", store.as_str()),
            ("Tags.member.2.Key", "coven-member"),
            ("Tags.member.2.Value", scope.member.as_str()),
        ]);
    }
    iam.tolerating(StorageFailure::AlreadyExists, "CreateUser", &create)
        .await?;
    let document = member_policy(&context.config, scope.store)?;
    let policy = match flavor.inline {
        true => {
            iam.call(
                "PutUserPolicy",
                &[
                    ("UserName", user),
                    ("PolicyName", INLINE_POLICY),
                    ("PolicyDocument", &document),
                ],
            )
            .await?;
            IamPolicy::Inline {
                name: INLINE_POLICY.into(),
            }
        }
        false => {
            let created = iam
                .tolerating(
                    StorageFailure::AlreadyExists,
                    "CreatePolicy",
                    &[("PolicyName", user), ("PolicyDocument", &document)],
                )
                .await?;
            let arn = match created {
                Some(body) => elements(&body, "Arn")?.into_iter().next(),
                None => iam.policy_arn(user).await?,
            }
            .ok_or_else(|| StorageFailure::Protocol.with_source("IAM returned no policy ARN"))?;
            iam.call(
                "AttachUserPolicy",
                &[("UserName", user), ("PolicyArn", &arn)],
            )
            .await?;
            IamPolicy::Managed { arn }
        }
    };
    // An earlier attempt's key may exist with a secret this attempt never held.
    iam.delete_keys(user).await?;
    let body = iam.call("CreateAccessKey", &[("UserName", user)]).await?;
    let id = elements(&body, "AccessKeyId")?.into_iter().next();
    let secret = elements(&body, "SecretAccessKey")?.into_iter().next();
    let (Some(access_key_id), Some(secret)) = (id, secret) else {
        return Err(StorageFailure::Protocol.with_source("IAM returned no access key"));
    };
    Ok(S3MemberKey {
        credentials: S3Credentials {
            access_key_id,
            secret_access_key: SecretText::new(secret),
        },
        principal: S3Principal::IamUser {
            user: user.into(),
            policy,
        },
    })
}

async fn roll_back(
    context: &AdminRequest,
    scope: &MintScope,
    flavor: Flavor,
) -> Result<(), StorageError> {
    let iam = Iam::new(context, flavor)?;
    let policy = match flavor.inline {
        true => Some(IamPolicy::Inline {
            name: INLINE_POLICY.into(),
        }),
        false => iam
            .policy_arn(&scope.name)
            .await?
            .map(|arn| IamPolicy::Managed { arn }),
    };
    iam.remove(&scope.name, policy.as_ref()).await
}

fn user(principal: &S3Principal) -> Result<(&str, &IamPolicy), StorageError> {
    match principal {
        S3Principal::IamUser { user, policy } => Ok((user, policy)),
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
    flavor: Flavor,
) -> Result<(), StorageError> {
    let (name, policy) = user(principal)?;
    let iam = Iam::new(context, flavor)?;
    iam.tolerating(
        StorageFailure::NotFound,
        "DeleteAccessKey",
        &[("UserName", name), ("AccessKeyId", access_key_id)],
    )
    .await?;
    // The user exists only to hold this key: remove it and its policy too.
    iam.remove(name, Some(policy)).await
}

async fn key_exists(
    context: &AdminRequest,
    access_key_id: &str,
    principal: &S3Principal,
    flavor: Flavor,
) -> Result<bool, StorageError> {
    let (name, _) = user(principal)?;
    let iam = Iam::new(context, flavor)?;
    Ok(iam
        .keys(name)
        .await?
        .is_some_and(|keys| keys.iter().any(|key| key == access_key_id)))
}

#[cfg(test)]
#[path = "s3_iam_tests.rs"]
mod tests;
