//! Member keys through AWS-style IAM (Appendix E.12): AWS IAM, Ceph RGW accounts and
//! Versity Gateway's standalone IAM with an inline user policy, SeaweedFS's
//! embedded IAM with a managed policy attached to the user, because its
//! PutUserPolicy drops conditions.
//!
//! Each mint call's user name is fresh (§20.10), so minting deletes nothing.
use super::minting::{member_policy, AdminRequest, KeyMinter, Pending};
use super::*;
use crate::providers::s3_admin_query::{elements, send, QueryService};
use crate::{access_admin::named_device, ListedKey, StoreListing, UnkeyedPrincipal};
use coven_format::{IamPolicy, S3Principal};
use coven_foundation::id_source::StoreId;
use std::{collections::BTreeMap, time::Duration};
use url::Url;

#[path = "s3_iam_listing.rs"]
mod listing;

const VERSION: &str = "2010-05-08";
/// The inline policy's name on each user coven makes.
const INLINE_POLICY: &str = "coven-member";

/// How a service's IAM gives a user its policy, and where its API is.
#[derive(Clone, Copy)]
struct Flavor {
    /// PutUserPolicy, users under `/coven/` with store and member tags;
    /// otherwise CreatePolicy and AttachUserPolicy on a plain user.
    inline: bool,
    api: IamApi,
}
/// Where a service's IAM API listens and which region signs for it.
#[derive(Clone, Copy)]
enum IamApi {
    /// AWS IAM's global endpoint, signing for us-east-1.
    Global,
    /// The location's S3 endpoint and region.
    Location,
    /// The admin access's IAM endpoint, signing for the location's region.
    Separate,
}
const AWS: Flavor = Flavor {
    inline: true,
    api: IamApi::Global,
};
const SEAWEEDFS: Flavor = Flavor {
    inline: false,
    api: IamApi::Location,
};
const CEPH_RGW: Flavor = Flavor {
    inline: true,
    api: IamApi::Location,
};
const VERSITY_GW: Flavor = Flavor {
    inline: true,
    api: IamApi::Separate,
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
            fn list_keys<'a>(
                context: &'a AdminRequest,
                store: StoreId,
            ) -> Pending<'a, StoreListing> {
                Box::pin(list(context, store, $flavor))
            }
            fn remove_unkeyed<'a>(
                context: &'a AdminRequest,
                _store: StoreId,
                principal: &'a UnkeyedPrincipal,
            ) -> Pending<'a, ()> {
                Box::pin(remove_empty(context, principal, $flavor))
            }
            KeyMinter {
                name: $label,
                mint: mint_key,
                roll_back: roll_back_key,
                revoke: revoke_key,
                key_exists: key_present,
                list: list_keys,
                remove_unkeyed,
                propagation: $propagation,
                separate_iam: matches!($flavor.api, IamApi::Separate),
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
// Squid+ account IAM shares the RGW endpoint. Sixty seconds is the retry
// window for the member-side check, not a verified propagation guarantee.
minter!(
    CEPH_RGW_IAM,
    "Ceph RGW IAM",
    CEPH_RGW,
    Duration::from_secs(60)
);
// Versity Gateway's standalone IAM service (`versitygw iam`), which the
// gateway reads over its private endpoint. A new key worked at once live;
// sixty seconds is the retry window, not a stated propagation bound.
minter!(
    VERSITY_GW_IAM,
    "Versity Gateway IAM",
    VERSITY_GW,
    Duration::from_secs(60)
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
        let endpoint = match flavor.api {
            IamApi::Global => {
                return Ok(Self {
                    context,
                    endpoint: context.endpoints.aws_iam.clone(),
                    region: "us-east-1".into(),
                })
            }
            IamApi::Location => endpoint.clone().ok_or_else(|| {
                StorageFailure::InvalidConfiguration.with_source("IAM needs the S3 endpoint")
            })?,
            IamApi::Separate => match &context.admin {
                AdminCredential::AccessKey {
                    iam_endpoint: Some(iam_endpoint),
                    ..
                } => iam_endpoint.clone(),
                _ => {
                    return Err(StorageFailure::InvalidConfiguration
                        .with_source("this service's admin access names its IAM endpoint"))
                }
            },
        };
        Ok(Self {
            context,
            endpoint,
            region: region.clone(),
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

    /// Every checked record; an explicitly tolerated missing principal holds no keys.
    async fn pages<T>(
        &self,
        listing: &listing::Listing<T>,
        parameters: &[(&str, &str)],
        absent: Option<StorageFailure>,
    ) -> Result<Vec<T>, StorageError> {
        let mut records = Vec::new();
        let mut marker: Option<String> = None;
        let mut pages = crate::providers::pagination::Pagination::new();
        loop {
            let mut sent = parameters.to_vec();
            if let Some(marker) = &marker {
                sent.push(("Marker", marker));
            }
            let body = match self.call(listing.action, &sent).await {
                Ok(body) => body,
                Err(error) if marker.is_none() && absent == Some(error.failure()) => {
                    return Ok(Vec::new())
                }
                Err(error) => return Err(error),
            };
            let page = listing.page(&body)?;
            records.extend(page.records);
            let Some(next) = page.next else {
                return Ok(records);
            };
            pages.check(&next)?;
            marker = Some(next);
        }
    }

    /// Every key id, or none if the user is absent.
    async fn keys(&self, user: &str) -> Result<Vec<String>, StorageError> {
        self.pages(
            &listing::KEYS,
            &[("UserName", user)],
            Some(StorageFailure::NotFound),
        )
        .await
    }

    async fn delete_keys(&self, user: &str) -> Result<(), StorageError> {
        for key in self.keys(user).await? {
            self.tolerating(
                StorageFailure::NotFound,
                "DeleteAccessKey",
                &[("UserName", user), ("AccessKeyId", &key)],
            )
            .await?;
        }
        Ok(())
    }

    /// Every local managed policy's ARN by name.
    async fn policies(&self) -> Result<BTreeMap<String, String>, StorageError> {
        let mut policies = BTreeMap::new();
        for (name, arn) in self
            .pages(&listing::POLICIES, &[("Scope", "Local")], None)
            .await?
        {
            if policies.insert(name, arn).is_some() {
                return Err(
                    StorageFailure::Protocol.with_source("IAM policy listing repeats a name")
                );
            }
        }
        Ok(policies)
    }

    /// A managed policy's ARN by name; None when no such policy exists.
    async fn policy_arn(&self, name: &str) -> Result<Option<String>, StorageError> {
        Ok(self.policies().await?.remove(name))
    }

    /// The names of the users a coven listing can hold: those under `/coven/`
    /// where users carry a path, every user otherwise.
    async fn users(&self, inline: bool) -> Result<Vec<String>, StorageError> {
        let parameters: &[(&str, &str)] = match inline {
            true => &[("PathPrefix", "/coven/")],
            false => &[],
        };
        self.pages(&listing::USERS, parameters, None).await
    }

    /// Remove the user's keys, its policy and the user, each already absent or not.
    async fn remove(&self, user: &str, policy: Option<&IamPolicy>) -> Result<(), StorageError> {
        self.delete_keys(user).await?;
        self.remove_principal(user, policy).await
    }

    /// Remove only the policy and user; this path never deletes a key.
    async fn remove_principal(
        &self,
        user: &str,
        policy: Option<&IamPolicy>,
    ) -> Result<(), StorageError> {
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
    // The user's name carries the store and the minting device (§14.3).
    let user = scope.key_name.as_str();
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
    let policy = named_policy(&iam, &scope.key_name, flavor).await?;
    iam.remove(&scope.key_name, policy.as_ref()).await
}

async fn named_policy(
    iam: &Iam<'_>,
    user: &str,
    flavor: Flavor,
) -> Result<Option<IamPolicy>, StorageError> {
    Ok(match flavor.inline {
        true => Some(IamPolicy::Inline {
            name: INLINE_POLICY.into(),
        }),
        false => iam
            .policy_arn(user)
            .await?
            .map(|arn| IamPolicy::Managed { arn }),
    })
}

async fn remove_empty(
    context: &AdminRequest,
    principal: &UnkeyedPrincipal,
    flavor: Flavor,
) -> Result<(), StorageError> {
    let iam = Iam::new(context, flavor)?;
    if !iam.keys(&principal.name).await?.is_empty() {
        return Err(StorageFailure::Conflict.with_source("the listed IAM user now holds a key"));
    }
    let policy = named_policy(&iam, &principal.name, flavor).await?;
    iam.remove_principal(&principal.name, policy.as_ref()).await
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
    Ok(iam.keys(name).await?.iter().any(|key| key == access_key_id))
}

/// Every key held by a user whose name coven made for this store, and every
/// such user holding none, with the minting device the name carries (§14.3).
async fn list(
    context: &AdminRequest,
    store: StoreId,
    flavor: Flavor,
) -> Result<StoreListing, StorageError> {
    let iam = Iam::new(context, flavor)?;
    let mut managed = None;
    let mut listed = StoreListing::default();
    for user in iam.users(flavor.inline).await? {
        let Some(device) = named_device(&user, store) else {
            continue;
        };
        // Only a complete key listing establishes that a listed user is keyless.
        let keys = iam
            .pages(&listing::KEYS, &[("UserName", &user)], None)
            .await?;
        if keys.is_empty() {
            listed.unkeyed.push(UnkeyedPrincipal { name: user, device });
            continue;
        }
        let policy = match flavor.inline {
            true => IamPolicy::Inline {
                name: INLINE_POLICY.into(),
            },
            false => {
                if managed.is_none() {
                    managed = Some(iam.policies().await?);
                }
                let arn = managed
                    .as_ref()
                    .and_then(|policies| policies.get(&user))
                    .ok_or_else(|| {
                        StorageFailure::Protocol
                            .with_source("an IAM user coven made holds keys without its policy")
                    })?;
                IamPolicy::Managed { arn: arn.clone() }
            }
        };
        for access_key_id in keys {
            listed.keys.push(ListedKey {
                access_key_id,
                principal: S3Principal::IamUser {
                    user: user.clone(),
                    policy: policy.clone(),
                },
                device,
            });
        }
    }
    Ok(listed)
}

#[cfg(test)]
#[path = "s3_iam_tests.rs"]
mod tests;
