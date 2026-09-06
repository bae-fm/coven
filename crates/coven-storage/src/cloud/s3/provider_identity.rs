use super::*;

pub(super) fn s3_access_key_id_hash(
    access_key_id: &str,
) -> coven_protocol::store_commit::ObjectHash {
    const DOMAIN: &[u8] = b"coven.s3-access-key-id.v1\0";
    let mut material = Vec::with_capacity(DOMAIN.len() + access_key_id.len());
    material.extend_from_slice(DOMAIN);
    material.extend_from_slice(access_key_id.as_bytes());
    coven_protocol::store_commit::ObjectHash::digest(&material)
}

pub(super) fn aws_caller_identity(
    account_id: &str,
    arn: &str,
    user_id: &str,
) -> Result<(String, coven_protocol::objects::AwsPrincipal), CloudHomeError> {
    use coven_protocol::objects::AwsPrincipal;

    if account_id.len() != 12 || !account_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(CloudHomeError::Configuration(
            "STS GetCallerIdentity returned a malformed AWS account id".to_string(),
        ));
    }
    let fields: Vec<_> = arn.splitn(6, ':').collect();
    if fields.len() != 6
        || fields[0] != "arn"
        || fields[1].is_empty()
        || !fields[3].is_empty()
        || fields[4] != account_id
    {
        return Err(CloudHomeError::Configuration(
            "STS GetCallerIdentity returned an unrecognized caller ARN".to_string(),
        ));
    }
    let principal = match (fields[2], fields[5]) {
        ("iam", "root") if user_id == account_id => AwsPrincipal::Root,
        ("iam", resource) if resource.starts_with("user/") && !user_id.is_empty() => {
            AwsPrincipal::User {
                arn: arn.to_string(),
                user_id: user_id.to_string(),
            }
        }
        ("sts", resource) if resource.starts_with("assumed-role/") => {
            let (role_id, session) = user_id.split_once(':').ok_or_else(|| {
                CloudHomeError::Configuration(
                    "STS assumed-role caller has no stable role-id prefix".to_string(),
                )
            })?;
            if role_id.is_empty() || session.is_empty() {
                return Err(CloudHomeError::Configuration(
                    "STS assumed-role caller has a malformed user id".to_string(),
                ));
            }
            AwsPrincipal::Role {
                role_id: role_id.to_string(),
            }
        }
        _ => {
            return Err(CloudHomeError::Configuration(
                "STS caller must be the account root, an IAM user, or an assumed role".to_string(),
            ));
        }
    };
    Ok((fields[1].to_string(), principal))
}

pub(super) fn sts_request_error(
    error: aws_sdk_sts::error::SdkError<
        aws_sdk_sts::operation::get_caller_identity::GetCallerIdentityError,
    >,
) -> CloudHomeError {
    s3_operation_error("request STS caller identity", error)
}
