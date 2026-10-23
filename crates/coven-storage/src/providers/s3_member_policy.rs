//! The policy a member key made in the provider's console needs, which guided
//! console steps show filled in for the store (§20.10).

use crate::{ObjectPrefix, StorageConfig, StorageError, StorageFailure};
use coven_foundation::id_source::StoreId;

/// The policy a prefix-scoped member key needs, as console steps show it
/// (Appendix E.12): object access under the store's prefix, listing only there, a
/// bucket-versioning read so setup can check the owner's setting, and the
/// multipart-upload listing a session needs to abort an initiation whose
/// reply was lost. AWS takes no prefix condition on that listing.
/// Never a `Deny` on `s3:prefix`: a request without one, such as a bucket
/// check, would match the negated condition and be denied.
pub(crate) fn member_policy(
    config: &StorageConfig,
    store: StoreId,
) -> Result<String, StorageError> {
    let StorageConfig::S3 { bucket, prefix, .. } = config else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3 configuration"));
    };
    let scope = prefix.key_prefix_of(&ObjectPrefix::all(store));
    Ok(serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                    "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
                ],
                "Resource": format!("arn:aws:s3:::{bucket}/{scope}*"),
            },
            {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": format!("arn:aws:s3:::{bucket}"),
                "Condition": {"StringLike": {"s3:prefix": format!("{scope}*")}},
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetBucketVersioning", "s3:ListBucketMultipartUploads"],
                "Resource": format!("arn:aws:s3:::{bucket}"),
            },
        ],
    })
    .to_string())
}

#[cfg(test)]
#[path = "s3_member_policy_tests.rs"]
mod tests;
