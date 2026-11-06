//! The policy a member key made in the provider's console needs, which guided
//! console steps show filled in for the store (§20.10).

use crate::{StorageConfig, StorageError, StorageFailure};

/// The policy a member key needs, as console steps show it (Appendix E.12):
/// object access in the store's bucket, listing it, a bucket-versioning read
/// so setup can check the owner's setting, and the multipart-upload listing
/// a session needs to abort an initiation whose reply was lost. The store
/// has the bucket to itself (§20.7), so nothing in it is another's.
pub(crate) fn member_policy(config: &StorageConfig) -> Result<String, StorageError> {
    let StorageConfig::S3 { bucket, .. } = config else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3 configuration"));
    };
    Ok(serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                    "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
                ],
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket", "s3:GetBucketVersioning", "s3:ListBucketMultipartUploads",
                ],
                "Resource": format!("arn:aws:s3:::{bucket}"),
            },
        ],
    })
    .to_string())
}

#[cfg(test)]
#[path = "s3_member_policy_tests.rs"]
mod tests;
