use super::{BlobBody, CloudHomeError};
use coven_protocol::objects::{ExactObjectRef, StorageError};
use std::path::Path;

pub(crate) fn accept_unchecked_create_response(
    created_response_was_observed: bool,
    object: &ExactObjectRef,
) -> Result<(), CloudHomeError> {
    if created_response_was_observed {
        Ok(())
    } else {
        Err(CloudHomeError::AlreadyExists(
            object.slot().logical_key().to_string(),
        ))
    }
}

pub(crate) async fn settle_exact_create<Verify, Verification>(
    operation: Result<(), CloudHomeError>,
    verify: Verify,
) -> Result<super::ExactCreateOutcome, CloudHomeError>
where
    Verify: FnOnce(bool) -> Verification,
    Verification: std::future::Future<Output = Result<(), CloudHomeError>>,
{
    match operation {
        Ok(()) => {
            verify(true).await?;
            Ok(super::ExactCreateOutcome::Created)
        }
        Err(CloudHomeError::AlreadyExists(_)) => {
            verify(false).await?;
            Ok(super::ExactCreateOutcome::AlreadyPresent)
        }
        Err(operation) => match verify(false).await {
            Ok(()) => Ok(super::ExactCreateOutcome::AlreadyPresent),
            Err(CloudHomeError::NotFound(_)) => Err(operation),
            Err(collision @ CloudHomeError::SlotCollision(_)) => Err(collision),
            Err(settlement) => Err(CloudHomeError::UnresolvedOutcome {
                operation: Box::new(operation),
                settlement: Box::new(settlement),
            }),
        },
    }
}

/// One immutable exact object together with a replayable source for its final
/// stored bytes. Provider adapters derive their own native checksums from this
/// source and can reopen it after an ambiguous create response.
#[derive(Clone, Copy)]
pub struct ExactUpload<'source> {
    object: &'source ExactObjectRef,
    source: ExactUploadSource<'source>,
}

#[derive(Clone, Copy)]
pub enum ExactUploadSource<'source> {
    Bytes(&'source [u8]),
    File(&'source Path),
}

impl<'source> ExactUpload<'source> {
    pub fn from_bytes(
        object: &'source ExactObjectRef,
        bytes: &'source [u8],
    ) -> Result<Self, StorageError> {
        object.verify(bytes)?;
        Ok(Self {
            object,
            source: ExactUploadSource::Bytes(bytes),
        })
    }

    pub async fn from_file(
        object: &'source ExactObjectRef,
        path: &'source Path,
    ) -> Result<Self, StorageError> {
        let (size, digest) = coven_foundation::local_file::file_facts(path)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        object.verify_stored_facts(
            path,
            size,
            coven_protocol::store_commit::ObjectHash::from_digest(digest),
        )?;
        Ok(Self {
            object,
            source: ExactUploadSource::File(path),
        })
    }

    pub fn object(&self) -> &ExactObjectRef {
        self.object
    }

    pub fn source(&self) -> ExactUploadSource<'source> {
        self.source
    }

    pub fn verify_stored_bytes(&self, bytes: &[u8]) -> Result<(), CloudHomeError> {
        self.object.verify(bytes).map_err(|_| {
            CloudHomeError::SlotCollision(self.object.slot().logical_key().to_string())
        })
    }

    pub async fn body(&self) -> Result<BlobBody, CloudHomeError> {
        match self.source {
            ExactUploadSource::Bytes(bytes) => Ok(BlobBody::from_bytes(bytes.to_vec())),
            ExactUploadSource::File(path) => BlobBody::from_file(path)
                .await
                .map_err(CloudHomeError::Transport),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coven_protocol::objects::ObjectSlot;
    use coven_protocol::store_commit::ObjectHash;

    fn object() -> ExactObjectRef {
        ExactObjectRef::new(
            ObjectSlot::logical("unchecked/object".to_string()).expect("logical slot"),
            5,
            ObjectHash::digest(b"bytes"),
        )
    }

    #[test]
    fn unchecked_accepts_only_a_witnessed_successful_create() {
        let object = object();

        assert!(accept_unchecked_create_response(true, &object).is_ok());
        assert!(matches!(
            accept_unchecked_create_response(false, &object),
            Err(CloudHomeError::AlreadyExists(key)) if key == "unchecked/object"
        ));
    }

    #[tokio::test]
    async fn unchecked_does_not_turn_occupied_or_ambiguous_results_into_success() {
        let object = object();
        let occupied = settle_exact_create(
            Err(CloudHomeError::AlreadyExists(
                "unchecked/object".to_string(),
            )),
            |_| async { accept_unchecked_create_response(false, &object) },
        )
        .await;
        assert!(matches!(
            occupied,
            Err(CloudHomeError::AlreadyExists(key)) if key == "unchecked/object"
        ));

        let ambiguous = settle_exact_create(
            Err(CloudHomeError::Transport("lost response".to_string())),
            |_| async { accept_unchecked_create_response(false, &object) },
        )
        .await;
        assert!(matches!(
            ambiguous,
            Err(CloudHomeError::UnresolvedOutcome { operation, settlement })
                if matches!(*operation, CloudHomeError::Transport(_))
                    && matches!(*settlement, CloudHomeError::AlreadyExists(_))
        ));
    }
}
