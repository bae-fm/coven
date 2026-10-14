//! Recorded CloudKit asset uploads and their atomic publication.

use super::{
    native,
    record::{self as record, Record, OBJECT_KEYS, PART_SIZE, SESSION_KEYS},
    CloudKitStorage,
};
use crate::{session::SessionState, *};
use coven_crypto::SecretText;
use objc2_cloud_kit::CKRecordSavePolicy;

#[derive(PartialEq)]
enum Publication {
    Pending,
    Published,
    Aborted,
}
fn publication_state(record: &Record) -> Result<Publication, StorageError> {
    match record.text("state")?.as_str() {
        "pending" => Ok(Publication::Pending),
        "published" => Ok(Publication::Published),
        "aborted" => Ok(Publication::Aborted),
        _ => Err(StorageFailure::Protocol.with_source("invalid CloudKit upload state")),
    }
}
fn token(session: &UploadSession) -> Result<&str, StorageError> {
    match &session.state {
        SessionState::CloudKit { id } => Ok(id.as_str()),
        _ => Err(StorageFailure::SessionMismatch.into()),
    }
}
fn check_record(record: &Record, session: &UploadSession) -> Result<u64, StorageError> {
    if record.text("path")? != session.path.as_str() || record.number("total")? != session.total {
        return Err(StorageFailure::SessionMismatch.into());
    }
    let confirmed = record.number("confirmed")?;
    if confirmed < session.confirmed
        || confirmed > session.total
        || (confirmed != session.total && confirmed % PART_SIZE as u64 != 0)
    {
        return Err(StorageFailure::Protocol
            .with_source("CloudKit upload progress disagrees with recording"));
    }
    Ok(confirmed)
}
impl CloudKitStorage {
    pub(super) fn prepare_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        // No server allocation precedes the caller's durable recording. The first
        // part creates the upload record and its first asset in one transaction.
        Ok(UploadSession {
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size: PART_SIZE,
            state: SessionState::CloudKit {
                id: SecretText::new(self.ids.new_id().to_string()),
            },
        })
    }
    pub(super) async fn resume(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        let name = record::upload_name(&session.path, token(session)?)?;
        let record = match self.fetch(&name, SESSION_KEYS).wait().await {
            Ok(record) => record,
            Err(error) if error.failure() == StorageFailure::NotFound && session.confirmed == 0 => {
                return Ok(())
            }
            Err(error) if error.failure() == StorageFailure::NotFound => {
                return Err(StorageFailure::SessionExpired.with_source(error))
            }
            Err(error) => return Err(error),
        };
        let confirmed = check_record(&record, session)?;
        match publication_state(&record)? {
            Publication::Aborted => return Err(StorageFailure::SessionExpired.into()),
            Publication::Pending => session.confirmed = confirmed,
            Publication::Published => {
                if confirmed != session.total {
                    return Err(StorageFailure::Protocol.into());
                }
                self.confirm_publication(session).await?;
                session.confirmed = confirmed;
                session.state = SessionState::Complete;
            }
        }
        Ok(())
    }
    pub(super) async fn save_part(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
    ) -> Result<(), StorageError> {
        let root = record::upload_name(&session.path, token(session)?)?;
        let mut upload = match self.fetch(&root, SESSION_KEYS).wait().await {
            Ok(record) => record,
            Err(error) if error.failure() == StorageFailure::NotFound && session.confirmed == 0 => {
                let mut record = Record::new("CovenUpload", &self.record_id(&root));
                record.set_text("path", session.path.as_str());
                record.set_number("total", session.total);
                record.set_number("confirmed", 0);
                record.set_text("state", "pending");
                record
            }
            Err(error) => return Err(error),
        };
        let confirmed = check_record(&upload, session)?;
        if publication_state(&upload)? != Publication::Pending {
            return Err(StorageFailure::InvalidPart.into());
        }
        if confirmed != session.confirmed {
            // A previous reply was lost. Prove these retry bytes match the exact
            // stored part before advancing the caller's durable recording.
            if confirmed != end {
                return Err(StorageFailure::InvalidPart.into());
            }
            let files = self.files.clone();
            let request = native::fetch(
                &self.database,
                &self.record_id(&record::part_name(&root, session.confirmed)),
                &["data"],
                move |record| record.asset_bytes(&files),
            );
            if request.wait().await? != bytes {
                return Err(StorageFailure::Protocol.with_source("CloudKit retry bytes differ"));
            }
            session.confirmed = confirmed;
            return Ok(());
        }
        let mut part = Record::new(
            "CovenAsset",
            &self.record_id(&record::part_name(&root, confirmed)),
        );
        // A chain keeps each owning record below CloudKit's 750-reference limit.
        // The chain is only for native deletion; reads address each part directly.
        let previous = if confirmed == 0 {
            root.clone()
        } else {
            record::part_name(&root, confirmed - PART_SIZE as u64)
        };
        part.set_reference("owner", &self.record_id(&previous));
        let asset = part.set_asset(&self.files, bytes)?;
        upload.set_number("confirmed", end);
        publication.check()?;
        native::modify(
            &self.database,
            vec![upload, part],
            &[],
            CKRecordSavePolicy::IfServerRecordUnchanged,
            None,
            vec![asset],
        )
        .wait()
        .await?;
        session.confirmed = end;
        Ok(())
    }
    async fn confirm_publication(&self, session: &UploadSession) -> Result<(), StorageError> {
        let object = self
            .fetch(session.path.as_str(), OBJECT_KEYS)
            .wait()
            .await?;
        if object.text("upload")? != token(session)? || object.metadata()?.size != session.total {
            return Err(StorageFailure::AlreadyExists.into());
        }
        Ok(())
    }
    pub(super) async fn publish(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        if session.confirmed != session.total {
            return Err(StorageFailure::InvalidPart.into());
        }
        let root = record::upload_name(&session.path, token(session)?)?;
        let mut upload = self.fetch(&root, SESSION_KEYS).wait().await?;
        if check_record(&upload, session)? != session.total {
            return Err(StorageFailure::InvalidPart.into());
        }
        match publication_state(&upload)? {
            Publication::Aborted => return Err(StorageFailure::SessionExpired.into()),
            Publication::Published => self.confirm_publication(session).await?,
            Publication::Pending => {
                let mut object = self.object(&session.path, session.total);
                object.set_text("upload", token(session)?);
                upload.set_text("state", "published");
                upload.set_reference("owner", &self.record_id(session.path.as_str()));
                publication.check()?;
                native::modify(
                    &self.database,
                    vec![object, upload],
                    &[],
                    CKRecordSavePolicy::IfServerRecordUnchanged,
                    Some(session.path.as_str()),
                    vec![],
                )
                .wait()
                .await?;
            }
        }
        session.state = SessionState::Complete;
        Ok(())
    }
    pub(super) async fn abort(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        let root = record::upload_name(&session.path, token(session)?)?;
        let mut upload = match self.fetch(&root, SESSION_KEYS).wait().await {
            Ok(record) => record,
            Err(error) if error.failure() == StorageFailure::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        check_record(&upload, session)?;
        match publication_state(&upload)? {
            Publication::Published => return Ok(()),
            Publication::Aborted => {}
            Publication::Pending => {
                // The conditional state transition excludes a concurrent publish.
                // A cleanup failure leaves an explicitly aborted, unpublishable
                // session for this initiating call to retry.
                upload.set_text("state", "aborted");
                publication.check()?;
                native::modify(
                    &self.database,
                    vec![upload],
                    &[],
                    CKRecordSavePolicy::IfServerRecordUnchanged,
                    None,
                    vec![],
                )
                .wait()
                .await?;
            }
        }
        publication.check()?;
        let request = native::modify(
            &self.database,
            vec![],
            &[self.record_id(&root)],
            CKRecordSavePolicy::IfServerRecordUnchanged,
            None,
            vec![],
        );
        match request.wait().await {
            Ok(_) => Ok(()),
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}
