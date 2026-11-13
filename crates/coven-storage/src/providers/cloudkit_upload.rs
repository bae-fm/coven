//! Recorded CloudKit asset uploads and their atomic publication.

use super::{
    native, query,
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
    if record.text("path")? != session.path().as_str() || record.number("total")? != session.total {
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
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<UploadSession, StorageError> {
        // No server allocation precedes the caller's durable recording. The first
        // part creates the upload record and its first asset in one transaction.
        Ok(UploadSession {
            device,
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size: PART_SIZE,
            state: SessionState::CloudKit { id: SecretText::new(self.ids.new_id().to_string()) },
        })
    }
    pub(super) async fn resume(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        let name = record::upload_name(session.path(), session.device, token(session)?)?;
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
        sent: &ByteProgress,
    ) -> Result<(), StorageError> {
        let root = record::upload_name(session.path(), session.device, token(session)?)?;
        let mut upload = match self.fetch(&root, SESSION_KEYS).wait().await {
            Ok(record) => record,
            Err(error) if error.failure() == StorageFailure::NotFound && session.confirmed == 0 => {
                let mut record = Record::new("CovenUpload", &self.record_id(&root));
                // Indexed, so another device finds the roots this one left (§17.10).
                record.set_text(
                    query::UPLOAD_DIRECTORY,
                    &coven_format::path::device_uploads(session.path().store(), session.device),
                );
                record.set_text("path", session.path().as_str());
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
                move |record| super::asset_bytes(&files, &record),
                None,
            );
            if request.wait().await? != bytes {
                return Err(StorageFailure::Protocol.with_source("CloudKit retry bytes differ"));
            }
            session.confirmed = confirmed;
            return Ok(());
        }
        let name = record::part_name(&root, confirmed);
        let mut part = Record::new("CovenAsset", &self.record_id(&name));
        // A chain keeps each owning record below CloudKit's 750-reference limit.
        // The chain is only for native deletion; reads address each part directly.
        let previous = if confirmed == 0 {
            root.clone()
        } else {
            record::part_name(&root, confirmed - PART_SIZE as u64)
        };
        part.set_reference("owner", &self.record_id(&previous));
        let asset = self.stage_asset(&mut part, bytes)?;
        upload.set_number("confirmed", end);
        publication.check()?;
        native::modify(
            &self.database,
            vec![upload, part],
            &[],
            CKRecordSavePolicy::IfServerRecordUnchanged,
            None,
            vec![asset],
            // `sent` counts from the part's start; the session record
            // carries no asset.
            Some(native::AssetProgress::bytes(
                name,
                0,
                bytes.len() as u64,
                bytes.len() as u64,
                sent,
            )),
        )
        .wait()
        .await?;
        session.confirmed = end;
        Ok(())
    }
    async fn confirm_publication(&self, session: &UploadSession) -> Result<(), StorageError> {
        let object = self.fetch(session.path().as_str(), OBJECT_KEYS).wait().await?;
        let root = record::upload_name(session.path(), session.device, token(session)?)?;
        if object.text("upload")? != root || object.metadata()?.size != session.total {
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
        let root = record::upload_name(session.path(), session.device, token(session)?)?;
        let mut upload = self.fetch(&root, SESSION_KEYS).wait().await?;
        if check_record(&upload, session)? != session.total {
            return Err(StorageFailure::InvalidPart.into());
        }
        match publication_state(&upload)? {
            Publication::Aborted => return Err(StorageFailure::SessionExpired.into()),
            Publication::Published => self.confirm_publication(session).await?,
            Publication::Pending => {
                let (mut object, obsolete) = (self.object(session.path(), session.total), vec![]);
                object.set_text("upload", &root);
                upload.set_text("state", "published");
                upload.set_reference("owner", &self.record_id(session.path().as_str()));
                publication.check()?;
                native::modify(
                    &self.database,
                    vec![object, upload],
                    &obsolete,
                    CKRecordSavePolicy::IfServerRecordUnchanged,
                    Some(session.path().as_str()),
                    vec![],
                    None,
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
        let root = record::upload_name(session.path(), session.device, token(session)?)?;
        self.end_root(Some(publication), &root, |upload| check_record(upload, session).map(drop))
            .await
    }

    /// The roots in `device`'s reserved folder that aren't published: the
    /// sessions it left. A published root belongs to its object, whose
    /// deletion deletes it.
    pub(super) async fn left_roots(
        &self,
        store: coven_foundation::id_source::StoreId,
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<Vec<crate::LeftUpload>, StorageError> {
        let folder = coven_format::path::device_uploads(store, device);
        let inside = format!("{folder}/");
        let mut left = Vec::new();
        let mut cursor = None;
        loop {
            let request = native::query(
                &self.database,
                &self.zone_id(),
                query::Selection::Uploads(folder.clone()),
                cursor,
                |record| Ok((record.name(), publication_state(&record)? == Publication::Published)),
                #[cfg(test)]
                self.query_pages.clone(),
            );
            let (roots, next) = request.wait().await?;
            for (root, published) in roots {
                if !root.starts_with(&inside) {
                    return Err(StorageFailure::Protocol
                        .with_source("CloudKit query returned a root outside its folder"));
                }
                if !published {
                    left.push(crate::LeftUpload(crate::left_upload::Left::CloudKitRoot(root)));
                }
            }
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok(left),
            }
        }
    }

    /// End the upload at `root`, which `check` accepts: mark it aborted, so a
    /// publication racing this can't save it, then delete it with the parts
    /// it owns. A published root stays with its object, and an absent one
    /// succeeds. A failed deletion leaves it aborted, never publishable, for
    /// a retry.
    pub(super) async fn end_root(
        &self,
        publication: Option<&PublicationRequest>,
        root: &str,
        check: impl Fn(&Record) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        let gate = || publication.map_or(Ok(()), PublicationRequest::check);
        let mut upload = match self.fetch(root, SESSION_KEYS).wait().await {
            Ok(record) => record,
            Err(error) if error.failure() == StorageFailure::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        check(&upload)?;
        match publication_state(&upload)? {
            Publication::Published => return Ok(()),
            Publication::Aborted => {}
            Publication::Pending => {
                // The conditional state transition excludes a concurrent publish.
                // A failed deletion leaves an explicitly aborted, unpublishable
                // session for a retry.
                upload.set_text("state", "aborted");
                gate()?;
                native::modify(
                    &self.database,
                    vec![upload],
                    &[],
                    CKRecordSavePolicy::IfServerRecordUnchanged,
                    None,
                    vec![],
                    None,
                )
                .wait()
                .await?;
            }
        }
        gate()?;
        let request = native::modify(
            &self.database,
            vec![],
            &[self.record_id(root)],
            CKRecordSavePolicy::IfServerRecordUnchanged,
            None,
            vec![],
            None,
        );
        match request.wait().await {
            Ok(_) => Ok(()),
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}
