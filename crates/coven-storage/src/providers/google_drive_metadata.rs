//! Exact Drive object lookup and paginated name discovery.
use super::*;

impl GoogleDriveStorage {
    pub(super) async fn copies(&self, path: &ObjectPath) -> Result<Vec<Value>, StorageError> {
        let query = format!(
            "'{}' in parents and name = '{}' and trashed = false",
            escape(&self.folder),
            escape(path.as_str())
        );
        let mut copies = Vec::new();
        let mut items = self.pages(&query).await?;
        let known = self.object_ids.lock().await.get(path).cloned();
        if let Some(id) = known {
            if !items.iter().any(|item| item["id"].as_str() == Some(&id)) {
                if let Some(item) = self.file_metadata(path, &id).await? {
                    items.push(item);
                }
            }
        }
        for item in items {
            let stored = http::timestamp(&item, "createdTime")?;
            let id = http::string(&item, "id")?.to_owned();
            copies.push(((stored, id), item));
        }
        copies.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(copies.into_iter().map(|(_, item)| item).collect())
    }
    pub(super) async fn find(&self, path: &ObjectPath) -> Result<Option<Value>, StorageError> {
        let known = self.object_ids.lock().await.get(path).cloned();
        if let Some(id) = known {
            if let Some(item) = self.file_metadata(path, &id).await? {
                return Ok(Some(item));
            }
            self.object_ids.lock().await.remove(path);
        }
        let found = self.copies(path).await?.into_iter().next();
        match found {
            None => Ok(None),
            Some(item) => {
                let id = http::string(&item, "id")?.to_owned();
                let found = self.file_metadata(path, &id).await?;
                if found.is_some() {
                    self.object_ids.lock().await.insert(path.clone(), id);
                }
                Ok(found)
            }
        }
    }
    pub(super) async fn file_metadata(
        &self,
        path: &ObjectPath,
        id: &str,
    ) -> Result<Option<Value>, StorageError> {
        let response = self.send(Method::GET, &self.url(&["files", id], &[("fields", "id,name,mimeType,size,createdTime,modifiedTime,headRevisionId,md5Checksum,parents,trashed,properties,ownedByMe,driveId")])?, Body::Empty).await?;
        let item = match http::json(PROVIDER, response).await {
            Ok(item) => item,
            Err(error) if error.failure() == StorageFailure::NotFound => {
                self.folder_metadata().await?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if http::string(&item, "id")? != id
            || http::string(&item, "name")? != path.as_str()
            || !http::array(&item, "parents")?
                .iter()
                .any(|parent| parent.as_str() == Some(&self.folder))
        {
            return Err(StorageFailure::Protocol.with_source("Drive returned another object"));
        }
        Ok(Some(item))
    }
    pub(super) async fn pages(&self, query: &str) -> Result<Vec<Value>, StorageError> {
        self.folder_metadata().await?;
        let mut token = None::<String>;
        let mut seen = Pagination::new();
        let mut files = Vec::new();
        loop {
            let mut parameters = vec![("q", query), ("fields", "nextPageToken,incompleteSearch,files(id,name,mimeType,size,createdTime,modifiedTime,headRevisionId,md5Checksum,parents,properties,ownedByMe,driveId)"), ("includeItemsFromAllDrives", "true"), ("pageSize", "1000")];
            if let Some(token) = &token {
                parameters.push(("pageToken", token));
            }
            let value = http::json(
                PROVIDER,
                self.send(
                    Method::GET,
                    &self.url(&["files"], &parameters)?,
                    Body::Empty,
                )
                .await?,
            )
            .await?;
            if let Some(incomplete) = value.get("incompleteSearch") {
                if incomplete.as_bool() != Some(false) {
                    return Err(StorageFailure::Protocol.with_source("Drive search was incomplete"));
                }
            }
            files.extend(http::array(&value, "files")?.iter().cloned());
            match value.get("nextPageToken") {
                None => break,
                Some(value) => {
                    let next = value
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .ok_or(StorageFailure::Protocol.with_source("invalid Drive page token"))?
                        .to_owned();
                    seen.check(&next)?;
                    token = Some(next);
                }
            }
        }
        Ok(files)
    }
}
