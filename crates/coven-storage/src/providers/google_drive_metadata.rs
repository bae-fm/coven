//! Exact Drive object lookup and paginated name discovery.
use super::*;
use coven_format::value::StorageTime;
use std::collections::BTreeSet;
const FOLDER: &str = "application/vnd.google-apps.folder";

// A directory can have several native folders after concurrent creation.
// Ordering is also the deterministic destination for new children.
pub(super) type FolderIds = BTreeSet<(StorageTime, String)>;

impl GoogleDriveStorage {
    pub(super) async fn writer_folders(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<Vec<DeviceId>, StorageError> {
        let directory = format!("{store}/store-log");
        let parents = self.directory(&directory, None).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let query = format!(
            "{} and trashed = false and mimeType = '{FOLDER}'",
            parent_query(&parents)
        );
        let mut writers = BTreeSet::new();
        let mut folders = BTreeMap::<String, FolderIds>::new();
        for item in self.pages(&query).await? {
            let name = http::string(&item, "name")?;
            check_child(&item, &parents, name)?;
            if item["mimeType"] != FOLDER {
                return Err(
                    StorageFailure::Protocol.with_source("Drive writer query returned a file")
                );
            }
            let path = ObjectPath::parse(&format!("{directory}/{name}/1"))?;
            writers.insert(path.device().expect("store-log writer"));
            folders
                .entry(format!("{directory}/{name}"))
                .or_default()
                .insert((
                    http::timestamp(&item, "createdTime")?,
                    http::string(&item, "id")?.into(),
                ));
        }
        let mut known = self.folder_ids.lock().await;
        for (path, ids) in folders {
            known.entry(path).or_default().extend(ids);
        }
        Ok(writers.into_iter().collect())
    }

    async fn directory(
        &self,
        path: &str,
        publication: Option<&PublicationRequest>,
    ) -> Result<Vec<String>, StorageError> {
        coven_format::path::validate_directory(path)?;
        if self.session.access().await == OAuthAccess::GoogleDrive(GoogleDriveAccess::File) {
            // Sharing can change on another device. A narrow grant must not
            // mistake its partial view of a shared folder for a complete listing.
            self.folder_metadata().await?;
        } else {
            self.root_checked
                .get_or_try_init(|| async {
                    self.folder_metadata().await?;
                    Ok::<_, StorageError>(())
                })
                .await?;
        }
        // Serialize folder creation, including callers publishing different paths.
        // Refresh every ancestor: a cached folder is not proof that no sibling
        // appeared after it was discovered.
        let mut known = self.folder_ids.lock().await;
        let mut discovered = BTreeMap::new();
        let mut parents = vec![self.folder.clone()];
        let mut directory = String::new();
        for name in path.split('/') {
            if !directory.is_empty() {
                directory.push('/');
            }
            directory.push_str(name);
            let mut folders = folder_ids(&self.named(&parents, name).await?)?;
            if let Some(cached) = known.get(&directory) {
                // Search omissions do not erase a known folder or its children.
                folders.extend(cached.iter().cloned());
            }
            if folders.is_empty() {
                let Some(publication) = publication else {
                    return Ok(Vec::new());
                };
                let parent = parents.first().expect("root or discovered parent folders");
                let item = http::json(
                    PROVIDER,
                    self.publish(
                        publication,
                        Method::POST,
                        &self.url(
                            &["files"],
                            &[("fields", "id,name,mimeType,parents,createdTime")],
                        )?,
                        Body::Json(json!({"name":name,"parents":[parent],"mimeType":FOLDER})),
                    )
                    .await?,
                )
                .await?;
                check_child(&item, std::slice::from_ref(parent), name)?;
                folders.extend(folder_ids(&[item])?);
                // Another device may have created a sibling during our POST.
                // Retain our reply too, even if search has not exposed it yet.
                folders.extend(folder_ids(&self.named(&parents, name).await?)?);
            }
            parents = folders.iter().map(|(_, id)| id.clone()).collect();
            discovered.insert(directory.clone(), folders);
        }
        known.extend(discovered);
        Ok(parents)
    }
    pub(super) async fn parent(&self, path: &ObjectPath) -> Result<Vec<String>, StorageError> {
        let directory = parent_path(path);
        if let Some(folders) = self.folder_ids.lock().await.get(directory) {
            return Ok(folders.iter().map(|(_, id)| id.clone()).collect());
        }
        self.directory(directory, None).await
    }
    pub(super) async fn create_parent(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
    ) -> Result<String, StorageError> {
        self.directory(parent_path(path), Some(publication))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| StorageFailure::Protocol.with_source("Drive did not create the parent"))
    }
    async fn named(&self, parents: &[String], name: &str) -> Result<Vec<Value>, StorageError> {
        let query = format!(
            "{} and name = '{}' and trashed = false",
            parent_query(parents),
            escape(name)
        );
        let items = self.pages(&query).await?;
        for item in &items {
            check_child(item, parents, name)?;
        }
        Ok(items)
    }
    pub(super) async fn copies(
        &self,
        path: &ObjectPath,
        uploaded_id: Option<&str>,
    ) -> Result<Vec<Value>, StorageError> {
        let parents = self.parent(path).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let mut copies = Vec::new();
        let mut items = self.named(&parents, path.file_name()).await?;
        let known = self.object_ids.lock().await.get(path).cloned();
        // Upload replies name a candidate, not the selected copy. Keep that ID
        // local until duplicate handling succeeds, even if search omits it.
        for id in known.as_deref().into_iter().chain(uploaded_id) {
            if !items.iter().any(|item| item["id"].as_str() == Some(id)) {
                if let Some(item) = self.file_metadata(path, id).await? {
                    items.push(item);
                }
            }
        }
        for item in items {
            object_metadata::drive(path, &item)?;
            let stored = http::timestamp(&item, "createdTime")?;
            let id = http::string(&item, "id")?.to_owned();
            copies.push(((stored, id), item));
        }
        copies.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(copies.into_iter().map(|(_, item)| item).collect())
    }
    pub(super) async fn cached(&self, path: &ObjectPath) -> Result<Option<Value>, StorageError> {
        let known = self.object_ids.lock().await.get(path).cloned();
        if let Some(id) = known {
            if let Some(item) = self.file_metadata(path, &id).await? {
                return Ok(Some(item));
            }
            self.object_ids.lock().await.remove(path);
        }
        Ok(None)
    }
    pub(super) async fn find(&self, path: &ObjectPath) -> Result<Option<Value>, StorageError> {
        if let Some(item) = self.cached(path).await? {
            return Ok(Some(item));
        }
        let found = self.copies(path, None).await?.into_iter().next();
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
        let parents = self.parent(path).await?;
        check_child(&item, &parents, path.file_name())?;
        if http::string(&item, "id")? != id {
            return Err(StorageFailure::Protocol.with_source("Drive returned another object"));
        }
        Ok(Some(item))
    }
    pub(super) async fn pages(&self, query: &str) -> Result<Vec<Value>, StorageError> {
        let mut token = None::<String>;
        let mut seen = Pagination::new();
        let mut files = BTreeMap::new();
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
            for item in http::array(&value, "files")? {
                let id = http::string(item, "id")?.to_owned();
                if files
                    .insert(id, item.clone())
                    .is_some_and(|previous| previous != *item)
                {
                    return Err(
                        StorageFailure::Protocol.with_source("conflicting Drive search entries")
                    );
                }
            }
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
        Ok(files.into_values().collect())
    }

    pub(super) async fn list_folder(
        &self,
        listing: &mut ObjectListing,
    ) -> Result<(), StorageError> {
        let directory = listing.prefix().as_str().trim_end_matches('/');
        let parents = self.directory(directory, None).await?;
        if parents.is_empty() {
            return Ok(());
        }
        let mut pending = BTreeMap::from([(directory.to_owned(), parents)]);
        let mut folders = BTreeMap::<String, FolderIds>::new();
        let mut objects = BTreeMap::new();
        while let Some((directory, parents)) = pending.pop_first() {
            let query = format!("{} and trashed = false", parent_query(&parents));
            for item in self.pages(&query).await? {
                let name = http::string(&item, "name")?;
                check_child(&item, &parents, name)?;
                let path = format!("{directory}/{name}");
                let id = http::string(&item, "id")?.to_owned();
                if item["mimeType"] == FOLDER {
                    coven_format::path::validate_directory(&path)?;
                    let created = http::timestamp(&item, "createdTime")?;
                    if folders
                        .entry(path.clone())
                        .or_default()
                        .insert((created, id.clone()))
                    {
                        pending.entry(path).or_default().push(id);
                    }
                } else {
                    if item["mimeType"]
                        .as_str()
                        .is_some_and(|kind| kind.starts_with("application/vnd.google-apps."))
                    {
                        return Err(StorageFailure::InvalidPath.into());
                    }
                    let path = ObjectPath::parse(&path)?;
                    let object = object_metadata::drive(&path, &item)?;
                    let created = http::timestamp(&item, "createdTime")?;
                    let keep = objects.entry(path).or_insert((object.clone(), created));
                    if (created, &object.object_id) < (keep.1, &keep.0.object_id) {
                        *keep = (object, created);
                    }
                }
            }
        }
        let known_objects: BTreeMap<_, _> = {
            let known = self.object_ids.lock().await;
            objects
                .keys()
                .filter_map(|path| known.get(path).map(|id| (path.clone(), id.clone())))
                .collect()
        };
        for (path, (object, created)) in &mut objects {
            if let Some(id) = known_objects
                .get(path)
                .filter(|id| **id != object.object_id)
            {
                // A listing can omit the already known copy. Only exact metadata
                // can establish that it disappeared before selecting another one.
                if let Some(item) = self.file_metadata(path, id).await? {
                    let stored = http::timestamp(&item, "createdTime")?;
                    if (stored, id) < (*created, &object.object_id) {
                        *object = object_metadata::drive(path, &item)?;
                        *created = stored;
                    }
                }
            }
        }
        // Publish discovered IDs only after every page and descendant succeeds.
        let mut known_folders = self.folder_ids.lock().await;
        for (object, _) in objects.values() {
            listing.insert(object.clone())?;
        }
        for (path, ids) in folders {
            known_folders.entry(path).or_default().extend(ids);
        }
        let mut known = self.object_ids.lock().await;
        known.extend(
            objects
                .into_iter()
                .map(|(path, (object, _))| (path, object.object_id)),
        );
        Ok(())
    }
}

fn parent_path(path: &ObjectPath) -> &str {
    path.as_str()
        .rsplit_once('/')
        .expect("validated object path")
        .0
}

pub(super) fn check_child(
    item: &Value,
    parents: &[String],
    name: &str,
) -> Result<(), StorageError> {
    if name.contains('/')
        || http::string(item, "name")? != name
        || item["trashed"] == true
        || !http::array(item, "parents")?.iter().any(|value| {
            parents
                .iter()
                .any(|parent| value.as_str() == Some(parent.as_str()))
        })
    {
        return Err(StorageFailure::Protocol.with_source("Drive returned another object"));
    }
    http::string(item, "id")?;
    Ok(())
}

fn folder_ids(items: &[Value]) -> Result<FolderIds, StorageError> {
    let mut found = FolderIds::new();
    for item in items {
        if item["mimeType"] != FOLDER {
            return Err(StorageFailure::InvalidPath.into());
        }
        found.insert((
            http::timestamp(item, "createdTime")?,
            http::string(item, "id")?.to_owned(),
        ));
    }
    Ok(found)
}

fn parent_query(parents: &[String]) -> String {
    let query = parents
        .iter()
        .map(|parent| format!("'{}' in parents", escape(parent)))
        .collect::<Vec<_>>()
        .join(" or ");
    if parents.len() == 1 {
        query
    } else {
        format!("({query})")
    }
}
