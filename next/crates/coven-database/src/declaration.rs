//! The application's declarations, independent of any SQLite connection.

/// How independently inserted rows choose their identity (§8.5).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowIdentity {
    /// A canonical lowercase UUID, version 4 or 7, in a primary-key component.
    IndependentUuid,
    /// Equal derived keys identify one row on every device.
    SharedKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AudienceSource {
    Store,
    Column(String),
    ForeignKey(String),
    Both { column: String, foreign_key: String },
}

/// A synced table's key, audience, files and shared triggers (§20.2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncedTable {
    pub(crate) name: String,
    pub(crate) identity: RowIdentity,
    pub(crate) key: Vec<String>,
    pub(crate) audience: AudienceSource,
    pub(crate) files: Option<FileDecl>,
    pub(crate) shared_triggers: Vec<String>,
}

impl SyncedTable {
    /// Declare a synced table with the one-column primary key `id`.
    pub fn new(name: impl Into<String>, identity: RowIdentity) -> Self {
        Self {
            name: name.into(),
            identity,
            key: vec!["id".into()],
            audience: AudienceSource::Store,
            files: None,
            shared_triggers: Vec::new(),
        }
    }

    /// The columns in PRIMARY KEY order.
    pub fn key_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.key = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Set the root's non-null text audience column. Declaring inheritance too is an error.
    pub fn audience_column(mut self, column: impl Into<String>) -> Self {
        let column = column.into();
        self.audience = match self.audience {
            AudienceSource::ForeignKey(foreign_key) | AudienceSource::Both { foreign_key, .. } => {
                AudienceSource::Both {
                    column,
                    foreign_key,
                }
            }
            AudienceSource::Store | AudienceSource::Column(_) => AudienceSource::Column(column),
        };
        self
    }

    /// Inherit the audience through this foreign key. Declaring a root column too is an error.
    pub fn audience_from(mut self, foreign_key: impl Into<String>) -> Self {
        let foreign_key = foreign_key.into();
        self.audience = match self.audience {
            AudienceSource::Column(column) | AudienceSource::Both { column, .. } => {
                AudienceSource::Both {
                    column,
                    foreign_key,
                }
            }
            AudienceSource::Store | AudienceSource::ForeignKey(_) => {
                AudienceSource::ForeignKey(foreign_key)
            }
        };
        self
    }

    /// Declare the file carried by each row. Opening and migrating check all
    /// four column names against this synced table's actual schema.
    pub fn carries_files(mut self, declaration: FileDecl) -> Self {
        self.files = Some(declaration);
        self
    }

    /// Declare a trigger as shared; all other triggers are local.
    pub fn shared_trigger(mut self, name: impl Into<String>) -> Self {
        self.shared_triggers.push(name.into());
        self
    }
}

/// Whether the file belongs to the user or was handed to coven by the app.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Provenance {
    /// The user's original, which coven never modifies or deletes.
    UserProvided,
    /// Bytes that coven owns.
    AppProvided,
}

/// When devices fill the cache with an uploaded file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CacheFill {
    /// When the row arrives.
    CacheEager,
    /// On first read.
    CacheLazy,
}

/// When a file becomes eligible for upload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Uploads {
    /// As soon as a write attaches it.
    WhenAttached,
    /// Only when the app requests upload.
    WhenAsked,
}

/// A table's file columns and declared file choices (§16, §20.2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileDecl {
    pub(crate) namespace: String,
    pub(crate) provenance: Provenance,
    pub(crate) uploads: Uploads,
    pub(crate) fill: CacheFill,
    pub(crate) id: String,
    pub(crate) size: String,
    pub(crate) hash: String,
    pub(crate) location: String,
    pub(crate) write_once: bool,
}

impl FileDecl {
    /// Declare a file namespace, origin, upload policy and cache policy.
    pub fn new(
        namespace: impl Into<String>,
        provenance: Provenance,
        uploads: Uploads,
        fill: CacheFill,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            provenance,
            uploads,
            fill,
            id: "id".into(),
            size: "size".into(),
            hash: "hash".into(),
            location: "location".into(),
            write_once: false,
        }
    }

    /// The file's identity column; defaults to `id`.
    pub fn with_id_column(mut self, column: impl Into<String>) -> Self {
        self.id = column.into();
        self
    }

    /// The file's byte-count column; defaults to `size`.
    pub fn with_size_column(mut self, column: impl Into<String>) -> Self {
        self.size = column.into();
        self
    }

    /// The content-hash column, written by coven; defaults to `hash`.
    pub fn with_hash_column(mut self, column: impl Into<String>) -> Self {
        self.hash = column.into();
        self
    }

    /// The location column, written by coven; defaults to `location`.
    pub fn with_location_column(mut self, column: impl Into<String>) -> Self {
        self.location = column.into();
        self
    }

    /// Declare that an existing file cannot be replaced.
    pub fn write_once(mut self) -> Self {
        self.write_once = true;
        self
    }

    pub(crate) fn columns(&self) -> [&str; 4] {
        [&self.id, &self.size, &self.hash, &self.location]
    }
}

#[cfg(test)]
#[path = "declaration_tests.rs"]
mod tests;
