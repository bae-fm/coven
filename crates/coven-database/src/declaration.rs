//! The application's declarations, independent of any SQLite connection.

/// How independently inserted rows choose their identity (§9.2).
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

/// A synced table's key, audience, files, shared triggers and whether only
/// each row's original creator writes it directly (E2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncedTable {
    pub(crate) name: String,
    pub(crate) identity: RowIdentity,
    pub(crate) key: Vec<String>,
    pub(crate) audience: AudienceSource,
    pub(crate) files: Option<FileDecl>,
    pub(crate) shared_triggers: Vec<String>,
    pub(crate) owner_only: bool,
}

impl SyncedTable {
    /// The table name exactly as supplied to the declaration.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The declared kind of row identity.
    pub fn identity(&self) -> &RowIdentity {
        &self.identity
    }

    /// The declared primary-key columns, in order; defaults to `id`.
    pub fn get_key_columns(&self) -> &[String] {
        &self.key
    }

    /// Whether direct writes are restricted to each row's original creator.
    pub fn is_owner_only(&self) -> bool {
        self.owner_only
    }

    /// The root audience column, including when inheritance was also declared.
    /// Opening refuses a declaration with both audience sources.
    pub fn get_audience_column(&self) -> Option<&str> {
        match &self.audience {
            AudienceSource::Column(column) | AudienceSource::Both { column, .. } => Some(column),
            AudienceSource::Store | AudienceSource::ForeignKey(_) => None,
        }
    }

    /// The audience foreign key, including when a root column was also declared.
    /// Opening refuses a declaration with both audience sources.
    pub fn get_audience_from(&self) -> Option<&str> {
        match &self.audience {
            AudienceSource::ForeignKey(foreign_key) | AudienceSource::Both { foreign_key, .. } => {
                Some(foreign_key)
            }
            AudienceSource::Store | AudienceSource::Column(_) => None,
        }
    }

    /// The file declaration supplied to `carries_files`, if any.
    pub fn files(&self) -> Option<&FileDecl> {
        self.files.as_ref()
    }

    /// The shared trigger names, in declaration order, including repetitions.
    pub fn shared_triggers(&self) -> &[String] {
        &self.shared_triggers
    }

    /// Declare a synced table with the one-column primary key `id`.
    pub fn new(name: impl Into<String>, identity: RowIdentity) -> Self {
        Self {
            name: name.into(),
            identity,
            key: vec!["id".into()],
            audience: AudienceSource::Store,
            files: None,
            shared_triggers: Vec::new(),
            owner_only: false,
        }
    }

    /// Only each row's original creating member may directly update or
    /// delete it (§9.6). Shared-trigger targets count as direct writes;
    /// foreign-key actions from authorized changes may still reach the row.
    /// No app owner column is needed. Changing this declaration for an
    /// existing table is a breaking schema change (§18.1).
    pub fn owner_only(mut self) -> Self {
        self.owner_only = true;
        self
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

/// A table's file columns and declared file choices (§17, E2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileDecl {
    pub(crate) namespace: String,
    pub(crate) cache_budget: u64,
    pub(crate) provenance: Provenance,
    pub(crate) fill: CacheFill,
    pub(crate) id: String,
    pub(crate) size: String,
    pub(crate) hash: String,
    pub(crate) location: String,
    pub(crate) write_once: bool,
}

impl FileDecl {
    /// The cache namespace exactly as declared.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The initial cache budget in bytes, as declared, not its current setting.
    pub fn cache_budget(&self) -> u64 {
        self.cache_budget
    }

    /// Whether the declared file belongs to the user or is supplied by the app.
    pub fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// The declared cache-fill policy.
    pub fn fill(&self) -> &CacheFill {
        &self.fill
    }

    /// The declared file identity column; defaults to `id`.
    pub fn id_column(&self) -> &str {
        &self.id
    }

    /// The declared byte-count column; defaults to `size`.
    pub fn size_column(&self) -> &str {
        &self.size
    }

    /// The declared content-hash column; defaults to `hash`.
    pub fn hash_column(&self) -> &str {
        &self.hash
    }

    /// The declared location column; defaults to `location`.
    pub fn location_column(&self) -> &str {
        &self.location
    }

    /// Whether the declaration forbids replacing an existing file.
    pub fn is_write_once(&self) -> bool {
        self.write_once
    }

    /// Declare a file namespace, its cache budget in bytes, origin and cache
    /// policy. The budget applies when the namespace is first declared;
    /// `set_cache_budget` changes it after. Every attached file is queued for
    /// upload in the attaching write.
    pub fn new(
        namespace: impl Into<String>,
        cache_budget: u64,
        provenance: Provenance,
        fill: CacheFill,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            cache_budget,
            provenance,
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
