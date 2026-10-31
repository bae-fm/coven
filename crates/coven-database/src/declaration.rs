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

/// A synced table's key, audience, file references, shared triggers and whether only
/// each row's original creator writes it directly (E2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncedTable {
    pub(crate) name: String,
    pub(crate) identity: RowIdentity,
    pub(crate) key: Vec<String>,
    pub(crate) audience: AudienceSource,
    pub(crate) files: Vec<FileColumn>,
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

    /// The declared file reference columns, in declaration order.
    pub fn file_columns(&self) -> &[FileColumn] {
        &self.files
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
            files: Vec::new(),
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

    /// Declare a column holding a file reference. Opening and migrating check
    /// the column against this synced table's actual schema.
    pub fn file_column(mut self, column: FileColumn) -> Self {
        self.files.push(column);
        self
    }

    /// Declare a trigger as shared; all other triggers are local.
    pub fn shared_trigger(mut self, name: impl Into<String>) -> Self {
        self.shared_triggers.push(name.into());
        self
    }
}

/// A namespace's kind: whose bytes its files are (§17.1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Provenance {
    /// The user's original, which coven never modifies or deletes.
    UserProvided,
    /// Bytes that coven owns.
    AppProvided,
}

/// When a write setting a file reference goes out (§17.4, E2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Binding {
    /// Once its file is stored, together with it: the write and every later
    /// write of this device wait for the upload. Its files are app files of
    /// at most `limit` bytes.
    Bound {
        /// The largest file the reference may name, in bytes.
        limit: u64,
    },
    /// At once; the file is on its uploader only until it is stored.
    Trailing,
}

/// The size limit of a bound reference that declares none: 1.5 MiB (§17.4).
pub const BOUND_FILE_LIMIT: u64 = 3 << 19;

/// A column holding a file's id, or NULL (§17.2, E2). It references the file
/// as a foreign key references its parent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileColumn {
    pub(crate) column: String,
    pub(crate) write_once: bool,
    pub(crate) binding: Binding,
}

impl FileColumn {
    /// A trailing file reference held in `column`.
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            write_once: false,
            binding: Binding::Trailing,
        }
    }

    /// Bound, naming files of at most `BOUND_FILE_LIMIT` bytes (§17.4).
    pub fn bound(self) -> Self {
        self.bound_with_limit(BOUND_FILE_LIMIT)
    }

    /// Bound, naming files of at most `limit` bytes (§17.4).
    pub fn bound_with_limit(mut self, limit: u64) -> Self {
        self.binding = Binding::Bound { limit };
        self
    }

    /// Refuse a write that changes the reference once it names a file.
    pub fn write_once(mut self) -> Self {
        self.write_once = true;
        self
    }

    /// The column exactly as declared.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// When a write setting the reference goes out.
    pub fn binding(&self) -> &Binding {
        &self.binding
    }

    /// Whether a reference, once it names a file, can't change.
    pub fn is_write_once(&self) -> bool {
        self.write_once
    }
}

/// A kind of file with its cache budget, declared when the store opens
/// (§17.1, §17.8, E1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Namespace {
    pub(crate) name: String,
    pub(crate) kind: Provenance,
    pub(crate) cache_budget: u64,
}

impl Namespace {
    /// A namespace of this kind. The budget applies when the namespace is
    /// first declared; `set_cache_budget` changes it after.
    pub fn new(name: impl Into<String>, kind: Provenance, cache_budget: u64) -> Self {
        Self {
            name: name.into(),
            kind,
            cache_budget,
        }
    }

    /// The name exactly as declared.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whose bytes its files are.
    pub fn kind(&self) -> &Provenance {
        &self.kind
    }

    /// The initial cache budget in bytes, as declared, not its current setting.
    pub fn cache_budget(&self) -> u64 {
        self.cache_budget
    }
}

#[cfg(test)]
#[path = "declaration_tests.rs"]
mod tests;
