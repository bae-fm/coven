//! Store names share the transaction that commits their sequence prefix.

use super::*;

impl Database {
    /// Save the name chosen for a local store before publishing its directory.
    /// This row is not sequence membership until slot 1 applies, the slot that
    /// will set the name.
    pub async fn initialize_store_name(&self, name: String) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute(
                        "INSERT INTO _coven_store(singleton,store,name,named_at,created) VALUES(1,?1,?2,?3,0) ON CONFLICT(singleton) DO UPDATE SET name=excluded.name WHERE created=0",
                        rusqlite::params![inner.access.directory.id().0.as_bytes().as_slice(), name, 1u64.to_be_bytes().as_slice()],
                    )?;
                    Ok(())
                })
            })
        }).await
    }

    /// The name in this device's committed state, including a local creation (E9).
    pub async fn store_name(&self) -> CovenResult<String> {
        self.read(|sql| sql.store_name()).await
    }

    /// The same name query, live (E4).
    pub fn subscribe_store_name(&self) -> LiveQuery<String> {
        self.subscribe(|sql| sql.store_name())
    }

    /// The committed name and the slot that set it, including a local
    /// creation's name, which slot 1 will set. A saved restore code keeps
    /// whichever name has the later slot (§13.1).
    pub async fn named_store(&self) -> CovenResult<(String, coven_merge::Slot)> {
        self.read(|sql| sql.named_store()).await
    }
}

impl DatabaseReadHandle {
    /// The current name in a consistent read-only snapshot (E9).
    pub async fn store_name(&self) -> CovenResult<String> {
        self.read(|sql| sql.store_name()).await
    }
}
