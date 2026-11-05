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
                    crate::store_state_tables::name_local_creation(
                        db,
                        inner.access.directory.id(),
                        &name,
                    )
                })
            })
        })
        .await
    }

    /// The same name query, live (E4).
    pub fn subscribe_store_name<E>(&self) -> LiveQuery<String, E>
    where
        E: From<DbError> + coven_foundation::task::CarriesBug + Send + 'static,
    {
        self.subscribe(|sql| sql.store_name().map_err(E::from))
    }

    /// The committed name and the slot that set it, including a local
    /// creation's name, which slot 1 will set. A saved restore code keeps
    /// whichever name has the later slot (§13.1).
    pub async fn named_store(&self) -> Result<(String, coven_merge::Slot), crate::DbError> {
        self.read(|sql| sql.named_store()).await
    }
}

impl<M: Mode> Database<M> {
    /// The name in this device's committed state, including a local creation
    /// on the writer (E9).
    pub async fn store_name(&self) -> Result<String, crate::DbError> {
        self.read(|sql| sql.store_name()).await
    }
}
