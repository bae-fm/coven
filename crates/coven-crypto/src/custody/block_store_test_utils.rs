//! An in-memory Block Store for tests: one phone's entries, its cloud backup,
//! and new phones set up from it.

use super::block_store::{
    BlockStoreApi, BlockStoreFailure, ENTRY_LIMIT, MAX_SIZE_EXCEEDED, TOO_MANY_ENTRIES,
};
use crate::SecretBytes;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Block Store's limit on the app's entries (`BlockstoreClient.MAX_ENTRY_COUNT`).
const ENTRY_COUNT_LIMIT: usize = 16;

/// One stored entry and whether it may go to the cloud backup.
struct Stored {
    bytes: Vec<u8>,
    backup_to_cloud: bool,
}

struct Phone {
    entries: BTreeMap<String, Stored>,
    /// What the last cloud backup of this phone holds.
    cloud: BTreeMap<String, Vec<u8>>,
    end_to_end_encryption: bool,
}

/// One phone's Block Store. It starts with a screen lock set, so cloud copies
/// are end-to-end encrypted.
pub(crate) struct MemoryBlockStore {
    phone: Mutex<Phone>,
}

impl MemoryBlockStore {
    pub(crate) fn new() -> Self {
        Self::holding(BTreeMap::new())
    }

    fn holding(entries: BTreeMap<String, Stored>) -> Self {
        Self {
            phone: Mutex::new(Phone {
                entries,
                cloud: BTreeMap::new(),
                end_to_end_encryption: true,
            }),
        }
    }

    fn phone(&self) -> std::sync::MutexGuard<'_, Phone> {
        self.phone
            .lock()
            .expect("in-memory Block Store lock is poisoned")
    }

    /// Whether a cloud copy would be end-to-end encrypted: the phone has a
    /// screen lock.
    pub(crate) fn set_end_to_end_encryption(&self, available: bool) {
        self.phone().end_to_end_encryption = available;
    }

    /// Google's periodic cloud backup of this phone: the entries stored with
    /// `backup_to_cloud`, and nothing else.
    pub(crate) fn back_up_to_cloud(&self) {
        let mut phone = self.phone();
        phone.cloud = phone
            .entries
            .iter()
            .filter(|(_, stored)| stored.backup_to_cloud)
            .map(|(key, stored)| (key.clone(), stored.bytes.clone()))
            .collect();
    }

    /// A new phone set up from this one by direct transfer: every entry.
    pub(crate) fn set_up_new_phone(&self) -> Self {
        let entries = self
            .phone()
            .entries
            .iter()
            .map(|(key, stored)| {
                (
                    key.clone(),
                    Stored {
                        bytes: stored.bytes.clone(),
                        backup_to_cloud: stored.backup_to_cloud,
                    },
                )
            })
            .collect();
        Self::holding(entries)
    }

    /// A new phone set up from this phone's last cloud backup.
    pub(crate) fn restore_new_phone_from_cloud(&self) -> Self {
        let entries = self
            .phone()
            .cloud
            .iter()
            .map(|(key, bytes)| {
                (
                    key.clone(),
                    Stored {
                        bytes: bytes.clone(),
                        backup_to_cloud: true,
                    },
                )
            })
            .collect();
        Self::holding(entries)
    }

    /// The keys this phone holds, the app's own included.
    #[cfg(test)]
    pub(crate) fn keys(&self) -> Vec<String> {
        self.phone().entries.keys().cloned().collect()
    }

    /// Whether the entry under `key` may go to the cloud backup.
    #[cfg(test)]
    pub(crate) fn backs_up(&self, key: &str) -> Option<bool> {
        self.phone()
            .entries
            .get(key)
            .map(|stored| stored.backup_to_cloud)
    }
}

fn refusal(status: i32) -> BlockStoreFailure {
    BlockStoreFailure {
        exception: "com.google.android.gms.common.api.ApiException".to_owned(),
        status: Some(status),
        message: None,
    }
}

impl BlockStoreApi for MemoryBlockStore {
    fn store(
        &self,
        key: &str,
        bytes: &[u8],
        backup_to_cloud: bool,
    ) -> Result<(), BlockStoreFailure> {
        let mut phone = self.phone();
        if key.len() + bytes.len() > ENTRY_LIMIT {
            return Err(refusal(MAX_SIZE_EXCEEDED));
        }
        if !phone.entries.contains_key(key) && phone.entries.len() >= ENTRY_COUNT_LIMIT {
            return Err(refusal(TOO_MANY_ENTRIES));
        }
        phone.entries.insert(
            key.to_owned(),
            Stored {
                bytes: bytes.to_vec(),
                backup_to_cloud,
            },
        );
        Ok(())
    }

    fn retrieve(&self, key: &str) -> Result<Option<SecretBytes>, BlockStoreFailure> {
        Ok(self
            .phone()
            .entries
            .get(key)
            .map(|stored| SecretBytes::new(stored.bytes.clone())))
    }

    fn retrieve_all(&self) -> Result<Vec<(String, SecretBytes)>, BlockStoreFailure> {
        Ok(self
            .phone()
            .entries
            .iter()
            .map(|(key, stored)| (key.clone(), SecretBytes::new(stored.bytes.clone())))
            .collect())
    }

    fn delete(&self, key: &str) -> Result<(), BlockStoreFailure> {
        self.phone().entries.remove(key);
        Ok(())
    }

    fn end_to_end_encryption_available(&self) -> Result<bool, BlockStoreFailure> {
        Ok(self.phone().end_to_end_encryption)
    }
}
