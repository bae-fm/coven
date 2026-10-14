//! In-memory key administration for tests of minting and verified deletion.
use super::*;
use crate::S3Credentials;

/// Key administration over an in-memory account: minted keys are IAM-style
/// users that exist until revoked or rolled back. Faults make one call fail.
#[derive(Clone, Default)]
pub struct MemoryKeyAdmin {
    state: Arc<std::sync::Mutex<KeyAdminState>>,
}

#[derive(Default)]
struct KeyAdminState {
    /// Principal name to its current key id.
    users: BTreeMap<String, String>,
    next: u32,
    calls: Vec<&'static str>,
    fail_mint: bool,
    fail_probe: bool,
    fail_delete: bool,
    keep_deleted: bool,
}

impl MemoryKeyAdmin {
    /// Each named call in order, for assertions.
    pub fn calls(&self) -> Vec<&'static str> {
        self.state.lock().unwrap().calls.clone()
    }
    /// The principal names whose users exist.
    pub fn users(&self) -> Vec<String> {
        self.state.lock().unwrap().users.keys().cloned().collect()
    }
    /// The next mint fails after creating its user.
    pub fn fail_next_mint(&self) {
        self.state.lock().unwrap().fail_mint = true;
    }
    /// The next member-side check fails.
    pub fn fail_next_probe(&self) {
        self.state.lock().unwrap().fail_probe = true;
    }
    /// The next deletion fails.
    pub fn fail_next_delete(&self) {
        self.state.lock().unwrap().fail_delete = true;
    }
    /// The next deletion answers success but the key stays readable.
    pub fn keep_next_deleted_key(&self) {
        self.state.lock().unwrap().keep_deleted = true;
    }
}

#[async_trait]
impl crate::AccessKeyAdmin for MemoryKeyAdmin {
    async fn mint(&self, scope: &crate::MintScope) -> Result<crate::S3MemberKey, StorageError> {
        let mut state = self.state.lock().unwrap();
        state.calls.push("mint");
        state.next += 1;
        let id = format!("minted-{}", state.next);
        state.users.insert(scope.name.clone(), id.clone());
        if std::mem::take(&mut state.fail_mint) {
            return Err(StorageFailure::Network.into());
        }
        Ok(crate::S3MemberKey {
            credentials: S3Credentials {
                access_key_id: id,
                secret_access_key: coven_crypto::SecretText::new(format!("secret-{}", state.next)),
            },
            principal: coven_format::S3Principal::IamUser {
                user: scope.name.clone(),
                policy: coven_format::IamPolicy::Inline {
                    name: "coven-member".into(),
                },
            },
        })
    }
    async fn probe(
        &self,
        scope: &crate::MintScope,
        key: &S3Credentials,
    ) -> Result<(), StorageError> {
        let mut state = self.state.lock().unwrap();
        state.calls.push("probe");
        if std::mem::take(&mut state.fail_probe) {
            return Err(StorageFailure::KeyBeyondStore.into());
        }
        match state.users.get(&scope.name) {
            Some(id) if *id == key.access_key_id => Ok(()),
            _ => Err(StorageFailure::Authentication.into()),
        }
    }
    async fn roll_back(&self, scope: &crate::MintScope) -> Result<(), StorageError> {
        let mut state = self.state.lock().unwrap();
        state.calls.push("roll_back");
        state.users.remove(&scope.name);
        Ok(())
    }
    async fn revoke(
        &self,
        access_key_id: &str,
        principal: &coven_format::S3Principal,
    ) -> Result<(), StorageError> {
        let mut state = self.state.lock().unwrap();
        state.calls.push("revoke");
        if std::mem::take(&mut state.fail_delete) {
            return Err(StorageFailure::PermissionDenied.into());
        }
        let coven_format::S3Principal::IamUser { user, .. } = principal else {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("a key made in the console is deleted there"));
        };
        if !std::mem::take(&mut state.keep_deleted)
            && state.users.get(user).is_some_and(|id| id == access_key_id)
        {
            state.users.remove(user);
        }
        Ok(())
    }
    async fn key_exists(
        &self,
        access_key_id: &str,
        principal: &coven_format::S3Principal,
    ) -> Result<bool, StorageError> {
        let mut state = self.state.lock().unwrap();
        state.calls.push("key_exists");
        let coven_format::S3Principal::IamUser { user, .. } = principal else {
            return Err(StorageFailure::InvalidConfiguration.into());
        };
        Ok(state.users.get(user).is_some_and(|id| id == access_key_id))
    }
}
