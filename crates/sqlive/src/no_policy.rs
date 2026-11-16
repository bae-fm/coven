//! What a host with no policy of its own opens a database with: nothing
//! beyond sqlive's own rules, and the engine's failure as the handle's.

use crate::authorization::{Policy, PolicyAction, Refresh};
use crate::connection::HostSql;
use crate::EngineFailure;
use std::convert::Infallible;
use std::sync::Arc;

/// The policy of a host that adds nothing to sqlive's own rules: it
/// refuses no statement.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoPolicy;

impl Policy for NoPolicy {
    type Refusal = Infallible;
    type Failure = HandleFailure;
    type Scope = ();

    fn judge(&self, _action: &PolicyAction<'_>) -> Result<(), Infallible> {
        Ok(())
    }

    fn check_sql(&self, _tokens: &[crate::sql::Token]) -> Result<(), Infallible> {
        Ok(())
    }

    fn begin_app_call(&self) {}

    fn host_object(&self, _name: &str) -> bool {
        false
    }

    fn statement_key(&self) -> u64 {
        0
    }

    fn enter(&self, _scope: &()) {}

    fn leave(&self, _scope: &()) {}

    fn refresh(&self, _sql: &HostSql<Self>, _extent: Refresh) -> rusqlite::Result<()> {
        Ok(())
    }
}

/// What a handle fails with when its host has no failure of its own: the
/// engine's failure that ended it, shared by every later call.
#[derive(Clone, Debug)]
pub struct HandleFailure(Arc<EngineFailure<HandleFailure, Infallible>>);

impl HandleFailure {
    /// The engine's failure that ended the handle.
    pub fn engine(&self) -> &EngineFailure<HandleFailure, Infallible> {
        &self.0
    }
}

impl From<EngineFailure<HandleFailure, Infallible>> for HandleFailure {
    fn from(failure: EngineFailure<HandleFailure, Infallible>) -> Self {
        Self(Arc::new(failure))
    }
}

impl std::fmt::Display for HandleFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &*self.0 {
            EngineFailure::Rollback => write!(f, "rolling back a failed transaction failed"),
            EngineFailure::Checkpoint(error) => write!(f, "a WAL checkpoint failed: {error}"),
            EngineFailure::Statistics(error) => {
                write!(f, "keeping the planner's statistics failed: {error}")
            }
            EngineFailure::Hook(error) => {
                write!(f, "removing a transaction's hook failed: {error}")
            }
        }
    }
}

impl std::error::Error for HandleFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &*self.0 {
            EngineFailure::Rollback => None,
            EngineFailure::Checkpoint(error)
            | EngineFailure::Statistics(error)
            | EngineFailure::Hook(error) => Some(error),
        }
    }
}
