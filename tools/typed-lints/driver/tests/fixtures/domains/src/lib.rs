// The crate `clippy::domain_owner_is_no_owner` checks under the policy of
// this folder: the line of each domain owner that is no owner ends in a
// comment naming the lint.

/// The records domain's owner, which holds the database's capability.
pub struct Records {
    pub connection: rusqlite::Connection,
}

/// The notes domain's owner, which holds no capability.
pub struct Notes(pub u32); // domain_owner_is_no_owner
