use super::*;

impl Database {
    pub(crate) fn set_write_status_on(
        conn: &Connection,
        write_id: &WriteId,
        status: &WriteStatus,
    ) -> Result<(), DbError> {
        let status = serde_json::to_string(status)
            .map_err(|error| DbError::context("serialize write status", error))?;
        let updated = conn
            .execute(
                "UPDATE store_writes SET status = ?2 WHERE write_id = ?1",
                rusqlite::params![write_id.as_str(), status],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(format!("write {write_id} does not exist")));
        }
        Ok(())
    }
}
