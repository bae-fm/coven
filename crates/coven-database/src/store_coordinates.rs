use super::*;

impl Database {
    pub fn sequence_from_sqlite(device_id: &str, value: i64) -> Result<u64, DbError> {
        let value = u64::try_from(value).map_err(|_| {
            DbError::Message(format!(
                "Store position for {device_id:?} contains negative sequence {value}"
            ))
        })?;
        if value == 0 {
            return Err(DbError::Message(format!(
                "Store position for {device_id:?} contains sequence zero"
            )));
        }
        Ok(value)
    }

    pub fn sequence_to_sqlite(device_id: &str, value: u64) -> Result<i64, DbError> {
        if value == 0 {
            return Err(DbError::Message(format!(
                "Store position for {device_id:?} cannot use sequence zero"
            )));
        }
        i64::try_from(value).map_err(|_| {
            DbError::Message(format!(
                "Store position for {device_id:?} exceeds SQLite INTEGER"
            ))
        })
    }
}
