impl super::PreparedStoreSnapshot {
    pub(crate) fn assert_prepared_row_for_test(&self, expected: &str) {
        self.read(|source, _| {
            let error = source
                .execute("UPDATE prepared_rows SET value = 'must stay read-only'", [])
                .expect_err("the installer must not mutate its prepared source");
            assert_eq!(
                error.sqlite_error_code(),
                Some(rusqlite::ErrorCode::ReadOnly)
            );
            assert_eq!(
                source
                    .query_row("SELECT value FROM prepared_rows", [], |row| row
                        .get::<_, String>(0))
                    .expect("read prepared row"),
                expected,
            );
            Ok(())
        })
        .expect("read prepared database");
    }
}
