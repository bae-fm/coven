impl super::PreparedStoreSnapshot {
    pub(crate) async fn assert_prepared_row_for_test(&self, expected: &str) {
        let bytes = self.image.read().await.expect("read sealed image");
        let mut source = rusqlite::Connection::open_in_memory().expect("open image reader");
        crate::connection_io::deserialize_database_image_into(&mut source, &bytes)
            .expect("open sealed rows");
        assert_eq!(
            source
                .query_row("SELECT value FROM prepared_rows", [], |row| row
                    .get::<_, String>(0))
                .expect("read prepared row"),
            expected,
        );
        drop(source);
    }
}
