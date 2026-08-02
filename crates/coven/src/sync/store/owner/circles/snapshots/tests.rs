use std::sync::Arc;

use super::*;
use crate::database::{Database, StoreDatabase};
use crate::sync::test_helpers::TestStore;

struct CircleSnapshotFixture {
    directory: tempfile::TempDir,
    database: Database,
    store_database: StoreDatabase,
    store: TestStore,
}

impl CircleSnapshotFixture {
    async fn initialize(local_device_id: &str) -> Self {
        let directory = tempfile::tempdir().expect("snapshot database directory");
        let database = Database::open(
            &directory.path().join("store.sqlite3"),
            crate::sync::test_helpers::test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            local_device_id.to_string(),
            Arc::new(crate::clock::SystemClock),
            &crate::sync::test_helpers::test_migrations(),
        )
        .expect("open Circle snapshot test database")
        .0;
        let store_database = StoreDatabase::new(&database);
        let store = TestStore::create_browsable(
            &database,
            "circle-snapshot-store",
            crate::keys::UserKeypair::generate(),
        )
        .await
        .expect("create Circle snapshot test Store");
        Self {
            directory,
            database,
            store_database,
            store,
        }
    }

    async fn apply_routing_schema(&self) {
        self.database
            .test_sql(|database| database.apply_coven_routing_schema())
            .await
            .expect("apply routing schema");
    }

    async fn install_active_circle(
        &self,
    ) -> (
        crate::protocol::circle::CircleId,
        crate::protocol::circle::CircleControlCoord,
    ) {
        self.database
            .test_sql(|database| {
                Ok(crate::sync::test_helpers::install_test_active_circle(
                    &database, "snap",
                ))
            })
            .await
            .expect("install active Circle")
    }

    async fn push_snapshots(&self) {
        self.store
            .push_circle_snapshots(
                &self.database,
                self.directory.path().join("snap-temp"),
                self.database.schema_version(),
                "2026-07-16T00:00:00Z",
                &crate::encryption::EncryptionService::from_key([42; 32]),
            )
            .await
            .expect("author Circle snapshots");
    }

    async fn publication_context(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        control: crate::protocol::circle::CircleControlCoord,
    ) -> (crate::encryption::EncryptionService, crate::KeyFingerprint) {
        self.store_database
            .circle_publication_context(circle_id, control)
            .await
            .expect("resolve Circle publication context")
    }

    async fn load_snapshot_metas(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        encryption: crate::encryption::EncryptionService,
    ) -> Vec<CircleSnapshotMeta> {
        self.store
            .load_circle_snapshot_metas(&self.database, circle_id, encryption)
            .await
            .expect("load Circle snapshot stream")
    }

    async fn read_snapshot_image(
        &self,
        selected: &CircleSnapshotMeta,
        encryption: crate::encryption::EncryptionService,
    ) -> Vec<u8> {
        self.store
            .read_circle_snapshot_image(selected, encryption)
            .await
            .expect("read Circle snapshot image")
    }

    async fn outsider_cannot_read_snapshot_meta(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        encryption: crate::encryption::EncryptionService,
    ) -> bool {
        self.store
            .circle_snapshot_meta_is_unreadable(circle_id, encryption)
            .await
    }

    fn store_root_hash(&self) -> crate::protocol::store_commit::ObjectHash {
        self.store.store_root_hash()
    }
}

#[tokio::test]
async fn circle_snapshot_authors_and_installs_as_a_bootstrap_image() {
    let fixture = CircleSnapshotFixture::initialize("circle-snapshot-device").await;
    fixture.apply_routing_schema().await;
    let (circle_id, control) = fixture.install_active_circle().await;
    let (encryption, key_fingerprint) = fixture
        .publication_context(circle_id, control.clone())
        .await;

    fixture.push_snapshots().await;

    let stream = fixture
        .load_snapshot_metas(circle_id, encryption.clone())
        .await;
    assert_eq!(stream.len(), 1);
    let selected = select_maximal_circle_snapshot(stream).expect("a maximal Circle snapshot");
    assert_eq!(selected.generation, 0);
    assert_eq!(selected.circle_id, circle_id);
    assert_eq!(selected.control, control);
    assert_eq!(selected.key_fingerprint, key_fingerprint);

    let image = fixture
        .read_snapshot_image(&selected, encryption.clone())
        .await;
    let routing_key =
        crate::protocol::circle::derive_row_routing_key(&encryption, fixture.store_root_hash())
            .expect("derive Circle row routing key");
    verify_circle_bootstrap_image(
        &image,
        &selected.bootstrap,
        circle_id,
        &crate::sync::test_helpers::test_synced_tables(),
        Some(&routing_key),
    )
    .expect("Circle snapshot is installable as a bootstrap image");
}

#[tokio::test]
async fn non_member_cannot_decrypt_circle_snapshot() {
    let fixture = CircleSnapshotFixture::initialize("circle-snapshot-outsider").await;
    fixture.apply_routing_schema().await;
    let (circle_id, _control) = fixture.install_active_circle().await;
    fixture.push_snapshots().await;

    let outsider = crate::encryption::EncryptionService::from_key([7u8; 32]);
    assert!(
        fixture
            .outsider_cannot_read_snapshot_meta(circle_id, outsider)
            .await
    );
}
