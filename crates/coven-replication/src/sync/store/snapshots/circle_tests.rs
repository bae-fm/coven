use std::sync::Arc;

use super::*;
use crate::sync::test_helpers::TestStore;
use coven_database::{Database, StoreDatabase};

struct CircleSnapshotFixture {
    directory: tempfile::TempDir,
    database: Database,
    database_store_dir: coven_foundation::store_dir::StoreDir,
    store_database: StoreDatabase,
    store: std::sync::Arc<TestStore>,
    signer: UserKeypair,
}

impl CircleSnapshotFixture {
    async fn initialize(local_device_id: &str) -> Self {
        let directory = tempfile::tempdir().expect("snapshot database directory");
        let database_store_dir = coven_foundation::store_dir::StoreDir::new(directory.path());
        let database = Database::open_synthetic_for_test(
            &directory.path().join("store.sqlite3"),
            database_store_dir.clone(),
            vec![coven_protocol::synced_schema::SyncedTable::new(
                "documents",
                coven_protocol::synced_schema::RowIdentity::IndependentUuid,
            )
            .scoped_by("audience")],
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            local_device_id.to_string(),
            Arc::new(coven_foundation::clock::SystemClock),
            &[coven_database::Migration::sql(
                1,
                "Circle documents",
                "CREATE TABLE documents (id TEXT PRIMARY KEY, audience TEXT, _updated_at TEXT NOT NULL) STRICT;",
            )],
        )
        .expect("open Circle snapshot test database");
        let store_database = StoreDatabase::new(&database);
        let signer = UserKeypair::generate();
        let (store, _) = TestStore::create_with_connection(
            &database,
            database_store_dir.clone(),
            "circle-snapshot-store",
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create Circle snapshot test Store");
        Self {
            directory,
            database,
            database_store_dir,
            store_database,
            store,
            signer,
        }
    }

    async fn install_active_circle(
        &self,
    ) -> (
        coven_protocol::circle::CircleId,
        coven_protocol::circle::CircleControlCoord,
    ) {
        let owner = self
            .store
            .bind_device_in(
                &self.database,
                self.database_store_dir.clone(),
                &self.signer,
            )
            .await
            .expect("bind Circle owner");
        let circle = owner
            .create_circle("0000000001000-0000-owner", "Household")
            .await
            .expect("publish Circle creation");
        let control = self
            .store_database
            .current_circle_control(circle)
            .await
            .expect("read accepted Circle control")
            .expect("Circle creation is accepted");
        (circle, control)
    }

    async fn push_snapshots(&self) {
        self.store
            .push_circle_snapshots(
                &self.database,
                self.database_store_dir.clone(),
                self.directory.path().join("snap-temp"),
                self.database.schema_version(),
                "2026-07-16T00:00:00Z",
                &coven_keys::encryption::EncryptionService::from_key([42; 32]),
            )
            .await
            .expect("author Circle snapshots");
    }

    async fn publication_context(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> coven_protocol::circle_activation::CircleEpochAccess {
        self.store_database
            .circle_publication_context(circle_id, control)
            .await
            .expect("resolve Circle publication context")
    }

    async fn load_snapshot_metas(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
    ) -> Vec<CircleSnapshotMeta> {
        self.store
            .load_circle_snapshot_metas(
                &self.database,
                self.database_store_dir.clone(),
                circle_id,
                access,
            )
            .await
            .expect("load Circle snapshot stream")
    }

    async fn read_snapshot_image(
        &self,
        selected: &CircleSnapshotMeta,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
    ) -> Vec<u8> {
        self.store
            .read_circle_snapshot_image(selected, access)
            .await
            .expect("read Circle snapshot image")
    }

    async fn outsider_cannot_read_snapshot_meta(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        encryption: coven_keys::encryption::EncryptionService,
    ) -> bool {
        self.store
            .circle_snapshot_meta_is_unreadable(circle_id, encryption)
            .await
    }

    fn store_root_hash(&self) -> coven_protocol::store_commit::ObjectHash {
        self.store.store_root_hash()
    }
}

#[tokio::test]
async fn circle_snapshot_authors_and_installs_as_a_bootstrap_image() {
    let fixture = CircleSnapshotFixture::initialize("circle-snapshot-device").await;
    let (circle_id, control) = fixture.install_active_circle().await;
    let access = fixture
        .publication_context(circle_id, control.clone())
        .await;
    let key_fingerprint = access.key_fingerprint();

    fixture.push_snapshots().await;

    let stream = fixture.load_snapshot_metas(circle_id, &access).await;
    assert_eq!(stream.len(), 1);
    let selected = select_maximal_circle_snapshot(stream).expect("a maximal Circle snapshot");
    assert_eq!(selected.generation, 0);
    assert_eq!(selected.circle_id, circle_id);
    assert_eq!(selected.control, control);
    assert_eq!(selected.key_fingerprint, key_fingerprint);

    let image = fixture.read_snapshot_image(&selected, &access).await;
    let routing_key = coven_protocol::circle::derive_row_routing_key(
        &coven_keys::encryption::EncryptionService::from_key([42; 32]),
        fixture.store_root_hash(),
    )
    .expect("derive Circle row routing key");
    fixture
        .store_database
        .verify_circle_bootstrap_image(
            image,
            selected.bootstrap.clone(),
            circle_id,
            Some(routing_key),
        )
        .await
        .expect("Circle snapshot is installable as a bootstrap image");
}

#[tokio::test]
async fn non_member_cannot_decrypt_circle_snapshot() {
    let fixture = CircleSnapshotFixture::initialize("circle-snapshot-outsider").await;
    let (circle_id, _control) = fixture.install_active_circle().await;
    fixture.push_snapshots().await;

    let outsider = coven_keys::encryption::EncryptionService::from_key([7u8; 32]);
    assert!(
        fixture
            .outsider_cannot_read_snapshot_meta(circle_id, outsider)
            .await
    );
}
