use std::sync::Arc;

use coven_foundation::store_dir::StoreDir;
use coven_keys::{encryption::EncryptionService, keys::UserKeypair};
use coven_storage::CloudSyncObjectStorage;

use super::{
    DeviceJoinJournalDatabase, InstalledDeviceJoinSnapshot, PendingDeviceJoinAuthority,
    SamePrincipalDeviceJoin,
};

impl InstalledDeviceJoinSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn complete_and_assert_circle_snapshot_clock_for_test(
        self,
        pending: &DeviceJoinJournalDatabase,
        storage: &Arc<dyn CloudSyncObjectStorage>,
        store_dir: &StoreDir,
        identity: &UserKeypair,
        join: SamePrincipalDeviceJoin,
        published_at: &str,
        routing: &EncryptionService,
        row_id: &str,
        row_stamp: &str,
    ) {
        let database = self.database.clone();
        assert!(database.stamp().as_str() < row_stamp);
        let completion = PendingDeviceJoinAuthority::prepare_same_principal_completion(
            pending,
            storage,
            store_dir,
            identity,
            join,
            self,
            published_at,
            Some(routing),
            None,
        )
        .await
        .expect("restore the Circle image and install the control-only tail");
        completion
            .complete()
            .await
            .expect("complete the device join");
        // The completion closed the joined database, so the row is read back
        // from the file it left, the way the host's next open finds it.
        let restored_stamp: String = coven_database::DatabaseImageTest::open(&store_dir.db_path())
            .expect("open the joined database file")
            .query_row(
                "SELECT _updated_at FROM documents WHERE id = ?1",
                [row_id],
                |row| row.get(0),
            )
            .expect("the Circle snapshot restored its row");
        assert_eq!(restored_stamp, row_stamp);
        let next = database.stamp();
        assert!(
            next > restored_stamp,
            "the first local write must follow the accepted Circle snapshot row: local={next}, restored={restored_stamp}",
        );
    }
}
