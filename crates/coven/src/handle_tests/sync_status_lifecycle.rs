//! The connection lifecycle as a status subscriber sees it.

use super::*;

fn status_name(rx: &tokio::sync::watch::Receiver<SyncLoopStatus>) -> String {
    format!("{:?}", *rx.borrow())
}

/// A host renders its cloud indicator from the status stream alone, so
/// connecting, stopping, and disconnecting each publish where the connection
/// now stands instead of leaving the last cycle's result in place.
#[tokio::test]
async fn connection_lifecycle_is_published_to_status_subscribers() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                test_keyring::install();

                let handle = status_test_handle("lib-status-lifecycle");
                let rx = handle.subscribe_sync_status();
                assert_eq!(status_name(&rx), "Disconnected");

                let home = Arc::new(InMemoryCloudHome::new());
                handle
                    .connect_sync_with_test_home(home.clone(), CloudCipher::Plaintext)
                    .await
                    .expect("connect over injected home");
                assert_eq!(
                    status_name(&rx),
                    "Offline",
                    "a new connection has had no provider operation succeed yet"
                );

                handle.stop_sync();
                assert_eq!(status_name(&rx), "Stopped");

                handle
                    .connect_sync_with_test_home(home, CloudCipher::Plaintext)
                    .await
                    .expect("reconnect over injected home");
                assert_eq!(status_name(&rx), "Offline");

                handle.disconnect_sync();
                assert_eq!(status_name(&rx), "Disconnected");
            })
            .await
            .expect("status lifecycle test task");
        })
        .await;
}
