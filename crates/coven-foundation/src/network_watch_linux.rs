//! The device's network on Linux: NetworkManager, on the system bus (api.md
//! E5). The network is usable while NetworkManager's connectivity is full,
//! or, where it checks none, while it is connected globally; its primary
//! connection names it, and its `Metered` says whether it is metered, or
//! guesses so. Without a system bus, or NetworkManager on it, coven can't
//! tell, and says so in the log as the store opens.

use super::{NetworkSink, NetworkWatchError, UsableNetwork};
use futures_lite::StreamExt;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use tokio::sync::oneshot;
use zbus::zvariant::OwnedObjectPath;

const SERVICE: &str = "org.freedesktop.NetworkManager";
const PATH: &str = "/org/freedesktop/NetworkManager";

/// `NM_CONNECTIVITY_UNKNOWN`: NetworkManager checks no connectivity, or
/// hasn't yet.
const CONNECTIVITY_UNKNOWN: u32 = 0;
/// `NM_CONNECTIVITY_FULL`.
const CONNECTIVITY_FULL: u32 = 4;
/// `NM_STATE_CONNECTED_GLOBAL`.
const CONNECTED_GLOBAL: u32 = 70;
/// `NM_METERED_YES` and `NM_METERED_GUESS_YES`.
const METERED: [u32; 2] = [1, 3];

/// The thread following NetworkManager, stopped and joined as this drops.
pub(super) struct ManagerWatch {
    stop: Option<oneshot::Sender<()>>,
    follow: Option<JoinHandle<()>>,
}

impl ManagerWatch {
    /// Follow NetworkManager's changes, then read its network as it is now.
    /// `None` where there is no system bus, or no NetworkManager on it.
    pub(super) fn start(sink: Arc<Mutex<NetworkSink>>) -> Result<Option<Self>, NetworkWatchError> {
        futures_lite::future::block_on(Self::started(sink))
    }

    async fn started(sink: Arc<Mutex<NetworkSink>>) -> Result<Option<Self>, NetworkWatchError> {
        let connection = match zbus::Connection::system().await {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "no system bus: coven can't tell the device's network, and holds no transfer for it"
                );
                return Ok(None);
            }
        };
        let bus = zbus::fdo::DBusProxy::new(&connection).await.map_err(NetworkWatchError::new)?;
        let name = zbus::names::BusName::try_from(SERVICE).map_err(NetworkWatchError::new)?;
        if !bus.name_has_owner(name).await.map_err(NetworkWatchError::new)? {
            tracing::warn!(
                "no NetworkManager: coven can't tell the device's network, and holds no transfer for it"
            );
            return Ok(None);
        }
        // Each read asks NetworkManager, never a copy that a change signal
        // may not have reached yet.
        let manager = zbus::proxy::Builder::<zbus::Proxy>::new(&connection)
            .destination(SERVICE)
            .and_then(|builder| builder.path(PATH))
            .and_then(|builder| builder.interface(SERVICE))
            .map_err(NetworkWatchError::new)?
            .cache_properties(zbus::proxy::CacheProperties::No)
            .build()
            .await
            .map_err(NetworkWatchError::new)?;
        let properties = zbus::fdo::PropertiesProxy::builder(&connection)
            .destination(SERVICE)
            .map_err(NetworkWatchError::new)?
            .path(PATH)
            .map_err(NetworkWatchError::new)?
            .build()
            .await
            .map_err(NetworkWatchError::new)?;
        // Followed before the first read, so no change falls between them.
        let changed = properties
            .receive_properties_changed()
            .await
            .map_err(NetworkWatchError::new)?
            .map(drop);
        let owner =
            manager.receive_owner_changed().await.map_err(NetworkWatchError::new)?.map(drop);
        let mut changes = changed.or(owner);
        let first = network(&manager).await.map_err(NetworkWatchError::new)?;
        sink.lock().expect("network sink poisoned").observe(first);
        let (stop, stopped) = oneshot::channel::<()>();
        let following = async move {
            while changes.next().await.is_some() {
                let read = network(&manager).await;
                let mut sink = sink.lock().expect("network sink poisoned");
                match read {
                    Ok(network) => sink.observe(network),
                    // NetworkManager stopped, or failed to answer: coven
                    // can't tell until it answers again.
                    Err(error) => {
                        tracing::error!(%error, "reading NetworkManager's network failed");
                        sink.cannot_tell();
                    }
                }
            }
            // The bus connection closed: nothing tells coven any more.
            tracing::error!("the system bus closed: coven can't tell the device's network");
            sink.lock().expect("network sink poisoned").cannot_tell();
        };
        let follow = std::thread::Builder::new()
            .name("coven network watch".into())
            .spawn(move || {
                futures_lite::future::block_on(futures_lite::future::or(following, async {
                    // Told to stop, or the watch is gone: either way, stop.
                    stopped.await.ok();
                }));
            })
            .map_err(NetworkWatchError::new)?;
        Ok(Some(Self { stop: Some(stop), follow: Some(follow) }))
    }
}

/// The usable network NetworkManager says the device is on, if any.
async fn network(manager: &zbus::Proxy<'_>) -> zbus::Result<Option<UsableNetwork>> {
    let connectivity: u32 = manager.get_property("Connectivity").await?;
    let usable = connectivity == CONNECTIVITY_FULL
        || (connectivity == CONNECTIVITY_UNKNOWN
            && manager.get_property::<u32>("State").await? == CONNECTED_GLOBAL);
    if !usable {
        return Ok(None);
    }
    let primary: OwnedObjectPath = manager.get_property("PrimaryConnection").await?;
    let metered: u32 = manager.get_property("Metered").await?;
    Ok(Some(UsableNetwork { id: primary.to_string(), metered: METERED.contains(&metered) }))
}

impl Drop for ManagerWatch {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            // Refused only once the thread has ended on its own, as it does
            // when the bus closes: then there is nothing to stop.
            stop.send(()).ok();
        }
        if let Some(follow) = self.follow.take() {
            if follow.join().is_err() {
                tracing::error!("the network watch's thread panicked");
            }
        }
    }
}
