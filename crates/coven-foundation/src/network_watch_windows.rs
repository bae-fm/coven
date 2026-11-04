//! The device's network on Windows: `NetworkInformation`'s internet
//! connection profile, and its `NetworkStatusChanged` event (api.md E5). The
//! network is usable while the profile has internet access; another profile
//! is another network. It is metered when its cost is fixed or variable,
//! when it roams, or when it is over its data limit.

use super::{NetworkSink, NetworkWatchError, UsableNetwork};
use std::sync::{Arc, Mutex};
use windows::Networking::Connectivity::{
    NetworkConnectivityLevel, NetworkCostType, NetworkInformation, NetworkStatusChangedEventHandler,
};

/// The event registration, removed as the watch drops.
pub(super) struct ProfileWatch {
    token: i64,
}

impl ProfileWatch {
    /// Follow the profile's changes, then read it as it is now. Each read
    /// and what it says reach the sink under its lock, so the last thing it
    /// hears is a read made after the last change.
    pub(super) fn start(sink: Arc<Mutex<NetworkSink>>) -> Result<Self, NetworkWatchError> {
        let followed = Arc::clone(&sink);
        let handler = NetworkStatusChangedEventHandler::new(move |_| {
            let mut sink = followed.lock().expect("network sink poisoned");
            match network() {
                Ok(network) => sink.observe(network),
                // Coven can't tell until the next change reads again.
                Err(error) => {
                    tracing::error!(%error, "reading the internet connection profile failed");
                    sink.cannot_tell();
                }
            }
            Ok(())
        });
        let token =
            NetworkInformation::NetworkStatusChanged(&handler).map_err(NetworkWatchError::new)?;
        // Dropped on a failed read, which removes the registration.
        let watch = Self { token };
        let read = {
            let mut sink = sink.lock().expect("network sink poisoned");
            network().map(|network| sink.observe(network))
        };
        read.map_err(NetworkWatchError::new)?;
        Ok(watch)
    }
}

impl Drop for ProfileWatch {
    fn drop(&mut self) {
        if let Err(error) = NetworkInformation::RemoveNetworkStatusChanged(self.token) {
            tracing::error!(%error, "removing the network watch failed");
        }
    }
}

/// The usable network the internet connection profile describes, if any.
fn network() -> windows::core::Result<Option<UsableNetwork>> {
    let profile = match NetworkInformation::GetInternetConnectionProfile() {
        Ok(profile) => profile,
        // With no internet connection Windows returns no profile, which the
        // bindings give as an error with no code.
        Err(error) if error.code().is_ok() => return Ok(None),
        Err(error) => return Err(error),
    };
    if profile.GetNetworkConnectivityLevel()? != NetworkConnectivityLevel::InternetAccess {
        return Ok(None);
    }
    let cost = profile.GetConnectionCost()?;
    let kind = cost.NetworkCostType()?;
    let metered = kind == NetworkCostType::Fixed
        || kind == NetworkCostType::Variable
        || cost.Roaming()?
        || cost.OverDataLimit()?;
    Ok(Some(UsableNetwork { id: profile.ProfileName()?.to_string(), metered }))
}
