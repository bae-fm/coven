package fm.coven;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.os.Build;
import android.os.PowerManager;

/**
 * Coven's watch of the device's network on Android (coven spec §6.7, api.md
 * E5). Android tells a network callback, a Java class, which JNI cannot
 * define; this one hands each callback to coven's native code, which keeps
 * what it means. The app packages it from coven-foundation's Android
 * library; coven loads and starts it itself.
 */
public final class NetworkWatch extends ConnectivityManager.NetworkCallback {
    /** The native watch this one tells, by its number. */
    private final long watch;
    private final ConnectivityManager connectivity;
    private final Context context;
    /** Doze ending, which Android before 10 reports no other way. */
    private final BroadcastReceiver idle;

    private NetworkWatch(Context context, long watch) {
        this.watch = watch;
        this.context = context.getApplicationContext();
        this.connectivity = this.context.getSystemService(ConnectivityManager.class);
        this.idle = Build.VERSION.SDK_INT < Build.VERSION_CODES.Q ? new IdleReceiver() : null;
    }

    /** Start telling watch {@code watch}: the current network first, then each change. */
    static NetworkWatch start(Context context, long watch) {
        NetworkWatch network = new NetworkWatch(context, watch);
        network.current();
        network.connectivity.registerDefaultNetworkCallback(network);
        if (network.idle != null) {
            network.context.registerReceiver(
                    network.idle, new IntentFilter(PowerManager.ACTION_DEVICE_IDLE_MODE_CHANGED));
        }
        return network;
    }

    /**
     * Stop telling it. A callback Android is already making may still
     * arrive; coven ignores it once its watch is gone.
     */
    void stop() {
        connectivity.unregisterNetworkCallback(this);
        if (idle != null) {
            context.unregisterReceiver(idle);
        }
    }

    /**
     * What the device's default network is now, before any callback. Android
     * gives no default network while it blocks it for the app, so a network
     * found here is not blocked.
     */
    private void current() {
        Network network = connectivity.getActiveNetwork();
        if (network == null) {
            lost(watch, 0);
            return;
        }
        available(watch, network.getNetworkHandle());
        NetworkCapabilities capabilities = connectivity.getNetworkCapabilities(network);
        if (capabilities != null) {
            onCapabilitiesChanged(network, capabilities);
        }
    }

    @Override
    public void onAvailable(Network network) {
        available(watch, network.getNetworkHandle());
    }

    @Override
    public void onCapabilitiesChanged(Network network, NetworkCapabilities capabilities) {
        boolean unmetered =
                capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_NOT_METERED)
                        || (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R
                                && capabilities.hasCapability(
                                        NetworkCapabilities.NET_CAPABILITY_TEMPORARILY_NOT_METERED));
        capabilities(
                watch,
                network.getNetworkHandle(),
                capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED),
                !unmetered);
    }

    @Override
    public void onBlockedStatusChanged(Network network, boolean blocked) {
        blocked(watch, network.getNetworkHandle(), blocked);
    }

    @Override
    public void onLost(Network network) {
        lost(watch, network.getNetworkHandle());
    }

    private final class IdleReceiver extends BroadcastReceiver {
        @Override
        public void onReceive(Context context, Intent intent) {
            PowerManager power = context.getSystemService(PowerManager.class);
            if (power != null && !power.isDeviceIdleMode()) {
                awake(watch);
            }
        }
    }

    private static native void available(long watch, long network);

    private static native void capabilities(
            long watch, long network, boolean validated, boolean metered);

    private static native void blocked(long watch, long network, boolean blocked);

    private static native void lost(long watch, long network);

    private static native void awake(long watch);
}
