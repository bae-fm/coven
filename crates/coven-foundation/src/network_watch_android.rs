//! The device's network on Android: `ConnectivityManager`'s default-network
//! callback (api.md E1, E5). A callback Android calls must be a Java class,
//! which JNI can't define, so coven's Android library holds one,
//! `fm.coven.NetworkWatch`, which hands each callback to the native methods
//! here. Coven loads it through the app's class loader, with the context the
//! app gives `ndk-context`, and registers those methods itself, so the app
//! writes no JNI. What each callback means is `DefaultNetwork`'s.

use super::default_network::DefaultNetwork;
use super::{heard, NetworkSink, NetworkWatchError};
use crate::java_exception::{caught, JniFailure};
use crate::locks::lock_unpoisoned;
use crate::task::BugSinkRef;
use jni::objects::{Global, JClass, JClassLoader, JObject, JValue};
use jni::sys::{jboolean, jlong};
use jni::{jni_sig, jni_str, Env, EnvUnowned, JavaVM, NativeMethod};
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::sync::{Arc, Mutex, Weak};

const WATCH_CLASS: &str = "fm.coven.NetworkWatch";

/// Every watch running in this process, by the number its Java object tells
/// each callback with. Android calls the native methods, which are the
/// process's, so they find their watch here; a callback for a watch already
/// stopped finds none and does nothing, however late it comes. Its scope is
/// the process, like the methods it serves.
static WATCHES: Mutex<Watches> = Mutex::new(Watches { next: 1, running: BTreeMap::new() });

struct Watches {
    next: i64,
    running: BTreeMap<i64, Weak<RunningWatch>>,
}

/// What one running watch's callbacks tell, and where a panic in them goes.
struct RunningWatch {
    network: Mutex<DefaultNetwork>,
    sink: Arc<Mutex<NetworkSink>>,
    bugs: BugSinkRef,
}

/// The running Java watch, stopped as this drops.
pub(super) struct JavaWatch {
    vm: JavaVM,
    watch: Global<JObject<'static>>,
    number: i64,
    /// What the callbacks tell, while the watch runs.
    _running: Arc<RunningWatch>,
}

impl JavaWatch {
    /// Load and start the Java watch, which tells the current default
    /// network before it returns. A panic in a callback goes to `bugs`.
    pub(super) fn start(
        sink: Arc<Mutex<NetworkSink>>,
        bugs: BugSinkRef,
    ) -> Result<Self, NetworkWatchError> {
        let running = Arc::new(RunningWatch {
            network: Mutex::new(DefaultNetwork::new(sink.clone())),
            sink,
            bugs,
        });
        let number = {
            let mut watches = lock_unpoisoned(&WATCHES);
            let number = watches.next;
            watches.next += 1;
            watches.running.insert(number, Arc::downgrade(&running));
            number
        };
        let started = Self::start_java(number);
        match started {
            Ok((vm, watch)) => Ok(Self { vm, watch, number, _running: running }),
            Err(failure) => {
                lock_unpoisoned(&WATCHES).running.remove(&number);
                Err(NetworkWatchError::platform("start the Java network watch", failure))
            }
        }
    }

    fn start_java(number: i64) -> Result<(JavaVM, Global<JObject<'static>>), JniFailure> {
        let android = ndk_context::android_context();
        // Safety: ndk-context holds the process's JavaVM pointer, set by the app.
        let vm = unsafe { JavaVM::from_raw(android.vm().cast()) };
        let watch = vm.attach_current_thread_for_scope(|env| -> Result<_, JniFailure> {
            // Safety: ndk-context holds a global reference to the app's
            // context for the life of the process.
            let context = unsafe { JObject::from_raw(env, android.context().cast()) };
            let loader = env
                .call_method(
                    &context,
                    jni_str!("getClassLoader"),
                    jni_sig!(() -> JClassLoader),
                    &[],
                )
                .map_err(|error| caught(env, error))?
                .l()?;
            let loader = env.cast_local::<JClassLoader>(loader)?;
            let name = env.new_string(WATCH_CLASS)?;
            let class = loader.load_class(env, name).map_err(|error| caught(env, error))?;
            register(env, &class).map_err(|error| caught(env, error))?;
            let watch = env
                .call_static_method(
                    &class,
                    jni_str!("start"),
                    jni_sig!((android.content.Context, jlong) -> fm.coven.NetworkWatch),
                    &[JValue::from(&context), JValue::Long(number)],
                )
                .map_err(|error| caught(env, error))?
                .l()?;
            Ok(env.new_global_ref(watch)?)
        })?;
        Ok((vm, watch))
    }
}

impl Drop for JavaWatch {
    fn drop(&mut self) {
        let stopped = self.vm.attach_current_thread_for_scope(|env| -> Result<(), JniFailure> {
            env.call_method(self.watch.as_obj(), jni_str!("stop"), jni_sig!(() -> void), &[])
                .map_err(|error| caught(env, error))?;
            Ok(())
        });
        if let Err(failure) = stopped {
            tracing::error!(%failure, "stopping the network watch failed");
        }
        lock_unpoisoned(&WATCHES).running.remove(&self.number);
    }
}

/// Register the Java watch's native methods.
fn register(env: &mut Env, class: &JClass) -> jni::errors::Result<()> {
    // Safety: each pointer is a function of the signature beside it, for a
    // static native method of `fm.coven.NetworkWatch`.
    unsafe {
        env.register_native_methods(
            class,
            &[
                NativeMethod::from_raw_parts(
                    jni_str!("available"),
                    jni_str!("(JJ)V"),
                    available as *mut c_void,
                ),
                NativeMethod::from_raw_parts(
                    jni_str!("capabilities"),
                    jni_str!("(JJZZ)V"),
                    capabilities as *mut c_void,
                ),
                NativeMethod::from_raw_parts(
                    jni_str!("blocked"),
                    jni_str!("(JJZ)V"),
                    blocked as *mut c_void,
                ),
                NativeMethod::from_raw_parts(
                    jni_str!("lost"),
                    jni_str!("(JJ)V"),
                    lost as *mut c_void,
                ),
                NativeMethod::from_raw_parts(
                    jni_str!("awake"),
                    jni_str!("(J)V"),
                    awake as *mut c_void,
                ),
            ],
        )
    }
}

/// Tell the running watch numbered `watch`, if it still runs. A panic in
/// what the watch does with it goes to its bug sink. Finding the watch
/// takes only the process's registry, whose changes are whole and which
/// nothing here changes; a panic there would end the process, as is right
/// for state the whole process shares.
fn told(watch: jlong, operation: &'static str, tell: impl FnOnce(&mut DefaultNetwork)) {
    let running = lock_unpoisoned(&WATCHES).running.get(&watch).and_then(Weak::upgrade);
    if let Some(running) = running {
        heard(&running.sink, running.bugs.as_ref(), operation, || {
            tell(&mut lock_unpoisoned(&running.network));
        });
    }
}

extern "system" fn available(_env: EnvUnowned, _class: JClass, watch: jlong, network: jlong) {
    told(watch, "hear a default network", |state| state.available(network));
}

extern "system" fn capabilities(
    _env: EnvUnowned,
    _class: JClass,
    watch: jlong,
    network: jlong,
    validated: jboolean,
    metered: jboolean,
) {
    told(watch, "hear a network's capabilities", |state| {
        state.capabilities(network, validated, metered);
    });
}

extern "system" fn blocked(
    _env: EnvUnowned,
    _class: JClass,
    watch: jlong,
    network: jlong,
    blocked: jboolean,
) {
    told(watch, "hear a network blocked", |state| state.blocked(network, blocked));
}

/// The network is gone; 0 when Android had no default network at all.
extern "system" fn lost(_env: EnvUnowned, _class: JClass, watch: jlong, network: jlong) {
    told(watch, "hear a network lost", |state| state.lost((network != 0).then_some(network)));
}

extern "system" fn awake(_env: EnvUnowned, _class: JClass, watch: jlong) {
    told(watch, "hear Doze end", DefaultNetwork::awake);
}
