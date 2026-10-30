//! Google's Block Store, reached through JNI with the application context the
//! app gives `ndk-context` (api.md E1).
//!
//! Play services' classes live in the app's APK, not in the boot class path,
//! so a thread attached from Rust loads them through the context's class
//! loader: `FindClass` there would see only the framework's classes. Each call
//! blocks on its `Task` with `Tasks.await`, which Android refuses on the main
//! thread; custody calls run off it.

use super::block_store::{BlockStoreApi, BlockStoreFailure};
use super::KeyError;
use crate::SecretBytes;
use jni::objects::{
    Global, JByteArray, JClass, JClassLoader, JObject, JObjectArray, JString, JValue,
};
use jni::signature::MethodSignature;
use jni::strings::JNIStr;
use jni::{jni_sig, jni_str, Env, JValueOwned, JavaVM};

const BLOCKSTORE: &str = "com.google.android.gms.auth.blockstore.Blockstore";
const STORE_BYTES_DATA_BUILDER: &str =
    "com.google.android.gms.auth.blockstore.StoreBytesData$Builder";
const RETRIEVE_BYTES_REQUEST_BUILDER: &str =
    "com.google.android.gms.auth.blockstore.RetrieveBytesRequest$Builder";
const DELETE_BYTES_REQUEST_BUILDER: &str =
    "com.google.android.gms.auth.blockstore.DeleteBytesRequest$Builder";
const TASKS: &str = "com.google.android.gms.tasks.Tasks";
const GOOGLE_API_AVAILABILITY: &str = "com.google.android.gms.common.GoogleApiAvailability";
const API_EXCEPTION: &str = "com.google.android.gms.common.api.ApiException";
const EXECUTION_EXCEPTION: &str = "java.util.concurrent.ExecutionException";
const COLLECTIONS: &str = "java.util.Collections";
/// `ConnectionResult.SUCCESS`.
const PLAY_SERVICES_AVAILABLE: i32 = 0;

/// The app's Block Store client, built once Google Play services answered
/// that it is available.
pub(crate) struct JniBlockStore {
    vm: JavaVM,
    context: Global<JObject<'static>>,
    loader: Global<JClassLoader<'static>>,
}

/// What a JNI call failed with: the JVM refused it, or a Java exception that
/// was caught and described.
enum Failure {
    Jni(jni::errors::Error),
    Java(BlockStoreFailure),
}

impl From<jni::errors::Error> for Failure {
    fn from(error: jni::errors::Error) -> Self {
        Self::Jni(error)
    }
}

impl From<Failure> for BlockStoreFailure {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Java(failure) => failure,
            Failure::Jni(error) => BlockStoreFailure {
                exception: "JNI".to_owned(),
                status: None,
                message: Some(error.to_string()),
            },
        }
    }
}

impl JniBlockStore {
    /// The client, or `None` on a phone without Google Play services: it has
    /// no Block Store, and keeps no saved entries (§13.1). Missing Block Store
    /// classes mean the app was built without its dependency, and fail.
    pub(crate) fn new() -> Result<Option<Self>, KeyError> {
        let android = ndk_context::android_context();
        // Safety: ndk-context holds the process's JavaVM pointer, set by the app.
        let vm = unsafe { JavaVM::from_raw(android.vm().cast()) };
        let built = vm.attach_current_thread_for_scope(|env| -> Result<_, Failure> {
            // Safety: ndk-context holds a global reference to the app's
            // context for the life of the process.
            let context = unsafe { JObject::from_raw(env, android.context().cast()) };
            let loader = call(
                env,
                &context,
                jni_str!("getClassLoader"),
                jni_sig!(() -> JClassLoader),
                &[],
            )?
            .l()?;
            let loader = env.cast_local::<JClassLoader>(loader)?;
            Ok(Self {
                vm: vm.clone(),
                context: env.new_global_ref(&context)?,
                loader: env.new_global_ref(loader)?,
            })
        });
        let client = built.map_err(|failure| unavailable(failure.into()))?;
        let status = client
            .with_env(|env, client| {
                // Every class a call needs loads here, so a missing one fails
                // building the keychain, not some later save.
                for name in [
                    BLOCKSTORE,
                    STORE_BYTES_DATA_BUILDER,
                    RETRIEVE_BYTES_REQUEST_BUILDER,
                    DELETE_BYTES_REQUEST_BUILDER,
                    TASKS,
                ] {
                    client.class(env, name)?;
                }
                let availability = client.class(env, GOOGLE_API_AVAILABILITY)?;
                let instance = call_static(
                    env,
                    &availability,
                    jni_str!("getInstance"),
                    jni_sig!(() -> com.google.android.gms.common.GoogleApiAvailability),
                    &[],
                )?
                .l()?;
                let context = client.context.as_obj();
                Ok(call(
                    env,
                    &instance,
                    jni_str!("isGooglePlayServicesAvailable"),
                    jni_sig!((android.content.Context) -> jint),
                    &[JValue::from(context)],
                )?
                .i()?)
            })
            .map_err(unavailable)?;
        if status == PLAY_SERVICES_AVAILABLE {
            Ok(Some(client))
        } else {
            tracing::warn!(
                status,
                "Google Play services is unavailable: this phone keeps no saved restore codes"
            );
            Ok(None)
        }
    }

    fn with_env<T>(
        &self,
        f: impl FnOnce(&mut Env, &Self) -> Result<T, Failure>,
    ) -> Result<T, BlockStoreFailure> {
        self.vm
            .attach_current_thread_for_scope(|env| f(env, self))
            .map_err(BlockStoreFailure::from)
    }

    /// Load a class through the app's class loader.
    fn class<'local>(&self, env: &mut Env<'local>, name: &str) -> Result<JClass<'local>, Failure> {
        let name = env.new_string(name)?;
        self.loader
            .load_class(env, name)
            .map_err(|error| caught(env, error))
    }

    fn client<'local>(&self, env: &mut Env<'local>) -> Result<JObject<'local>, Failure> {
        let blockstore = self.class(env, BLOCKSTORE)?;
        let context = self.context.as_obj();
        Ok(call_static(
            env,
            &blockstore,
            jni_str!("getClient"),
            jni_sig!(
                (android.content.Context)
                    -> com.google.android.gms.auth.blockstore.BlockstoreClient
            ),
            &[JValue::from(context)],
        )?
        .l()?)
    }

    /// Wait for a `Task` and take its result; a failed task's exception is
    /// described, with its `ApiException` status.
    fn wait<'local>(
        &self,
        env: &mut Env<'local>,
        task: &JObject,
    ) -> Result<JObject<'local>, Failure> {
        let tasks = self.class(env, TASKS)?;
        Ok(call_static(
            env,
            &tasks,
            jni_str!("await"),
            jni_sig!((com.google.android.gms.tasks.Task) -> java.lang.Object),
            &[JValue::from(task)],
        )?
        .l()?)
    }

    /// A builder's `build()`, after the builder's setters ran.
    fn new_builder<'local>(
        &self,
        env: &mut Env<'local>,
        name: &str,
    ) -> Result<JObject<'local>, Failure> {
        let class = self.class(env, name)?;
        env.new_object(&class, jni_sig!(() -> void), &[])
            .map_err(|error| caught(env, error))
    }

    fn singleton_list<'local>(
        &self,
        env: &mut Env<'local>,
        key: &str,
    ) -> Result<JObject<'local>, Failure> {
        let collections = self.class(env, COLLECTIONS)?;
        let key = env.new_string(key)?;
        Ok(call_static(
            env,
            &collections,
            jni_str!("singletonList"),
            jni_sig!((java.lang.Object) -> java.util.List),
            &[JValue::from(&key)],
        )?
        .l()?)
    }

    /// The response map of a `RetrieveBytesRequest` built by `configure`.
    fn retrieve_map<'local>(
        &self,
        env: &mut Env<'local>,
        configure: impl FnOnce(&mut Env<'local>, &JObject<'local>) -> Result<(), Failure>,
    ) -> Result<JObject<'local>, Failure> {
        let builder = self.new_builder(env, RETRIEVE_BYTES_REQUEST_BUILDER)?;
        configure(env, &builder)?;
        let request = call(
            env,
            &builder,
            jni_str!("build"),
            jni_sig!(() -> com.google.android.gms.auth.blockstore.RetrieveBytesRequest),
            &[],
        )?
        .l()?;
        let client = self.client(env)?;
        let task = call(
            env,
            &client,
            jni_str!("retrieveBytes"),
            jni_sig!(
                (com.google.android.gms.auth.blockstore.RetrieveBytesRequest)
                    -> com.google.android.gms.tasks.Task
            ),
            &[JValue::from(&request)],
        )?
        .l()?;
        let response = self.wait(env, &task)?;
        Ok(call(
            env,
            &response,
            jni_str!("getBlockstoreDataMap"),
            jni_sig!(() -> java.util.Map),
            &[],
        )?
        .l()?)
    }
}

/// Call an instance method; a Java exception it throws is caught and described.
fn call<'local, 'sig, 'args>(
    env: &mut Env<'local>,
    object: &JObject,
    name: &JNIStr,
    sig: impl AsRef<MethodSignature<'sig, 'args>>,
    args: &[JValue],
) -> Result<JValueOwned<'local>, Failure> {
    env.call_method(object, name, sig, args)
        .map_err(|error| caught(env, error))
}

/// Call a static method; a Java exception it throws is caught and described.
fn call_static<'local, 'sig, 'args>(
    env: &mut Env<'local>,
    class: &JClass,
    name: &JNIStr,
    sig: impl AsRef<MethodSignature<'sig, 'args>>,
    args: &[JValue],
) -> Result<JValueOwned<'local>, Failure> {
    env.call_static_method(class, name, sig, args)
        .map_err(|error| caught(env, error))
}

/// A `BlockstoreData`'s bytes.
fn data_bytes(env: &mut Env, data: &JObject) -> Result<SecretBytes, Failure> {
    let bytes = call(env, data, jni_str!("getBytes"), jni_sig!(() -> byte[]), &[])?.l()?;
    let bytes = env.cast_local::<JByteArray>(bytes)?;
    Ok(SecretBytes::new(env.convert_byte_array(&bytes)?))
}

impl BlockStoreApi for JniBlockStore {
    fn store(
        &self,
        key: &str,
        bytes: &[u8],
        backup_to_cloud: bool,
    ) -> Result<(), BlockStoreFailure> {
        self.with_env(|env, client| {
            let builder = client.new_builder(env, STORE_BYTES_DATA_BUILDER)?;
            let array = env.byte_array_from_slice(bytes)?;
            let key = env.new_string(key)?;
            for (name, sig, value) in [
                (
                    jni_str!("setBytes"),
                    jni_sig!(
                        (byte[]) -> com.google.android.gms.auth.blockstore.StoreBytesData::Builder
                    ),
                    JValue::from(&array),
                ),
                (
                    jni_str!("setKey"),
                    jni_sig!(
                        (java.lang.String)
                            -> com.google.android.gms.auth.blockstore.StoreBytesData::Builder
                    ),
                    JValue::from(&key),
                ),
                (
                    jni_str!("setShouldBackupToCloud"),
                    jni_sig!(
                        (jboolean) -> com.google.android.gms.auth.blockstore.StoreBytesData::Builder
                    ),
                    JValue::Bool(backup_to_cloud),
                ),
            ] {
                call(env, &builder, name, sig, &[value])?;
            }
            let data = call(
                env,
                &builder,
                jni_str!("build"),
                jni_sig!(() -> com.google.android.gms.auth.blockstore.StoreBytesData),
                &[],
            )?
            .l()?;
            let blockstore = client.client(env)?;
            let task = call(
                env,
                &blockstore,
                jni_str!("storeBytes"),
                jni_sig!(
                    (com.google.android.gms.auth.blockstore.StoreBytesData)
                        -> com.google.android.gms.tasks.Task
                ),
                &[JValue::from(&data)],
            )?
            .l()?;
            client.wait(env, &task)?;
            Ok(())
        })
    }

    fn retrieve(&self, key: &str) -> Result<Option<SecretBytes>, BlockStoreFailure> {
        self.with_env(|env, client| {
            let keys = client.singleton_list(env, key)?;
            let map = client.retrieve_map(env, |env, builder| {
                call(
                    env,
                    builder,
                    jni_str!("setKeys"),
                    jni_sig!(
                        (java.util.List)
                            -> com.google.android.gms.auth.blockstore.RetrieveBytesRequest::Builder
                    ),
                    &[JValue::from(&keys)],
                )?;
                Ok(())
            })?;
            let key = env.new_string(key)?;
            let data = call(
                env,
                &map,
                jni_str!("get"),
                jni_sig!((java.lang.Object) -> java.lang.Object),
                &[JValue::from(&key)],
            )?
            .l()?;
            if data.is_null() {
                return Ok(None);
            }
            Ok(Some(data_bytes(env, &data)?))
        })
    }

    fn retrieve_all(&self) -> Result<Vec<(String, SecretBytes)>, BlockStoreFailure> {
        self.with_env(|env, client| {
            let map = client.retrieve_map(env, |env, builder| {
                call(
                    env,
                    builder,
                    jni_str!("setRetrieveAll"),
                    jni_sig!(
                        (jboolean)
                            -> com.google.android.gms.auth.blockstore.RetrieveBytesRequest::Builder
                    ),
                    &[JValue::Bool(true)],
                )?;
                Ok(())
            })?;
            let keys = call(
                env,
                &map,
                jni_str!("keySet"),
                jni_sig!(() -> java.util.Set),
                &[],
            )?
            .l()?;
            let keys = call(
                env,
                &keys,
                jni_str!("toArray"),
                jni_sig!(() -> java.lang.Object[]),
                &[],
            )?
            .l()?;
            let keys = env.cast_local::<JObjectArray>(keys)?;
            let mut entries = Vec::new();
            for index in 0..keys.len(env)? {
                let key = keys.get_element(env, index)?;
                let data = call(
                    env,
                    &map,
                    jni_str!("get"),
                    jni_sig!((java.lang.Object) -> java.lang.Object),
                    &[JValue::from(&key)],
                )?
                .l()?;
                let name = env.cast_local::<JString>(key)?.try_to_string(env)?;
                entries.push((name, data_bytes(env, &data)?));
            }
            Ok(entries)
        })
    }

    fn delete(&self, key: &str) -> Result<(), BlockStoreFailure> {
        self.with_env(|env, client| {
            let keys = client.singleton_list(env, key)?;
            let builder = client.new_builder(env, DELETE_BYTES_REQUEST_BUILDER)?;
            call(
                env,
                &builder,
                jni_str!("setKeys"),
                jni_sig!(
                    (java.util.List)
                        -> com.google.android.gms.auth.blockstore.DeleteBytesRequest::Builder
                ),
                &[JValue::from(&keys)],
            )?;
            let request = call(
                env,
                &builder,
                jni_str!("build"),
                jni_sig!(() -> com.google.android.gms.auth.blockstore.DeleteBytesRequest),
                &[],
            )?
            .l()?;
            let blockstore = client.client(env)?;
            let task = call(
                env,
                &blockstore,
                jni_str!("deleteBytes"),
                jni_sig!(
                    (com.google.android.gms.auth.blockstore.DeleteBytesRequest)
                        -> com.google.android.gms.tasks.Task
                ),
                &[JValue::from(&request)],
            )?
            .l()?;
            // The task's Boolean says whether anything was deleted; an absent
            // key is already the state asked for.
            client.wait(env, &task)?;
            Ok(())
        })
    }

    fn end_to_end_encryption_available(&self) -> Result<bool, BlockStoreFailure> {
        self.with_env(|env, client| {
            let blockstore = client.client(env)?;
            let task = call(
                env,
                &blockstore,
                jni_str!("isEndToEndEncryptionAvailable"),
                jni_sig!(() -> com.google.android.gms.tasks.Task),
                &[],
            )?
            .l()?;
            let available = client.wait(env, &task)?;
            Ok(call(
                env,
                &available,
                jni_str!("booleanValue"),
                jni_sig!(() -> jboolean),
                &[],
            )?
            .z()?)
        })
    }
}

/// Take a pending Java exception off the thread and describe it: its class,
/// message, and the status of the `ApiException` it is or wraps. A JNI error
/// with no exception pending stays as it is.
fn caught(env: &mut Env, error: jni::errors::Error) -> Failure {
    if !matches!(error, jni::errors::Error::JavaException) {
        return Failure::Jni(error);
    }
    let Some(throwable) = env.exception_occurred() else {
        return Failure::Jni(error);
    };
    env.exception_clear();
    match describe(env, &throwable) {
        Ok(failure) => Failure::Java(failure),
        Err(describing) => {
            env.exception_clear();
            Failure::Jni(describing)
        }
    }
}

fn describe(env: &mut Env, throwable: &JObject) -> Result<BlockStoreFailure, jni::errors::Error> {
    let exception = class_name(env, throwable)?;
    let cause = if exception == EXECUTION_EXCEPTION {
        env.call_method(
            throwable,
            jni_str!("getCause"),
            jni_sig!(() -> java.lang.Throwable),
            &[],
        )?
        .l()?
    } else {
        JObject::null()
    };
    let api_exception = if cause.is_null() { throwable } else { &cause };
    let status = if class_name(env, api_exception)? == API_EXCEPTION {
        Some(
            env.call_method(
                api_exception,
                jni_str!("getStatusCode"),
                jni_sig!(() -> jint),
                &[],
            )?
            .i()?,
        )
    } else {
        None
    };
    let message = env
        .call_method(
            api_exception,
            jni_str!("getMessage"),
            jni_sig!(() -> java.lang.String),
            &[],
        )?
        .l()?;
    let message = if message.is_null() {
        None
    } else {
        Some(env.cast_local::<JString>(message)?.try_to_string(env)?)
    };
    Ok(BlockStoreFailure {
        exception: class_name(env, api_exception)?,
        status,
        message,
    })
}

fn class_name(env: &mut Env, object: &JObject) -> Result<String, jni::errors::Error> {
    let class = env.get_object_class(object)?;
    let name = env
        .call_method(
            &class,
            jni_str!("getName"),
            jni_sig!(() -> java.lang.String),
            &[],
        )?
        .l()?;
    env.cast_local::<JString>(name)?.try_to_string(env)
}

fn unavailable(failure: BlockStoreFailure) -> KeyError {
    KeyError::Unavailable(Box::new(failure))
}
