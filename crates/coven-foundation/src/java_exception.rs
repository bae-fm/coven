//! A Java exception a JNI call threw, taken off the thread and described,
//! for coven's Android code: Block Store's calls (§13.1) and the network
//! watch (api.md E5). A JNI call that throws leaves the exception pending
//! on the thread, where any later call would fail; this clears it and keeps
//! what it said.

use jni::objects::{JObject, JString};
use jni::{jni_sig, jni_str, Env};

const API_EXCEPTION: &str = "com.google.android.gms.common.api.ApiException";
const EXECUTION_EXCEPTION: &str = "java.util.concurrent.ExecutionException";

/// What a Java exception said.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JavaException {
    /// The exception's class name: the cause's, when an `ExecutionException`
    /// wraps one.
    pub exception: String,
    /// The `ApiException` status code, when it is one.
    pub status: Option<i32>,
    /// The exception's message, when it has one.
    pub message: Option<String>,
}

impl std::fmt::Display for JavaException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.exception)?;
        if let Some(status) = self.status {
            write!(f, ", status {status}")?;
        }
        if let Some(message) = &self.message {
            write!(f, ": {message}")?;
        }
        Ok(())
    }
}

impl std::error::Error for JavaException {}

/// What a JNI call failed with: the JVM refused it, or a Java exception,
/// taken off the thread.
#[derive(Debug)]
pub enum JniFailure {
    /// The JVM refused the call.
    Jni(jni::errors::Error),
    /// The call threw this exception.
    Java(JavaException),
}

impl From<jni::errors::Error> for JniFailure {
    fn from(error: jni::errors::Error) -> Self {
        Self::Jni(error)
    }
}

impl std::fmt::Display for JniFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Jni(error) => write!(f, "JNI: {error}"),
            Self::Java(exception) => exception.fmt(f),
        }
    }
}

impl std::error::Error for JniFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Jni(error) => Some(error),
            Self::Java(exception) => Some(exception),
        }
    }
}

/// Take a pending Java exception off the thread and describe it: its class,
/// message, and the status of the `ApiException` it is or wraps. A JNI
/// error with no exception pending stays as it is.
pub fn caught(env: &mut Env, error: jni::errors::Error) -> JniFailure {
    if !matches!(error, jni::errors::Error::JavaException) {
        return JniFailure::Jni(error);
    }
    let Some(throwable) = env.exception_occurred() else {
        return JniFailure::Jni(error);
    };
    env.exception_clear();
    match describe(env, &throwable) {
        Ok(exception) => JniFailure::Java(exception),
        Err(describing) => {
            env.exception_clear();
            JniFailure::Jni(describing)
        }
    }
}

fn describe(env: &mut Env, throwable: &JObject) -> Result<JavaException, jni::errors::Error> {
    let exception = class_name(env, throwable)?;
    let cause = if exception == EXECUTION_EXCEPTION {
        env.call_method(throwable, jni_str!("getCause"), jni_sig!(() -> java.lang.Throwable), &[])?
            .l()?
    } else {
        JObject::null()
    };
    let described = if cause.is_null() { throwable } else { &cause };
    let exception = class_name(env, described)?;
    let status = if exception == API_EXCEPTION {
        Some(
            env.call_method(described, jni_str!("getStatusCode"), jni_sig!(() -> jint), &[])?
                .i()?,
        )
    } else {
        None
    };
    let message = env
        .call_method(described, jni_str!("getMessage"), jni_sig!(() -> java.lang.String), &[])?
        .l()?;
    let message = if message.is_null() {
        None
    } else {
        Some(env.cast_local::<JString>(message)?.try_to_string(env)?)
    };
    Ok(JavaException { exception, status, message })
}

fn class_name(env: &mut Env, object: &JObject) -> Result<String, jni::errors::Error> {
    let class = env.get_object_class(object)?;
    let name =
        env.call_method(&class, jni_str!("getName"), jni_sig!(() -> java.lang.String), &[])?.l()?;
    env.cast_local::<JString>(name)?.try_to_string(env)
}
