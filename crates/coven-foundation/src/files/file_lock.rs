//! OS file locks that work on every platform coven runs on.
//!
//! std's `File::lock` family returns `Unsupported` on Android: its platform
//! list names `linux` but not `android`. Unix systems therefore take `flock`
//! through rustix, the call std makes on Linux and Apple platforms, so the
//! code Android runs is the code every Unix test runs. Windows keeps std,
//! which locks with `LockFileEx`.

use std::fs::{File, TryLockError};

/// Whether other holders may share the lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Coexists with other shared holders and excludes exclusive ones.
    Shared,
    /// Excludes every other holder.
    Exclusive,
}

/// An OS lock on an open file, released when dropped. Separate opens of one
/// file exclude each other, within a process and across processes.
#[derive(Debug)]
pub struct FileLock {
    file: File,
}

impl FileLock {
    /// Wait for the lock on `file`.
    pub fn lock(file: File, mode: LockMode) -> std::io::Result<Self> {
        os::lock(&file, mode)?;
        Ok(Self { file })
    }

    /// Lock `file` unless another holder excludes it, which is
    /// `TryLockError::WouldBlock`.
    pub fn try_lock(file: File, mode: LockMode) -> Result<Self, TryLockError> {
        os::try_lock(&file, mode)?;
        Ok(Self { file })
    }

    #[cfg(test)]
    pub(crate) fn inherited_file(&self) -> File {
        self.file.try_clone().unwrap()
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // Closing alone leaves the lock held while a concurrently forked child
        // retains the open file before exec closes its inherited descriptors.
        os::unlock(&self.file).expect("release a held file lock");
    }
}

#[cfg(unix)]
mod os {
    use super::LockMode;
    use rustix::fs::{flock, FlockOperation};
    use std::fs::{File, TryLockError};

    pub(super) fn lock(file: &File, mode: LockMode) -> std::io::Result<()> {
        let operation = match mode {
            LockMode::Shared => FlockOperation::LockShared,
            LockMode::Exclusive => FlockOperation::LockExclusive,
        };
        Ok(flock(file, operation)?)
    }

    pub(super) fn try_lock(file: &File, mode: LockMode) -> Result<(), TryLockError> {
        let operation = match mode {
            LockMode::Shared => FlockOperation::NonBlockingLockShared,
            LockMode::Exclusive => FlockOperation::NonBlockingLockExclusive,
        };
        flock(file, operation).map_err(|errno| {
            if errno == rustix::io::Errno::WOULDBLOCK {
                TryLockError::WouldBlock
            } else {
                TryLockError::Error(errno.into())
            }
        })
    }

    pub(super) fn unlock(file: &File) -> std::io::Result<()> {
        Ok(flock(file, FlockOperation::Unlock)?)
    }
}

#[cfg(windows)]
#[expect(
    clippy::disallowed_methods,
    reason = "std locks Windows files with LockFileEx; only its Android stub is refused"
)]
mod os {
    use super::LockMode;
    use std::fs::{File, TryLockError};

    pub(super) fn lock(file: &File, mode: LockMode) -> std::io::Result<()> {
        match mode {
            LockMode::Shared => file.lock_shared(),
            LockMode::Exclusive => file.lock(),
        }
    }

    pub(super) fn try_lock(file: &File, mode: LockMode) -> Result<(), TryLockError> {
        match mode {
            LockMode::Shared => file.try_lock_shared(),
            LockMode::Exclusive => file.try_lock(),
        }
    }

    pub(super) fn unlock(file: &File) -> std::io::Result<()> {
        file.unlock()
    }
}

#[cfg(test)]
#[path = "file_lock_tests.rs"]
mod tests;
