//! What lists a store's unfinished first installation for the app to
//! resume, and reclaiming one that nothing refers to (§13.4, §11.6).

use super::*;

/// What lists a store's unfinished first installation for its call to
/// resume: a join's saved code or a restore's record, never both (§13.4).
pub(super) enum Kept {
    Join(SecretText),
    Restore(SecretText),
    Nothing,
}

/// Begin a call's first installation of `id`. One an earlier call left,
/// which no saved code or record lists, is removed first: a join that
/// stopped before it saved its code, or after it deleted it, holds a saved
/// request that only the code it was made for opens, and keeping it would
/// fail every later invite of the store (§13.4).
pub(super) async fn begin_installation(
    builder: &CovenBuilder,
    id: StoreId,
    entries: &StoreKeychain,
) -> Result<BootstrapStore, BootstrapError> {
    let pending = builder.directories.begin_bootstrap(id, builder.ids.as_ref()).await?;
    if !pending.resumed() || entries.invite_code()?.is_some() || entries.restore_record()?.is_some()
    {
        return Ok(pending);
    }
    tracing::info!(store = %id, "removing an unfinished installation nothing refers to");
    pending.cancel()?;
    drop(pending);
    Ok(builder.directories.begin_bootstrap(id, builder.ids.as_ref()).await?)
}

/// Read what lists the store's unfinished first installation. One that keeps
/// neither a join's saved code nor a restore's record is what a crash or a
/// dropped call left before either could write to storage: nothing refers
/// to it, so once no call holds it, its space is reclaimed. Read again under
/// the installation's lock, so a call that began meanwhile is never touched
/// (§13.4, §11.6). An entry coven can't read fails the call.
pub(super) async fn kept(
    builder: &CovenBuilder,
    entries: &StoreKeychain,
    store: StoreId,
) -> Result<Kept, BootstrapError> {
    let read = || -> Result<Kept, BootstrapError> {
        if let Some(code) = entries.invite_code()? {
            return Ok(Kept::Join(code));
        }
        Ok(entries.restore_record()?.map_or(Kept::Nothing, |(code, _)| Kept::Restore(code)))
    };
    match read()? {
        Kept::Nothing => {}
        kept => return Ok(kept),
    }
    // A call holds it: what it keeps by now is what lists it.
    let Some(lock) = builder.directories.lock_unfinished_first(store).await? else {
        return read();
    };
    let kept = read()?;
    if let Kept::Nothing = kept {
        tracing::info!(%store, "reclaiming an unfinished installation nothing refers to");
        let bugs = builder.directories.bugs().clone();
        crate::task::blocking_completing("reclaim an unfinished installation", bugs, move || {
            lock.remove_directory()
        })
        .await
        .map_err(coven_foundation::files::FileError::from)
        .and_then(|removed| removed)
        .map_err(BootstrapDirectoryError::from)?;
    }
    Ok(kept)
}
