# Atomic cloud-home connection ownership

## Current contradiction

Cloud setup is split between the host and Coven. The host writes provider
credentials, asks Coven to probe them, restores the previous credentials on a
probe failure, persists its cloud config, generates an opaque-home master key,
records the key fingerprint in a second config field, and finally asks Coven to
connect. Each step can fail after an earlier durable write. The host therefore
duplicates Coven's key state and owns rollback for dependencies retained by
Coven.

OAuth setup has the same split in a different order: the storage layer writes
tokens before the host records the provider config or Coven establishes the
connection. CloudKit records the provider config before its connection has
succeeded.

## Target boundary

The host-to-Coven bridge dispatches every asynchronous Coven call onto a
Coven-owned runtime before constructing the operation's future. The host thread
only submits the operation and awaits its result; it never polls Coven's nested
provider futures on a foreign thread stack. Provider implementations keep their
own runtime ownership where required: the S3 client and all work that constructs
or polls it run on the S3 runtime, keyring calls run on the keyring worker, and
each installed sync loop runs on its prepared sync thread. These are execution
owners, not overlapping retries or alternate call paths.

`CovenHandle` owns cloud setup as one operation. Provider credentials and a
newly generated opaque-home master key remain proposed values while Coven
builds the cloud home, prepares the sync components, and starts the replacement
loop. Coven then persists the proposed key material and installs the prepared
connection. Any failure before installation stops the prepared loop and rolls
back every keyring write made by that attempt; rollback failure is returned with
the original failure.

The credential capability used by provider implementations can be either
durable or proposed. OAuth refresh writes through that same capability, so a
provider cannot bypass the setup transaction by refreshing a token during
connection. After commit, the capability writes refreshes to the durable
store-scoped keyring entry.

Returning stores use a separate connect operation. It never generates a key:
an opaque home whose custody cannot supply its established key is locked and
connection fails loudly. A new-home setup may generate a key because that
operation owns its creation and rollback.

Disconnect and lock also run under the connection lifecycle owner. Disconnect
removes provider credentials before dropping the installed connection; a
credential failure preserves that connection. Forgetting the master key removes
it before dropping every operation that retained its unlocked value; a custody
failure likewise preserves the connection. Neither operation returns while an
installed sync path still retains material it reported as removed.

The public key state is exactly:

- `NotRequired` for browsable homes;
- `Available` when opaque-home custody supplies the key;
- `Locked` when an opaque home has no available key.

No fingerprint or key-presence flag crosses the host boundary. Importing a
serialized key for join and recovery remains a separate custody operation
because the caller already possesses that key; generating a key outside cloud
setup is removed.

Provider setup returns the completed `CloudHomeConfig` and key state only after
the connection is installed. The host persists that returned config. S3
credentials and OAuth tokens never return to the host after entry/authorization;
CloudKit continues to receive its host-supplied driver as an operational
capability, not key material.

## Coven changes

- Keep the bridge boundary as the only host-to-Coven async dispatcher, and audit
  inner runtimes so no layer repeats that dispatch merely to compensate for a
  host stack.
- Replace direct `StoreKeys` access in cloud providers with a retained cloud
  credential custody capability that supports proposed values, commit, and
  rollback.
- Make S3, OAuth, and CloudKit setup methods on `CovenHandle` prepare and install
  the provider connection while owning master-key creation and credential
  persistence.
- Prepare a replacement sync connection before mutating the installed
  connection. A failed proposed connection leaves the previous connection in
  place.
- Replace public master-key fingerprint and initialization calls with the three
  state outcomes above. Keep serialized-key import for join/recovery.
- Keep returning-store `connect_sync` strict: it uses the configured home and
  established custody state and never creates missing material.

## Bae changes

- Route S3, OAuth, and CloudKit setup through the corresponding Coven operation.
- Persist the returned cloud config only after Coven reports a connected home.
- Remove `encryption_key_stored`, `encryption_key_fingerprint`, their config
  writes, bridge fields, launch gates, tests, and UI derivations.
- Replace direct cloud-credential keyring reads/writes and OAuth token custody
  with Coven's setup methods.
- Drive launch and lock UI from Coven's three-state key result.
- Pin every Coven crate to the Coven commit containing this boundary.

## Verification

- A generated bridge call constructs and polls its Coven future on the
  Coven-owned runtime; provider setup cannot overflow a host callback stack.
- The bridge dispatcher, provider runtimes, keyring worker, upload cancellation,
  and sync-loop thread each have one distinct owner and no redundant nested
  dispatcher remains.
- A failed opaque S3 setup leaves neither the proposed credentials nor a newly
  generated master key and preserves an installed prior connection.
- A failure to persist credentials rolls back a newly persisted master key and
  leaves the prior credentials unchanged.
- OAuth authorization followed by connection failure leaves no durable tokens.
- Browsable setup performs no master-key custody write.
- Returning opaque connect reports `Locked` without generating a key.
- Successful S3, OAuth, and CloudKit setup returns the completed config and
  installs a connection whose sync and blob paths use it.
- Bae persists config after success, contains no fingerprint/presence state, and
  reconnects a returning store from Coven's state.
- Coven and Bae builds, tests, self-review, stale-pattern searches, hooks, and
  exact dependency-pin checks pass before each repository fast-forwards main.
