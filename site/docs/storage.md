# Storage

coven has no server of its own. A store lives in cloud storage the household
already has: an S3-compatible bucket, a Dropbox or OneDrive folder, or iCloud
through CloudKit. Everything that keeps a store consistent, such as exactly one
device winning slot 41, rests on a handful of things the provider does. This
page names them, then says how each provider supplies them and how the app
sets storage up.

## What coven needs from a provider

A store lives on exactly one provider. Every member reaches it with their own
provider account, or on S3 their own access key.

- **Create once.** Create an object only if its path is empty, and refuse an
  occupied path without touching its bytes. Ana and Ben both try to create
  slot 41; the provider must store exactly one and refuse the other, whether the
  two requests race or one is a retry, and whether the object goes up in one
  request or in parts. Without this, two devices would each believe they hold
  slot 41 and history would split. A provider that can't do this can't hold a
  store.
- **Conditional replace.** Replace the floor only if its revision is still the
  one the device read, in one step. Without this, two devices moving the floor
  at once could overwrite each other.
- **Complete reads,** whole and by byte range, streamed so the reader checks
  each chunk as it arrives.
- **Exact status.** Asked about one exact path, the provider answers "absent"
  or the object's size, time, id and revision. An error is never absence: a
  timeout read as "absent" would make a present slot look like the end of the
  sequence.
- **Listing** one prefix. The sequence never depends on a listing; listings
  serve files and setup checks.
- **Delete** an object by its exact path.
- **Sharing,** where the provider has it: grant and take back one member's
  access. S3 has no sharing that works the same on every service, so coven
  shares nothing there; each member has their own access key instead.

Times come from the provider, never the device's clock, and coven keeps each
with the precision the provider reported. They describe what the provider did;
they don't order slots or act as deadlines.

## Where a store lives

Every object path starts with the store's id:

| Path | Holds |
| --- | --- |
| `<store>/seq/<n>` | one slot of the sequence, created once |
| `<store>/floor` | the first kept slot and the snapshots each audience relies on; the only object ever replaced |
| `<store>/snapshots/<audience>/<device>/<n>` | one snapshot, found through its announcement |
| `<store>/files/<device>/<file>` | one uploaded file |
| `<store>/join-requests/<invite>` | a join request |

So several stores can share one Dropbox or OneDrive folder, one CloudKit zone,
or one S3 bucket and prefix, except on an S3 service whose member keys reach a
whole bucket ([below](#a-bucket-of-its-own)). Finding the sequence never lists
anything: reading `seq/41` never enumerates Ana's 20,000 photos, or another
store's objects.

An S3 location's base prefix is an
[`S3Prefix`](rustdoc:struct:coven::S3Prefix). Build it with
`S3Prefix::parse(typed)` from what the person typed: slashes at its ends mean
nothing, so `/music/stores/` and `music/stores` are one location, and only
slashes is the bucket's root. Its error,
[`S3PrefixError`](rustdoc:enum:coven::S3PrefixError), names the folder at fault:
two slashes in a row, a character other than an ASCII letter or digit, `-` or
`_`, or a folder name over 255 bytes.

## Setting up storage

A store works on the device before any storage exists. Setting storage up
creates the store at a location, the first time, and connects this device to
it. It returns the person's restore code and starts syncing.

```rust
match handle
    .setup_s3_storage(storage, "Ana's laptop", access_key_id, SecretText::new(secret))
    .await
{
    Ok(setup) => {
        remember(setup.connection.storage);
        show_blurred_qr_code(setup.restore_code.as_str());
    }
    Err(error) => show_setup_error(error),
}
```

There is one call per kind of provider: `setup_s3_storage`,
`setup_oauth_storage` for Dropbox and OneDrive, and `setup_cloudkit_storage`.

Before setup commits anything, it checks the provider with the credentials just
supplied: it creates a fresh sealed test object, requires a second create at
that path to be refused, reads it back whole and by range, runs a scoped
listing, deletes it, and confirms it is gone. It also checks that the floor can
be replaced at a matching revision and not at a stale one. A failure returns
`StorageSetupError::ProviderCheck` naming the
[`StorageCheck`](rustdoc:enum:coven::StorageCheck) that failed, and leaves the
previous connection in place. Credentials, keys, location and restore code
commit together once the connection is ready.

Setting up a store that already exists elsewhere fails with
`StorageFailure::StoreExists`: another installation claimed slot 1 there first,
so this device should restore or join instead. After `disconnect_storage`,
setup reconnects at the recorded location; a different location fails with
`StorageSetupError::LocationChangeUnsupported`, because the store's other
devices still use the old one.

## Dropbox and OneDrive

**Signing in.** coven does the sign-in itself, so no token passes through the
app. The app registers its own provider apps with
[`OAuthClients`](rustdoc:struct:coven::OAuthClients) (coven ships none) and gives
the builder one [`OAuthPresenter`](rustdoc:trait:coven::OAuthPresenter):
something that opens an authorization page and returns the provider's redirect,
or a cancellation. On desktops coven supplies `DesktopOAuthPresenter`, which
opens the browser and listens locally; iOS and Android apps supply their
platform's sign-in sheet. `authenticate` runs the sign-in; coven builds the
request, checks the redirect, exchanges the code and keeps the tokens.

```rust
handle.authenticate(OAuthAccess::OneDrive).await?;
handle
    .setup_oauth_storage(StorageConfig::OneDrive { drive_id, folder_id }, "Ana's laptop")
    .await?;
```

Coven uses a token until the provider rejects it with HTTP 401, then refreshes
once and retries that request once. If that fails, the caller gets the failure;
`OAuthError::Reauthorize` means the person has to sign in again. Token expiry
never consults the device clock: Ben sets his laptop's clock back a year, and
his token still works until Dropbox rejects it.

**Dropbox access levels.** Dropbox offers two kinds of app key. An *App-folder*
key reaches only the app's own folder, and Dropbox refuses sharing inside it. A
*Full Dropbox* key reaches the whole account and can share. The level is fixed
when the store is created, by its
[`DropboxLocation`](rustdoc:enum:coven::DropboxLocation): `AppFolder` for a
store only one person will use, `SharedFolder { namespace_id }` for one that can
be shared. With both keys registered, the app asks the person; there is no
default. Ana chooses not-shared for her Notes store; when she later tries to
invite Carol, `create_invite` fails with
`StorageFailure::DropboxAppFolderNotShareable` before changing anything at
Dropbox.

On Dropbox, OneDrive and iCloud, coven shares a store and takes sharing back
only from the account that holds it. That is coven's rule; Dropbox editors and
CloudKit administrators could also do it
([Sharing](/docs/sharing#provider-access)).

## iCloud (CloudKit)

CloudKit is built into coven on Apple platforms. The app supplies its container
identifier and the iCloud entitlement for it, and calls
`setup_cloudkit_storage(container, device_name)`. The store is a zone in the
owner's private database, shared with members through a CloudKit share. Other
platforms refuse CloudKit at setup with `StorageFailure::ProviderUnavailable`.

## S3 services

S3 is a protocol many services implement, and they differ on exactly the
points coven depends on: which header makes a write create-once, whether
multipart uploads honour it, how objects are dated. Garage, for example,
accepts a write to an occupied name, so it can't hold a store.

coven describes a service's behaviour as data,
[`S3Settings`](rustdoc:struct:coven::S3Settings), one choice per behaviour. A
location's `profile` picks them:

- **none:** the endpoint's host picks the service. No endpoint is AWS,
  `storage.googleapis.com` is Google Cloud Storage, and hosts ending in
  `.r2.cloudflarestorage.com` are Cloudflare R2;
- **a known service by name** (`S3Profile::Named`): `Aws`, `Gcs`, `R2`, or one
  of the self-hosted `RustFs`, `SeaweedFs`, `CephRgw` and `VersityGw`, which have
  no fixed hostname;
- **custom settings** (`S3Profile::Custom`).

An endpoint that matches no known host and names no profile is detected at
setup, and its settings are recorded as the location's custom profile, so the
restore code and every other device use the same ones.

**Detection.** `handle.detect_s3_settings(..)` probes a bucket with the
person's key, writing only fresh objects under the store's `.coven-check`
prefix and deleting them afterwards. Its [`S3Detection`](rustdoc:struct:coven::S3Detection)
holds the detected settings and one result per probe, so an app with an
advanced-settings screen can prefill it, and `detection.check(&settings)` says
whether an override agrees with what was observed. Setup runs detection on
every S3 location anyway and refuses settings that contradict it, naming the
first failed check: `CreateOnce`, `MultipartCreateOnce`, `PublicationTime` or
`Behavior`. A service no setting makes safe, such as Garage, fails
`CreateOnce`.

### A bucket of its own

Each member has their own S3 access key, limited as narrowly as the service
allows. On AWS, GCS, RustFS, SeaweedFS, Ceph RGW and Versity Gateway a key can
be limited to the store's prefix, so stores there can share a bucket. R2's
API tokens reach a whole bucket (its temporary credentials can be limited to a
prefix, but last at most 7 days), and so does every custom profile; there, a
member of one store could delete another store's objects in the same bucket. So on
those services **every store has its own bucket from creation**. A location
with a base prefix fails with `StorageFailure::OwnBucketRequired { reason:
Prefix }`, and creating a store in a bucket that holds anything else fails with
`reason: Occupied`. Ana creates Household in the R2 bucket `household`;
creating Garden in that bucket is refused, so Garden gets the bucket `garden`.

### Versioned buckets

A versioned bucket can break create-once in a way one-at-a-time tests don't
see: a versioned Ceph RGW bucket accepted two racing creates of one name as
separate versions. So setup reads the bucket's versioning state, and `Enabled`
or `Suspended` requires the profile to be certified with versioning on;
otherwise setup fails with `StorageSetupError::S3VersioningUnsupported`. Setup
never changes the bucket's versioning.

### Certification

A service is *certified* when coven's live suite has raced many writers against
a real bucket and every race kept exactly one winner. Racing matters:
SeaweedFS 4.48 refused occupied names one request at a time, but two racing
multipart completions of one name both succeeded, so its profile now publishes
every object by a conditional copy, which passed.

| Service | Certified |
| --- | --- |
| Google Cloud Storage | yes, versioning off only |
| Cloudflare R2 | yes (R2 has no versioning) |
| RustFS 1.0.1 | yes, versioning off and on |
| SeaweedFS 4.48 | yes, versioning off and on, publishing every object by copy |
| Ceph RGW 20.2.4 | versioning off only |
| Versity Gateway 1.9.0 | yes, versioning off and on |
| AWS S3 | no: there is no live account to run against |

Certification never skips setup's own checks. Every S3 location, certified or
not, runs them.

**Clock skew.** S3 signs requests with the device's clock. When a service
answers `RequestTimeTooSkewed`, coven retries once using the offset the
response's date gives; if that still fails, it reports
`StorageFailure::ClockSkew`.

**Member keys.** The person makes and deletes each member's S3 key in the
provider's console. [Sharing](/docs/sharing#s3-access-keys) covers it.

## Large objects

A file or writes slot up to the provider's single-request limit, or 32 MiB,
whichever is less, goes up in one create. A larger one goes up in parts. Where
the provider's own resumable upload keeps unfinished parts invisible, refuses an
occupied name and dates the object by when it was complete, coven uses it:
Dropbox's upload sessions, CloudKit's atomic save, and conditional multipart
completion on most S3 services. Where it doesn't, coven uploads to a private
temporary at `<store>/.coven-uploads/<session>` and publishes it with one copy
that refuses an occupied name: OneDrive always, and AWS and GCS for multipart
uploads.

One S3 copy moves at most 5 GiB, so on a profile that publishes by copy (AWS,
SeaweedFS) a larger object fails with `StorageFailure::CopyTooLarge` before any
request.

## Errors

Every storage failure carries one [`StorageFailure`](rustdoc:enum:coven::StorageFailure)
classification through setup, sync status, uploads and reads, inside a
[`StorageError`](rustdoc:enum:coven::StorageError) that keeps the provider's
native cause. `Network` is no route, a timeout or an interrupted response;
`Authentication` and `PermissionDenied` are refused credentials or access;
`QuotaExceeded`, `RateLimited`, `NotFound`, `AlreadyExists`, `ContainerNotFound`
and the rest name what the provider said. `error.retryable()` is true only for
network interruptions and throttling. A provider's response body is never
printed, because providers can echo credentials.

## What a store costs

A quiet pass makes two requests: the next slot, which is missing, and the
floor. Each kind of work adds its own:

| Work | Adds |
| --- | --- |
| a slot arrives | its read |
| a claim | its create, and a floor read after it lands |
| losing a race | the occupied slot's read, and each further attempt |
| admin preparation | its actual requests: key copies, access changes, snapshot uploads |
| a snapshot | its upload and its announcement |
| a reload | the floor's snapshots, and the slots after them |
| deleting history | one conditional floor write, and the deletes |
| files | their own reads, upload parts and deletes, outside the pass |

A device far behind finds the end of the sequence in a logarithmic number of
existence checks, then reads each slot it still needs or reloads from
snapshots. Provider metadata requests, redirects, listing pages, upload parts
and retries are counted separately.
