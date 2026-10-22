# Native CloudKit conformance host

The ignored `providers::cloudkit::tests::cloudkit_live` test calls the real adapter.
It creates an isolated custom zone in `iCloud.fm.bae.desktop`, exercises create-once,
exact status, publication metadata, conditional floor replacement revisions,
ranges, recorded multipart continuation, paginated directory queries and deletion. It keeps incomplete uploads pending while checking
listings, puts unrelated objects in the same zone and a second zone, waits one
hour before checking completeness, and deletes both test zones after success.
It is never part of an ordinary cargo test run.

The container is the bae app's own, and real stores use the same
`coven-<store>` zone names, so every live test saves a `CovenLiveTestZone`
marker record named `coven-live-test-zone` into each zone it creates before
anything else, naming the test in its `test` field. No store writes that record
type or name, and a test zone can only be deleted after its marker is read
back. A failed assertion in the conformance or two-account tests leaves the
zone available for inspection. After
investigating, run the ignored
`providers::cloudkit::tests::live_zone::tests::cloudkit_live_remove_leftover_test_zones`
test from the same host. It deletes only `coven-<store>` zones holding the
marker, never touches any other zone, and prints each zone it found, deleted
and kept. Zones from runs before the marker existed carry none and are kept;
remove those through CloudKit Console.

Use macOS 12 or later, signed in to an iCloud account. The host is an `.app` bundle
with `CFBundleIdentifier` **`fm.bae.desktop`**, `CFBundlePackageType` **`APPL`**,
`CFBundleExecutable` **`CovenCloudKitTests`**, and ordinary bundle version keys.
Use an existing Apple Development signing identity and a macOS development
provisioning profile authorizing that bundle id, this Mac, and the container.
Embed the profile at `Contents/embedded.provisionprofile`. No certificates,
identifiers, containers, profiles or other developer resources are created by
these tests or by the preparation commands below.

The signed executable must claim these entitlements, with identity values from
that profile (the App ID prefix and Team ID must not be guessed):

```xml
<dict>
  <key>com.apple.application-identifier</key>
  <string>APP_ID_PREFIX.fm.bae.desktop</string>
  <key>com.apple.developer.team-identifier</key>
  <string>TEAM_ID</string>
  <key>com.apple.developer.icloud-container-identifiers</key>
  <array><string>iCloud.fm.bae.desktop</string></array>
  <key>com.apple.developer.icloud-container-environment</key>
  <string>Development</string>
  <key>com.apple.developer.icloud-services</key>
  <array><string>CloudKit</string></array>
</dict>
```

If the selected development profile also uses
`com.apple.developer.icloud-container-development-container-identifiers`, copy
its authorized development container array into the signature. A sandboxed host
also needs `com.apple.security.app-sandbox = true` and
`com.apple.security.network.client = true`. The test does not use subscriptions,
push notifications, iCloud Documents, or key-value storage.
Apple documents the identity/profile relationship in
[TN3125](https://developer.apple.com/documentation/technotes/tn3125-inside-code-signing-provisioning-profiles),
and the container environment in
[CKContainer](https://developer.apple.com/documentation/cloudkit/ckcontainer).
The entitlement definitions are
[iCloud services](https://developer.apple.com/documentation/bundleresources/entitlements/com.apple.developer.icloud-services),
[container identifiers](https://developer.apple.com/documentation/bundleresources/entitlements/com.apple.developer.icloud-container-identifiers),
and [Development environment](https://developer.apple.com/documentation/bundleresources/entitlements/com.apple.developer.icloud-container-environment).

Build without running:

```sh
PATH=$HOME/.elan/bin:$PATH \
  cargo test -p coven-storage --lib --no-run --message-format=json \
  > /private/tmp/coven-cloudkit-test-build.json
```

Read the `executable` field of the `compiler-artifact` record for the
`coven_storage` library test. Copy that executable into
`CovenCloudKitTests.app/Contents/MacOS/CovenCloudKitTests`, add the Info.plist and
profile described above, and sign the bundle with the authorized entitlements.
The Rust test binary is the host executable: its generated `libtest` main is the
entry point, and the selected `#[tokio::test]` creates its runtime and awaits the
native completion channels. No Swift callback implementation or FFI test shim
is required. Launch that signed bundle's executable, not the cargo build copy:

```sh
CovenCloudKitTests.app/Contents/MacOS/CovenCloudKitTests \
  --exact providers::cloudkit::tests::cloudkit_live \
  --ignored --nocapture --test-threads=1
```

These are instructions for a later live run; preparation does not execute them.

## Development schema

The selected container's Development schema must allow these record types and
fields. CloudKit creates types/fields while saving in Development; use CloudKit
Console to make the directory fields below queryable before
the listing assertions.
The adapter cannot create indexes through the record framework API.

| Record type | Fields |
| --- | --- |
| `CovenObject` | `path: String`, `size: String`, `data: Asset`, `upload: String`, `directory1: String` (queryable), `directory2: String` (queryable), `directory3: String` (queryable), `directory4: String` (queryable) |
| `CovenUpload` | `path: String`, `total: String`, `confirmed: String`, `state: String`, `owner: Reference` |
| `CovenAsset` | `data: Asset`, `owner: Reference` |
| `CovenLiveTestZone` | `test: String` (live tests only) |

`directory1` through `directory4` contain each existing ancestor directory,
without a trailing slash. For `<store>/snapshots/store/7/2` these are `<store>`,
`<store>/snapshots`, `<store>/snapshots/store`, and `<store>/snapshots/store/7`.
Shallower objects omit nonexistent ancestors. Each prefix chooses its depth's
field and compares the entire directory with `==`; no prefix filter is used.
Metadata listings return `path` and `size`, alongside the native
record id, dates and change tag. Queries do not request assets.

`size`, `total`, and `confirmed` contain decimal unsigned byte counts. A published
object has either one asset or an upload id. Multipart records use reserved names;
only `CovenObject` participates in object listings. Production deployments require
that same schema and indexes deployed to Production through CloudKit Console.

The conformance assertions count each submitted `CKQueryOperation`, including
every cursor continuation, and count its matched records independently of the
logical listing call. Listings with more than 100 matching objects require
cursor pages. Slots and the floor use exact record IDs and require no query. The observations retain each native operation id for
correlation with CloudKit Console server logs. These are native operation counts,
not measurements of the framework's private network traffic or metadata byte
encoding; server logs are still needed to account for requests hidden inside an
operation under §21.1.

## Sharing conformance on two accounts

The ignored `providers::cloudkit::access::tests::cloudkit_live_sharing` test
covers grant, metadata validation, repeated acceptance, recipient account identity,
read/write participation, and access removal. Both Macs need the signed host above;
one uses the zone owner's iCloud account and the other the invited account.
On both hosts set `COVEN_CLOUDKIT_TEST_EMAIL` to the recipient's iCloud email and
`COVEN_CLOUDKIT_TEST_INVITATION` to a private local file path. Set
`COVEN_CLOUDKIT_TEST_ROLE=owner` on the owner and `recipient` on the recipient.
Select this test with `--exact providers::cloudkit::access::tests::cloudkit_live_sharing
--ignored --nocapture --test-threads=1` instead of the single-account test.

Start the owner first. It writes the invitation file with mode 0600 and waits for
input. Transfer that file privately to the recipient's selected path and start its
test. After the recipient has accepted, checked its account identity and written
the shared object, enter `accepted` on the owner's stdin. The owner verifies the
write and removes access. Enter `revoked` on the recipient's stdin; it requires
its next read to fail. After that test succeeds, enter `verified` on the owner's
stdin. The owner deletes the test zone. Both tests remove their invitation files
after success. These files contain a secret share URL; do not commit or publish them.

## Inviting the owner's own account

The ignored `providers::cloudkit::access::tests::cloudkit_live_inviting_the_owner_is_refused`
test runs on one host. Set `COVEN_CLOUDKIT_TEST_OWNER_EMAIL` to an address of
the signed-in iCloud account. It creates a marked zone, grants that address,
requires `InviteeIsStoreOwner`, and deletes the zone.

## Create-once race

The ignored `providers::cloudkit::tests::race::tests::cloudkit_live_create_once_race`
test measures whether a CloudKit save of a new record gives exactly one winner
when several writers create the same `seq/<n>` path at once. It runs 200 rounds of 2
writers, then 50 rounds of 8. Every writer is its own `CloudKitStorage`
instance and connection, but `CKContainer.containerWithIdentifier` hands every
instance in one process the same `CKContainer` and `CKDatabase` objects, so a
process's writers share the framework's operation handling and its per-user
CloudKit daemon; the test prints how many distinct objects it got. Each round
uses a fresh path.
Every writer is released at one instant of this Mac's clock, and each round
requires exactly one success, every other writer refused as
`AlreadyExists`, and the winner's bytes when the path is read back. A rate
limit or network failure permits another attempt only when no definite
violation was observed and read-back succeeded. Two successes, changed or
missing winner bytes, and unexpected errors still fail immediately. An
inconclusive attempt uses a fresh path after the requested wait; five such
attempts in a row fail the test. The test prints, per race size, the winners,
each refusal's cause with its native error code, inconclusive attempts and
what their path held, how many rounds had every writer start before any
finished, and every anomaly. It
stops on the first anomaly or non-overlapping round.

`COVEN_CLOUDKIT_TEST_ROLE` selects the mode. `single` runs every writer on the
signed-in account, in one process. This mode also checks lost slot and floor
replies by withholding a completed request's success and settling by an exact
read. Equal and different slot retries preserve the occupant; stale floor
retries preserve its bytes and revision. It then runs 200 two-writer and 50
eight-writer conditional floor replacement races, requiring one winner from
the same observed revision and the winner's bytes on read-back. It deletes
only its own marked zone after success or an assertion failure:

```sh
COVEN_CLOUDKIT_TEST_ROLE=single \
  CovenCloudKitTests.app/Contents/MacOS/CovenCloudKitTests \
  --exact providers::cloudkit::tests::race::tests::cloudkit_live_create_once_race \
  --ignored --nocapture --test-threads=1
```

`owner` and `recipient` race two iCloud accounts on one Mac: one writer per
account in each of the 200 rounds, then four per account in each of the 50.
The recipient runs in a second macOS user's logged-in session, signed in to
the second iCloud account, launching its own copy of the same signed host;
copy the bundle somewhere that user can read, such as `/Users/Shared`, with
`ditto`, which keeps the signature. On both, set `COVEN_CLOUDKIT_TEST_EMAIL`
to the recipient's iCloud email and `COVEN_CLOUDKIT_TEST_INVITATION` to a
private file path in that user's home directory.

Start the owner first. It creates and marks the zone, shares it with the
recipient, creates a directory named `coven-cloudkit-race-<random id>` in
`/Users/Shared` (sticky and writable by both users), and writes the invitation
file with mode 0600. Copy that file to the recipient's path as its owner, for
example with `sudo install -m 600 -o <recipient user> <owner path> <recipient path>`,
and start the recipient's test with `COVEN_CLOUDKIT_TEST_ROLE=recipient`. It
accepts the share and writes through its shared database. The two processes
then step every attempt through files in that directory: the owner announces
the path, waits until the recipient is ready, and sets a release instant
250 ms ahead on the clock both share; after the race the recipient writes its
outcomes there and the owner tallies the round. The owner waits up to an hour
for the recipient to join and ten minutes for each later step. After success
the owner deletes the zone and the directory and both remove their invitation
files. A process that fails tells the other to stop; the directory and zone
stay for inspection, and the leftover-zone test above removes the zone.
