# Native CloudKit conformance host

The ignored `providers::cloudkit::tests::cloudkit_live` test calls the real adapter.
It creates an isolated custom zone in `iCloud.fm.bae.desktop`, exercises create-once,
exact status, publication metadata, position and clock replacement revisions,
ranges, recorded multipart continuation, paginated scoped queries
and deletion, waits one hour
before checking listing completeness, and deletes its test zone after success.
It is never part of an ordinary cargo test run. A failed assertion leaves the zone
available for inspection; remove that test zone explicitly after investigating.

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
PATH=$HOME/.elan/bin:$PATH lockf -k "$CARGO_TARGET_DIR/.full-check.lock" \
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
Console to make `CovenObject.path` queryable before the listing assertions.
The adapter cannot create indexes through the record framework API.

| Record type | Fields |
| --- | --- |
| `CovenObject` | `path: String` (queryable), `size: String`, `data: Asset`, `upload: String` |
| `CovenUpload` | `path: String`, `total: String`, `confirmed: String`, `state: String`, `owner: Reference` |
| `CovenAsset` | `data: Asset`, `owner: Reference` |

`size`, `total`, and `confirmed` contain decimal unsigned byte counts. A published
object has either one asset or an upload id. Multipart records use reserved names;
only `CovenObject` participates in object listings. Production deployments require
that same schema and indexes deployed to Production through CloudKit Console.

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
