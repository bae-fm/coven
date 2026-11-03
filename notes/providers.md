# Providers: terms and the S3 services coven doesn't name

Why coven reaches each provider the way it does, from the providers' own
terms, and what the documentation of S3 services coven has no profile for
says about them. Section numbers point into [spec/coven.md](../spec/coven.md);
certified S3 services and their evidence are in
[spec/s3-certification.md](../spec/s3-certification.md). Nothing here was
tested live unless it says so.

## What the terms require

- **Each member signs in with their own Dropbox account.** Dropbox's
  developer terms (§2.6(c)) forbid using "any Dropbox login credentials or
  access tokens in a manner that allows multiple users of the Developer App to
  access a single Dropbox account". So a shared store is a folder each member
  mounts in their own account (§20.2), and a restore code carries the storage
  location but never a sign-in token (§13.1): a code that moved the owner's
  token to another person would break that term.
- **CloudKit only on Apple platforms.** Apple's developer agreement licenses
  the iCloud service to "Applications", software "for specific use with an
  Apple-branded product running iOS, iPadOS, macOS, tvOS, visionOS, or
  watchOS", and to "Web Software" with feature parity to such an app. An
  Android, Windows or Linux app is neither, so CloudKit Web Services from one
  falls outside the license. Coven builds CloudKit in on Apple platforms only;
  other platforms refuse it at setup with `StorageFailure::ProviderUnavailable`
  (§20.2).
- **Each app registers its own keys.** Dropbox: developers who wrap its
  platform for others "must still sign up for their own Dropbox app key", one
  key per app (one app on several platforms may share one). Apple: an app may
  not share its iCloud entitlement "with any third party", so each app has its
  own CloudKit container. OneDrive: each app registers its own client. Coven
  ships no key, client or container of its own; the app embedding it supplies
  them.
- **Usage counts against the member's own account** (§20.1). Dropbox
  documents its rate limits per user who linked the app, and bills no API
  use. CloudKit's private and shared databases count against the user's own
  iCloud storage, or the zone owner's; its public database counts against the
  app's container, and Apple says a user "can't do anything about" it when
  full. Google counts every Drive API request against the app's Cloud project,
  which is why coven has no Drive provider; GCS through S3 bills the bucket's
  owner.

## S3 services without a profile

Coven names seven S3 services (§20.7); any other endpoint is detected and
checked at setup, and refused if it fails. What the documentation of other
services says on the points coven depends on:

| Service | Create-once (single PUT / multipart) | Consistency | Price per TB-month; egress | Finest member key scope |
|---|---|---|---|---|
| Wasabi | not documented | "always consistent" | $7.99; free up to stored; 1 TB and 90-day minimums | prefix (AWS-style IAM, `s3:prefix`) |
| Hetzner Object Storage | PUT implied, multipart unknown (runs Ceph RGW); unsupported on versioned buckets | not documented | €6.49 a month including 1 TB stored and 1 TB egress | bucket, through a bucket policy; no API makes keys (console only) |
| Scaleway | yes / yes | strong, including listings | €8–16; 75 GB of egress free | prefix through a bucket policy, only with a project of its own |
| Storj | yes / yes, from its gateway's source | strong read after write | $7; $7 per TB egress | prefix (the restriction is computed locally on an access grant) |
| Tigris | yes / not documented; a Global bucket is consistent only per region, so use a single- or multi-region one | strong for single- and multi-region buckets | $20; free | bucket |
| OVHcloud | yes / yes; its source checks with a HEAD before writing, a possible race | not documented | about €7–14; free | object prefix through a user policy |
| Linode / Akamai | not documented; runs Ceph, completion likely unchecked | strong, including listings | $5 a month including 250 GB, then $20 | not checked |

### Expected to be refused

Setup refuses a service that accepts a write to an occupied path, and these
are expected to fail that check:

- **Backblaze B2** answers conditional writes with 501.
- **DigitalOcean Spaces** documents `If-None-Match` for GET and HEAD only.
- **Cubbit** accepts the conditions and ignores them, overwriting the object.
- **Supabase Storage** always overwrites, by its source.
- **OpenStack Swift before 2.36** (Infomaniak among others) answers 501.
- **Alibaba Cloud OSS** answers 501.
- **Garage** ignores `If-None-Match` and `If-Match` on every write: they are
  parsed only for GET, HEAD and a copy's source, while PutObject,
  CompleteMultipartUpload and a copy's destination insert unconditionally
  (`src/api/s3/put.rs`, `multipart.rs`, `copy.rs`). Its documentation calls
  conditional writes impossible without consensus. Setup refuses it with
  `CreateOnce`, and the certification suite fails it (spec/s3-certification.md).

### MinIO

MinIO has no profile. Its community repository is archived (last release
2025-10-15); its successor, AIStor, is a separate product. An endpoint
running it is detected and checked like any other unnamed service.
