# Storage providers compared (updated 2026-10-14)

**M** = measured live by us. **D** = from the provider's documentation or source, not tested by us. **?** = not
known yet. Sources: live tests on main (`*_live` modules, the S3 certification suite, spec/s3-certification.md),
probes in the job's tmp/*-probe.py, and the research in [s3-provider-survey.md](s3-provider-survey.md).

Status: **Works** = live tests pass on main. **Certified** = S3 profile certified by the suite. **To certify** =
on the list, needs an account and a run. **Refuse** = expected to fail coven's setup check (refusal confirmed
live where marked M). Every S3 setup runs detection and the safety check against the real bucket, so a service
that differs from its documentation is refused, never trusted.

## Cloud drives

| Provider | Status | Usage | Who | Access and approval | Sharing; whose quota | Price; egress | Refuses taken name (PUT / multipart) | Partial uploads hidden | Timestamp = completion | Revision on replace | Listing lag / consistency | Fix needed | Free tier to certify | Live tested |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Google Drive | Works | 5 | consumers | `drive.file`; full `drive` at first invite (M); restricted-scope review, 100 users before it (D) | folder shared by email (M); uploader pays (D) | person's plan | Yes (M) | Yes (M) | Yes (M) | head revision (M) | 0.99 s median, 5.9 s max (M, 2026-10-14) | None | free account | 2026-10-14, 8/8 (access upgrade 2026-10-13; needs browser sign-ins) |
| Dropbox | Works | 5 | consumers | App folder or Full, fixed at creation (M); production review, 500 dev users (D) | member mounts the folder (M); every member pays full size (M) | person's plan | 409 (M) | Yes (M) | Yes (M) | rev; same for identical bytes (M) | 1.1–1.4 s (M, 2026-10-13 probe; the live suite has no lag test) | None | free account | 2026-10-14, 7/7 incl. sharing and revoke, plus App folder |
| OneDrive | Works | 5 | consumers, Microsoft shops | `Files.ReadWrite`; no review for personal accounts (D) | invite, open via share (D); owner pays (D) | person's plan | Yes (M) | via copy fix (M) | via copy's last-modified (M) | eTag/cTag (M) | 0.93 s median, 1.9 s max (M, 2026-10-14) | Temp name + conditional copy | free account | 2026-10-14, 11/11 |
| iCloud (CloudKit) | Works | 5 | Apple users | app container + entitlement; Apple developer account (D) | share the record zone (D); ? | person's plan | `serverRecordChanged` (M) | atomic saves (M) | creationDate (M) | recordChangeTag (M) | ? (1-hour margin covers it) | Listing by exact directory fields | free account | 2026-10-14, signed host (after a fixture fix); sharing test needs two accounts |

## Hosted S3 services

One adapter with a profile per service; any other endpoint is detected and checked at setup. Sharing on S3 is always a key per member made in the console, with no approval; the bucket owner pays. Usage is 1–5 from the survey (customers, revenue, rclone/restic/NAS integrations, HN volume).

| Provider | Status | Usage | Who | Price; egress | Refuses taken name (PUT / multipart) | Partial uploads hidden | Timestamp = completion | Revision on replace | Listing lag / consistency | Fix needed | Free tier to certify | Live tested |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| AWS S3 | To certify (later) | 5 | everyone | $23; $90/TB after 100 GB | Yes / Yes (D) | ? | No, start (D) | ETag/version (D) | strong, incl. listing (D) | Copy publication for multipart (on main, docs only); objects over 5 GiB refused (one CopyObject's limit) | $100–200 credit | No account yet |
| Cloudflare R2 | Certified | 5 | developers | $15; free | Yes / Yes (M) | Yes (M) | Yes (M) | ETag (M) | 0.22 s median, 0.36 s max (M) | Own bucket per store (keys scope only to a bucket, T118) | 10 GB | 2026-10-14 suite 5 PASS (every race one winner) |
| GCS (S3 interop) | Certified | 5 | developers, enterprise | $20; paid | generation-match / via copy (M) | Yes (M) | via copy (M) | generation (M) | 0.13 s median, 0.22 s max (M) | Temp name + conditional copy; member keys need bucket metadata read (`roles/storage.legacyBucketReader`) for the versioning check | $300 credit | 2026-10-14 suite 5 PASS unversioned (every race one winner, incl. PUT racing copy); no versioned run |
| Wasabi | To certify | 4 (100k customers) | enterprise, MSPs, Veeam | $7.99; free to stored; 1 TB / 90-day minimums | not documented | ? | ? | ? | "always consistent" (D) | test decides | 30 days, 1 TB | — |
| Hetzner Object Storage | To certify | 4 | EU budget backups, devs | €6.49/mo incl. 1 TB + 1 TB egress | PUT implied / multipart unknown (Ceph RGW) (D) | ? | ? | ? | not documented | test decides | none (cents/hour) | — |
| Scaleway | To certify | 3 | EU devs, residency | €8–16; 75 GB free egress | Yes / Yes (D) | ? | ? | ? | strong incl. listing (D) | ? | 750 GB for 90 days | — |
| Storj | To certify | 3 (~44 PB) | devs, global | $7; $7/TB egress | Yes / Yes, source only (D) | ? | ? | ? | strong read-after-write (D) | ? | 30 days, no card | — |
| Tigris (Fly, Railway) | To certify | 3 | devs, AI | $20; free | Yes / not documented (D) | ? | ? | ? | strong for single/multi-region buckets (D) | Own bucket per store (keys scope only to a bucket, T118) | 5 GB | — |
| OVHcloud | To certify | 3 (1.6M customers) | EU residency, enterprise | ~€7–14; free egress | Yes / Yes (D); HEAD-then-write race in source | ? | ? | ? | not documented | race test decides | $200 credit (US) | — |
| Linode / Akamai | To certify | 3 | devs | $5/mo incl. 250 GB; then $20 | not documented; Ceph, completion likely unchecked | ? | ? | ? | strong incl. listing (D) | test decides | ? | — |
| Backblaze B2 | Refuse | 5 (120k B2 customers, ~5 EB) | consumer backups, devs | $6.95; free to 3× stored | 501 (D) | ? | ? | ? | ? | none possible | 10 GB | — |
| DigitalOcean Spaces | Refuse (expected) | 4 | devs on DO | $5/mo incl. 250 GiB; then ~$20 | header documented for GET/HEAD only (D) | ? | ? | ? | ? | none expected | $5 credit | — |

## Self-hosted S3 servers

Run by the user; free; certified against a local container. Selected by profile name (no hostname), or detected at setup.

| Provider | Status | Usage | Who | Refuses taken name (PUT / multipart) | Partial uploads hidden | Timestamp = completion | Revision on replace | Listing lag / consistency | Fix needed | Live tested |
|---|---|---|---|---|---|---|---|---|---|---|
| RustFS | Certified | 4 (34.9k stars) | self-hosters | Yes / Yes (M) | Yes (M) | Yes (M) | ETag (M) | ms (M) | None (no checksums; byte comparison) | 2026-10-14 suite 5 PASS, unversioned and versioned (1.0.1); member keys PASS |
| Ceph RGW ≥ 20.2.1 | Certified: unversioned buckets (suite 5, 2026-10-14)<br>Refused: versioned buckets | 3–4 (17.1k stars) | self-hosters, enterprise; under Hetzner, Vultr, Linode | Yes / Yes from 20.2.1 (M: 20.2.0 overwrote) | Yes (M) | Yes (M) | ETag (M) | ms (M) | Versioned bucket: two concurrent `If-None-Match` writers both succeed (M, 199/200); setup's one-at-a-time check misses it | 2026-10-14 suite 5 PASS unversioned; versioned: every writer accepted in every race (20.2.4); member keys PASS |
| Versity Gateway | Certified | ? | self-hosters, POSIX filesystems | Yes / Yes, and on copy (M) | Yes (M) | Yes (M) | ETag (M) | ms (M) | None; member keys through its standalone IAM (separate process and port, given with the admin key) | 2026-10-15 suite 5 PASS unversioned and versioned (1.9.0, POSIX backend); member keys PASS |
| SeaweedFS | Certified, versioned too (suite 5, 2026-10-15) | 4 (35.5k stars) | self-hosters | Yes / sequential only: racing conditional completions both succeed (M, 4.48, 198/200), and unversioned a conditional PUT racing a conditional copy both succeed (M, 14/200, then 17/200); copies racing copies keep one winner (M) | Yes (M) | Yes (M) | ETag (M) | ms (M) | Every object published by conditional copy (T208): single-request creates are staged and copied too, so no conditional PUT races a copy | 2026-10-15 suite 5 PASS unversioned and versioned (every object by copy); member keys PASS (2026-10-14) |
| Garage | Refused (M) | 2 (4.7k stars) | small self-hosted clusters | ignored on every write (M), also under concurrency: both or all writers accepted in every race (M) | Yes (M) | No, start (M) | ETag (M) | ms (M) | none possible | 2026-10-14 suite 5 FAIL (2.4.1), setup refuses `CreateOnce` |

Skipped (survey): Cubbit (accepts and ignores conditions, silent overwrite), Supabase Storage (source always
overwrites), Infomaniak and other OpenStack Swift < 2.36 (501), Alibaba/Tencent/Huawei, Contabo, Telnyx, Zenko,
and low-usage undocumented services (Impossible Cloud, IONOS, UpCloud, Synology C2, Filebase, Bunny, Lyve, Akave).
Not yet considered beyond a note: WebDAV (Nextcloud, NAS, pCloud, Koofr, kDrive, Hetzner Storage Box), Azure Blob,
Box.

Notes
- Listing lag matters because coven trusts a listing only for objects stored more than 1 hour before the
  provider's time (the listing margin). Every measured lag is under 6 seconds.
- "Revision changes on every replace" matters for replaceable objects (positions, clock objects). Coven seals
  them with fresh random nonces, so their bytes always differ.
- Suite 4 races conditional writes (2 writers x200, 8 writers x50) with versioning off and on; suite 5 races the
  named profile's own publication, including a PUT racing a multipart or copy publication. Not yet confirmed
  covered: quoted/unquoted If-Match ETags, a list-after-write check (survey findings).
- Full re-run of every reachable provider on the newest suites: 2026-10-14, evidence in spec/s3-certification.md
  ("Full battery, 2026-10-14"). AWS (no account) remains; GCS suite 5 and Versity Gateway ran later the same day.
