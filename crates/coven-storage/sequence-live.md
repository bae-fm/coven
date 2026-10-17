# Live sequence checks

These ignored tests use the existing mode-0600 credential files; their loaders
never print credential values. Run under the shared Cargo test lane lock.

- S3: `COVEN_LIVE_S3_ENV=<file> COVEN_LIVE_S3_PROFILE=<name> cargo test -p coven-storage s3_live_sequence -- --ignored --nocapture`
- Dropbox: `COVEN_LIVE_DROPBOX_ENV=<owner-file> COVEN_LIVE_DROPBOX_B_ENV=<recipient-file> cargo test -p coven-storage dropbox_live_sequence -- --ignored --nocapture`
- OneDrive: `COVEN_LIVE_ONEDRIVE_ENV=<owner-file> COVEN_LIVE_ONEDRIVE_B_ENV=<recipient-file> cargo test -p coven-storage onedrive_live_sequence -- --ignored --nocapture`
- CloudKit: the signed host's `single` role in [cloudkit-live.md](cloudkit-live.md#create-once-race).

Slot races use `seq/<n>`, with 200 two-writer and 50 eight-writer rounds.
Distinct connections submit distinct bytes; their request intervals must
overlap, exactly one wins, and exact reads must return that winner's bytes.
Dropbox and OneDrive alternate the owner and recipient accounts in each race.
S3 retains the native multipart and mixed-publication races so adapter reply
recovery cannot conceal two accepted publications. Its selected profile and
bucket versioning are checked before any race.

A throttled writer makes the whole round uncounted. The harness awaits every
writer, then waits at least the longest provider retry delay before further
requests, including cleanup (using the response's Date for HTTP-date delays).
Known violations still fail; observed winner bytes and revisions must pass
before a round can retry with a fresh slot or the floor's newly read revision.
Summaries include discarded throttled rounds. Rounds are spaced by one second;
repeated throttling uses coven's ordinary exponential delay when longer than
the provider's request. A stage stops after 100 throttled rounds. Auxiliary
reads and lost-reply checks retry throttling up to 20 attempts; other errors
and any known atomicity violation fail the run.

Floor races use 200 two-writer and 50 eight-writer rounds against one observed
revision. Lost-reply checks await a real write, withhold its successful reply,
then settle by reading; they also test occupied slot retries and stale floor
retries. They do not inject a network failure inside the provider's transport.

Each run owns a fresh store namespace. S3 cleanup includes its temporaries,
uploads, historical versions and delete markers; drive tests delete their own
folders and shares. CloudKit deletes only its own marked zone. A failure stops
that provider's run, including later stages; a run does not update certification.
