# S3 certification evidence

Suite version 1 passed GCS and R2 on 2026-10-13, and suite version 2 passed
RustFS and SeaweedFS the same day. Version 2 differs only in recording a
service that returns no checksum as such, which §4 permits; version 1 failed
detection for it. AWS is uncertified because no live account is available.
Suite version 3 runs the production detection that setup uses (§4); on
2026-10-13 it reproduced the GCS, R2, RustFS and SeaweedFS entries and passed
the setup safety check at each, and the full suite 3 passed RustFS. Its output
is in [Detection](#detection-suite-3).
Suite version 4 records bucket versioning and adds the concurrent conditional
publication checks described in [Concurrent certification](#concurrent-certification-suite-4).
Earlier certificates do not establish versioned-bucket support. On
2026-10-14 suite 4 passed RustFS with versioning disabled and enabled, and
failed SeaweedFS in both states, withdrawing its suite 2 certificate.
Suite version 5 races a named profile's own publication. The same day it
passed SeaweedFS's copy publication in both states, but failed the
unversioned bucket on a conditional PUT racing a copy, so SeaweedFS stayed
uncertified and refused ([suite 5](#seaweedfs-448-copy-publication-suite-5)).
On 2026-10-15 its entry, now publishing every object by copy, passed suite 5
with versioning disabled and enabled
([SeaweedFS every-object copy publication](#seaweedfs-every-object-copy-publication-2026-10-15)).
A [full battery](#full-battery-2026-10-14) run later that day ran suite 5
against R2, RustFS, Ceph RGW, SeaweedFS and Garage in every bucket state they
support, the cloud drives' and CloudKit's live tests, and the member-key
lifecycle: R2 and RustFS (both states) passed suite 5, Ceph RGW passed
unversioned and failed versioned, SeaweedFS failed unversioned, Garage failed
every race, and GCS's key could not read versioning, so suite 5 did not run
there. After the key was granted bucket metadata access, GCS passed suite 5,
and Versity Gateway 1.9.0 passed it with versioning disabled and enabled
([GCS and Versity Gateway](#gcs-and-versity-gateway-2026-10-14)). On
2026-10-15 the named `VersityGw` entry passed suite 5 in both states and its
minter passed the member-key lifecycle
([Versity Gateway profile](#versity-gateway-profile-2026-10-15)).
The selected entries are in
[`s3_profile.rs`](../crates/coven-storage/src/providers/s3_profile.rs); the
contract and certification procedure are in [§4](coven.md#4-storage-providers-and-access).

The output below retains native observations and every requirement result.
Account endpoints, keys and object names are omitted. `list_end_s` measures
elapsed time from the successful upload reply through the listing response;
these samples do not establish the one-hour completeness bound. Every case's
cleanup passed after deleting its objects and aborting its remaining uploads.
The bucket-check setting was removed after these runs: every profile tells a
missing bucket by a zero-entry listing at the missing object's key (§4). Its
`selected=` and `bucket_check` values are omitted below; the HEAD Bucket
observations are kept as recorded.

## GCS

```text
native signing=GoogleHmac result=accepted
native signing=AwsSigV4 result=accepted
native single_put IfNoneMatch empty=accepted taken=accepted preserved=false
native single_put GenerationMatch empty=accepted taken=AlreadyExists HTTP 412 preserved=true
native partial_upload HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
§4 native partial_uploads_hidden PASS
native conditional_completion empty=accepted
native multipart_timestamp=Completion start_before=SystemTime { tv_sec: 1791902391, tv_nsec: 0 } start_after=SystemTime { tv_sec: 1791902391, tv_nsec: 0 } completion_before=SystemTime { tv_sec: 1791902396, tv_nsec: 0 } completion_after=SystemTime { tv_sec: 1791902396, tv_nsec: 0 } published=SystemTime { tv_sec: 1791902396, tv_nsec: 0 }
native conditional_completion taken=accepted preserved=false enforced=false
native copy header=x-goog-if-generation-match empty=accepted taken=AlreadyExists HTTP 412 preserved=true later_than_source=true
native copy header=cf-copy-destination-if-none-match empty=accepted taken=accepted preserved=false later_than_source=true
native listing=V2 objects=8 pages_forced_at=1
native checksum_mode=accepted
native bucket_head=accepted bucket_list=accepted
native checksum_present=true checksum_scope=ScopeStated revision=Generation response_headers=Google
§4 native_behavior_detection PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1791902430, tv_nsec: 0 } published=SystemTime { tv_sec: 1791902430, tv_nsec: 0 } after=SystemTime { tv_sec: 1791902430, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1791902432, tv_nsec: 0 } published=SystemTime { tv_sec: 1791902433, tv_nsec: 0 } after=SystemTime { tv_sec: 1791902433, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=false
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag provider_case=S3_object trial=1 stored_at=SystemTime { tv_sec: 1791902435, tv_nsec: 0 } list_start_s=0.054883958 list_end_s=0.127515708 present=true
listing_lag provider_case=S3_new_device_folder trial=1 stored_at=SystemTime { tv_sec: 1791902435, tv_nsec: 0 } list_start_s=0.054468958 list_end_s=0.127699125 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.127699125 median_s=0.127699125 max_s=0.127699125 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=2 stored_at=SystemTime { tv_sec: 1791902436, tv_nsec: 0 } list_start_s=0.055193667 list_end_s=0.192246500 present=true
listing_lag provider_case=S3_new_device_folder trial=2 stored_at=SystemTime { tv_sec: 1791902436, tv_nsec: 0 } list_start_s=0.054901834 list_end_s=0.243573875 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.243573875 median_s=0.243573875 max_s=0.243573875 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=3 stored_at=SystemTime { tv_sec: 1791902436, tv_nsec: 0 } list_start_s=0.056775375 list_end_s=0.129964583 present=true
listing_lag provider_case=S3_new_device_folder trial=3 stored_at=SystemTime { tv_sec: 1791902436, tv_nsec: 0 } list_start_s=0.056510500 list_end_s=0.172670625 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.172670625 median_s=0.172670625 max_s=0.172670625 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=4 stored_at=SystemTime { tv_sec: 1791902437, tv_nsec: 0 } list_start_s=0.046282917 list_end_s=0.090197458 present=true
listing_lag provider_case=S3_new_device_folder trial=4 stored_at=SystemTime { tv_sec: 1791902437, tv_nsec: 0 } list_start_s=0.047605917 list_end_s=0.177694333 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.177694333 median_s=0.177694333 max_s=0.177694333 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=5 stored_at=SystemTime { tv_sec: 1791902437, tv_nsec: 0 } list_start_s=0.099023708 list_end_s=0.182689625 present=true
listing_lag provider_case=S3_new_device_folder trial=5 stored_at=SystemTime { tv_sec: 1791902437, tv_nsec: 0 } list_start_s=0.098685958 list_end_s=0.210188750 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.210188750 median_s=0.210188750 max_s=0.210188750 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=6 stored_at=SystemTime { tv_sec: 1791902437, tv_nsec: 0 } list_start_s=0.051765333 list_end_s=0.128885750 present=true
listing_lag provider_case=S3_new_device_folder trial=6 stored_at=SystemTime { tv_sec: 1791902437, tv_nsec: 0 } list_start_s=0.051479333 list_end_s=0.155944625 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.155944625 median_s=0.155944625 max_s=0.155944625 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=7 stored_at=SystemTime { tv_sec: 1791902438, tv_nsec: 0 } list_start_s=0.049205333 list_end_s=0.114563625 present=true
listing_lag provider_case=S3_new_device_folder trial=7 stored_at=SystemTime { tv_sec: 1791902438, tv_nsec: 0 } list_start_s=0.048926458 list_end_s=0.148329250 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.148329250 median_s=0.148329250 max_s=0.148329250 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=8 stored_at=SystemTime { tv_sec: 1791902438, tv_nsec: 0 } list_start_s=0.113837958 list_end_s=0.215074250 present=true
listing_lag provider_case=S3_new_device_folder trial=8 stored_at=SystemTime { tv_sec: 1791902438, tv_nsec: 0 } list_start_s=0.113398292 list_end_s=0.246772458 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.246772458 median_s=0.246772458 max_s=0.246772458 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=9 stored_at=SystemTime { tv_sec: 1791902438, tv_nsec: 0 } list_start_s=0.056674917 list_end_s=0.123454417 present=true
listing_lag provider_case=S3_new_device_folder trial=9 stored_at=SystemTime { tv_sec: 1791902438, tv_nsec: 0 } list_start_s=0.056330042 list_end_s=0.245412500 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.245412500 median_s=0.245412500 max_s=0.245412500 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=10 stored_at=SystemTime { tv_sec: 1791902439, tv_nsec: 0 } list_start_s=0.050578959 list_end_s=0.116475000 present=true
listing_lag provider_case=S3_new_device_folder trial=10 stored_at=SystemTime { tv_sec: 1791902439, tv_nsec: 0 } list_start_s=0.050310375 list_end_s=0.148079000 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.148079000 median_s=0.148079000 max_s=0.148079000 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=11 stored_at=SystemTime { tv_sec: 1791902439, tv_nsec: 0 } list_start_s=0.052572250 list_end_s=0.103507209 present=true
listing_lag provider_case=S3_new_device_folder trial=11 stored_at=SystemTime { tv_sec: 1791902439, tv_nsec: 0 } list_start_s=0.052274000 list_end_s=0.163107292 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.163107292 median_s=0.163107292 max_s=0.163107292 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=12 stored_at=SystemTime { tv_sec: 1791902439, tv_nsec: 0 } list_start_s=0.053225542 list_end_s=0.104816167 present=true
listing_lag provider_case=S3_new_device_folder trial=12 stored_at=SystemTime { tv_sec: 1791902439, tv_nsec: 0 } list_start_s=0.052951209 list_end_s=0.213408875 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.213408875 median_s=0.213408875 max_s=0.213408875 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=13 stored_at=SystemTime { tv_sec: 1791902440, tv_nsec: 0 } list_start_s=0.060069667 list_end_s=0.121450334 present=true
listing_lag provider_case=S3_new_device_folder trial=13 stored_at=SystemTime { tv_sec: 1791902440, tv_nsec: 0 } list_start_s=0.059761625 list_end_s=0.163357042 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.163357042 median_s=0.163357042 max_s=0.163357042 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=14 stored_at=SystemTime { tv_sec: 1791902440, tv_nsec: 0 } list_start_s=0.054499959 list_end_s=0.126051084 present=true
listing_lag provider_case=S3_new_device_folder trial=14 stored_at=SystemTime { tv_sec: 1791902440, tv_nsec: 0 } list_start_s=0.054223042 list_end_s=0.172796750 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.172796750 median_s=0.172796750 max_s=0.172796750 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=15 stored_at=SystemTime { tv_sec: 1791902440, tv_nsec: 0 } list_start_s=0.050694958 list_end_s=0.101017667 present=true
listing_lag provider_case=S3_new_device_folder trial=15 stored_at=SystemTime { tv_sec: 1791902440, tv_nsec: 0 } list_start_s=0.050438167 list_end_s=0.204408375 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.204408375 median_s=0.204408375 max_s=0.204408375 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=16 stored_at=SystemTime { tv_sec: 1791902441, tv_nsec: 0 } list_start_s=0.050721084 list_end_s=0.113272375 present=true
listing_lag provider_case=S3_new_device_folder trial=16 stored_at=SystemTime { tv_sec: 1791902441, tv_nsec: 0 } list_start_s=0.050486584 list_end_s=0.155401875 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.155401875 median_s=0.155401875 max_s=0.155401875 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=17 stored_at=SystemTime { tv_sec: 1791902441, tv_nsec: 0 } list_start_s=0.051856917 list_end_s=0.123100667 present=true
listing_lag provider_case=S3_new_device_folder trial=17 stored_at=SystemTime { tv_sec: 1791902441, tv_nsec: 0 } list_start_s=0.051585583 list_end_s=0.182562042 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.182562042 median_s=0.182562042 max_s=0.182562042 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=18 stored_at=SystemTime { tv_sec: 1791902441, tv_nsec: 0 } list_start_s=0.048707084 list_end_s=0.105182459 present=true
listing_lag provider_case=S3_new_device_folder trial=18 stored_at=SystemTime { tv_sec: 1791902441, tv_nsec: 0 } list_start_s=0.048432667 list_end_s=0.205082667 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.205082667 median_s=0.205082667 max_s=0.205082667 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=19 stored_at=SystemTime { tv_sec: 1791902442, tv_nsec: 0 } list_start_s=0.059516084 list_end_s=0.119854334 present=true
listing_lag provider_case=S3_new_device_folder trial=19 stored_at=SystemTime { tv_sec: 1791902442, tv_nsec: 0 } list_start_s=0.059238750 list_end_s=0.183518542 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.183518542 median_s=0.183518542 max_s=0.183518542 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=20 stored_at=SystemTime { tv_sec: 1791902442, tv_nsec: 0 } list_start_s=0.047019667 list_end_s=0.109782958 present=true
listing_lag provider_case=S3_new_device_folder trial=20 stored_at=SystemTime { tv_sec: 1791902442, tv_nsec: 0 } list_start_s=0.046737083 list_end_s=0.163967833 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.163967833 median_s=0.163967833 max_s=0.163967833 omitted_trials=0 timeouts=0
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.090197458 median_s=0.120652334 max_s=0.215074250 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
certification suite=1 PASS
```

## R2

```text
native signing=GoogleHmac result=Refused HTTP 400
native signing=AwsSigV4 result=accepted
native single_put IfNoneMatch empty=accepted taken=AlreadyExists HTTP 412 preserved=true
native single_put GenerationMatch empty=accepted taken=accepted preserved=false
native partial_upload HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
§4 native partial_uploads_hidden PASS
native conditional_completion empty=accepted
native multipart_timestamp=Completion start_before=SystemTime { tv_sec: 1791902446, tv_nsec: 0 } start_after=SystemTime { tv_sec: 1791902446, tv_nsec: 0 } completion_before=SystemTime { tv_sec: 1791902451, tv_nsec: 0 } completion_after=SystemTime { tv_sec: 1791902452, tv_nsec: 0 } published=SystemTime { tv_sec: 1791902452, tv_nsec: 0 }
native conditional_completion taken=AlreadyExists HTTP 412 preserved=true enforced=true
native listing=V2 objects=5 pages_forced_at=1
native checksum_mode=accepted
native bucket_head=accepted bucket_list=accepted
native checksum_present=true checksum_scope=UnstatedIgnored revision=ETag response_headers=S3
§4 native_behavior_detection PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1791902484, tv_nsec: 0 } published=SystemTime { tv_sec: 1791902484, tv_nsec: 0 } after=SystemTime { tv_sec: 1791902485, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1791902487, tv_nsec: 0 } published=SystemTime { tv_sec: 1791902488, tv_nsec: 0 } after=SystemTime { tv_sec: 1791902488, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=false
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag provider_case=S3_object trial=1 stored_at=SystemTime { tv_sec: 1791902492, tv_nsec: 0 } list_start_s=0.074561667 list_end_s=0.191709000 present=true
listing_lag provider_case=S3_new_device_folder trial=1 stored_at=SystemTime { tv_sec: 1791902492, tv_nsec: 0 } list_start_s=0.074407875 list_end_s=0.197818458 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.197818458 median_s=0.197818458 max_s=0.197818458 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=2 stored_at=SystemTime { tv_sec: 1791902493, tv_nsec: 0 } list_start_s=0.117565042 list_end_s=0.195362542 present=true
listing_lag provider_case=S3_new_device_folder trial=2 stored_at=SystemTime { tv_sec: 1791902493, tv_nsec: 0 } list_start_s=0.119392750 list_end_s=0.369216209 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.369216209 median_s=0.369216209 max_s=0.369216209 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=3 stored_at=SystemTime { tv_sec: 1791902494, tv_nsec: 0 } list_start_s=0.093373667 list_end_s=0.172800167 present=true
listing_lag provider_case=S3_new_device_folder trial=3 stored_at=SystemTime { tv_sec: 1791902494, tv_nsec: 0 } list_start_s=0.092881209 list_end_s=0.276652375 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.276652375 median_s=0.276652375 max_s=0.276652375 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=4 stored_at=SystemTime { tv_sec: 1791902494, tv_nsec: 0 } list_start_s=0.073923708 list_end_s=0.185695875 present=true
listing_lag provider_case=S3_new_device_folder trial=4 stored_at=SystemTime { tv_sec: 1791902494, tv_nsec: 0 } list_start_s=0.073492458 list_end_s=0.306789042 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.306789042 median_s=0.306789042 max_s=0.306789042 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=5 stored_at=SystemTime { tv_sec: 1791902495, tv_nsec: 0 } list_start_s=0.169854000 list_end_s=0.258068875 present=true
listing_lag provider_case=S3_new_device_folder trial=5 stored_at=SystemTime { tv_sec: 1791902495, tv_nsec: 0 } list_start_s=0.169291125 list_end_s=0.417682709 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.417682709 median_s=0.417682709 max_s=0.417682709 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=6 stored_at=SystemTime { tv_sec: 1791902496, tv_nsec: 0 } list_start_s=0.077871167 list_end_s=0.238944125 present=true
listing_lag provider_case=S3_new_device_folder trial=6 stored_at=SystemTime { tv_sec: 1791902496, tv_nsec: 0 } list_start_s=0.077366042 list_end_s=0.314016917 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.314016917 median_s=0.314016917 max_s=0.314016917 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=7 stored_at=SystemTime { tv_sec: 1791902497, tv_nsec: 0 } list_start_s=0.103955166 list_end_s=0.177279666 present=true
listing_lag provider_case=S3_new_device_folder trial=7 stored_at=SystemTime { tv_sec: 1791902497, tv_nsec: 0 } list_start_s=0.103508833 list_end_s=0.278323875 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.278323875 median_s=0.278323875 max_s=0.278323875 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=8 stored_at=SystemTime { tv_sec: 1791902497, tv_nsec: 0 } list_start_s=0.129056833 list_end_s=0.255422167 present=true
listing_lag provider_case=S3_new_device_folder trial=8 stored_at=SystemTime { tv_sec: 1791902497, tv_nsec: 0 } list_start_s=0.128842458 list_end_s=0.304347833 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.304347833 median_s=0.304347833 max_s=0.304347833 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=9 stored_at=SystemTime { tv_sec: 1791902498, tv_nsec: 0 } list_start_s=0.088782250 list_end_s=0.189545584 present=true
listing_lag provider_case=S3_new_device_folder trial=9 stored_at=SystemTime { tv_sec: 1791902498, tv_nsec: 0 } list_start_s=0.088235709 list_end_s=0.309915625 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.309915625 median_s=0.309915625 max_s=0.309915625 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=10 stored_at=SystemTime { tv_sec: 1791902499, tv_nsec: 0 } list_start_s=0.165724541 list_end_s=0.238224250 present=true
listing_lag provider_case=S3_new_device_folder trial=10 stored_at=SystemTime { tv_sec: 1791902499, tv_nsec: 0 } list_start_s=0.165326208 list_end_s=0.329699541 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.329699541 median_s=0.329699541 max_s=0.329699541 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=11 stored_at=SystemTime { tv_sec: 1791902499, tv_nsec: 0 } list_start_s=0.103477167 list_end_s=0.198603792 present=true
listing_lag provider_case=S3_new_device_folder trial=11 stored_at=SystemTime { tv_sec: 1791902499, tv_nsec: 0 } list_start_s=0.103035959 list_end_s=0.298793042 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.298793042 median_s=0.298793042 max_s=0.298793042 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=12 stored_at=SystemTime { tv_sec: 1791902500, tv_nsec: 0 } list_start_s=0.083537416 list_end_s=0.165327750 present=true
listing_lag provider_case=S3_new_device_folder trial=12 stored_at=SystemTime { tv_sec: 1791902500, tv_nsec: 0 } list_start_s=0.083217291 list_end_s=0.230116750 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.230116750 median_s=0.230116750 max_s=0.230116750 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=13 stored_at=SystemTime { tv_sec: 1791902501, tv_nsec: 0 } list_start_s=0.130310458 list_end_s=0.209152083 present=true
listing_lag provider_case=S3_new_device_folder trial=13 stored_at=SystemTime { tv_sec: 1791902501, tv_nsec: 0 } list_start_s=0.129866625 list_end_s=0.277000541 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.277000541 median_s=0.277000541 max_s=0.277000541 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=14 stored_at=SystemTime { tv_sec: 1791902501, tv_nsec: 0 } list_start_s=0.078941042 list_end_s=0.165146125 present=true
listing_lag provider_case=S3_new_device_folder trial=14 stored_at=SystemTime { tv_sec: 1791902501, tv_nsec: 0 } list_start_s=0.078626959 list_end_s=0.222403084 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.222403084 median_s=0.222403084 max_s=0.222403084 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=15 stored_at=SystemTime { tv_sec: 1791902502, tv_nsec: 0 } list_start_s=0.086875333 list_end_s=0.176974458 present=true
listing_lag provider_case=S3_new_device_folder trial=15 stored_at=SystemTime { tv_sec: 1791902502, tv_nsec: 0 } list_start_s=0.086573458 list_end_s=0.253820083 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.253820083 median_s=0.253820083 max_s=0.253820083 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=16 stored_at=SystemTime { tv_sec: 1791902503, tv_nsec: 0 } list_start_s=0.089071042 list_end_s=0.195231958 present=true
test providers::s3::tests::live_tests::s3_certification has been running for over 60 seconds
listing_lag provider_case=S3_new_device_folder trial=16 stored_at=SystemTime { tv_sec: 1791902503, tv_nsec: 0 } list_start_s=0.088753167 list_end_s=0.272268958 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.272268958 median_s=0.272268958 max_s=0.272268958 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=17 stored_at=SystemTime { tv_sec: 1791902503, tv_nsec: 0 } list_start_s=0.105529250 list_end_s=0.181256375 present=true
listing_lag provider_case=S3_new_device_folder trial=17 stored_at=SystemTime { tv_sec: 1791902503, tv_nsec: 0 } list_start_s=0.105162750 list_end_s=0.290315625 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.290315625 median_s=0.290315625 max_s=0.290315625 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=18 stored_at=SystemTime { tv_sec: 1791902504, tv_nsec: 0 } list_start_s=0.090774375 list_end_s=0.165120834 present=true
listing_lag provider_case=S3_new_device_folder trial=18 stored_at=SystemTime { tv_sec: 1791902504, tv_nsec: 0 } list_start_s=0.090395667 list_end_s=0.253060750 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.253060750 median_s=0.253060750 max_s=0.253060750 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=19 stored_at=SystemTime { tv_sec: 1791902505, tv_nsec: 0 } list_start_s=0.098114208 list_end_s=0.187997375 present=true
listing_lag provider_case=S3_new_device_folder trial=19 stored_at=SystemTime { tv_sec: 1791902505, tv_nsec: 0 } list_start_s=0.097772583 list_end_s=0.316073416 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.316073416 median_s=0.316073416 max_s=0.316073416 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=20 stored_at=SystemTime { tv_sec: 1791902506, tv_nsec: 0 } list_start_s=0.073768750 list_end_s=0.161738125 present=true
listing_lag provider_case=S3_new_device_folder trial=20 stored_at=SystemTime { tv_sec: 1791902506, tv_nsec: 0 } list_start_s=0.073383959 list_end_s=0.222022042 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.222022042 median_s=0.222022042 max_s=0.222022042 omitted_trials=0 timeouts=0
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.161738125 median_s=0.188771479 max_s=0.258068875 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
certification suite=1 PASS
```

## Self-hosted services

Each service ran as one local podman container on the test machine, reached
over plain `http` with path-style requests and selected without a profile
name, so the suite started from AWS's profile and printed the detected entry as
`Uncatalogued`; the committed entries use their own names and no endpoint
match.

### RustFS

RustFS image `rustfs/rustfs:latest`, built 2026-10-03
(`sha256:1803faef5762`), region `us-east-1`. Certified by suite 2.
`If-None-Match: *` refused occupied names on PUT and multipart completion with
HTTP 412, preserving bytes and time, and multipart `Last-Modified` fell between
the completion markers. No checksum was returned on HEAD or GET, with or
without checksum mode. Under suite 1 the same server failed only that
detection step.

```text
native signing=GoogleHmac result=Refused HTTP 400
native signing=AwsSigV4 result=accepted
native single_put IfNoneMatch empty=accepted taken=AlreadyExists HTTP 412 preserved=true
native single_put GenerationMatch empty=accepted taken=accepted preserved=false
native partial_upload HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
§4 native partial_uploads_hidden PASS
native conditional_completion empty=accepted
native multipart_timestamp=Completion start_before=SystemTime { tv_sec: 1791918360, tv_nsec: 0 } start_after=SystemTime { tv_sec: 1791918360, tv_nsec: 0 } completion_before=SystemTime { tv_sec: 1791918362, tv_nsec: 0 } completion_after=SystemTime { tv_sec: 1791918362, tv_nsec: 0 } published=SystemTime { tv_sec: 1791918362, tv_nsec: 0 }
native conditional_completion taken=AlreadyExists HTTP 412 preserved=true enforced=true
native listing=V2 objects=5 pages_forced_at=1
native checksum_mode=accepted
native bucket_head=accepted bucket_list=accepted
native checksum_present=false checksum_scope=Absent revision=ETag response_headers=S3
§4 native_behavior_detection PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1791918365, tv_nsec: 0 } published=SystemTime { tv_sec: 1791918366, tv_nsec: 0 } after=SystemTime { tv_sec: 1791918366, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } published=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } after=SystemTime { tv_sec: 1791918368, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag provider_case=S3_new_device_folder trial=1 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.002042958 list_end_s=0.003949667 present=true
listing_lag provider_case=S3_object trial=1 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.002138583 list_end_s=0.004668708 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.003949667 median_s=0.003949667 max_s=0.003949667 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=2 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.002021917 list_end_s=0.004118708 present=true
listing_lag provider_case=S3_new_device_folder trial=2 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001970042 list_end_s=0.005679833 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005679833 median_s=0.005679833 max_s=0.005679833 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=3 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001937375 list_end_s=0.003493583 present=true
listing_lag provider_case=S3_new_device_folder trial=3 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001889875 list_end_s=0.007782625 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.007782625 median_s=0.007782625 max_s=0.007782625 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=4 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001966750 list_end_s=0.004014417 present=true
listing_lag provider_case=S3_new_device_folder trial=4 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001895833 list_end_s=0.008522417 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.008522417 median_s=0.008522417 max_s=0.008522417 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=5 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.002076875 list_end_s=0.004350459 present=true
listing_lag provider_case=S3_new_device_folder trial=5 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.002024334 list_end_s=0.005961042 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005961042 median_s=0.005961042 max_s=0.005961042 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=6 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001922500 list_end_s=0.003340916 present=true
listing_lag provider_case=S3_new_device_folder trial=6 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001874833 list_end_s=0.004935833 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.004935833 median_s=0.004935833 max_s=0.004935833 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=7 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001864750 list_end_s=0.003719875 present=true
listing_lag provider_case=S3_new_device_folder trial=7 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001826667 list_end_s=0.005271625 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005271625 median_s=0.005271625 max_s=0.005271625 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=8 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001925625 list_end_s=0.003585000 present=true
listing_lag provider_case=S3_new_device_folder trial=8 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001874292 list_end_s=0.005186333 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005186333 median_s=0.005186333 max_s=0.005186333 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=9 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001931125 list_end_s=0.003614333 present=true
listing_lag provider_case=S3_new_device_folder trial=9 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001884375 list_end_s=0.005159583 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005159583 median_s=0.005159583 max_s=0.005159583 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=10 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001827625 list_end_s=0.003564833 present=true
listing_lag provider_case=S3_new_device_folder trial=10 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001794458 list_end_s=0.005065875 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005065875 median_s=0.005065875 max_s=0.005065875 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=11 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001908500 list_end_s=0.003948667 present=true
listing_lag provider_case=S3_new_device_folder trial=11 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001853000 list_end_s=0.005510708 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005510708 median_s=0.005510708 max_s=0.005510708 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=12 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001914750 list_end_s=0.003787250 present=true
listing_lag provider_case=S3_new_device_folder trial=12 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001862417 list_end_s=0.005405542 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005405542 median_s=0.005405542 max_s=0.005405542 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=13 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001877000 list_end_s=0.003596000 present=true
listing_lag provider_case=S3_new_device_folder trial=13 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001830333 list_end_s=0.005202708 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005202708 median_s=0.005202708 max_s=0.005202708 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=14 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001977292 list_end_s=0.003869542 present=true
listing_lag provider_case=S3_new_device_folder trial=14 stored_at=SystemTime { tv_sec: 1791918368, tv_nsec: 0 } list_start_s=0.001922167 list_end_s=0.005373500 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005373500 median_s=0.005373500 max_s=0.005373500 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=15 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.002135083 list_end_s=0.004563500 present=true
listing_lag provider_case=S3_new_device_folder trial=15 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.002057125 list_end_s=0.006164208 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.006164208 median_s=0.006164208 max_s=0.006164208 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=16 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.001910083 list_end_s=0.003845875 present=true
listing_lag provider_case=S3_new_device_folder trial=16 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.001859250 list_end_s=0.005236000 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005236000 median_s=0.005236000 max_s=0.005236000 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=17 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.001882125 list_end_s=0.003528750 present=true
listing_lag provider_case=S3_new_device_folder trial=17 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.001833666 list_end_s=0.005012000 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005012000 median_s=0.005012000 max_s=0.005012000 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=18 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.001817250 list_end_s=0.003419708 present=true
listing_lag provider_case=S3_new_device_folder trial=18 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.001779833 list_end_s=0.004798500 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.004798500 median_s=0.004798500 max_s=0.004798500 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=19 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.001946791 list_end_s=0.003556000 present=true
listing_lag provider_case=S3_new_device_folder trial=19 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.001896000 list_end_s=0.005030666 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005030666 median_s=0.005030666 max_s=0.005030666 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=20 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.002026125 list_end_s=0.004116042 present=true
listing_lag provider_case=S3_new_device_folder trial=20 stored_at=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } list_start_s=0.001974417 list_end_s=0.005247542 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005247542 median_s=0.005247542 max_s=0.005247542 omitted_trials=0 timeouts=0
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.003340916 median_s=0.003753562 max_s=0.004668708 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
S3Profile {
    name: S3ProfileName::Uncatalogued,
    endpoint: Some(EndpointMatch::Host("localhost")),
    signing: Signing::AwsSigV4,
    single_put: SinglePut::IfNoneMatch,
    multipart: Multipart::ConditionalCompletion,
    multipart_timestamp: MultipartTimestamp::Completion,
    checksums: Checksums::Absent,
    listing: Listing::V2,
    revision: Revision::ETag,
    response_headers: ResponseHeaders::S3,
    request_checksums: false,
    certification: Certification::Passed { date: "2026-10-13", suite: 2 },
},
certification suite=2 PASS
```

### SeaweedFS

SeaweedFS 4.48 (`chrislusf/seaweedfs:latest`, `sha256:4e61d15fd359`) S3
gateway, region `us-east-1`. Certified by suite 2, with the same observations
as RustFS: HTTP 412 refusals on PUT and multipart completion, a
completion-time multipart `Last-Modified`, and no checksum returned. Google
HMAC signing was refused with HTTP 403 rather than 400.
Suite 4 later failed SeaweedFS and withdrew this certificate; see
[SeaweedFS 4.48](#seaweedfs-448).

```text
native signing=GoogleHmac result=PermissionDenied HTTP 403
native signing=AwsSigV4 result=accepted
native single_put IfNoneMatch empty=accepted taken=AlreadyExists HTTP 412 preserved=true
native single_put GenerationMatch empty=accepted taken=accepted preserved=false
native partial_upload HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
§4 native partial_uploads_hidden PASS
native conditional_completion empty=accepted
native multipart_timestamp=Completion start_before=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } start_after=SystemTime { tv_sec: 1791918369, tv_nsec: 0 } completion_before=SystemTime { tv_sec: 1791918371, tv_nsec: 0 } completion_after=SystemTime { tv_sec: 1791918371, tv_nsec: 0 } published=SystemTime { tv_sec: 1791918371, tv_nsec: 0 }
native conditional_completion taken=AlreadyExists HTTP 412 preserved=true enforced=true
native listing=V2 objects=5 pages_forced_at=1
native checksum_mode=accepted
native bucket_head=accepted bucket_list=accepted
native checksum_present=false checksum_scope=Absent revision=ETag response_headers=S3
§4 native_behavior_detection PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1791918374, tv_nsec: 0 } published=SystemTime { tv_sec: 1791918374, tv_nsec: 0 } after=SystemTime { tv_sec: 1791918374, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } published=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } after=SystemTime { tv_sec: 1791918376, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag provider_case=S3_new_device_folder trial=1 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001795625 list_end_s=0.002819458 present=true
listing_lag provider_case=S3_object trial=1 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001846083 list_end_s=0.003651750 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002819458 median_s=0.002819458 max_s=0.002819458 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=2 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001936667 list_end_s=0.002900834 present=true
listing_lag provider_case=S3_new_device_folder trial=2 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001854417 list_end_s=0.003769500 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.003769500 median_s=0.003769500 max_s=0.003769500 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=3 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001990584 list_end_s=0.003151417 present=true
listing_lag provider_case=S3_new_device_folder trial=3 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001943000 list_end_s=0.004393250 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.004393250 median_s=0.004393250 max_s=0.004393250 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=4 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001885500 list_end_s=0.002954500 present=true
listing_lag provider_case=S3_new_device_folder trial=4 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001835083 list_end_s=0.004780000 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.004780000 median_s=0.004780000 max_s=0.004780000 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=5 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001883791 list_end_s=0.003240875 present=true
listing_lag provider_case=S3_new_device_folder trial=5 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001849208 list_end_s=0.005342666 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005342666 median_s=0.005342666 max_s=0.005342666 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=6 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001897250 list_end_s=0.003198583 present=true
listing_lag provider_case=S3_new_device_folder trial=6 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001838416 list_end_s=0.005930625 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.005930625 median_s=0.005930625 max_s=0.005930625 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=7 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001847875 list_end_s=0.003285666 present=true
listing_lag provider_case=S3_new_device_folder trial=7 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001810458 list_end_s=0.006878541 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.006878541 median_s=0.006878541 max_s=0.006878541 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=8 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001873167 list_end_s=0.003044875 present=true
listing_lag provider_case=S3_new_device_folder trial=8 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001824542 list_end_s=0.007492625 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.007492625 median_s=0.007492625 max_s=0.007492625 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=9 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.002022166 list_end_s=0.003642375 present=true
listing_lag provider_case=S3_new_device_folder trial=9 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001972416 list_end_s=0.008072208 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.008072208 median_s=0.008072208 max_s=0.008072208 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=10 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001898417 list_end_s=0.003149375 present=true
listing_lag provider_case=S3_new_device_folder trial=10 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001855583 list_end_s=0.008679833 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.008679833 median_s=0.008679833 max_s=0.008679833 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=11 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001772375 list_end_s=0.003090042 present=true
listing_lag provider_case=S3_new_device_folder trial=11 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001742583 list_end_s=0.009064542 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.009064542 median_s=0.009064542 max_s=0.009064542 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=12 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001882291 list_end_s=0.003131125 present=true
listing_lag provider_case=S3_new_device_folder trial=12 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001830958 list_end_s=0.009910875 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.009910875 median_s=0.009910875 max_s=0.009910875 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=13 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001889333 list_end_s=0.003444583 present=true
listing_lag provider_case=S3_new_device_folder trial=13 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001850625 list_end_s=0.010103750 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.010103750 median_s=0.010103750 max_s=0.010103750 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=14 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001915708 list_end_s=0.003440041 present=true
listing_lag provider_case=S3_new_device_folder trial=14 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001870083 list_end_s=0.011385250 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.011385250 median_s=0.011385250 max_s=0.011385250 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=15 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001847917 list_end_s=0.003598375 present=true
listing_lag provider_case=S3_new_device_folder trial=15 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001814958 list_end_s=0.011761042 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.011761042 median_s=0.011761042 max_s=0.011761042 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=16 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001809167 list_end_s=0.003738917 present=true
listing_lag provider_case=S3_new_device_folder trial=16 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001765417 list_end_s=0.011731500 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.011731500 median_s=0.011731500 max_s=0.011731500 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=17 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001877958 list_end_s=0.003931250 present=true
listing_lag provider_case=S3_new_device_folder trial=17 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001840875 list_end_s=0.012763292 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.012763292 median_s=0.012763292 max_s=0.012763292 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=18 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001871833 list_end_s=0.003577083 present=true
listing_lag provider_case=S3_new_device_folder trial=18 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001825917 list_end_s=0.015149333 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.015149333 median_s=0.015149333 max_s=0.015149333 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=19 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001873208 list_end_s=0.004244208 present=true
listing_lag provider_case=S3_new_device_folder trial=19 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001822542 list_end_s=0.015255125 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.015255125 median_s=0.015255125 max_s=0.015255125 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=20 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001821334 list_end_s=0.004299125 present=true
listing_lag provider_case=S3_new_device_folder trial=20 stored_at=SystemTime { tv_sec: 1791918376, tv_nsec: 0 } list_start_s=0.001783917 list_end_s=0.016759417 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.016759417 median_s=0.016759417 max_s=0.016759417 omitted_trials=0 timeouts=0
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.002900834 median_s=0.003362853 max_s=0.004299125 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
S3Profile {
    name: S3ProfileName::Uncatalogued,
    endpoint: Some(EndpointMatch::Host("localhost")),
    signing: Signing::AwsSigV4,
    single_put: SinglePut::IfNoneMatch,
    multipart: Multipart::ConditionalCompletion,
    multipart_timestamp: MultipartTimestamp::Completion,
    checksums: Checksums::Absent,
    listing: Listing::V2,
    revision: Revision::ETag,
    response_headers: ResponseHeaders::S3,
    request_checksums: false,
    certification: Certification::Passed { date: "2026-10-13", suite: 2 },
},
certification suite=2 PASS
```

## Detection (suite 3)

`s3_detection` ran production detection from each env location: from the
catalogued GCS and R2 hosts' own settings, and from the plain S3 settings for
the self-hosted servers, which name no profile. Each run printed every
observation, the derived settings, the setup safety check's verdict for that
location, and the catalogued entries the settings reproduce. RustFS and
SeaweedFS have identical entries, so each reproduces both. Both also enforced
`If-None-Match: *` on `CopyObject`, which their entries do not need.

### GCS detection

```text
detect Signing(GoogleHmac) held=true accepted
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect SinglePut(GenerationMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1791920579..1791920579 completion=1791920580..1791920581 published=1791920581
detect ConditionalCompletion held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(GenerationMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(CloudflareIfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyTimestamp held=true condition=GenerationMatch source=1791920578 copy=1791920581
detect ChecksumMode held=true accepted
detect PassiveChecksum held=true checksum=true unstated=false google_hash=true generation=true
detect ChecksumReturned held=true checksum=true unstated=false google_hash=true generation=true
detect ChecksumScopeStated held=true checksum=true unstated=false google_hash=true generation=true
detect GoogleGeneration held=true checksum=true unstated=false google_hash=true generation=true
detect GoogleHeaders held=true checksum=true unstated=false google_hash=true generation=true
detect BucketHead held=true accepted
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: GoogleHmac, single_put: GenerationMatch, multipart: Copy(GenerationMatch), multipart_timestamp: Completion, checksums: ScopeStated, listing: V2, revision: Generation, response_headers: Google, request_checksums: false }
setup_safety_check PASS
catalogued entries reproduced: [Gcs]
```

### R2 detection

```text
detect Signing(GoogleHmac) held=false Refused HTTP 400
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect SinglePut(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1791920590..1791920591 completion=1791920593..1791920594 published=1791920594
detect ConditionalCompletion held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(CloudflareIfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyTimestamp held=true condition=GenerationMatch source=1791920588 copy=1791920596
detect ChecksumMode held=true accepted
detect PassiveChecksum held=false checksum=true unstated=true google_hash=false generation=false
detect ChecksumReturned held=true checksum=true unstated=true google_hash=false generation=false
detect ChecksumScopeStated held=false checksum=true unstated=true google_hash=false generation=false
detect GoogleGeneration held=false checksum=true unstated=true google_hash=false generation=false
detect GoogleHeaders held=false checksum=true unstated=true google_hash=false generation=false
detect BucketHead held=true accepted
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: UnstatedIgnored, listing: V2, revision: ETag, response_headers: S3, request_checksums: true }
setup_safety_check PASS
catalogued entries reproduced: [R2]
```

### RustFS detection

```text
detect Signing(GoogleHmac) held=false Refused HTTP 400
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect SinglePut(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1791920609..1791920609 completion=1791920610..1791920610 published=1791920610
detect ConditionalCompletion held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(CloudflareIfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyTimestamp held=true condition=GenerationMatch source=1791920609 copy=1791920610
detect ChecksumMode held=true accepted
detect PassiveChecksum held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumReturned held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumScopeStated held=true checksum=false unstated=false google_hash=false generation=false
detect GoogleGeneration held=false checksum=false unstated=false google_hash=false generation=false
detect GoogleHeaders held=false checksum=false unstated=false google_hash=false generation=false
detect BucketHead held=true accepted
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_safety_check PASS
catalogued entries reproduced: [RustFs, SeaweedFs]
```

### SeaweedFS detection

```text
detect Signing(GoogleHmac) held=false PermissionDenied HTTP 403
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect SinglePut(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1791920612..1791920612 completion=1791920613..1791920613 published=1791920613
detect ConditionalCompletion held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(CloudflareIfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyTimestamp held=true condition=GenerationMatch source=1791920612 copy=1791920613
detect ChecksumMode held=true accepted
detect PassiveChecksum held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumReturned held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumScopeStated held=true checksum=false unstated=false google_hash=false generation=false
detect GoogleGeneration held=false checksum=false unstated=false google_hash=false generation=false
detect GoogleHeaders held=false checksum=false unstated=false google_hash=false generation=false
detect BucketHead held=true accepted
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_safety_check PASS
catalogued entries reproduced: [RustFs, SeaweedFs]
```

## Concurrent certification (suite 4)

Each trial prepares distinct 64-KiB payloads for one fresh name, releases all
writers from a barrier, and records every request's start and end relative to
that trial's clock. The latest start must precede the earliest end. Exactly one
native publication request must succeed; all others must return a failed
condition (`AlreadyExists` or `Conflict`), and a read must return the winner's
bytes. Adapter retry/recovery cannot turn a second accepted write into a
refusal in this measurement. The checks are:

- `concurrent_put_2`: 200 trials of two conditional PUTs.
- `concurrent_put_8`: 50 trials of eight conditional PUTs.
- `concurrent_multipart_publication_2`: 200 trials of two conditional
  completions, or the profile's publication copies after temporary assembly.
- `concurrent_multipart_publication_8`: 50 trials of eight such publications.
- `concurrent_put_and_multipart_publication`: 200 trials of a conditional PUT
  racing a completion or publication copy.
- `concurrent_replace_current_revision`: 200 trials of two PUTs conditioned
  on the same current ETag or GCS generation.

All six checks affect the verdict. The suite records versioning before and
after the run, requires it to stay unchanged, and prints the setup verdict
separately: certification must be able to test a bucket setup refuses.
Cleanup lists every version and delete marker before deleting them by version
ID, as well as removing current objects and incomplete uploads. GCS version
listings use `x-goog-interop-list-objects-format: enabled` for the XML API's
[S3-compatible version IDs and pagination](https://docs.cloud.google.com/storage/docs/xml-api/get-bucket-list).
A versioning certificate requires a passing run with versioning `Enabled`.

### Ceph RGW 20.2.4

Run on 2026-10-14 against the supplied local Ceph RGW 20.2.4 server, selecting
`COVEN_LIVE_S3_PROFILE=CephRgw`. Its detected settings equal RustFS's: AWS
signing, conditional PUT and multipart completion, completion timestamps,
no returned checksum, V2 listing and ETag revisions.
Unversioned conditional replacement losers use HTTP 409
`ConcurrentModification`, which the adapter classifies as `Conflict`.

#### Versioning disabled

`ceph-rgw.env` reported `Disabled` before and after the suite. All 900 trials
had overlapping requests, exactly one success, refused competitors, and the
winner's stored bytes. Every existing contract and every cleanup passed.
The unversioned profile is certified by suite 4 on 2026-10-14; its
`versioned_buckets` property remains `Uncertified`.

Each race's first trial and all aggregates and check verdicts follow:

```text
bucket versioning=Disabled
setup_safety_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=259 end_us=36543 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=62 end_us=44286 outcome=AlreadyExists HTTP 412
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=284 end_us=51640 outcome=accepted
race Put writers=8 trial=1 writer=1 start_us=579 end_us=69028 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=509 end_us=68692 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=367 end_us=110904 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=440 end_us=51683 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=649 end_us=110862 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=713 end_us=68715 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=48 end_us=68651 outcome=AlreadyExists HTTP 412
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=161 end_us=106114 outcome=accepted
race Multipart writers=2 trial=1 writer=1 start_us=11 end_us=131399 outcome=AlreadyExists HTTP 412
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=374 end_us=138063 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=173 end_us=219319 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=301 end_us=138081 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=254 end_us=138095 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=216 end_us=219112 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=340 end_us=252616 outcome=accepted
race Multipart writers=8 trial=1 writer=6 start_us=409 end_us=219095 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=43 end_us=178725 outcome=AlreadyExists HTTP 412
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=149 end_us=84038 outcome=accepted
race Mixed writers=2 trial=1 writer=1 start_us=11 end_us=154303 outcome=AlreadyExists HTTP 412
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=145 end_us=76514 outcome=Conflict HTTP 409
race Replace writers=2 trial=1 writer=1 start_us=14 end_us=65757 outcome=accepted
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
§4 revision_changes_on_replace PASS cleanup=PASS
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=4 PASS
```

#### Versioning enabled

`ceph-rgw-versioned.env` reported `Enabled` before and after the suite. Every
one of the 900 overlapping trials accepted every writer: 2,400 successful
requests where exclusivity required 900. Sequential detection and the existing
adapter contracts passed; setup refused the bucket through
`S3VersioningUnsupported`. Every case's cleanup passed, including versions and
delete markers. No versioned certificate was issued.

The following retains each check's verdict, every race aggregate, and each
race's first trial with all writers' request intervals (microseconds from the
trial's origin). The live suite prints those intervals for every trial.

```text
bucket versioning=Enabled
setup_safety_check refused: S3 bucket versioning Enabled is not certified for this profile
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=284 end_us=89682 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=81 end_us=78865 outcome=accepted
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_put_2 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_2 FAIL cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=678 end_us=139244 outcome=accepted
race Put writers=8 trial=1 writer=1 start_us=533 end_us=150250 outcome=accepted
race Put writers=8 trial=1 writer=2 start_us=603 end_us=125264 outcome=accepted
race Put writers=8 trial=1 writer=3 start_us=303 end_us=115243 outcome=accepted
race Put writers=8 trial=1 writer=4 start_us=386 end_us=163935 outcome=accepted
race Put writers=8 trial=1 writer=5 start_us=745 end_us=154156 outcome=accepted
race Put writers=8 trial=1 writer=6 start_us=459 end_us=161183 outcome=accepted
race Put writers=8 trial=1 writer=7 start_us=51 end_us=122160 outcome=accepted
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=400 violations=50
§4 concurrent_put_8 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_8 FAIL cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=158 end_us=92866 outcome=accepted
race Multipart writers=2 trial=1 writer=1 start_us=13 end_us=122427 outcome=accepted
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_multipart_publication_2 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_multipart_publication_2 FAIL cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=188 end_us=451307 outcome=accepted
race Multipart writers=8 trial=1 writer=1 start_us=359 end_us=597061 outcome=accepted
race Multipart writers=8 trial=1 writer=2 start_us=274 end_us=473206 outcome=accepted
race Multipart writers=8 trial=1 writer=3 start_us=428 end_us=584728 outcome=accepted
race Multipart writers=8 trial=1 writer=4 start_us=319 end_us=558952 outcome=accepted
race Multipart writers=8 trial=1 writer=5 start_us=234 end_us=485125 outcome=accepted
race Multipart writers=8 trial=1 writer=6 start_us=393 end_us=584857 outcome=accepted
race Multipart writers=8 trial=1 writer=7 start_us=49 end_us=365934 outcome=accepted
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=400 violations=50
§4 concurrent_multipart_publication_8 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_multipart_publication_8 FAIL cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=150 end_us=150365 outcome=accepted
race Mixed writers=2 trial=1 writer=1 start_us=17 end_us=242881 outcome=accepted
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_put_and_multipart_publication cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_and_multipart_publication FAIL cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=300 end_us=297029 outcome=accepted
race Replace writers=2 trial=1 writer=1 start_us=23 end_us=270197 outcome=accepted
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_replace_current_revision cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_replace_current_revision FAIL cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
§4 revision_changes_on_replace PASS cleanup=PASS
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Enabled stable=true
certification suite=4 FAIL
```

### RustFS 1.0.1

Run on 2026-10-14 against a local podman container of `rustfs/rustfs:latest`
(RustFS 1.0.1, built 2026-10-03, `sha256:3bc0a69f7636`), selecting
`COVEN_LIVE_S3_PROFILE=RustFs`, with one bucket left unversioned and a second
with versioning `Enabled` before the run. Detection in both buckets
reproduced the RustFS entry. Both runs passed every check, and versioning
held its state through each. Conditional replacement losers received HTTP
412 `PreconditionFailed`, classified as `AlreadyExists`. The profile is
certified by suite 4 on 2026-10-14, and so is its `versioned_buckets`
property: unlike Ceph RGW 20.2.4, a versioned RustFS bucket let exactly one
conditional writer win every race.

#### RustFS versioning disabled

Each race's first trial and all aggregates and check verdicts follow:

```text
bucket versioning=Disabled
setup_safety_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=508 end_us=34858 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 writer=1 start_us=81 end_us=34631 outcome=accepted
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=470 end_us=31469 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=233 end_us=30705 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=308 end_us=31736 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=551 end_us=29997 outcome=accepted
race Put writers=8 trial=1 writer=4 start_us=396 end_us=30926 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=618 end_us=31193 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=685 end_us=30039 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=59 end_us=30440 outcome=AlreadyExists HTTP 412
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=277 end_us=23107 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=20 end_us=23459 outcome=accepted
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=188 end_us=21351 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=247 end_us=20169 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=135 end_us=21143 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=340 end_us=19985 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=296 end_us=20505 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=381 end_us=21667 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=414 end_us=20857 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=31 end_us=19704 outcome=accepted
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=123 end_us=16949 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=11 end_us=17012 outcome=accepted
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=104 end_us=39600 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=8 end_us=39086 outcome=accepted
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
§4 revision_changes_on_replace PASS cleanup=PASS
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=4 PASS
```

#### RustFS versioning enabled

The setup verdict printed at the start is the profile's state before this
run certified it.

```text
bucket versioning=Enabled
setup_safety_check refused: S3 bucket versioning Enabled is not certified for this profile
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=339 end_us=89303 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=58 end_us=89242 outcome=AlreadyExists HTTP 412
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=524 end_us=78526 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=979 end_us=81968 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=806 end_us=77984 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=1426 end_us=75656 outcome=accepted
race Put writers=8 trial=1 writer=4 start_us=1125 end_us=79632 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=1584 end_us=82637 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=1263 end_us=76170 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=92 end_us=80889 outcome=AlreadyExists HTTP 412
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=244 end_us=31413 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=21 end_us=32628 outcome=accepted
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=334 end_us=20566 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=160 end_us=21116 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=204 end_us=21294 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=296 end_us=20048 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=259 end_us=20299 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=371 end_us=20732 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=405 end_us=20893 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=53 end_us=19344 outcome=accepted
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=112 end_us=28889 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=11 end_us=29095 outcome=accepted
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=112 end_us=27073 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=9 end_us=26821 outcome=accepted
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
§4 revision_changes_on_replace PASS cleanup=PASS
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Enabled stable=true
certification suite=4 PASS
```

### SeaweedFS 4.48

Run on 2026-10-14 against a local podman container of
`chrislusf/seaweedfs:latest` (SeaweedFS 4.48, `530be3e37`,
`sha256:f1f303474940`, `weed server -s3 -s3.iam.readOnly=false`), selecting
`COVEN_LIVE_S3_PROFILE=SeaweedFs`, in an unversioned bucket and in one with
versioning `Enabled`. Detection reproduced the SeaweedFS entry in both, and
conditional PUTs and conditional replacements kept exactly one winner in every
trial. Conditional multipart completion did not: in both buckets two racing
`CompleteMultipartUpload` requests with `If-None-Match: *` both succeeded in
198 of 200 trials, and 49 of 50 eight-writer trials accepted more than one
(389 and 393 acceptances where 50 were allowed). Two of 200 trials of a PUT
racing a completion accepted both; in each the completion, which finished
last, also succeeded. Sequential checks, including the setup safety check's
occupied-name completion, passed. Every cleanup passed.

Suite 4 fails SeaweedFS with versioning disabled as well as enabled. Its suite
2 certificate predates the concurrent checks, so the profile is now
`Uncertified`, and `versioned_buckets` stays `Uncertified`: setup
refuses versioned SeaweedFS buckets.

A failing two-writer multipart trial and the failing mixed trial (writer 0
the PUT, writer 1 the completion), unversioned:

```text
race Multipart writers=2 trial=2 writer=0 start_us=66 end_us=1414 outcome=accepted
race Multipart writers=2 trial=2 writer=1 start_us=8 end_us=1929 outcome=accepted
race Multipart writers=2 trial=2 overlap=true successes=2 pass=false
race Mixed writers=2 trial=27 writer=0 start_us=42 end_us=1750 outcome=accepted
race Mixed writers=2 trial=27 writer=1 start_us=5 end_us=2144 outcome=accepted
race Mixed writers=2 trial=27 overlap=true successes=2 pass=false
```

#### SeaweedFS versioning disabled

```text
bucket versioning=Disabled
setup_safety_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=184 end_us=4156 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 writer=1 start_us=21 end_us=3887 outcome=accepted
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=544 end_us=1013306 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=222 end_us=9645 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=388 end_us=9357 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=302 end_us=5020995 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=456 end_us=8274 outcome=accepted
race Put writers=8 trial=1 writer=5 start_us=619 end_us=2015700 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=752 end_us=3016790 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=51 end_us=4019791 outcome=AlreadyExists HTTP 412
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=77 end_us=1799 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=10 end_us=1622 outcome=accepted
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=398 violations=198
§4 concurrent_multipart_publication_2 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_multipart_publication_2 FAIL cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=308 end_us=3524 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=106 end_us=3283 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=236 end_us=3228 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=70 end_us=3264 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=272 end_us=3204 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=160 end_us=2870 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=198 end_us=3801 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=22 end_us=2167 outcome=accepted
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=389 violations=49
§4 concurrent_multipart_publication_8 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_multipart_publication_8 FAIL cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=74 end_us=2373 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=11 end_us=2405 outcome=accepted
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=202 violations=2
§4 concurrent_put_and_multipart_publication cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_and_multipart_publication FAIL cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=98 end_us=3206 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=8 end_us=2258 outcome=accepted
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
§4 revision_changes_on_replace PASS cleanup=PASS
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=4 FAIL
```

#### SeaweedFS versioning enabled

```text
bucket versioning=Enabled
setup_safety_check refused: S3 bucket versioning Enabled is not certified for this profile
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=164 end_us=4389 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=20 end_us=1006861 outcome=AlreadyExists HTTP 412
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=2933 end_us=24892 outcome=accepted
race Put writers=8 trial=1 writer=1 start_us=2831 end_us=126000 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=3270 end_us=3030530 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=3343 end_us=5042959 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=3412 end_us=125936 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=3015 end_us=2029293 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=3094 end_us=1025682 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=2490 end_us=4040706 outcome=AlreadyExists HTTP 412
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=66 end_us=1817 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=10 end_us=1622 outcome=accepted
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=398 violations=198
§4 concurrent_multipart_publication_2 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_multipart_publication_2 FAIL cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=125 end_us=3431 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=267 end_us=3744 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=183 end_us=3657 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=222 end_us=3456 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=83 end_us=3690 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=340 end_us=3675 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=302 end_us=3468 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=29 end_us=1935 outcome=accepted
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=393 violations=49
§4 concurrent_multipart_publication_8 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_multipart_publication_8 FAIL cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=70 end_us=4365 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=9 end_us=2374 outcome=accepted
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=202 violations=2
§4 concurrent_put_and_multipart_publication cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_and_multipart_publication FAIL cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=131 end_us=1006074 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=8 end_us=4164 outcome=accepted
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
§4 revision_changes_on_replace PASS cleanup=PASS
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Enabled stable=true
certification suite=4 FAIL
```

### SeaweedFS 4.48 copy publication (suite 5)

Suite 5 races a named profile's own publication: suite 4 raced the detected
settings, whose conditional completion SeaweedFS's entry no longer uses. Run on
2026-10-14 against a local podman container of `chrislusf/seaweedfs:latest`
(SeaweedFS 4.48, `530be3e37`, `sha256:8f17c8c82e03`, `weed server -s3
-s3.config=<identities>`), selecting `COVEN_LIVE_S3_PROFILE=SeaweedFs`, in an
unversioned bucket and in one with versioning `Enabled`. The candidate
publishes multipart uploads by `CopyObject` with `If-None-Match: *`.

Copy publication held: in both buckets every two- and eight-writer copy race
kept exactly one winner (200 and 50 trials), as did conditional PUTs and
conditional replacements. The unversioned bucket still failed: in 14 of 200
trials a conditional PUT racing a conditional copy of the same name both
succeeded. The versioned bucket passed every check, including that race.
Detection again chose conditional completion, which its sequential probes
cannot tell apart from a racing failure; the named entry passed the safety
check against the same observations.

Suite 5 fails SeaweedFS with versioning disabled, so the profile stays
`Uncertified` and setup keeps refusing it (`S3ProfileRefused`). The versioned
pass does not certify versioned buckets on its own: the profile has no
passing unversioned run. A failing mixed trial (writer 0 the PUT, writer 1
the copy), unversioned:

```text
race Mixed writers=2 trial=4 writer=0 start_us=92 end_us=2284 outcome=accepted
race Mixed writers=2 trial=4 writer=1 start_us=10 end_us=2698 outcome=accepted
race Mixed writers=2 trial=4 overlap=true successes=2 pass=false
```

#### SeaweedFS copy publication, versioning disabled

```text
bucket versioning=Disabled
setup_safety_check PASS
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: Copy(IfNoneMatch), multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check refused: S3 profile SeaweedFs is refused: concurrent create-once publication failed certification
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=273 end_us=6489 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=60 end_us=19550 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=701 end_us=1008346 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=779 end_us=3015509 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=504 end_us=8085 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=260 end_us=6660 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=427 end_us=2010233 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=855 end_us=5968 outcome=accepted
race Put writers=8 trial=1 writer=6 start_us=346 end_us=13295 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=48 end_us=7061 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=115 end_us=2818 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=18 end_us=2255 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=777 end_us=56804 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=897 end_us=56844 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=1023 end_us=3013380 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=485 end_us=2012377 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=1137 end_us=1008520 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=279 end_us=4018072 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=650 end_us=5988 outcome=accepted
race Multipart writers=8 trial=1 writer=7 start_us=58 end_us=58300 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=98 end_us=2303 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=12 end_us=2332 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=214 violations=14
§4 concurrent_put_and_multipart_publication cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_and_multipart_publication FAIL cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=99 end_us=4340 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=8 end_us=3394 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
§4 revision_changes_on_replace PASS cleanup=PASS
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=5 FAIL
```

#### SeaweedFS copy publication, versioning enabled

```text
bucket versioning=Enabled
setup_safety_check refused: S3 bucket versioning Enabled is not certified for this profile
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: Copy(IfNoneMatch), multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check refused: S3 profile SeaweedFs is refused: concurrent create-once publication failed certification
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=396 end_us=6537 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=76 end_us=1009096 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=2218 end_us=19216 outcome=accepted
race Put writers=8 trial=1 writer=1 start_us=828 end_us=23447 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=2481 end_us=2023860 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=1173 end_us=4032615 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=1601 end_us=3026792 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=636 end_us=5032273 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=1883 end_us=1022817 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=145 end_us=20869 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=189 end_us=1011056 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=17 end_us=9071 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=112 end_us=3014985 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=322 end_us=1004912 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=490 end_us=7029962 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=380 end_us=2014367 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=212 end_us=4023056 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=433 end_us=6028011 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=271 end_us=4810 outcome=accepted
race Multipart writers=8 trial=1 writer=7 start_us=53 end_us=5023365 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=96 end_us=4110 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=12 end_us=3526 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=285 end_us=1012564 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=28 end_us=10729 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
§4 revision_changes_on_replace PASS cleanup=PASS
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Enabled stable=true
certification suite=5 PASS
```

### Services without a versioned certificate

On 2026-10-14 the supplied GCS HMAC key (`gcs-s3.env`) returned HTTP 403
`AccessDenied` for bucket metadata and for an attempted second-bucket creation.
No second bucket was created and the existing bucket setting was not changed.
The versioned suite was not run; GCS therefore has no versioning certificate.
The Rust XML read produced:

```text
bucket versioning=Unknown failure=PermissionDenied status=Some(403)
setup_versioning_check refused: S3 bucket versioning is unknown; this profile requires a successful GetBucketVersioning (s3:GetBucketVersioning; GCS storage.buckets.get): S3: PermissionDenied: S3 request failed
```

Member access needs `storage.buckets.get` to read the XML versioning state;
creating a certification bucket also requires the owner's bucket-creation
permission.

R2 has no object-versioning feature. Its official
[S3 compatibility table](https://developers.cloudflare.com/r2/api/s3/api/)
lists both `GetBucketVersioning` and `PutBucketVersioning` as unsupported.
There is no versioning-on run to perform. The adapter records this as
`Unavailable` only at the hosted R2 endpoint; an arbitrary endpoint merely
named R2 still has to establish its bucket's state. The live read with `r2.env`
returned HTTP 403; the Rust adapter recorded:

```text
bucket versioning=Unavailable
setup_versioning_check PASS
```

SeaweedFS's versioned certificate comes from its every-object copy
publication (below). AWS has no reachable
live account and has no versioned certificate.

## Member keys

On 2026-10-14 `s3_member_key_lifecycle` ran each self-hosted minter against a
local podman container with the profile named by `COVEN_LIVE_S3_PROFILE` and
the location prefix `coven-live`. With the service's admin key it mints a key
for a fresh store and reads it listed, runs setup's member-side check, then
uses the member key to write and list another store's prefix, read an object
the admin wrote there, and write at the bucket's root outside the base
prefix; each must be refused with permission denied. It then revokes the key,
reads it absent, revokes it again, and lists its own prefix with it until the
service refuses the key. A rollback of the attempt's name and the cleanup of
both stores follow. Before RustFS passed, its second revocation failed: RustFS
answers a delete of an absent service account with HTTP 400
`InvalidRequest` ("service account not exist") where `info-service-account`
answers 404; the minter now reads absence back after a refused delete.

- RustFS 1.0.1 (`rustfs/rustfs:latest`, built 2026-10-03,
  `sha256:3bc0a69f7636`), admin access the server's root key.
- SeaweedFS 4.48 (`chrislusf/seaweedfs:latest`, `530be3e37`,
  `sha256:f1f303474940`) run as `weed server -s3 -s3.config=<identities>
  -s3.iam.readOnly=false`, admin access a static identity with the `Admin`
  action. Afterwards its IAM listed no users or policies but that identity.
- Ceph RGW 20.2.4 (`quay.io/ceph/ceph:v20.2.4`, `sha256:6e6bc7b28fa1`), admin
  access the root user of an RGW account (`radosgw-admin account create`,
  then `user create --account-id=<id> --account-root`), which also created
  the bucket so the account owns it.
- Versity Gateway 1.9.0, run on 2026-10-15 with its standalone IAM service
  ([below](#versity-gateway-profile-2026-10-15)), admin access the gateway's
  root key with `COVEN_LIVE_S3_IAM_ENDPOINT` naming the IAM service.

Each run printed, apart from the cargo harness lines:

```text
mint PASS principal=RustFsServiceAccount
key_exists_after_mint=true
member_check PASS
write_other_store refused=PermissionDenied
read_other_store refused=PermissionDenied
list_other_store refused=PermissionDenied
write_outside_base_prefix refused=PermissionDenied
revoke PASS
key_exists_after_revoke=false
revoke_again PASS
revoked_key refused=Authentication after_s=0
member_key_lifecycle PASS
roll_back PASS cleanup PASS
```

SeaweedFS, Ceph RGW and Versity Gateway printed the same lines with
`principal=IamUser`.

## Full battery, 2026-10-14

Run on 2026-10-14 between 20:55 and 22:50 UTC from `main` at `e5718b511`,
with the three live-harness fixes committed alongside this evidence (named
below where they mattered). Suite version 5 is the current suite: it runs
every check of suites 1–4 (detection, setup safety, the adapter contracts,
publication times, replacement revisions, listing lag, and the six
concurrent races) plus the named profile's own publication races. For every
S3 location the battery ran `s3_detection`, `s3_bucket_versioning` and
`s3_certification`; for each self-hosted service with a minter it also ran
`s3_member_key_lifecycle`. Self-hosted services ran one at a time in a local
podman machine (libkrun, 2 GiB), reached over plain `http`, selected by
profile name where one is catalogued; Garage names no profile, so it ran with
detected settings. The cloud drives ran every ignored `*_live` test, and
CloudKit ran `cloudkit_live` in the signed host described in
[cloudkit-live.md](../crates/coven-storage/cloudkit-live.md).

| Provider | Build | Bucket | Suite 5 races (2×200, 8×50, mixed 200, replace 200) | Sequential contracts | Listing lag median / max | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| R2 | hosted | no versioning (`Unavailable`) | one winner in every trial | all PASS | 0.222 s / 0.358 s | suite 5 PASS |
| GCS | hosted | unknown (read refused) | not run: the key cannot read versioning | not run | — | detection reproduces `Gcs`; setup refuses this key (passed suite 5 [later](#gcs-and-versity-gateway-2026-10-14)) |
| RustFS | 1.0.1 | Disabled | one winner in every trial | all PASS | 0.010 s / 0.027 s | suite 5 PASS |
| RustFS | 1.0.1 | Enabled | one winner in every trial | all PASS | 0.008 s / 0.012 s | suite 5 PASS (versioned) |
| Ceph RGW | 20.2.4 | Disabled | one winner in every trial | all PASS | 0.047 s / 0.051 s | suite 5 PASS |
| Ceph RGW | 20.2.4 | Enabled | every writer accepted in every trial | all PASS | 0.008 s / 0.017 s | suite 5 FAIL |
| SeaweedFS | 4.48 | Disabled | PUT racing copy: both accepted in 17 of 200; others one winner | all PASS | 0.004 s / 0.010 s | suite 5 FAIL |
| SeaweedFS | 4.48 | Enabled | one winner in every trial | all PASS | 0.005 s / 0.006 s | suite 5 PASS (setup still refuses the profile) |
| Garage | 2.4.1 | Disabled | every race fails (see below) | create-once, multipart and timestamps FAIL | 0.003 s / 0.004 s | suite 5 FAIL; setup refuses `CreateOnce` |

Time resolution: every S3 service returned whole-second `Last-Modified`
values (`tv_nsec: 0` throughout). Dropbox and OneDrive report whole seconds,
Drive and CloudKit milliseconds. Listing lag is measured from the upload
reply to the first listing that contains the object (20 trials; new device
folders also 20 for S3); it does not establish the one-hour bound.

Compared with the earlier evidence above: R2 had only a suite 1 certificate
and now passes suite 5; RustFS passes suite 5 in both bucket states where it
had suite 4; Ceph RGW reproduces its suite 4 split (unversioned pass,
versioned failure with every writer accepted); SeaweedFS's unversioned mixed
race failed in 17 of 200 trials (14 of 200 before) while its versioned bucket
passed again; Garage, previously refused at setup only, now has a full suite
run, failing every race. GCS could not run suite 5, and the first CloudKit
run failed in its fixture (both below).

### Hosted S3

#### R2, suite 5

Detection and the suite ran from the catalogued R2 host's settings. The
bucket's versioning reads as `Unavailable` at R2's catalogued hostname, and
the run stayed in that state.

```text
bucket versioning=Unavailable
detect Signing(GoogleHmac) held=false Refused HTTP 400
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect SinglePut(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1792011469..1792011469 completion=1792011471..1792011472 published=1792011472
detect ConditionalCompletion held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(CloudflareIfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyTimestamp held=true condition=GenerationMatch source=1792011466 copy=1792011473
detect ChecksumMode held=true accepted
detect PassiveChecksum held=false checksum=true unstated=true google_hash=false generation=false
detect ChecksumReturned held=true checksum=true unstated=true google_hash=false generation=false
detect ChecksumScopeStated held=false checksum=true unstated=true google_hash=false generation=false
detect GoogleGeneration held=false checksum=true unstated=true google_hash=false generation=false
detect GoogleHeaders held=false checksum=true unstated=true google_hash=false generation=false
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: UnstatedIgnored, listing: V2, revision: ETag, response_headers: S3, request_checksums: true }
setup_safety_check PASS
catalogued entries reproduced: [R2]
differs from Aws: multipart=Copy(IfNoneMatch)/ConditionalCompletion multipart_timestamp=Start/Completion checksums=ScopeStated/UnstatedIgnored
differs from Gcs: signing=GoogleHmac/AwsSigV4 single_put=GenerationMatch/IfNoneMatch multipart=Copy(GenerationMatch)/ConditionalCompletion checksums=ScopeStated/UnstatedIgnored revision=Generation/ETag response_headers=Google/S3 request_checksums=false/true
differs from RustFs: checksums=Absent/UnstatedIgnored request_checksums=false/true
differs from SeaweedFs: multipart=Copy(IfNoneMatch)/ConditionalCompletion checksums=Absent/UnstatedIgnored request_checksums=false/true
differs from CephRgw: checksums=Absent/UnstatedIgnored request_checksums=false/true
```

```text
bucket versioning=Unavailable
setup_safety_check PASS
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: UnstatedIgnored, listing: V2, revision: ETag, response_headers: S3, request_checksums: true }
setup_profile_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=409 end_us=294587 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=22 end_us=342079 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=1085 end_us=546415 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=561 end_us=320022 outcome=accepted
race Put writers=8 trial=1 writer=2 start_us=1384 end_us=376427 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=740 end_us=376480 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=1498 end_us=432694 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=894 end_us=335371 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=1597 end_us=448503 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=100 end_us=345856 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=258 end_us=278357 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=20 end_us=204920 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=212 end_us=277216 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=376 end_us=284868 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=333 end_us=376159 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=459 end_us=513649 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=420 end_us=301254 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=496 end_us=272043 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=273 end_us=288614 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=55 end_us=213811 outcome=accepted
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=207 end_us=323946 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=48 end_us=297397 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=210 end_us=523184 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=10 end_us=426746 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1792012710, tv_nsec: 0 } published=SystemTime { tv_sec: 1792012711, tv_nsec: 0 } after=SystemTime { tv_sec: 1792012711, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1792012714, tv_nsec: 0 } published=SystemTime { tv_sec: 1792012714, tv_nsec: 0 } after=SystemTime { tv_sec: 1792012715, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=false
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=false
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.156540042 median_s=0.221673271 max_s=0.358039292 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Unavailable stable=true
certification suite=5 PASS
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.227654459 median_s=0.325715583 max_s=0.584699834 (aggregated from per-trial lines)
```

#### GCS

`gcs-s3.env`'s HMAC key cannot read bucket versioning: `GetBucketVersioning`
returns HTTP 403 `AccessDenied`, as on the earlier 2026-10-14 read. Detection
records that state as unknown and otherwise reproduces the `Gcs` entry, but
setup refuses the bucket: the `Gcs` profile has no versioned-bucket
certificate, so it requires a successful versioning read
(`S3VersioningUnknown`). Suite 5 requires a known versioning state and
stopped at that assertion before any race; GCS keeps its suite 1
certificate and has no suite 4 or 5 run. Running it needs a key whose service
account has `storage.buckets.get` on the bucket.

The first detection run failed outright: the live harness's own cleanup read
versioning again after detection and treated the refusal as a failure. The
harness now lists the probe namespace's versions in that state instead
(an unknown state may hold retained versions), and detection completed:

```text
bucket versioning=Unknown cause=S3: PermissionDenied: S3 request failed
detect Signing(GoogleHmac) held=true accepted
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect SinglePut(GenerationMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1792018299..1792018299 completion=1792018301..1792018301 published=1792018301
detect ConditionalCompletion held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(GenerationMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(CloudflareIfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyTimestamp held=true condition=GenerationMatch source=1792018298 copy=1792018302
detect ChecksumMode held=true accepted
detect PassiveChecksum held=true checksum=true unstated=false google_hash=true generation=true
detect ChecksumReturned held=true checksum=true unstated=false google_hash=true generation=true
detect ChecksumScopeStated held=true checksum=true unstated=false google_hash=true generation=true
detect GoogleGeneration held=true checksum=true unstated=false google_hash=true generation=true
detect GoogleHeaders held=true checksum=true unstated=false google_hash=true generation=true
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: GoogleHmac, single_put: GenerationMatch, multipart: Copy(GenerationMatch), multipart_timestamp: Completion, checksums: ScopeStated, listing: V2, revision: Generation, response_headers: Google, request_checksums: false }
setup_safety_check refused: S3 bucket versioning is unknown; this profile requires a successful GetBucketVersioning (s3:GetBucketVersioning; GCS storage.buckets.get): S3: PermissionDenied: S3 request failed
catalogued entries reproduced: [Gcs]
differs from Aws: signing=AwsSigV4/GoogleHmac single_put=IfNoneMatch/GenerationMatch multipart=Copy(IfNoneMatch)/Copy(GenerationMatch) multipart_timestamp=Start/Completion revision=ETag/Generation response_headers=S3/Google request_checksums=true/false
differs from R2: signing=AwsSigV4/GoogleHmac single_put=IfNoneMatch/GenerationMatch multipart=ConditionalCompletion/Copy(GenerationMatch) checksums=UnstatedIgnored/ScopeStated revision=ETag/Generation response_headers=S3/Google request_checksums=true/false
differs from RustFs: signing=AwsSigV4/GoogleHmac single_put=IfNoneMatch/GenerationMatch multipart=ConditionalCompletion/Copy(GenerationMatch) checksums=Absent/ScopeStated revision=ETag/Generation response_headers=S3/Google
differs from SeaweedFs: signing=AwsSigV4/GoogleHmac single_put=IfNoneMatch/GenerationMatch multipart=Copy(IfNoneMatch)/Copy(GenerationMatch) checksums=Absent/ScopeStated revision=ETag/Generation response_headers=S3/Google
differs from CephRgw: signing=AwsSigV4/GoogleHmac single_put=IfNoneMatch/GenerationMatch multipart=ConditionalCompletion/Copy(GenerationMatch) checksums=Absent/ScopeStated revision=ETag/Generation response_headers=S3/Google
```

```text
bucket versioning=Unknown failure=PermissionDenied status=Some(403)
setup_versioning_check refused: S3 bucket versioning is unknown; this profile requires a successful GetBucketVersioning (s3:GetBucketVersioning; GCS storage.buckets.get): S3: PermissionDenied: S3 request failed
```

### Self-hosted S3

Detection reproduced the same settings in each service's versioned bucket as
in its unversioned one (every observation identical apart from timestamps).
RustFS, SeaweedFS and Ceph RGW detect the settings of the RustFS and Ceph RGW
entries; SeaweedFS's named entry, which publishes multipart uploads by copy,
is the suite 5 candidate there.

#### RustFS 1.0.1

`rustfs/rustfs:latest` (RustFS 1.0.1, built 2026-10-03, image
`sha256:3bc0a69f7636`, digest `sha256:1803faef5762`), region `us-east-1`,
`COVEN_LIVE_S3_PROFILE=RustFs`, buckets `coven-live` (versioning disabled)
and `coven-live-versioned` (enabled before the run).

```text
bucket versioning=Disabled
detect Signing(GoogleHmac) held=false Refused HTTP 400
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect SinglePut(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1792011314..1792011314 completion=1792011315..1792011315 published=1792011315
detect ConditionalCompletion held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(CloudflareIfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyTimestamp held=true condition=GenerationMatch source=1792011314 copy=1792011315
detect ChecksumMode held=true accepted
detect PassiveChecksum held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumReturned held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumScopeStated held=true checksum=false unstated=false google_hash=false generation=false
detect GoogleGeneration held=false checksum=false unstated=false google_hash=false generation=false
detect GoogleHeaders held=false checksum=false unstated=false google_hash=false generation=false
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_safety_check PASS
catalogued entries reproduced: [RustFs, CephRgw]
differs from Aws: multipart=Copy(IfNoneMatch)/ConditionalCompletion multipart_timestamp=Start/Completion checksums=ScopeStated/Absent request_checksums=true/false
differs from Gcs: signing=GoogleHmac/AwsSigV4 single_put=GenerationMatch/IfNoneMatch multipart=Copy(GenerationMatch)/ConditionalCompletion checksums=ScopeStated/Absent revision=Generation/ETag response_headers=Google/S3
differs from R2: checksums=UnstatedIgnored/Absent request_checksums=true/false
differs from SeaweedFs: multipart=Copy(IfNoneMatch)/ConditionalCompletion
```

Versioning disabled:

```text
bucket versioning=Disabled
setup_safety_check PASS
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=223 end_us=25163 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=56 end_us=25232 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=323 end_us=21851 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=481 end_us=24726 outcome=accepted
race Put writers=8 trial=1 writer=2 start_us=670 end_us=26123 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=411 end_us=22035 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=255 end_us=25228 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=550 end_us=25797 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=608 end_us=25535 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=86 end_us=24940 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=138 end_us=34559 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=11 end_us=34864 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=233 end_us=34882 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=374 end_us=35711 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=410 end_us=34107 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=292 end_us=35214 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=179 end_us=34664 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=445 end_us=34351 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=337 end_us=35435 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=49 end_us=33137 outcome=accepted
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=148 end_us=17329 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=12 end_us=17496 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=153 end_us=72513 outcome=accepted
race Replace writers=2 trial=1 writer=1 start_us=8 end_us=72943 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1792011461, tv_nsec: 0 } published=SystemTime { tv_sec: 1792011461, tv_nsec: 0 } after=SystemTime { tv_sec: 1792011461, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1792011463, tv_nsec: 0 } published=SystemTime { tv_sec: 1792011463, tv_nsec: 0 } after=SystemTime { tv_sec: 1792011463, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.005751208 median_s=0.009557417 max_s=0.027306375 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=5 PASS
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.009163584 median_s=0.014619771 max_s=0.050472709 (aggregated from per-trial lines)
```

Versioning enabled:

```text
bucket versioning=Enabled
setup_safety_check PASS
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=270 end_us=128482 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=21 end_us=128443 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=427 end_us=97496 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=499 end_us=97539 outcome=accepted
race Put writers=8 trial=1 writer=2 start_us=354 end_us=99179 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=568 end_us=99629 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=268 end_us=100812 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=627 end_us=98580 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=691 end_us=97979 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=59 end_us=100304 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=162 end_us=59814 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=17 end_us=60232 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=336 end_us=81015 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=186 end_us=71824 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=238 end_us=81660 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=415 end_us=81096 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=297 end_us=71856 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=449 end_us=79285 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=377 end_us=81430 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=47 end_us=72273 outcome=accepted
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=147 end_us=70898 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=15 end_us=70950 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=143 end_us=63111 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=17 end_us=62908 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1792014042, tv_nsec: 0 } published=SystemTime { tv_sec: 1792014042, tv_nsec: 0 } after=SystemTime { tv_sec: 1792014042, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1792014044, tv_nsec: 0 } published=SystemTime { tv_sec: 1792014044, tv_nsec: 0 } after=SystemTime { tv_sec: 1792014044, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.004443083 median_s=0.008207187 max_s=0.012160792 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Enabled stable=true
certification suite=5 PASS
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.003992958 median_s=0.012666605 max_s=0.020480916 (aggregated from per-trial lines)
```

#### Ceph RGW 20.2.4

`quay.io/ceph/ceph:v20.2.4` (digest `sha256:6e6bc7b28fa1`), single node
from the same bootstrap as before, region `default`,
`COVEN_LIVE_S3_PROFILE=CephRgw`. The unversioned bucket again passed every
check. The versioned bucket again accepted every concurrent conditional
writer: 2,400 successful requests across the 900 trials where 900 were
allowed, conditional replacements included. Sequential detection and every
adapter contract passed there, which is why only the races can refuse it.

```text
bucket versioning=Disabled
detect Signing(GoogleHmac) held=false Refused HTTP 400
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect SinglePut(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1792016057..1792016059 completion=1792016060..1792016060 published=1792016060
detect ConditionalCompletion held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(CloudflareIfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyTimestamp held=true condition=GenerationMatch source=1792016057 copy=1792016060
detect ChecksumMode held=true accepted
detect PassiveChecksum held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumReturned held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumScopeStated held=true checksum=false unstated=false google_hash=false generation=false
detect GoogleGeneration held=false checksum=false unstated=false google_hash=false generation=false
detect GoogleHeaders held=false checksum=false unstated=false google_hash=false generation=false
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_safety_check PASS
catalogued entries reproduced: [RustFs, CephRgw]
differs from Aws: multipart=Copy(IfNoneMatch)/ConditionalCompletion multipart_timestamp=Start/Completion checksums=ScopeStated/Absent request_checksums=true/false
differs from Gcs: signing=GoogleHmac/AwsSigV4 single_put=GenerationMatch/IfNoneMatch multipart=Copy(GenerationMatch)/ConditionalCompletion checksums=ScopeStated/Absent revision=Generation/ETag response_headers=Google/S3
differs from R2: checksums=UnstatedIgnored/Absent request_checksums=true/false
differs from SeaweedFs: multipart=Copy(IfNoneMatch)/ConditionalCompletion
```

Versioning disabled:

```text
bucket versioning=Disabled
setup_safety_check PASS
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=481 end_us=70627 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=43 end_us=73915 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=413 end_us=119543 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=633 end_us=162295 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=487 end_us=120012 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=702 end_us=119601 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=322 end_us=162352 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=555 end_us=119646 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=772 end_us=119889 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=45 end_us=119571 outcome=accepted
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=208 end_us=106288 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=43 end_us=169716 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=311 end_us=203272 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=486 end_us=203198 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=225 end_us=103315 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=407 end_us=179509 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=364 end_us=103339 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=270 end_us=179475 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=449 end_us=203230 outcome=accepted
race Multipart writers=8 trial=1 writer=7 start_us=53 end_us=208531 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=276 end_us=35585 outcome=accepted
race Mixed writers=2 trial=1 writer=1 start_us=33 end_us=91474 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=165 end_us=93169 outcome=Conflict HTTP 409
race Replace writers=2 trial=1 writer=1 start_us=14 end_us=75126 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1792017440, tv_nsec: 0 } published=SystemTime { tv_sec: 1792017440, tv_nsec: 0 } after=SystemTime { tv_sec: 1792017440, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1792017442, tv_nsec: 0 } published=SystemTime { tv_sec: 1792017442, tv_nsec: 0 } after=SystemTime { tv_sec: 1792017442, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=false
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.006870292 median_s=0.046508229 max_s=0.051339875 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=5 PASS
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.048483834 median_s=0.055042042 max_s=0.098800583 (aggregated from per-trial lines)
```

Versioning enabled:

```text
bucket versioning=Enabled
setup_safety_check refused: S3 bucket versioning Enabled is not certified for this profile
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=320 end_us=73639 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=63 end_us=65003 outcome=accepted
race Put writers=2 trial=1 overlap=true successes=2 pass=false
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_put_2 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_2 FAIL cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=827 end_us=409440 outcome=accepted
race Put writers=8 trial=1 writer=1 start_us=755 end_us=309757 outcome=accepted
race Put writers=8 trial=1 writer=2 start_us=492 end_us=384095 outcome=accepted
race Put writers=8 trial=1 writer=3 start_us=386 end_us=403826 outcome=accepted
race Put writers=8 trial=1 writer=4 start_us=583 end_us=316591 outcome=accepted
race Put writers=8 trial=1 writer=5 start_us=669 end_us=296198 outcome=accepted
race Put writers=8 trial=1 writer=6 start_us=913 end_us=349674 outcome=accepted
race Put writers=8 trial=1 writer=7 start_us=65 end_us=377299 outcome=accepted
race Put writers=8 trial=1 overlap=true successes=8 pass=false
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=400 violations=50
§4 concurrent_put_8 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_8 FAIL cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=268 end_us=117620 outcome=accepted
race Multipart writers=2 trial=1 writer=1 start_us=19 end_us=177462 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=2 pass=false
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_multipart_publication_2 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_multipart_publication_2 FAIL cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=269 end_us=983324 outcome=accepted
race Multipart writers=8 trial=1 writer=1 start_us=316 end_us=917992 outcome=accepted
race Multipart writers=8 trial=1 writer=2 start_us=471 end_us=917951 outcome=accepted
race Multipart writers=8 trial=1 writer=3 start_us=518 end_us=885270 outcome=accepted
race Multipart writers=8 trial=1 writer=4 start_us=429 end_us=846176 outcome=accepted
race Multipart writers=8 trial=1 writer=5 start_us=381 end_us=713843 outcome=accepted
race Multipart writers=8 trial=1 writer=6 start_us=212 end_us=644516 outcome=accepted
race Multipart writers=8 trial=1 writer=7 start_us=50 end_us=886441 outcome=accepted
race Multipart writers=8 trial=1 overlap=true successes=8 pass=false
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=400 violations=50
§4 concurrent_multipart_publication_8 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_multipart_publication_8 FAIL cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=438 end_us=181066 outcome=accepted
race Mixed writers=2 trial=1 writer=1 start_us=13 end_us=269710 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=2 pass=false
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_put_and_multipart_publication cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_and_multipart_publication FAIL cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=348 end_us=122475 outcome=accepted
race Replace writers=2 trial=1 writer=1 start_us=27 end_us=116236 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=2 pass=false
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_replace_current_revision cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_replace_current_revision FAIL cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1792018180, tv_nsec: 0 } published=SystemTime { tv_sec: 1792018180, tv_nsec: 0 } after=SystemTime { tv_sec: 1792018180, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1792018182, tv_nsec: 0 } published=SystemTime { tv_sec: 1792018183, tv_nsec: 0 } after=SystemTime { tv_sec: 1792018183, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.006592791 median_s=0.008440958 max_s=0.016831458 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Enabled stable=true
certification suite=5 FAIL
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.050193625 median_s=0.052847604 max_s=0.064471375 (aggregated from per-trial lines)
```

#### SeaweedFS 4.48

`chrislusf/seaweedfs:latest` (SeaweedFS 4.48, `530be3e37`, image
`sha256:8f17c8c82e03`, digest `sha256:4e61d15fd359`), `weed server -s3
-s3.config=<identities> -s3.iam.readOnly=false`, region `us-east-1`,
`COVEN_LIVE_S3_PROFILE=SeaweedFs`. As on the earlier suite 5 run, copy
publication kept one winner in every two- and eight-writer trial, and the
unversioned bucket let a conditional PUT and a conditional copy of one name
both succeed, now in 17 of 200 trials. The versioned bucket passed every
check.

```text
bucket versioning=Disabled
detect Signing(GoogleHmac) held=false PermissionDenied HTTP 403
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect SinglePut(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1792011486..1792011486 completion=1792011487..1792011487 published=1792011487
detect ConditionalCompletion held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(CloudflareIfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyTimestamp held=true condition=GenerationMatch source=1792011486 copy=1792011487
detect ChecksumMode held=true accepted
detect PassiveChecksum held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumReturned held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumScopeStated held=true checksum=false unstated=false google_hash=false generation=false
detect GoogleGeneration held=false checksum=false unstated=false google_hash=false generation=false
detect GoogleHeaders held=false checksum=false unstated=false google_hash=false generation=false
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_safety_check PASS
catalogued entries reproduced: [RustFs, CephRgw]
differs from Aws: multipart=Copy(IfNoneMatch)/ConditionalCompletion multipart_timestamp=Start/Completion checksums=ScopeStated/Absent request_checksums=true/false
differs from Gcs: signing=GoogleHmac/AwsSigV4 single_put=GenerationMatch/IfNoneMatch multipart=Copy(GenerationMatch)/ConditionalCompletion checksums=ScopeStated/Absent revision=Generation/ETag response_headers=Google/S3
differs from R2: checksums=UnstatedIgnored/Absent request_checksums=true/false
differs from SeaweedFs: multipart=Copy(IfNoneMatch)/ConditionalCompletion
```

Versioning disabled:

```text
bucket versioning=Disabled
setup_safety_check PASS
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: Copy(IfNoneMatch), multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check refused: S3 profile SeaweedFs is refused: concurrent create-once publication failed certification
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=340 end_us=9801 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=66 end_us=11425 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=1273 end_us=32943 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=1175 end_us=34552 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=575 end_us=21168 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=1356 end_us=19898 outcome=accepted
race Put writers=8 trial=1 writer=4 start_us=1433 end_us=34070 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=955 end_us=34112 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=1090 end_us=25189 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=195 end_us=1027369 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=269 end_us=1008554 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=26 end_us=6062 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=319 end_us=4311 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=359 end_us=4270 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=402 end_us=4247 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=277 end_us=5754 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=164 end_us=4608 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=227 end_us=5736 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=443 end_us=4359 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=41 end_us=4337 outcome=accepted
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=73 end_us=2493 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=10 end_us=2142 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race Mixed writers=2 trial=28 writer=0 start_us=68 end_us=2330 outcome=accepted
race Mixed writers=2 trial=28 writer=1 start_us=9 end_us=2301 outcome=accepted
race Mixed writers=2 trial=28 overlap=true successes=2 pass=false
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=217 violations=17
§4 concurrent_put_and_multipart_publication cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_and_multipart_publication FAIL cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=131 end_us=3657 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=8 end_us=2344 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1792013523, tv_nsec: 0 } published=SystemTime { tv_sec: 1792013523, tv_nsec: 0 } after=SystemTime { tv_sec: 1792013523, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1792013525, tv_nsec: 0 } published=SystemTime { tv_sec: 1792013525, tv_nsec: 0 } after=SystemTime { tv_sec: 1792013525, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.002935000 median_s=0.004050416 max_s=0.009939292 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=5 FAIL
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.003953959 median_s=0.011821583 max_s=0.021057625 (aggregated from per-trial lines)
```

Versioning enabled:

```text
bucket versioning=Enabled
setup_safety_check refused: S3 bucket versioning Enabled is not certified for this profile
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: Copy(IfNoneMatch), multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check refused: S3 profile SeaweedFs is refused: concurrent create-once publication failed certification
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=272 end_us=1013980 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 writer=1 start_us=26 end_us=10349 outcome=accepted
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=720 end_us=2014313 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=647 end_us=4024217 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=362 end_us=1013040 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=789 end_us=10821 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=485 end_us=5023016 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=571 end_us=6024690 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=849 end_us=3020101 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=53 end_us=9807 outcome=accepted
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=71 end_us=2829 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=11 end_us=2716 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=249 end_us=1009139 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=491 end_us=9272 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=538 end_us=2012493 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=389 end_us=3012954 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=316 end_us=10610 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=441 end_us=2010211 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=584 end_us=9224 outcome=accepted
race Multipart writers=8 trial=1 writer=7 start_us=75 end_us=1009434 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=254 end_us=11789 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=25 end_us=8924 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=307 end_us=12715 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=19 end_us=7829 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1792015469, tv_nsec: 0 } published=SystemTime { tv_sec: 1792015469, tv_nsec: 0 } after=SystemTime { tv_sec: 1792015469, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1792015471, tv_nsec: 0 } published=SystemTime { tv_sec: 1792015471, tv_nsec: 0 } after=SystemTime { tv_sec: 1792015471, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.003320958 median_s=0.004520708 max_s=0.005760458 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Enabled stable=true
certification suite=5 PASS
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.003357000 median_s=0.005646709 max_s=0.008410833 (aggregated from per-trial lines)
```

#### Garage 2.4.1

`dxflrs/garage:v2.4.1` (image `sha256:2711bbf081dc`, digest
`sha256:9c96caa2612d`), one node with a 1 GB layout, sqlite metadata, region
`garage`, no profile named (Garage has no catalogued entry), so the candidate
is the detected settings. Garage accepted every conditional write to an
occupied name, sequentially and concurrently: both writers succeeded in all
200 two-writer PUT, mixed and replacement trials, all eight in every
eight-writer PUT trial, both in 197 of 200 two-writer completions (the other
three losers received HTTP 404 `NoSuchUpload`, not a condition refusal), and
all eight in 16 of 50 eight-writer completions (336 acceptances where 50 were
allowed). Its multipart `Last-Modified` dates initiation, so a multipart
object's time fell before its completion marker. Revision changes, listing
lag and immediate device folders passed; the reads, listing, status and
deletion requirements share a case with create-once and print FAIL with it.
Setup refuses it with `CreateOnce`.
`s3_bucket_versioning` first failed in the harness, which connected without
settings to an uncatalogued endpoint; it now reads with the plain S3
settings and printed `bucket versioning=Disabled`,
`setup_versioning_check PASS`.

```text
bucket versioning=Disabled
detect Signing(GoogleHmac) held=false Refused HTTP 400
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect SinglePut(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=false start=1792011487..1792011487 completion=1792011489..1792011489 published=1792011487
detect ConditionalCompletion held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(CloudflareIfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyTimestamp held=true condition=GenerationMatch source=1792011487 copy=1792011489
detect ChecksumMode held=true accepted
detect PassiveChecksum held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumReturned held=false checksum=false unstated=false google_hash=false generation=false
detect ChecksumScopeStated held=true checksum=false unstated=false google_hash=false generation=false
detect GoogleGeneration held=false checksum=false unstated=false google_hash=false generation=false
detect GoogleHeaders held=false checksum=false unstated=false google_hash=false generation=false
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Start, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_safety_check refused: storage check CreateOnce: Protocol: the single-PUT condition did not refuse an occupied name with its bytes and time unchanged
catalogued entries reproduced: []
differs from Aws: multipart=Copy(IfNoneMatch)/ConditionalCompletion checksums=ScopeStated/Absent request_checksums=true/false
differs from Gcs: signing=GoogleHmac/AwsSigV4 single_put=GenerationMatch/IfNoneMatch multipart=Copy(GenerationMatch)/ConditionalCompletion multipart_timestamp=Completion/Start checksums=ScopeStated/Absent revision=Generation/ETag response_headers=Google/S3
differs from R2: multipart_timestamp=Completion/Start checksums=UnstatedIgnored/Absent request_checksums=true/false
differs from RustFs: multipart_timestamp=Completion/Start
differs from SeaweedFs: multipart=Copy(IfNoneMatch)/ConditionalCompletion multipart_timestamp=Completion/Start
differs from CephRgw: multipart_timestamp=Completion/Start
```

```text
bucket versioning=Disabled
setup_safety_check refused: storage check CreateOnce: Protocol: the single-PUT condition did not refuse an occupied name with its bytes and time unchanged
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Start, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check PASS
§4 native_behavior_detection FAIL
race Put writers=2 trial=1 writer=0 start_us=351 end_us=6234 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=54 end_us=6737 outcome=accepted
race Put writers=2 trial=1 overlap=true successes=2 pass=false
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_put_2 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_2 FAIL cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=571 end_us=16554 outcome=accepted
race Put writers=8 trial=1 writer=1 start_us=659 end_us=11000 outcome=accepted
race Put writers=8 trial=1 writer=2 start_us=733 end_us=11842 outcome=accepted
race Put writers=8 trial=1 writer=3 start_us=480 end_us=12119 outcome=accepted
race Put writers=8 trial=1 writer=4 start_us=802 end_us=13562 outcome=accepted
race Put writers=8 trial=1 writer=5 start_us=868 end_us=13692 outcome=accepted
race Put writers=8 trial=1 writer=6 start_us=931 end_us=11768 outcome=accepted
race Put writers=8 trial=1 writer=7 start_us=171 end_us=13435 outcome=accepted
race Put writers=8 trial=1 overlap=true successes=8 pass=false
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=400 violations=50
§4 concurrent_put_8 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_8 FAIL cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=147 end_us=2866 outcome=SessionExpired HTTP 404
race Multipart writers=2 trial=1 writer=1 start_us=12 end_us=1597 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=false
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=397 violations=200
§4 concurrent_multipart_publication_2 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_multipart_publication_2 FAIL cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=238 end_us=4186 outcome=SessionExpired HTTP 404
race Multipart writers=8 trial=1 writer=1 start_us=102 end_us=4203 outcome=SessionExpired HTTP 404
race Multipart writers=8 trial=1 writer=2 start_us=293 end_us=4055 outcome=SessionExpired HTTP 404
race Multipart writers=8 trial=1 writer=3 start_us=266 end_us=3950 outcome=SessionExpired HTTP 404
race Multipart writers=8 trial=1 writer=4 start_us=146 end_us=4257 outcome=SessionExpired HTTP 404
race Multipart writers=8 trial=1 writer=5 start_us=179 end_us=4412 outcome=SessionExpired HTTP 404
race Multipart writers=8 trial=1 writer=6 start_us=209 end_us=3973 outcome=SessionExpired HTTP 404
race Multipart writers=8 trial=1 writer=7 start_us=27 end_us=1329 outcome=accepted
race Multipart writers=8 trial=1 overlap=true successes=1 pass=false
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=336 violations=50
§4 concurrent_multipart_publication_8 cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_multipart_publication_8 FAIL cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=95 end_us=3953 outcome=accepted
race Mixed writers=2 trial=1 writer=1 start_us=10 end_us=1890 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=2 pass=false
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_put_and_multipart_publication cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_put_and_multipart_publication FAIL cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=128 end_us=4550 outcome=accepted
race Replace writers=2 trial=1 writer=1 start_us=8 end_us=3175 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=2 pass=false
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=400 violations=200
§4 concurrent_replace_current_revision cause=Protocol: concurrent conditional publication violated exclusivity, bytes or overlap
§4 concurrent_replace_current_revision FAIL cleanup=PASS
§4 create_once_single_put, whole_object_reads, ranged_reads, prefix_listing, exact_status, deletion, setup_provider_check assertion_failed
§4 create_once_single_put FAIL cleanup=PASS
§4 whole_object_reads FAIL cleanup=PASS
§4 ranged_reads FAIL cleanup=PASS
§4 prefix_listing FAIL cleanup=PASS
§4 exact_status FAIL cleanup=PASS
§4 deletion FAIL cleanup=PASS
§4 setup_provider_check FAIL cleanup=PASS
§4 create_once_multipart, partial_uploads_hidden, multipart_resume assertion_failed
§4 create_once_multipart FAIL cleanup=PASS
§4 partial_uploads_hidden FAIL cleanup=PASS
§4 multipart_resume FAIL cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1792013569, tv_nsec: 0 } published=SystemTime { tv_sec: 1792013569, tv_nsec: 0 } after=SystemTime { tv_sec: 1792013569, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1792013571, tv_nsec: 0 } published=SystemTime { tv_sec: 1792013569, tv_nsec: 0 } after=SystemTime { tv_sec: 1792013571, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion, multipart_timestamp_equals_completion cause=Protocol: publication outside completion markers
§4 single_put_timestamp_equals_completion FAIL cleanup=PASS
§4 multipart_timestamp_equals_completion FAIL cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.002343958 median_s=0.002686708 max_s=0.004283125 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=5 FAIL
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.002690791 median_s=0.007916958 max_s=0.025912875 (aggregated from per-trial lines)
```

#### Member keys

`s3_member_key_lifecycle` passed against all three minters with the same
lines as the earlier [member key](#member-keys) runs: RustFS
(`principal=RustFsServiceAccount`, root key), SeaweedFS
(`principal=IamUser`, static `Admin` identity) and Ceph RGW
(`principal=IamUser`, root user of an RGW account that owns the bucket).
Every out-of-prefix write, read and listing was refused with
`PermissionDenied`; the revoked key was refused (`Authentication`) on the
first listing, `after_s=0`; rollback and cleanup passed.

### Cloud drives

All three refresh tokens were still valid.

- **Dropbox** (Full Dropbox app, shared folder; second account for
  sharing): all 7 `dropbox_live` tests passed, and the App-folder test
  `provider_access_live_dropbox_app_folder` passed. Occupied creates returned
  `AlreadyExists` with original bytes kept; an unfinished upload was absent
  from listing, status and reads (`NotFound`); `server_modified` fell inside
  the completion markers after 3, 10 and 20 s pauses (e.g. 20 s pause:
  finish markers `[1792015588, 1792015590]`, listed `1792015590`); immutable
  time stayed fixed; an identical-byte overwrite kept the `rev` and a
  different same-size overwrite changed it; the recipient read and wrote, and
  read `PermissionDenied` after revocation. The suite has no listing-lag
  measurement for Dropbox.
- **Google Drive**: all 8 `drive_live` tests passed. Publication time is the
  first complete publication: after 3, 10 and 20 s pauses the object's
  `createdTime` (22:08:21.098Z, 22:08:47.498Z, 22:09:23.739Z) fell between the
  marker created before the final part (22:08:19.307Z, 22:08:45.935Z,
  22:09:22.147Z) and the one after completion. An unfinished upload was
  unlisted and its native ID read 404. Listing lag over 20 objects: min
  0.859 s, median 0.992 s, max 5.943 s; a new device folder 0.269 s, its first
  object 0.218 s, discovery after the entry 0.251 s. The two-sign-in
  `provider_access_live_google_upgrade` needs a person at the browser and was
  not run.
- **OneDrive** (personal): all 11 `onedrive_live` tests passed. The
  conditional copy of a 251,658,241-byte temporary returned 202, completed,
  and stamped `lastModifiedDateTime` 22:16:25Z between the copy markers
  (22:16:23Z, 22:16:26Z), after the temporary's creation (22:15:04Z); bytes
  matched exactly; a conflicting copy returned 409 and changed nothing; the
  time, eTag and listing stayed unchanged for 610 s. Publication times after
  3, 10 and 20 s pauses fell inside the completion markers. Listing lag over
  20 objects: min 0.603 s, median 0.935 s, max 1.860 s; a new device folder
  0.921 s, its first object 1.405 s.

### CloudKit

The first `cloudkit_live` run failed after 1,003 s in its own fixture: it
began a multipart upload of a store-log entry, which the connection has
refused with `InvalidPath` since store-log entries became single-request
publications (`fc480c31f`). The fixture's unfinished upload now targets a
device log, and the test prints its zone names so a failed run's zones can be
removed. The rerun passed in 4,057 s, including the one-hour listing margin
wait, and deleted both test zones. The failed run's zone in
`iCloud.fm.bae.desktop` (Development) was left behind unnamed and still needs
removal through CloudKit Console. Live tests now mark their zones, and
the marker-only cleanup then found `_defaultZone` and two unmarked zones,
`coven-1d0b0aa4-d7b3-49d1-ad86-0fbdd781d625` and
`coven-30d4c11b-b55c-48aa-bb27-d377656f35ad`, and kept all three: a
real bae store's zone and an older run's look the same without a marker.
The two-account
`cloudkit_live_sharing` test needs a second Mac and iCloud account and was
not run.

### Not run

- AWS S3: no account.
- Versity Gateway: its container could not be started in this session; it
  ran later the same day ([below](#gcs-and-versity-gateway-2026-10-14)).

## GCS and Versity Gateway, 2026-10-14

Run on 2026-10-14 between 23:21 and 23:38 UTC from `main` at `450ccf609`,
after the full battery above, with the same harness: `s3_bucket_versioning`,
`s3_detection` and `s3_certification` (suite 5) per bucket.

| Provider | Build | Bucket | Suite 5 races (2×200, 8×50, mixed 200, replace 200) | Sequential contracts | Listing lag median / max | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| GCS | hosted | Disabled | one winner in every trial | all PASS | 0.128 s / 0.223 s | suite 5 PASS |
| Versity Gateway | 1.9.0 | Disabled | one winner in every trial | all PASS | 0.010 s / 0.062 s | suite 5 PASS (detected settings) |
| Versity Gateway | 1.9.0 | Enabled | one winner in every trial | all PASS | 0.009 s / 0.012 s | suite 5 PASS (detected settings) |

Both return whole-second `Last-Modified` values.

### GCS, suite 5

The test key's service account was granted `roles/storage.legacyBucketReader`
on the bucket, so `GetBucketVersioning` now reads `Disabled`, setup's
versioning check passes, and suite 5 ran from the catalogued GCS host's
settings. GCS's copy publication (`x-goog-if-generation-match: 0` on the copy)
kept exactly one winner in every race, including the conditional PUT racing a
conditional copy that SeaweedFS fails. This replaces the earlier "not run"
GCS row above. No versioned bucket was run, so GCS still has no
versioned-bucket evidence.

```text
bucket versioning=Disabled
detect Signing(GoogleHmac) held=true accepted
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect SinglePut(GenerationMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1792020322..1792020322 completion=1792020324..1792020324 published=1792020324
detect ConditionalCompletion held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(GenerationMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(IfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(CloudflareIfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyTimestamp held=true condition=GenerationMatch source=1792020321 copy=1792020325
detect ChecksumMode held=true accepted
detect PassiveChecksum held=true checksum=true unstated=false google_hash=true generation=true
detect ChecksumReturned held=true checksum=true unstated=false google_hash=true generation=true
detect ChecksumScopeStated held=true checksum=true unstated=false google_hash=true generation=true
detect GoogleGeneration held=true checksum=true unstated=false google_hash=true generation=true
detect GoogleHeaders held=true checksum=true unstated=false google_hash=true generation=true
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: GoogleHmac, single_put: GenerationMatch, multipart: Copy(GenerationMatch), multipart_timestamp: Completion, checksums: ScopeStated, listing: V2, revision: Generation, response_headers: Google, request_checksums: false }
setup_safety_check PASS
catalogued entries reproduced: [Gcs]
```

```text
bucket versioning=Disabled
setup_safety_check PASS
candidate S3Settings { signing: GoogleHmac, single_put: GenerationMatch, multipart: Copy(GenerationMatch), multipart_timestamp: Completion, checksums: ScopeStated, listing: V2, revision: Generation, response_headers: Google, request_checksums: false }
setup_profile_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=769 end_us=227748 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 writer=1 start_us=495 end_us=203774 outcome=accepted
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=1153 end_us=373711 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=668 end_us=344326 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=1000 end_us=379506 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=1457 end_us=379425 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=1312 end_us=379469 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=1605 end_us=388311 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=843 end_us=302134 outcome=accepted
race Put writers=8 trial=1 writer=7 start_us=235 end_us=350604 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=157 end_us=102895 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=13 end_us=88282 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=644 end_us=110965 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=720 end_us=110928 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=508 end_us=97650 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=788 end_us=114413 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=432 end_us=97688 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=577 end_us=93438 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=331 end_us=93488 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=99 end_us=79672 outcome=accepted
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=261 end_us=129491 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=37 end_us=101807 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=199 end_us=171007 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=18 end_us=141436 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=StorageTime { seconds: 1792021057, nanos: 0 } published=StorageTime { seconds: 1792021058, nanos: 0 } after=StorageTime { seconds: 1792021058, nanos: 0 }
publication multipart=true before=StorageTime { seconds: 1792021060, nanos: 0 } published=StorageTime { seconds: 1792021060, nanos: 0 } after=StorageTime { seconds: 1792021060, nanos: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=false
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.114644917 median_s=0.128301000 max_s=0.222592625 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=5 PASS
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.133164875 median_s=0.187475084 max_s=0.260287542 (aggregated from per-trial lines)
```

### Versity Gateway 1.9.0

`versity/versitygw:latest` (Versity Gateway v1.9.0, build `ca4c8d30f579`,
built 2026-10-13, image `sha256:d0b4ec48c463`, digest `sha256:f9768331c89d`),
one local podman container (libkrun machine) run as `versitygw --port :10000
--iam-dir /tmp/vgw posix --versioning-dir /tmp/vers /tmp/vgw` with generated
root credentials, region `us-east-1`, reached over plain `http`. It has no
catalogued profile, so no profile was named and the candidate is the detected
settings: AWS signing, `If-None-Match: *` on PUT and on multipart completion
and copy, completion-time multipart timestamps, checksums returned with their
scope stated, V2 listing and ETag revisions — the plain S3 settings, which no
catalogued entry has. The POSIX backend supports versioning with
`--versioning-dir`; detection found the same settings in the unversioned
`coven-live` bucket and the versioned `coven-live-versioned` one. Both runs
passed every check, versioning stayed in its state through each, and every
cleanup passed (versions and delete markers included). Setup admits the
unversioned bucket through detection and refuses the versioned one: detected
settings never inherit a versioned certificate.

Detection, unversioned bucket (the versioned bucket's observations are
identical apart from timestamps and the versioning line):

```text
bucket versioning=Disabled
detect Signing(GoogleHmac) held=false PermissionDenied HTTP 403
detect Signing(AwsSigV4) held=true accepted
detect SinglePut(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect SinglePut(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect PartialUploadHidden held=true HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
detect CompletionTimestamp held=true start=1792020125..1792020125 completion=1792020126..1792020126 published=1792020126
detect ConditionalCompletion held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(GenerationMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyPublication(IfNoneMatch) held=true empty=accepted taken=AlreadyExists HTTP 412 preserved=true
detect CopyPublication(CloudflareIfNoneMatch) held=false empty=accepted taken=accepted preserved=false
detect CopyTimestamp held=true condition=GenerationMatch source=1792020125 copy=1792020126
detect ChecksumMode held=true accepted
detect PassiveChecksum held=false checksum=true unstated=false google_hash=false generation=false
detect ChecksumReturned held=true checksum=true unstated=false google_hash=false generation=false
detect ChecksumScopeStated held=true checksum=true unstated=false google_hash=false generation=false
detect GoogleGeneration held=false checksum=true unstated=false google_hash=false generation=false
detect GoogleHeaders held=false checksum=true unstated=false google_hash=false generation=false
detect BucketList held=true accepted
detect Listing(V1) held=true objects=10 pages_forced_at=1
detect Listing(V2) held=true objects=10 pages_forced_at=1 agrees_with_v1=true
detected S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: ScopeStated, listing: V2, revision: ETag, response_headers: S3, request_checksums: true }
setup_safety_check PASS
catalogued entries reproduced: []
differs from Aws: multipart=Copy(IfNoneMatch)/ConditionalCompletion multipart_timestamp=Start/Completion
differs from Gcs: signing=GoogleHmac/AwsSigV4 single_put=GenerationMatch/IfNoneMatch multipart=Copy(GenerationMatch)/ConditionalCompletion revision=Generation/ETag response_headers=Google/S3 request_checksums=false/true
differs from R2: checksums=UnstatedIgnored/ScopeStated
differs from RustFs: checksums=Absent/ScopeStated request_checksums=false/true
differs from SeaweedFs: multipart=Copy(IfNoneMatch)/ConditionalCompletion checksums=Absent/ScopeStated request_checksums=false/true
differs from CephRgw: checksums=Absent/ScopeStated request_checksums=false/true
```

Versioning disabled:

```text
bucket versioning=Disabled
setup_safety_check PASS
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: ScopeStated, listing: V2, revision: ETag, response_headers: S3, request_checksums: true }
setup_profile_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=502 end_us=9588 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 writer=1 start_us=117 end_us=9184 outcome=accepted
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=758 end_us=14729 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=331 end_us=19756 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=495 end_us=15365 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=595 end_us=14759 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=413 end_us=14645 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=675 end_us=24464 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=850 end_us=14932 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=128 end_us=14709 outcome=accepted
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=91 end_us=16133 outcome=accepted
race Multipart writers=2 trial=1 writer=1 start_us=9 end_us=16265 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=419 end_us=30324 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=268 end_us=30293 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=210 end_us=30451 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=348 end_us=27392 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=452 end_us=30260 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=383 end_us=30369 outcome=accepted
race Multipart writers=8 trial=1 writer=6 start_us=310 end_us=30167 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=36 end_us=30210 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=130 end_us=2817 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=14 end_us=2707 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=139 end_us=24625 outcome=accepted
race Replace writers=2 trial=1 writer=1 start_us=10 end_us=29271 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=StorageTime { seconds: 1792020150, nanos: 0 } published=StorageTime { seconds: 1792020150, nanos: 0 } after=StorageTime { seconds: 1792020150, nanos: 0 }
publication multipart=true before=StorageTime { seconds: 1792020152, nanos: 0 } published=StorageTime { seconds: 1792020152, nanos: 0 } after=StorageTime { seconds: 1792020152, nanos: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.007298042 median_s=0.009639604 max_s=0.062066791 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=5 PASS
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.009282792 median_s=0.012293875 max_s=0.065397291 (aggregated from per-trial lines)
```

Versioning enabled:

```text
bucket versioning=Enabled
setup_safety_check refused: S3 bucket versioning Enabled is not certified for this profile
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: ScopeStated, listing: V2, revision: ETag, response_headers: S3, request_checksums: true }
setup_profile_check PASS
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=436 end_us=34053 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 writer=1 start_us=45 end_us=20851 outcome=accepted
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=3322 end_us=45900 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=3624 end_us=41935 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=3540 end_us=25318 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=3713 end_us=23996 outcome=accepted
race Put writers=8 trial=1 writer=4 start_us=3795 end_us=24399 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=3876 end_us=24582 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=3426 end_us=24334 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=2988 end_us=25154 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=275 end_us=5704 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=26 end_us=5795 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=1218 end_us=9806 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=330 end_us=8204 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=1376 end_us=8450 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=714 end_us=8086 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=841 end_us=9151 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=1065 end_us=8287 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=477 end_us=8157 outcome=accepted
race Multipart writers=8 trial=1 writer=7 start_us=99 end_us=9610 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=240 end_us=4679 outcome=accepted
race Mixed writers=2 trial=1 writer=1 start_us=23 end_us=4902 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=318 end_us=29934 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=21 end_us=6612 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=StorageTime { seconds: 1792020197, nanos: 0 } published=StorageTime { seconds: 1792020197, nanos: 0 } after=StorageTime { seconds: 1792020197, nanos: 0 }
publication multipart=true before=StorageTime { seconds: 1792020199, nanos: 0 } published=StorageTime { seconds: 1792020199, nanos: 0 } after=StorageTime { seconds: 1792020199, nanos: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.007047083 median_s=0.008913500 max_s=0.011952458 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Enabled stable=true
certification suite=5 PASS
listing_lag SUMMARY provider_case=S3_new_device_folder trials=20 min_s=0.007451917 median_s=0.042806770 max_s=0.079583500 (aggregated from per-trial lines)
```

These results justified a catalogued `VersityGw` profile with the detected
settings. Its key scope and member-key minting were not yet established;
both are [below](#versity-gateway-profile-2026-10-15).

## Versity Gateway profile, 2026-10-15

Run on 2026-10-15 between 02:37 and 02:46 UTC from `impl-t196` on `main` at
`3825dd2af`, with the catalogued `VersityGw` entry and its minter added. The
image is the one above (`versity/versitygw:v1.9.0`, build `ca4c8d30f579`,
image `sha256:d0b4ec48c463`, digest `sha256:f9768331c89d`), now run as two
processes in one podman container (libkrun machine, 2 GiB): the standalone
IAM service, `versitygw --port :7076 iam --dir <iam> --private-ports
<socket>`, and the gateway, `versitygw --port :10000
--iam-standalone-endpoint <socket> posix --versioning-dir <versions> <root>`,
both with the same generated root key and region `us-east-1`, reached over
plain `http`. The gateway created `coven-live` and `coven-live-versioned`,
and enabled versioning on the second.

Standalone IAM is in 1.9.0. Its API (IAM query protocol, version
`2010-05-08`, signed for the gateway's region; a signature for another
region is refused with `SignatureDoesNotMatch`) listens only on the IAM
service's own port: the gateway's S3 port answers an IAM `POST` with
`MethodNotAllowed`. `CreateUser` takes the `/coven/` path and tags,
`PutUserPolicy` takes the member policy unchanged, and the gateway enforces
it on S3 requests: by hand, a key of a user holding the policy for
`base/store1/` wrote, read and listed there, read the bucket's versioning,
and was refused `AccessDenied` ("no identity-based policy allows") for a
write under `base/store2/`, a write at the bucket's root, listings of
`base/store2/`, `base/` and the whole bucket, and a listing of the other
bucket. `DeleteUser` with a policy attached is refused with
`DeleteConflict`; absent users, keys and policies answer `NoSuchEntity`.
A deleted key was refused with `InvalidAccessKeyId` at once and on every
read in the following 18 seconds. The gateway's own admin API
(`versitygw admin create-user`) has no per-user policy, so the minter does
not use it.

| Provider | Build | Bucket | Suite 5 races (2×200, 8×50, mixed 200, replace 200) | Sequential contracts | Listing lag median / max | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| Versity Gateway | 1.9.0 | Disabled | one winner in every trial | all PASS | 0.005 s / 0.006 s | suite 5 PASS (`VersityGw`) |
| Versity Gateway | 1.9.0 | Enabled | one winner in every trial | all PASS | 0.009 s / 0.017 s | suite 5 PASS (`VersityGw`) |

Detection observed what it observed on 2026-10-14 and reproduced the new
entry (`catalogued entries reproduced: [VersityGw]`). The named entry now
passes setup's safety check in the versioned bucket too, which detected
settings never could. Versioning disabled (versioning enabled printed the
same lines apart from `versioning=Enabled`, timings and the listing lag):

```text
bucket versioning=Disabled
setup_safety_check PASS
catalogued entries reproduced: [VersityGw]
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, multipart: ConditionalCompletion, multipart_timestamp: Completion, checksums: ScopeStated, listing: V2, revision: ETag, response_headers: S3, request_checksums: true }
setup_profile_check PASS
§4 native_behavior_detection PASS
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.003700250 median_s=0.004639333 max_s=0.006211875 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=5 PASS
```

`s3_member_key_lifecycle` with `versitygw.env` (the root key),
`COVEN_LIVE_S3_PROFILE=VersityGw` and
`COVEN_LIVE_S3_IAM_ENDPOINT=http://localhost:7076/` printed, apart from the
cargo harness lines:

```text
mint PASS principal=IamUser
key_exists_after_mint=true
member_check PASS
write_other_store refused=PermissionDenied
read_other_store refused=PermissionDenied
list_other_store refused=PermissionDenied
write_outside_base_prefix refused=PermissionDenied
revoke PASS
key_exists_after_revoke=false
revoke_again PASS
revoked_key refused=Authentication after_s=0
member_key_lifecycle PASS
roll_back PASS cleanup PASS
```

Afterwards the IAM service listed no users and `coven-live` held no objects.

Later on 2026-10-15 (T237), with users named by key name
(`coven-<store>-<device>-<attempt>`, 64 characters) and the store listing
added, the same container and command printed, apart from the cargo harness
lines:

```text
mint PASS principal=IamUser
key_exists_after_mint=true
store_listing_after_mint=true listed=1
member_check PASS
write_other_store refused=PermissionDenied
read_other_store refused=PermissionDenied
list_other_store refused=PermissionDenied
write_outside_base_prefix refused=PermissionDenied
revoke PASS
key_exists_after_revoke=false
revoke_again PASS
store_listing_after_revoke=0
revoked_key refused=Authentication after_s=0
member_key_lifecycle PASS
roll_back PASS cleanup PASS
```

By hand, the IAM service also took a user path such as
`/coven/<store>/<member>/<device>/` and filtered `ListUsers` by
`PathPrefix`. Afterwards it listed no users. RustFS, SeaweedFS and Ceph RGW
were not running for this change; their listings are checked only against
the in-process fakes.

## SeaweedFS every-object copy publication, 2026-10-15

Suite 5 showed SeaweedFS 4.48 keeping one winner whenever copies raced
copies, while a conditional PUT racing a conditional copy of one name could
both succeed in an unversioned bucket. Its entry now publishes every object
by copy (`S3CopyPublication::Every(IfNoneMatch)`): a single-request create
stages its bytes at a reserved temporary and publishes it with one
`CopyObject` carrying `If-None-Match: *`, as multipart uploads already did.
The suite races each writer as the entry publishes, so the PUT and mixed
races were copies against copies here; the replacement race still sends
conditional PUTs, which this entry never uses for creates.

Run on 2026-10-15 from `seg-t208` (based on `main` at `24ca3a4d7`) against a
local podman container of `chrislusf/seaweedfs:4.48` (image
`sha256:8f17c8c82e03`, the image of the suite 5 run; `weed server -dir=/data
-s3 -s3.config=<identities> -master.volumeSizeLimitMB=128 -volume.max=16`),
selecting `COVEN_LIVE_S3_PROFILE=SeaweedFs`, in a new unversioned bucket and
a new bucket with versioning `Enabled`. Each race below shows its first
trial and its summary; every trial of every race kept exactly one winner,
with the winner's bytes stored and all intervals overlapping, in both
buckets. Both runs pass, so the entry records suite 5 certificates for
unversioned and versioned buckets and setup admits it. The
`setup_profile_check` and `setup_safety_check` lines are the verdicts of the
code under test before this entry changed: the run predates the recorded
certificate.

### Versioning disabled

```text
bucket versioning=Disabled
setup_safety_check PASS
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, copy_publication: Every(IfNoneMatch), multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check refused: S3 profile SeaweedFs is refused: concurrent create-once publication failed certification
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=298 end_us=5370 outcome=accepted
race Put writers=2 trial=1 writer=1 start_us=114 end_us=5658 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=228 end_us=1029405 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=290 end_us=3031852 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=612 end_us=6035720 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=553 end_us=7036552 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=483 end_us=2030207 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=359 end_us=28863 outcome=accepted
race Put writers=8 trial=1 writer=6 start_us=411 end_us=5034466 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=69 end_us=4032928 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=261 end_us=7997 outcome=accepted
race Multipart writers=2 trial=1 writer=1 start_us=23 end_us=1010730 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=264 end_us=3899 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=228 end_us=3869 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=299 end_us=3857 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=190 end_us=3889 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=4 start_us=339 end_us=4143 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=137 end_us=3831 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=95 end_us=4064 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=47 end_us=2789 outcome=accepted
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=275 end_us=6674 outcome=accepted
race Mixed writers=2 trial=1 writer=1 start_us=26 end_us=8084 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=371 end_us=12795 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=35 end_us=10057 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=StorageTime { seconds: 1792039267, nanos: 0 } published=StorageTime { seconds: 1792039267, nanos: 0 } after=StorageTime { seconds: 1792039267, nanos: 0 }
publication multipart=true before=StorageTime { seconds: 1792039269, nanos: 0 } published=StorageTime { seconds: 1792039269, nanos: 0 } after=StorageTime { seconds: 1792039269, nanos: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.003241417 median_s=0.007254062 max_s=0.051068708 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Disabled stable=true
certification suite=5 PASS
```

### Versioning enabled

```text
bucket versioning=Enabled
setup_safety_check refused: S3 bucket versioning Enabled is not certified for this profile
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, copy_publication: Every(IfNoneMatch), multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
setup_profile_check refused: S3 profile SeaweedFs is refused: concurrent create-once publication failed certification
§4 native_behavior_detection PASS
race Put writers=2 trial=1 writer=0 start_us=177 end_us=1103174 outcome=AlreadyExists HTTP 412
race Put writers=2 trial=1 writer=1 start_us=29 end_us=103739 outcome=accepted
race Put writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_2 PASS cleanup=PASS
race Put writers=8 trial=1 writer=0 start_us=368 end_us=6015747 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=1 start_us=144 end_us=5015086 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=2 start_us=404 end_us=1009321 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=3 start_us=245 end_us=3011742 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=4 start_us=287 end_us=7017420 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=5 start_us=193 end_us=2011271 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=6 start_us=327 end_us=4013341 outcome=AlreadyExists HTTP 412
race Put writers=8 trial=1 writer=7 start_us=33 end_us=8739 outcome=accepted
race Put writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_put_8 PASS cleanup=PASS
race Multipart writers=2 trial=1 writer=0 start_us=276 end_us=1011247 outcome=AlreadyExists HTTP 412
race Multipart writers=2 trial=1 writer=1 start_us=37 end_us=6570 outcome=accepted
race Multipart writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_multipart_publication_2 PASS cleanup=PASS
race Multipart writers=8 trial=1 writer=0 start_us=1263 end_us=2042938 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=1 start_us=1110 end_us=2042572 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=2 start_us=765 end_us=39647 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=3 start_us=939 end_us=18926 outcome=accepted
race Multipart writers=8 trial=1 writer=4 start_us=344 end_us=1041918 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=5 start_us=253 end_us=40434 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=6 start_us=457 end_us=2021194 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 writer=7 start_us=55 end_us=1019932 outcome=AlreadyExists HTTP 412
race Multipart writers=8 trial=1 overlap=true successes=1 pass=true
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
§4 concurrent_multipart_publication_8 PASS cleanup=PASS
race Mixed writers=2 trial=1 writer=0 start_us=247 end_us=81728 outcome=AlreadyExists HTTP 412
race Mixed writers=2 trial=1 writer=1 start_us=24 end_us=8086 outcome=accepted
race Mixed writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_put_and_multipart_publication PASS cleanup=PASS
race Replace writers=2 trial=1 writer=0 start_us=344 end_us=117660 outcome=AlreadyExists HTTP 412
race Replace writers=2 trial=1 writer=1 start_us=19 end_us=11680 outcome=accepted
race Replace writers=2 trial=1 overlap=true successes=1 pass=true
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 concurrent_replace_current_revision PASS cleanup=PASS
§4 create_once_single_put PASS cleanup=PASS
§4 whole_object_reads PASS cleanup=PASS
§4 ranged_reads PASS cleanup=PASS
§4 prefix_listing PASS cleanup=PASS
§4 exact_status PASS cleanup=PASS
§4 deletion PASS cleanup=PASS
§4 setup_provider_check PASS cleanup=PASS
§4 create_once_multipart PASS cleanup=PASS
§4 partial_uploads_hidden PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
publication multipart=false before=StorageTime { seconds: 1792040511, nanos: 0 } published=StorageTime { seconds: 1792040511, nanos: 0 } after=StorageTime { seconds: 1792040511, nanos: 0 }
publication multipart=true before=StorageTime { seconds: 1792040513, nanos: 0 } published=StorageTime { seconds: 1792040513, nanos: 0 } after=StorageTime { seconds: 1792040513, nanos: 0 }
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=false
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.006007416 median_s=0.012257978 max_s=0.190505875 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
bucket versioning_after=Enabled stable=true
certification suite=5 PASS
```

## SeaweedFS rerun, 2026-10-15

Later on 2026-10-15 suite 5 was run again against SeaweedFS 4.48
(`chrislusf/seaweedfs:4.48`, the flags of the run above) in a new
unversioned bucket, from the working tree that became `26dda7270`. It
failed three times in a row. Conditional PUTs and conditional replacements
kept one winner in every trial, but racing multipart publications, and a
single-request create racing a multipart publication, were both accepted:

| Run | `concurrent_multipart_publication_2` | `concurrent_multipart_publication_8` | `concurrent_put_and_multipart_publication` |
| --- | --- | --- | --- |
| 1 | 177 of 200 trials failed | 48 of 50 | 28 of 200 |
| 2 | 154 of 200 | 48 of 50 | 23 of 200 |
| 3 | 148 of 200 | 45 of 50 | 21 of 200 |

The run above recorded no failing trial in these races, so the certificate
was withdrawn and setup refused the profile until it was explained. Its
explanation is below: these runs raced conditional completion, not the
entry's copies. The third run's summaries and a failing trial:

```text
bucket versioning=Disabled
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
race Multipart writers=2 trial=2 writer=0 start_us=246 end_us=4306 outcome=accepted
race Multipart writers=2 trial=2 writer=1 start_us=20 end_us=3997 outcome=accepted
race Multipart writers=2 trial=2 overlap=true successes=2 pass=false
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=348 violations=148
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=347 violations=45
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=221 violations=21
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
bucket versioning_after=Disabled stable=true
certification suite=5 FAIL
```

### Why the rerun failed

The rerun put `COVEN_LIVE_S3_PROFILE=SeaweedFs` in its env file. The suite
reads the profile from the process environment and ignored the file's line,
so the location named no profile and the suite raced the detected settings.
Detection cannot see that SeaweedFS needs copy publication, so those
settings publish multipart uploads by conditional completion, which
SeaweedFS 4.48 lets two racers both win (suite 4). Every rerun log shows it:

```text
differs from SeaweedFs: copy_publication=Every(IfNoneMatch)/Never
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, copy_publication: Never, multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
```

No copy was sent: the failing multipart races were conditional completions,
the mixed race a conditional PUT against one, and the passing PUT races
plain conditional PUTs. The passing run's candidate was the entry's own,
`copy_publication: Every(IfNoneMatch)`. The suite now refuses an env file
holding any setting it does not read, naming it, so a misplaced profile
name stops the run instead of changing what it certifies.

## SeaweedFS reruns as the named entry, 2026-10-15

Run on 2026-10-15 from `seg-t222` (based on `main` at `26dda7270`, with the
publication code unchanged) against `chrislusf/seaweedfs:4.48` (image
`sha256:8f17c8c82e03`; `weed server -dir=/data -s3
-s3.config=<identities> -master.volumeSizeLimitMB=128 -volume.max=16`,
container memory 768 MiB), with `COVEN_LIVE_S3_PROFILE=SeaweedFs` in the
process environment, three times in a new unversioned bucket and three
times in a new bucket with versioning `Enabled`. Every run's candidate was
the entry's `copy_publication: Every(IfNoneMatch)`, every trial of every
race kept exactly one winner with its bytes stored and all intervals
overlapping, and every run printed `certification suite=5 PASS`:

| Run | Bucket | PUT 2 / 8 | Multipart 2 / 8 | Mixed | Replace | Suite |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Unversioned | 0 of 200 / 0 of 50 failed | 0 of 200 / 0 of 50 | 0 of 200 | 0 of 200 | PASS |
| 2 | Unversioned | 0 / 0 | 0 / 0 | 0 | 0 | PASS |
| 3 | Unversioned | 0 / 0 | 0 / 0 | 0 | 0 | PASS |
| 1 | Versioned | 0 / 0 | 0 / 0 | 0 | 0 | PASS |
| 2 | Versioned | 0 / 0 | 0 / 0 | 0 | 0 | PASS |
| 3 | Versioned | 0 / 0 | 0 / 0 | 0 | 0 | PASS |

The first unversioned run shared its container with the other buckets;
each later run had a fresh container holding only its bucket. Six
attempts are not counted. A first versioned attempt was stopped by the
harness's 20-minute time limit during the replacement race, after 152
passing replacement trials and passing every other race. Five attempts in
the shared container stopped at detection with a network failure: each
bucket is a SeaweedFS collection with volumes of its own, and with
`-volume.max=16` the seventh bucket found no writable volume (`failed to
find writable volumes for collection`). Both runs pass in both bucket
states, so the entry keeps its suite 5 certificates for unversioned and
versioned buckets and setup admits it. The third versioned run:

```text
bucket versioning=Enabled
setup_safety_check refused: S3 bucket versioning Enabled is not certified for this profile
candidate S3Settings { signing: AwsSigV4, single_put: IfNoneMatch, copy_publication: Every(IfNoneMatch), multipart_timestamp: Completion, checksums: Absent, listing: V2, revision: ETag, response_headers: S3, request_checksums: false }
§4 native_behavior_detection PASS
race SUMMARY Put writers=2 trials=200 overlapping=200 successes=200 violations=0
race SUMMARY Put writers=8 trials=50 overlapping=50 successes=50 violations=0
race SUMMARY Multipart writers=2 trials=200 overlapping=200 successes=200 violations=0
race SUMMARY Multipart writers=8 trials=50 overlapping=50 successes=50 violations=0
race SUMMARY Mixed writers=2 trials=200 overlapping=200 successes=200 violations=0
race SUMMARY Replace writers=2 trials=200 overlapping=200 successes=200 violations=0
§4 create_once_multipart PASS cleanup=PASS
§4 multipart_resume PASS cleanup=PASS
§4 single_put_timestamp_equals_completion PASS cleanup=PASS
§4 multipart_timestamp_equals_completion PASS cleanup=PASS
§4 listing_lag PASS cleanup=PASS
bucket versioning_after=Enabled stable=true
certification suite=5 PASS
```

The versioned runs' `setup_safety_check` refusal is that of the code under
test, whose entry recorded no versioned certificate while they ran.
