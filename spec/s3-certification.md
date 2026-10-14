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
2026-10-14 suite 4 passed RustFS with versioning disabled and enabled.
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

SeaweedFS's profile refuses enabled, suspended and unknown versioning until a
versioned certification is recorded. AWS has no reachable
live account and likewise has no versioned certificate.

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

SeaweedFS and Ceph RGW printed the same lines with `principal=IamUser`.
