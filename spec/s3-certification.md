# S3 certification evidence

Suite version 1 passed GCS and R2 on 2026-10-13, and suite version 2 passed
RustFS and SeaweedFS the same day. Version 2 differs only in recording a
service that returns no checksum as such, which §4 permits; version 1 failed
detection for it. AWS is uncertified because no live account is available.
Garage ran suite 1 on 2026-10-13 and is uncertified; its section names the
failed checks.
The selected entries are in
[`s3_profile.rs`](../crates/coven-storage/src/providers/s3_profile.rs); the
contract and certification procedure are in [§4](coven.md#4-storage-providers-and-access).

The output below retains native observations and every requirement result.
Account endpoints, keys and object names are omitted. `list_end_s` measures
elapsed time from the successful upload reply through the listing response;
these samples do not establish the one-hour completeness bound. Every case's
cleanup passed after deleting its objects and aborting its remaining uploads.

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
native bucket_head=accepted bucket_list=accepted selected=List
native checksum_present=true checksum_scope=ScopeStated revision=Generation response_headers=Google bucket_check=List
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
native bucket_head=accepted bucket_list=accepted selected=Head
native checksum_present=true checksum_scope=UnstatedIgnored revision=ETag response_headers=S3 bucket_check=Head
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
match. For Garage, native detection ran every probe and listed each missing
behavior on its `native missing=` line before printing that entry, and the
adapter cases after the failed detection used AWS's profile.

### Garage

Garage v2.4.1 (`dxflrs/garage:v2.4.1`), single node, region `garage`, under
suite 1. Uncertified. Suite 2 would record its absent checksum, but not its
missing create-once behavior. Every create-once route the adapter has was ignored: both
single-PUT conditions, conditional multipart completion and both copy
destination conditions replaced the occupied object. Multipart
`Last-Modified` recorded initiation, not completion. No checksum was returned.
No behavior in §4's menu covers this: certification needs a create-once
condition that Garage enforces on single PUT, plus either enforced conditional
completion with a completion timestamp or an enforced conditional copy. The
setup check refuses Garage with `CreateOnce`.

```text
native signing=GoogleHmac result=Refused HTTP 400
native signing=AwsSigV4 result=accepted
native single_put IfNoneMatch empty=accepted taken=accepted preserved=false
native single_put GenerationMatch empty=accepted taken=accepted preserved=false
native partial_upload HEAD=NotFound HTTP 404 GET=NotFound HTTP 404 listed=false
§4 native partial_uploads_hidden PASS
native conditional_completion empty=accepted
native multipart_timestamp=Start start_before=SystemTime { tv_sec: 1791918000, tv_nsec: 0 } start_after=SystemTime { tv_sec: 1791918000, tv_nsec: 0 } completion_before=SystemTime { tv_sec: 1791918003, tv_nsec: 0 } completion_after=SystemTime { tv_sec: 1791918003, tv_nsec: 0 } published=SystemTime { tv_sec: 1791918000, tv_nsec: 0 }
native conditional_completion taken=accepted preserved=false enforced=false
native copy header=x-goog-if-generation-match empty=accepted taken=accepted preserved=false later_than_source=true
native copy header=cf-copy-destination-if-none-match empty=accepted taken=accepted preserved=false later_than_source=true
native listing=V2 objects=8 pages_forced_at=1
native checksum_mode=accepted
native bucket_head=accepted bucket_list=accepted selected=Head
native checksum_present=false checksum_scope=ScopeStated revision=ETag response_headers=S3 bucket_check=Head
native missing=create-once PUT, conditional copy publication, returned checksum
S3Profile {
    name: S3ProfileName::Uncatalogued,
    endpoint: Some(EndpointMatch::Host("localhost")),
    signing: Signing::AwsSigV4,
    single_put: SinglePut::IfNoneMatch,
    multipart: Multipart::ConditionalCompletion,
    multipart_timestamp: MultipartTimestamp::Start,
    checksums: Checksums::ScopeStated,
    listing: Listing::V2,
    revision: Revision::ETag,
    bucket_check: BucketCheck::Head,
    response_headers: ResponseHeaders::S3,
    request_checksums: true,
    certification: Certification::Uncertified,
},
§4 native_behavior_detection cause=Protocol: native behaviors missing
§4 native_behavior_detection FAIL cleanup=PASS

thread 'providers::s3::tests::live_tests::s3_certification' (203489727) panicked at crates/coven-storage/src/providers/s3_live_tests.rs:709:10:
called `Result::unwrap()` on an `Err` value: Caused { failure: Protocol, source: "create-once refusal" }
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
§4 create_once_single_put, whole_object_reads, ranged_reads, prefix_listing, exact_status, deletion, setup_provider_check assertion_failed
§4 create_once_single_put FAIL cleanup=PASS
§4 whole_object_reads FAIL cleanup=PASS
§4 ranged_reads FAIL cleanup=PASS
§4 prefix_listing FAIL cleanup=PASS
§4 exact_status FAIL cleanup=PASS
§4 deletion FAIL cleanup=PASS
§4 setup_provider_check FAIL cleanup=PASS

thread 'providers::s3::tests::live_tests::s3_certification' (203489727) panicked at crates/coven-storage/src/providers/s3_live_tests.rs:736:10:
called `Result::unwrap_err()` on an `Ok` value: ()
§4 create_once_multipart, partial_uploads_hidden, multipart_resume assertion_failed
§4 create_once_multipart FAIL cleanup=PASS
§4 partial_uploads_hidden FAIL cleanup=PASS
§4 multipart_resume FAIL cleanup=PASS
publication multipart=false before=SystemTime { tv_sec: 1791918009, tv_nsec: 0 } published=SystemTime { tv_sec: 1791918009, tv_nsec: 0 } after=SystemTime { tv_sec: 1791918009, tv_nsec: 0 }
publication multipart=true before=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } published=SystemTime { tv_sec: 1791918009, tv_nsec: 0 } after=SystemTime { tv_sec: 1791918011, tv_nsec: 0 }
§4 single_put_timestamp_equals_completion, multipart_timestamp_equals_completion cause=Protocol: publication outside completion markers
§4 single_put_timestamp_equals_completion FAIL cleanup=PASS
§4 multipart_timestamp_equals_completion FAIL cleanup=PASS
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
replacement changed=true same_size=true same_tick=true
§4 revision_changes_on_replace PASS cleanup=PASS
listing_lag provider_case=S3_new_device_folder trial=1 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001803584 list_end_s=0.002517667 present=true
listing_lag provider_case=S3_object trial=1 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001870750 list_end_s=0.003163209 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002517667 median_s=0.002517667 max_s=0.002517667 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=2 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001787333 list_end_s=0.002573708 present=true
listing_lag provider_case=S3_new_device_folder trial=2 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001737875 list_end_s=0.002982125 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002982125 median_s=0.002982125 max_s=0.002982125 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=3 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001756542 list_end_s=0.002455459 present=true
listing_lag provider_case=S3_new_device_folder trial=3 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001711584 list_end_s=0.002761917 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002761917 median_s=0.002761917 max_s=0.002761917 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=4 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001678459 list_end_s=0.002369959 present=true
listing_lag provider_case=S3_new_device_folder trial=4 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001633750 list_end_s=0.002732042 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002732042 median_s=0.002732042 max_s=0.002732042 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=5 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001666042 list_end_s=0.002404958 present=true
listing_lag provider_case=S3_new_device_folder trial=5 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001623083 list_end_s=0.002761250 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002761250 median_s=0.002761250 max_s=0.002761250 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=6 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001798625 list_end_s=0.002541750 present=true
listing_lag provider_case=S3_new_device_folder trial=6 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001751791 list_end_s=0.003046083 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.003046083 median_s=0.003046083 max_s=0.003046083 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=7 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001759125 list_end_s=0.002449000 present=true
listing_lag provider_case=S3_new_device_folder trial=7 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001716916 list_end_s=0.003013375 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.003013375 median_s=0.003013375 max_s=0.003013375 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=8 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001684667 list_end_s=0.002383750 present=true
listing_lag provider_case=S3_new_device_folder trial=8 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001644709 list_end_s=0.002907292 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002907292 median_s=0.002907292 max_s=0.002907292 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=9 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001654917 list_end_s=0.002390167 present=true
listing_lag provider_case=S3_new_device_folder trial=9 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001619000 list_end_s=0.002861333 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002861333 median_s=0.002861333 max_s=0.002861333 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=10 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001651958 list_end_s=0.002346916 present=true
listing_lag provider_case=S3_new_device_folder trial=10 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001595833 list_end_s=0.002804166 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002804166 median_s=0.002804166 max_s=0.002804166 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=11 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001699500 list_end_s=0.002399083 present=true
listing_lag provider_case=S3_new_device_folder trial=11 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001652333 list_end_s=0.002811291 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002811291 median_s=0.002811291 max_s=0.002811291 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=12 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001686834 list_end_s=0.002433750 present=true
listing_lag provider_case=S3_new_device_folder trial=12 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001638417 list_end_s=0.002839459 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002839459 median_s=0.002839459 max_s=0.002839459 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=13 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.002087375 list_end_s=0.002820584 present=true
listing_lag provider_case=S3_new_device_folder trial=13 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.002013000 list_end_s=0.003261792 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.003261792 median_s=0.003261792 max_s=0.003261792 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=14 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001792041 list_end_s=0.002522791 present=true
listing_lag provider_case=S3_new_device_folder trial=14 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001740583 list_end_s=0.003056833 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.003056833 median_s=0.003056833 max_s=0.003056833 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=15 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001708417 list_end_s=0.002652000 present=true
listing_lag provider_case=S3_new_device_folder trial=15 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001671875 list_end_s=0.003219583 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.003219583 median_s=0.003219583 max_s=0.003219583 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=16 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.002134000 list_end_s=0.003287167 present=true
listing_lag provider_case=S3_new_device_folder trial=16 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.002018459 list_end_s=0.003915542 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.003915542 median_s=0.003915542 max_s=0.003915542 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=17 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.002133417 list_end_s=0.003046750 present=true
listing_lag provider_case=S3_new_device_folder trial=17 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.002012792 list_end_s=0.003455375 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.003455375 median_s=0.003455375 max_s=0.003455375 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=18 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001751042 list_end_s=0.002588834 present=true
listing_lag provider_case=S3_new_device_folder trial=18 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001691000 list_end_s=0.002814500 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002814500 median_s=0.002814500 max_s=0.002814500 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=19 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001761000 list_end_s=0.002518917 present=true
listing_lag provider_case=S3_new_device_folder trial=19 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001718334 list_end_s=0.003051584 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.003051584 median_s=0.003051584 max_s=0.003051584 omitted_trials=0 timeouts=0
listing_lag provider_case=S3_object trial=20 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001713250 list_end_s=0.002424458 present=true
listing_lag provider_case=S3_new_device_folder trial=20 stored_at=SystemTime { tv_sec: 1791918011, tv_nsec: 0 } list_start_s=0.001672500 list_end_s=0.002837833 present=true
listing_lag SUMMARY provider_case=S3_new_device_folder trials=1 min_s=0.002837833 median_s=0.002837833 max_s=0.002837833 omitted_trials=0 timeouts=0
listing_lag SUMMARY provider_case=S3_object trials=20 min_s=0.002346916 median_s=0.002487188 max_s=0.003287167 omitted_trials=0 timeouts=0
§4 listing_lag PASS cleanup=PASS
§4 immediate_device_folders PASS cleanup=PASS
certification suite=1 FAIL
```

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
native bucket_head=accepted bucket_list=accepted selected=Head
native checksum_present=false checksum_scope=Absent revision=ETag response_headers=S3 bucket_check=Head
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
    bucket_check: BucketCheck::Head,
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
native bucket_head=accepted bucket_list=accepted selected=Head
native checksum_present=false checksum_scope=Absent revision=ETag response_headers=S3 bucket_check=Head
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
    bucket_check: BucketCheck::Head,
    response_headers: ResponseHeaders::S3,
    request_checksums: false,
    certification: Certification::Passed { date: "2026-10-13", suite: 2 },
},
certification suite=2 PASS
```
