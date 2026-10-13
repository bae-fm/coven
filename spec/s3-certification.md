# S3 certification evidence

Suite version 1 passed GCS and R2 on 2026-10-13. AWS is uncertified because
no live account is available. The selected entries are in
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
