import CovenStorelog

#print axioms CovenStorelog.Examples.CircleDeletion.valid_history
#print axioms CovenStorelog.Examples.CircleDeletion.causal_arrivals
#print axioms CovenStorelog.Examples.CircleDeletion.no_conflict
#print axioms CovenStorelog.Examples.CircleDeletion.both_apply
#print axioms CovenStorelog.Examples.CircleDeletion.alone_valid
#print axioms CovenStorelog.Examples.CircleDeletion.alone_conflict
#print axioms CovenStorelog.Examples.CircleDeletion.removal_beats_rename

#print axioms CovenStorelog.publication_requires_seal
#print axioms CovenStorelog.sealing_requires_approval
#print axioms CovenStorelog.dropped_join_declined
#print axioms CovenStorelog.Examples.seal_then_entry
#print axioms CovenStorelog.Examples.carol_join_declined

#print axioms CovenStorelog.effect_created
#print axioms CovenStorelog.scan_root_suffix
#print axioms CovenStorelog.scan_root
#print axioms CovenStorelog.settleN_created
#print axioms CovenStorelog.resolve_created
#print axioms CovenStorelog.admin_invariant
#print axioms CovenStorelog.closed_has_admin

#print axioms CovenStorelog.scan_views_congr
#print axioms CovenStorelog.settleN_views_congr
#print axioms CovenStorelog.settle_views_congr
#print axioms CovenStorelog.resolve_eq
#print axioms CovenStorelog.range_filter_bound
#print axioms CovenStorelog.resolve_bound_independent

#print axioms CovenStorelog.Examples.Gifts.valid_history
#print axioms CovenStorelog.Examples.Gifts.causal_arrivals
#print axioms CovenStorelog.Examples.Gifts.removal_names_no_circle
#print axioms CovenStorelog.Examples.Gifts.conflicts
#print axioms CovenStorelog.Examples.Gifts.first_pass
#print axioms CovenStorelog.Examples.Gifts.carol_stays

#print axioms CovenStorelog.before_irrefl
#print axioms CovenStorelog.before_trans
#print axioms CovenStorelog.before_total
#print axioms CovenStorelog.before_asymm
#print axioms CovenStorelog.store_removal_circle_add

#print axioms CovenStorelog.Accounting.skip
#print axioms CovenStorelog.Accounting.keep
#print axioms CovenStorelog.Accounting.drop
#print axioms CovenStorelog.scan_accounting
#print axioms CovenStorelog.settleN_accounting
#print axioms CovenStorelog.resolve_partition
#print axioms CovenStorelog.report_exactly_dropped

#print axioms CovenStorelog.step_spec
#print axioms CovenStorelog.closed_insert
#print axioms CovenStorelog.insert_entrySet
#print axioms CovenStorelog.run_isSpec
#print axioms CovenStorelog.causal_ready
#print axioms CovenStorelog.timestampOrder_causal
#print axioms CovenStorelog.full_history_causal
#print axioms CovenStorelog.author_view_reached
#print axioms CovenStorelog.author_past_causal
#print axioms CovenStorelog.isSpec_unique
#print axioms CovenStorelog.storelog_converges
#print axioms CovenStorelog.reports_converge

#print axioms CovenStorelog.Examples.circle_examples_valid
#print axioms CovenStorelog.Examples.circle_created_by_member
#print axioms CovenStorelog.Examples.circle_renamed
#print axioms CovenStorelog.Examples.circle_deleted
#print axioms CovenStorelog.Examples.circle_delete_beats_reset
#print axioms CovenStorelog.Examples.circle_key_rotation
#print axioms CovenStorelog.Examples.circle_leaving
#print axioms CovenStorelog.Examples.outsider_cannot_manage
#print axioms CovenStorelog.Examples.empty_circle_deleted
#print axioms CovenStorelog.Examples.member_removal_updates_circles
#print axioms CovenStorelog.Examples.store_removal_beats_circle_add

#print axioms CovenStorelog.Examples.member_examples_valid
#print axioms CovenStorelog.Examples.opening_log
#print axioms CovenStorelog.Examples.example_add_and_promote
#print axioms CovenStorelog.Examples.example_remove_phone
#print axioms CovenStorelog.Examples.new_device_registers_itself
#print axioms CovenStorelog.Examples.example_lower_role
#print axioms CovenStorelog.Examples.example_mutual_removal
#print axioms CovenStorelog.Examples.example_carol
#print axioms CovenStorelog.Examples.carol_device
#print axioms CovenStorelog.Examples.equal_adds_combine
#print axioms CovenStorelog.Examples.three_admins
#print axioms CovenStorelog.Examples.removed_author_view
#print axioms CovenStorelog.Examples.dropped_removal_allows_phone

#print axioms CovenStorelog.Examples.history_valid
#print axioms CovenStorelog.Examples.valid_history
#print axioms CovenStorelog.Examples.causal_arrivals
#print axioms CovenStorelog.Examples.entries_concurrent
#print axioms CovenStorelog.Examples.each_entry_allowed_in_its_view
#print axioms CovenStorelog.Examples.first_pass_drops_add
#print axioms CovenStorelog.Examples.second_pass_drops_removal
#print axioms CovenStorelog.Examples.losing_removal_discards_add
#print axioms CovenStorelog.Examples.add_has_no_surviving_opponent
#print axioms CovenStorelog.Examples.dropped_add_meets_conditions

#print axioms CovenStorelog.causalCheck_sound
#print axioms CovenStorelog.validCheck_sound
#print axioms CovenStorelog.every_order

#print axioms CovenStorelog.Examples.version_examples_valid
#print axioms CovenStorelog.Examples.same_version_snapshot
#print axioms CovenStorelog.Examples.identical_version_raises
#print axioms CovenStorelog.Examples.version_raised
#print axioms CovenStorelog.Examples.store_reset_tie
#print axioms CovenStorelog.Examples.circle_reset_tie
#print axioms CovenStorelog.Examples.equal_resets_combine
#print axioms CovenStorelog.Examples.later_reset

#print axioms CovenStorelog.checkedEffect_sound
#print axioms CovenStorelog.scan_state
#print axioms CovenStorelog.settleN_state
#print axioms CovenStorelog.resolve_safe
#print axioms CovenStorelog.resolve_references
#print axioms CovenStorelog.scan_kept
#print axioms CovenStorelog.settleN_kept
#print axioms CovenStorelog.kept_received
#print axioms CovenStorelog.authority_uses_author_view
#print axioms CovenStorelog.devices_have_members
#print axioms CovenStorelog.circles_have_members
#print axioms CovenStorelog.circles_nonempty
#print axioms CovenStorelog.device_add_authority
#print axioms CovenStorelog.device_removal_authority
#print axioms CovenStorelog.circle_delete_authority

#print axioms CovenStorelog.lookup_some_mem
#print axioms CovenStorelog.lookup_erase_same
#print axioms CovenStorelog.lookup_erase_other
#print axioms CovenStorelog.lookup_put
#print axioms CovenStorelog.member_put
#print axioms CovenStorelog.member_erase
#print axioms CovenStorelog.references_empty
#print axioms CovenStorelog.References.putMembers
#print axioms CovenStorelog.References.withDevices
#print axioms CovenStorelog.References.withCircles
#print axioms CovenStorelog.References.putCircle
#print axioms CovenStorelog.References.eraseCircle
#print axioms CovenStorelog.withoutMember_members
#print axioms CovenStorelog.References.removeMember
#print axioms CovenStorelog.effect_references

#print axioms CovenStorelog.scan_progress
#print axioms CovenStorelog.countP_lt_of
#print axioms CovenStorelog.restart_decreases
#print axioms CovenStorelog.settleN_total

#print axioms CovenStorelog.settle_eq_some
#print axioms CovenStorelog.authorViews_at
#print axioms CovenStorelog.reported_to_author
#print axioms CovenStorelog.report_only_author
