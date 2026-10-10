import CovenMerge

/-! Run `lake env lean CovenMerge/Axioms.lean` to print the axioms each main result uses. -/

open CovenMerge

-- merged state
#print axioms merge_converges
#print axioms snapshot_converges
#print axioms run_isSpec
#print axioms isSpec_unique
#print axioms causalOrder_of_ts_sorted
-- removal rules
#print axioms newman
#print axioms kill_unique_normal
#print axioms normal_least
#print axioms fires_monotone
#print axioms stratified_unique
#print axioms any_order_removal
#print axioms removed_has_rule
#print axioms device_converges
#print axioms rule_order_converges
#print axioms UniqueOnce.unique_with_others
#print axioms UniqueOnce.judged_once
-- audiences
#print axioms audience_converges
#print axioms audiences_agree
#print axioms removal_local
#print axioms rivals_closed
#print axioms fingerprint_local
#print axioms Moved.agree
#print axioms Moved.store_wins
-- the spec's examples
#print axioms Title.example_8_1
#print axioms Title.example_8_2
#print axioms Delete.example_8_3
#print axioms TwoReasons.example_8
#print axioms FK.example_8_4
#print axioms KeyChange.example_8_5_key
#print axioms Stamp.example_8_5_stamp
#print axioms Comeback.example_8_5_subnote
#print axioms Comeback.example_8_5_step3
#print axioms Check.example_8_6
#print axioms DeletedCircle.example_14_7
#print axioms SetNull.example_8_4_later

-- entry-named removal and recovery (§8, §14.7)
#print axioms deleted_rule_names_entry
#print axioms named_rules_nonempty
#print axioms circle_entries_preserve_merge
#print axioms entry_device_converges
#print axioms entry_device_rule_order
#print axioms entry_rule_order
#print axioms row_returns
#print axioms same_key_shows_once
-- migration generations and retained history (§17.1)
#print axioms independent_rule_order
#print axioms nonfinal_does_not_decide
#print axioms nonfinal_only_untouched
#print axioms hidden_generation_delete
#print axioms migration_keeps_generation_record
#print axioms hidden_delete_even
#print axioms migration_removes_live_loss
#print axioms late_edit_recorded
#print axioms migration_converges
#print axioms migration_capture_converges
#print axioms freeze_preserves
#print axioms freeze_preserves_values
#print axioms drop_preserves_pending
#print axioms drop_freezes_affected
#print axioms drop_preserves_all_pending
-- resets, including the author (§19.3)
#print axioms reset_valid
#print axioms reset_causal
#print axioms reset_exact
#print axioms reset_ignored_no_effect
#print axioms reset_covered_not_reapplied
#print axioms post_reset_ignores_old_dependency
#print axioms reset_converges
#print axioms reset_every_device
#print axioms reset_other_audience
-- accounting and the complete boundary result (§3)
#print axioms value_accounted
#print axioms hidden_value_frozen
#print axioms reset_ignored_never_schema_loss
#print axioms boundary_accounted
#print axioms applies_iff_post
#print axioms boundary_converges
#print axioms boundary_value_accounted
#print axioms boundary_rule_order
#print axioms boundary_reset_no_loss
#print axioms excluded_values_recorded
-- concrete histories
#print axioms IntegrityExamples.nonfinal_parent_and_child
#print axioms IntegrityExamples.migration_retains_reversible_rows
#print axioms IntegrityExamples.migration_writes_valid
#print axioms IntegrityExamples.migration_and_late_edit
#print axioms IntegrityExamples.dropped_schema_keeps_losses
#print axioms IntegrityExamples.same_key_after_return
#print axioms IntegrityExamples.reset_author_and_remote
#print axioms IntegrityExamples.reset_before_schema_exclusion
#print axioms IntegrityExamples.reset_writes_valid
#print axioms IntegrityExamples.reset_generations
#print axioms IntegrityExamples.reset_snapshot_correct
#print axioms IntegrityExamples.consumed_is_not_input
#print axioms no_reset_cannot_ignore
#print axioms IntegrityExamples.migration_without_reset
#print axioms snapshot_valid
#print axioms snapshot_causal
#print axioms snapshot_inputs_converge
