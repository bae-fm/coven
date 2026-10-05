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
#print axioms SetDefault.example_8_4_default
#print axioms DeletedCircle.example_14_7
#print axioms SetNull.example_8_4_later
