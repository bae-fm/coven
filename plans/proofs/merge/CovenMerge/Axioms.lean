import CovenMerge.Counterexamples
import CovenMerge.PostRules

/-! Run `lake env lean CovenMerge/Axioms.lean` to print the axioms each main result uses. -/

open CovenMerge

#print axioms merge_converges
#print axioms snapshot_converges
#print axioms causalOrder_of_ts_sorted
#print axioms newman
#print axioms kill_unique_normal
#print axioms spec_unique_not_confluent
#print axioms stratified_unique
#print axioms end_to_end
#print axioms Literal.ce1_bens_phone
#print axioms Literal.ce1_carols_tablet
#print axioms Literal.ce2_gen_write
#print axioms Literal.ce3_replaced_by
#print axioms Literal.ce4_cascade
#print axioms Literal.ce5_set_null
#print axioms Literal.ce6_unique
#print axioms Literal.ce7_check
#print axioms Literal.ce8_unique_cascade
#print axioms Literal.ce9_ancestor
#print axioms Example.example_8_1
#print axioms normal_least
