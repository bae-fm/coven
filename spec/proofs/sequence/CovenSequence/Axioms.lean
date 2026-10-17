import CovenSequence

#print axioms CovenSequence.Claim.conclude_correct
#print axioms CovenSequence.Claim.occupant_claimed
#print axioms CovenSequence.Claim.exactly_one_claimant
#print axioms CovenSequence.Claim.no_compare_lands_twice
#print axioms CovenSequence.Claim.floor_check_sound
#print axioms CovenSequence.Claim.stale_create_wins_deleted_slot

#print axioms CovenSequence.prefix_determines_state
#print axioms CovenSequence.valid_prefix

#print axioms CovenSequence.cutoff

#print axioms CovenSequence.first_slot_wins
#print axioms CovenSequence.owner_stays

#print axioms CovenSequence.read_ok
#print axioms CovenSequence.reads_ok
#print axioms CovenSequence.read_progress
#print axioms CovenSequence.reaches_head
#print axioms CovenSequence.joiner_reaches_head
#print axioms CovenSequence.readers_agree
#print axioms CovenSequence.keyless_passes_only_others
#print axioms CovenSequence.keyless_stops
#print axioms CovenSequence.ReaderExamples.slot4_refused
#print axioms CovenSequence.ReaderExamples.stale_floor_strands_joiner

#print axioms CovenSequence.store_good
#print axioms CovenSequence.store_reader
#print axioms CovenSequence.stores_good
#print axioms CovenSequence.claim_floor_check

#print axioms CovenSequence.dev_ok
#print axioms CovenSequence.device_reaches_head
#print axioms CovenSequence.device_stops
#print axioms CovenSequence.stopped_stays
#print axioms CovenSequence.deletion_keeps_reachable

#print axioms CovenSequence.Operation.step_req
#print axioms CovenSequence.Operation.step_inv
#print axioms CovenSequence.Operation.steps_inv
#print axioms CovenSequence.Operation.operation_outcome
#print axioms CovenSequence.Operation.forgetful_lands_twice
#print axioms CovenSequence.Operation.eager_undo_leaks

#print axioms CovenSequence.Settle.lastClaim_ge
#print axioms CovenSequence.Settle.step_claims
#print axioms CovenSequence.Settle.claims_ge
#print axioms CovenSequence.Settle.settle_from_floor
#print axioms CovenSequence.Settle.settle_correct
#print axioms CovenSequence.Settle.lands_once_never_lost
#print axioms CovenSequence.Settle.state_alone_cannot_settle
#print axioms CovenSequence.Settle.admin_state_cannot_settle
#print axioms CovenSequence.Settle.cstep_signed
#print axioms CovenSequence.Settle.foreign_slot_means_copy

#print axioms CovenSequence.Deadline.bounded_retention
#print axioms CovenSequence.Deadline.uncovered_has_no_deadline
#print axioms CovenSequence.Deadline.cut_delete_allowed
#print axioms CovenSequence.Causality.authored_readThrough
#print axioms CovenSequence.Causality.catchup_preserves_originals
#print axioms CovenSequence.Causality.ordered_application_causal
#print axioms CovenSequence.Causality.past_transitive
#print axioms CovenSequence.Causality.receivers_agree
#print axioms CovenSequence.Causality.own_predecessor_is_read
#print axioms CovenSequence.Causality.intervening_write_not_read

#print axioms CovenSequence.LiveClaims.latest_ge
#print axioms CovenSequence.LiveClaims.winner_counted_setter
#print axioms CovenSequence.LiveClaims.winner_live
#print axioms CovenSequence.LiveClaims.deleted_releases
#print axioms CovenSequence.LiveClaims.first_live_claim
#print axioms CovenSequence.LiveClaims.within_slot
#print axioms CovenSequence.LiveClaims.first_after_release
#print axioms CovenSequence.LiveClaims.readers_agree
#print axioms CovenSequence.LiveClaims.older_loser_returns
#print axioms CovenSequence.Owners.selected_creation
#print axioms CovenSequence.Owners.lower_write_number_wins
#print axioms CovenSequence.Owners.later_claim_preserves_creator
#print axioms CovenSequence.Owners.move_preserves_owner
#print axioms CovenSequence.Owners.retained_creator

#print axioms CovenSequence.Authority.every_prefix_has_admin
#print axioms CovenSequence.Authority.applied_membership_authorized
#print axioms CovenSequence.Authority.last_admin_refused
#print axioms CovenSequence.Preparations.ordinary_covers_boundary
#print axioms CovenSequence.Preparations.preparation_announced
#print axioms CovenSequence.Preparations.activation_requires_preparation
#print axioms CovenSequence.Preparations.preparation_no_effect
#print axioms CovenSequence.Preparations.reset_replaces
#print axioms CovenSequence.Preparations.abandoned_supplies_no_coverage
#print axioms CovenSequence.Authority.store_reset_authorized
#print axioms CovenSequence.Preparations.activated_is_usable
#print axioms CovenSequence.Preparations.announcement_uploaded
#print axioms CovenSequence.Preparations.abandoned_not_usable
#print axioms CovenSequence.Preparations.ordinary_matches_schema_reset
