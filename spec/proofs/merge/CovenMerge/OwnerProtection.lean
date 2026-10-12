import CovenMerge.Ownership

/-! State-based move/delete protection reads merged identities and retained
references. A batch rejects every offending operation together, then rebuilds.
Rejections are retained within that atomic evaluation; a new received set
starts anew. The final view uses all three ordinary removal passes. -/
namespace CovenMerge.Ownership

variable {Row K Member Op : Type} [DecidableEq Row] [DecidableEq K] [DecidableEq Member]

/-- Following the declared audience parent, not arbitrary foreign keys. The
closure includes the root and concurrent descendants in the merged graph. -/
def carried (rows : List Row) (parent : Row → Option Row) (root : Row) : Row → Bool :=
  close rows (fun D r => (parent r).any D) (fun r => decide (r = root))

theorem carried_root (rows : List Row) (parent : Row → Option Row) (root : Row) :
    carried rows parent root root = true :=
  star_grow (close_star rows (fun D r => (parent r).any D) (fun r => decide (r = root))) root (by simp)

theorem carried_child (rows : List Row) (parent : Row → Option Row) (root child p : Row)
    (mem : child ∈ rows) (link : parent child = some p)
    (ancestor : carried rows parent root p = true) : carried rows parent root child = true := by
  exact normal_closed (close_normal rows (fun D r => (parent r).any D) (fun r => decide (r = root)))
    child mem (by simpa [FiresP, link, carried] using ancestor)

private theorem parent_monotone (parent : Row → Option Row) :
    MonotoneRules (FiresP (fun D r => (parent r).any D)) := by
  intro D E r sub fired
  cases hp : parent r with
  | none => simp [FiresP, hp] at fired
  | some p => simpa [FiresP, hp] using sub p (by simpa [FiresP, hp] using fired)

theorem carried_order_independent (a b : List Row) (parent : Row → Option Row) (root : Row)
    (same : ∀ r, r ∈ a ↔ r ∈ b) : carried a parent root = carried b parent root := by
  have least (rows : List Row) := normal_least (parent_monotone parent)
    (close_star rows (fun D r => (parent r).any D) (fun r => decide (r = root)))
    (close_normal rows (fun D r => (parent r).any D) (fun r => decide (r = root)))
  have ab : ∀ r, carried a parent root r = true → carried b parent root r = true :=
    (least a).2.2 _ (fun r hr hf => (least b).1 r ((same r).mp hr) hf) (least b).2.1
  have ba : ∀ r, carried b parent root r = true → carried a parent root r = true :=
    (least b).2.2 _ (fun r hr hf => (least a).1 r ((same r).mpr hr) hf) (least a).2.1
  funext r
  exact Bool.eq_iff_iff.mpr ⟨ab r, ba r⟩

structure MoveState (Row Member Audience : Type) where
  rows : List Row
  present : Row → Bool
  owner : Row → Option Member
  parent : Row → Option Row
  canRead : Member → Audience → Bool

variable {Audience : Type}

def moveBlocker (s : MoveState Row Member Audience) (root : Row) (destination : Audience) : Option Row :=
  s.rows.find? fun r => s.present r && carried s.rows s.parent root r &&
    (s.owner r).any (fun m => !s.canRead m destination)

def moveDenied (s : MoveState Row Member Audience) (root : Row) (destination : Audience) : Bool :=
  (moveBlocker s root destination).isSome

def moveReason {W : Type} (s : MoveState Row Member Audience) (root : Row) (destination : Audience) :
    Option (Cause W Member Row) :=
  (moveBlocker s root destination).bind fun r => (s.owner r).map (fun member => .move r member)

theorem move_reason_complete {W : Type} (s : MoveState Row Member Audience)
    (root : Row) (destination : Audience) (bad : moveDenied s root destination = true) :
    ∃ r member, moveReason (W := W) s root destination = some (.move r member) ∧
      r ∈ s.rows ∧ s.present r = true ∧ carried s.rows s.parent root r = true ∧
      s.owner r = some member ∧ s.canRead member destination = false := by
  cases blocker : moveBlocker s root destination with
  | none => simp [moveDenied, blocker] at bad
  | some r =>
    have found := List.find?_some (show s.rows.find? _ = some r from blocker)
    have mem := List.mem_of_find?_eq_some (show s.rows.find? _ = some r from blocker)
    simp only [Bool.and_eq_true, Option.any_eq_true, Bool.not_eq_true'] at found
    obtain ⟨⟨present, affected⟩, member, owned, unreadable⟩ := found
    exact ⟨r, member, by simp [moveReason, blocker, owned], mem, present, affected, owned, unreadable⟩

theorem move_order_independent (s : MoveState Row Member Audience) (rows : List Row)
    (same : ∀ r, r ∈ s.rows ↔ r ∈ rows) (root : Row) (destination : Audience) :
    moveDenied s root destination = moveDenied { s with rows := rows } root destination := by
  have carry := carried_order_independent s.rows rows s.parent root same
  apply Bool.eq_iff_iff.mpr
  simp only [moveDenied, moveBlocker, List.find?_isSome]
  constructor
  · rintro ⟨r, mem, h⟩
    exact ⟨r, (same r).mp mem, by simpa only [carry] using h⟩
  · rintro ⟨r, mem, h⟩
    exact ⟨r, (same r).mpr mem, by simpa only [carry] using h⟩

theorem unread_owner_blocks_move (s : MoveState Row Member Audience) (root r : Row)
    (destination : Audience) (m : Member) (mem : r ∈ s.rows) (present : s.present r = true)
    (carriedRow : carried s.rows s.parent root r = true) (owned : s.owner r = some m)
    (unreadable : s.canRead m destination = false) : moveDenied s root destination = true := by
  simp only [moveDenied, moveBlocker, List.find?_isSome]
  exact ⟨r, mem, by simp [present, carriedRow, owned, unreadable]⟩

theorem move_safe (s : MoveState Row Member Audience) (root : Row) (destination : Audience)
    (safe : moveDenied s root destination = false) (r : Row) (m : Member)
    (mem : r ∈ s.rows) (present : s.present r = true)
    (carriedRow : carried s.rows s.parent root r = true) (owned : s.owner r = some m) :
    s.canRead m destination = true := by
  cases h : s.canRead m destination
  · have := unread_owner_blocks_move s root r destination m mem present carriedRow owned h
    simp [safe] at this
  · rfl

/-- The move guard records the protected descendant and its unreadable owner
on the original change, including a root audience edit. -/
theorem move_recorded {W Col Table : Type} [DecidableEq W]
    (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (s : MoveState Row Member Audience)
    (root : Row) (destination : Audience)
    (guard : W → Row → Option (Cause W Member Row)) (values : W → Row → Col → Option W)
    (received : List W) (w : W) (r : Row) (ch : Change Col)
    (mem : w ∈ received) (change : M.chg w r = some ch) (author : allowed P w r = true)
    (verdict : guard w r = moveReason s root destination) (bad : moveDenied s root destination = true) :
    ∃ protectedRow member record,
      loss M valid p P guard values received w r = some record ∧
      record.cause = .move protectedRow member ∧ record.values = values w r ∧
      s.owner protectedRow = some member ∧ s.canRead member destination = false := by
  obtain ⟨target, member, reason, _, _, _, owned, unreadable⟩ := move_reason_complete (W := W) s root destination bad
  exact ⟨target, member, _, guard_recorded M valid p P guard values received w r ch _ mem change author
    (by rw [verdict, reason]), rfl, rfl, owned, unreadable⟩

/-! A single completed-view comparison exhibits masking counterexamples.
The member-context delete guard is in `OwnerDeleteRule`;
`OwnerDeleteContexts` retains the exhaustive comparison model. -/
namespace ViewComparison

/-- Supplied merged inputs for a proposed deletion trial, including
reference/null/CHECK effects. Its inputs are computed before rules;
no partially removed view is fed back as merge input. -/
structure DeleteTrial (Row K Member : Type) where
  before : Inputs Row K
  after : Inputs Row K
  owner : Row → Option Member
  actor : Member

def deleteDenied (t : DeleteTrial Row K Member) : Bool :=
  t.before.rows.any fun r => (view t.before).shown r && !(view t.after).shown r &&
    (t.owner r).any (fun m => decide (m ≠ t.actor))

theorem hidden_owner_blocks_delete (t : DeleteTrial Row K Member) (r : Row) (m : Member)
    (mem : r ∈ t.before.rows) (before : (view t.before).shown r = true)
    (after : (view t.after).shown r = false) (owned : t.owner r = some m) (other : m ≠ t.actor) :
    deleteDenied t = true := by
  simp only [deleteDenied, List.any_eq_true]
  exact ⟨r, mem, by simp [before, after, owned, other]⟩

theorem delete_safe (t : DeleteTrial Row K Member) (safe : deleteDenied t = false)
    (r : Row) (m : Member) (mem : r ∈ t.before.rows) (before : (view t.before).shown r = true)
    (owned : t.owner r = some m) (other : m ≠ t.actor) : (view t.after).shown r = true := by
  cases after : (view t.after).shown r
  · have := hidden_owner_blocks_delete t r m mem before after owned other
    simp [safe] at this
  · rfl

/-- Every trial runs the specified three passes, in any permissible internal
order. Restoring a deletion reruns all three, rather than un-hiding one row. -/
theorem trial_rule_order (t : DeleteTrial Row K Member) {before after : Row → Bool}
    (hb : Stratified t.before.rows (FiresP (fires t.before)) (rivalBefore t.before) (start t.before) before)
    (ha : Stratified t.after.rows (FiresP (fires t.after)) (rivalBefore t.after) (start t.after) after) :
    before = removal t.before ∧ after = removal t.after :=
  ⟨any_order_removal t.before hb, any_order_removal t.after ha⟩

end ViewComparison

/-- All candidates of a round are judged against the same completed input.
The list is in canonical operation order, not transport arrival order. -/
def keepRound (bad : List Op → Op → Bool) (active : List Op) : List Op :=
  active.filter (fun op => !bad active op)

def settle (bad : List Op → Op → Bool) (active : List Op) : List Op :=
  if (keepRound bad active).length = active.length then active else settle bad (keepRound bad active)
termination_by active.length
decreasing_by
  have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
  omega

/-- Trials consume the active set, not its enumeration. With that contract,
rejection also agrees when the candidate lists arrive in different orders. -/
theorem settle_permutation (bad : List Op → Op → Bool)
    (stateOnly : ∀ a b, a.Perm b → ∀ op, bad a op = bad b op)
    (a b : List Op) (same : a.Perm b) : (settle bad a).Perm (settle bad b) := by
  have next : (keepRound bad a).Perm (keepRound bad b) := by
    unfold keepRound
    have pred : (fun op => !bad a op) = (fun op => !bad b op) := by
      funext op; rw [stateOnly a b same op]
    rw [pred]
    exact same.filter _
  rw [settle.eq_def bad a, settle.eq_def bad b]
  by_cases fixed : (keepRound bad a).length = a.length
  · have fixedB : (keepRound bad b).length = b.length := by
      rw [← next.length_eq, ← same.length_eq]; exact fixed
    simp only [fixed, fixedB, ↓reduceIte]
    exact same
  · have fixedB : (keepRound bad b).length ≠ b.length := by
      rw [← next.length_eq, ← same.length_eq]; exact fixed
    simp only [fixed, fixedB, ↓reduceIte]
    exact settle_permutation bad stateOnly (keepRound bad a) (keepRound bad b) next
termination_by a.length
decreasing_by
  have bound : (keepRound bad a).length ≤ a.length := List.length_filter_le _ _
  omega

/-- A filter of unchanged length kept every candidate, so each is safe. -/
theorem round_same_safe (bad : List Op → Op → Bool) (active : List Op)
    (same : (keepRound bad active).length = active.length) :
    ∀ op ∈ active, bad active op = false := by
  have eq : keepRound bad active = active := List.filter_eq_self.mpr (by
    have all := List.length_filter_eq_length_iff.mp same
    exact all)
  intro op mem
  have hm : op ∈ keepRound bad active := by rw [eq]; exact mem
  simpa [keepRound, List.mem_filter] using (List.mem_filter.mp hm).2

theorem settles_safe (bad : List Op → Op → Bool) (active : List Op) :
    ∀ op ∈ settle bad active, bad (settle bad active) op = false := by
  rw [settle]
  split
  · exact round_same_safe bad active (by assumption)
  · exact settles_safe bad (keepRound bad active)
termination_by active.length
decreasing_by
  have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
  omega

theorem settles_subset (bad : List Op → Op → Bool) (active : List Op) :
    ∀ op ∈ settle bad active, op ∈ active := by
  rw [settle]
  split
  · exact fun _ h => h
  · intro op h
    exact (List.mem_filter.mp (settles_subset bad (keepRound bad active) op h)).1
termination_by active.length
decreasing_by
  have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
  omega

theorem settle_idempotent (bad : List Op → Op → Bool) (active : List Op) :
    settle bad (settle bad active) = settle bad active := by
  have eq : keepRound bad (settle bad active) = settle bad active := by
    apply List.filter_eq_self.mpr
    intro op h
    simp [settles_safe bad active op h]
  rw [settle]
  simp only [eq, ↓reduceIte]

/-- The whole rejection trace is finite, with at most one strict round per
candidate. This is a bound, not a timeout or a partial-result fallback. -/
def rounds (bad : List Op → Op → Bool) (active : List Op) : Nat :=
  if (keepRound bad active).length = active.length then 0 else 1 + rounds bad (keepRound bad active)
termination_by active.length
decreasing_by
  have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
  omega

theorem rounds_bounded (bad : List Op → Op → Bool) (active : List Op) :
    rounds bad active ≤ active.length := by
  rw [rounds]
  split
  · omega
  · have ih := rounds_bounded bad (keepRound bad active)
    have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
    omega
termination_by active.length
decreasing_by
  have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
  omega

def RoundStep (bad : List Op → Op → Bool) (a b : List Op) : Prop :=
  b = keepRound bad a ∧ b.length < a.length

/-- Every excluded candidate has an actual rejecting round as evidence.
The reason cannot be recomputed only from the final state: a later rejection
may have removed the original conflict. -/
theorem rejected_has_witness [DecidableEq Op] (bad : List Op → Op → Bool)
    (active : List Op) (op : Op) (mem : op ∈ active) (lost : op ∉ settle bad active) :
    ∃ round, Star (RoundStep bad) active round ∧ op ∈ round ∧ bad round op = true := by
  by_cases rejects : bad active op = true
  · exact ⟨active, .refl _, mem, rejects⟩
  · have kept : op ∈ keepRound bad active := by
      apply List.mem_filter.mpr
      exact ⟨mem, by simpa using rejects⟩
    rw [settle] at lost
    split at lost
    · exact (lost mem).elim
    · obtain ⟨round, path, present, badAt⟩ := rejected_has_witness bad (keepRound bad active) op kept lost
      have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
      exact ⟨round, .head ⟨rfl, by omega⟩ path, present, badAt⟩
termination_by active.length
decreasing_by
  have bound : (keepRound bad active).length ≤ active.length := List.length_filter_le _ _
  omega

/-- Retain the reason from the first rejecting round. Recomputing it from the
final state could erase a loss when another rejection removes its blocker. -/
def rejectionReason {Reason : Type} [DecidableEq Op]
    (trial : List Op → Op → Option Reason) (active : List Op) (op : Op) : Option Reason :=
  if op ∈ active then
    match trial active op with
    | some reason => some reason
    | none =>
      if (keepRound (fun round candidate => (trial round candidate).isSome) active).length =
          active.length then none
      else rejectionReason trial (keepRound (fun round candidate => (trial round candidate).isSome) active) op
  else none
termination_by active.length
decreasing_by
  have bound : (keepRound (fun round candidate => (trial round candidate).isSome) active).length ≤
      active.length := List.length_filter_le _ _
  omega

/-- The retained records correspond exactly to the rejected candidates. -/
theorem rejection_record_exact {Reason : Type} [DecidableEq Op]
    (trial : List Op → Op → Option Reason) (active : List Op) (op : Op) :
    (rejectionReason trial active op).isSome = true ↔
      op ∈ active ∧ op ∉ settle (fun round candidate => (trial round candidate).isSome) active := by
  let bad := fun round candidate => (trial round candidate).isSome
  by_cases mem : op ∈ active
  · cases reason : trial active op with
    | some reason =>
      have lost : op ∉ settle bad active := by
        intro kept
        rw [settle.eq_def] at kept
        split at kept
        · have h := round_same_safe bad active (by assumption) op mem
          simp [bad, reason] at h
        · have next := settles_subset bad (keepRound bad active) op kept
          simp [keepRound, bad, reason] at next
      rw [rejectionReason.eq_def]
      simp [mem, reason, lost, bad]
    | none =>
      have kept : op ∈ keepRound bad active := by simp [keepRound, mem, bad, reason]
      by_cases fixed : (keepRound bad active).length = active.length
      · rw [rejectionReason.eq_def, settle.eq_def]
        simp only [mem, ↓reduceIte, reason]
        change (if (keepRound bad active).length = active.length then none else
          rejectionReason trial (keepRound bad active) op).isSome = true ↔
          True ∧ op ∉ (if (keepRound bad active).length = active.length then active else
            settle bad (keepRound bad active))
        simp [fixed, mem]
      · rw [rejectionReason.eq_def, settle.eq_def]
        simp only [mem, ↓reduceIte, reason]
        change (if (keepRound bad active).length = active.length then none else
          rejectionReason trial (keepRound bad active) op).isSome = true ↔
          True ∧ op ∉ (if (keepRound bad active).length = active.length then active else
            settle bad (keepRound bad active))
        simp only [fixed, ↓reduceIte, true_and]
        simpa only [kept, true_and] using rejection_record_exact trial (keepRound bad active) op
  · rw [rejectionReason.eq_def]
    simp [mem]
termination_by active.length
decreasing_by
  simp_all only [bad]
  have bound : (keepRound (fun round candidate => (trial round candidate).isSome) active).length ≤
      active.length := List.length_filter_le _ _
  omega

theorem rejection_reason_witness {Reason : Type} [DecidableEq Op]
    (trial : List Op → Op → Option Reason) (active : List Op) (op : Op) (reason : Reason)
    (recorded : rejectionReason trial active op = some reason) :
    ∃ round, Star (RoundStep (fun round candidate => (trial round candidate).isSome)) active round ∧
      op ∈ round ∧ trial round op = some reason := by
  rw [rejectionReason.eq_def] at recorded
  split at recorded
  · rename_i mem
    cases h : trial active op with
    | some found =>
      simp only [h, Option.some.injEq] at recorded
      exact ⟨active, .refl _, mem, by rw [h, recorded]⟩
    | none =>
      simp only [h] at recorded
      split at recorded
      · contradiction
      · obtain ⟨round, path, member, cause⟩ := rejection_reason_witness trial _ op reason recorded
        have bound : (keepRound (fun round candidate => (trial round candidate).isSome) active).length ≤
            active.length := List.length_filter_le _ _
        exact ⟨round, .head ⟨rfl, by omega⟩ path, member, cause⟩
  · contradiction
termination_by active.length
decreasing_by
  have bound : (keepRound (fun round candidate => (trial round candidate).isSome) active).length ≤
      active.length := List.length_filter_le _ _
  omega

theorem rejection_keeps {Reason : Type} [DecidableEq Op]
    (trial : List Op → Op → Option Reason) (active : List Op) (op : Op) :
    (rejectionReason trial active op).isNone =
      (!decide (op ∈ active) || decide (op ∈ settle (fun round candidate => (trial round candidate).isSome) active)) := by
  have exactRecord := rejection_record_exact trial active op
  cases record : rejectionReason trial active op <;>
    simp only [record, Option.isSome_none, Option.isSome_some, Bool.false_eq_true,
      true_iff, false_iff] at exactRecord
  · by_cases mem : op ∈ active
    · have kept : op ∈ settle (fun round candidate => (trial round candidate).isSome) active := by
        simpa [mem] using exactRecord
      simp [kept]
    · simp [mem]
  · simp [exactRecord.1, exactRecord.2]

/-- A merge-state-derived guard has the same result in every causal arrival
order. The function must rebuild each trial from retained changes; its purity
alone does not establish that it enumerates the actual SQL effects. -/
theorem protection_converges {W Col Table : Type} [DecidableEq W]
    (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member) (candidates : List Op)
    (trial : St W Row Col → List Op → Op → Bool) {a b : List W}
    (ca : CausalOrder M a) (cb : CausalOrder M b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    settle (trial (merged M valid p P (fun _ _ => none) a)) candidates =
      settle (trial (merged M valid p P (fun _ _ => none) b)) candidates := by
  rw [converges M valid p P (fun _ _ => none) ca cb same]

/-- Compose merged-state guard derivation with the real filtered row merge,
ordinary rule losses and per-change ownership/dependency records. -/
theorem guarded_result_converges {W Col Table : Type} [DecidableEq W] [DecidableEq Col]
    (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (P : Policy W Row Table Member)
    (derive : St W Row Col → W → Row → Option (Cause W Member Row))
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    {a b : List W} (ca : CausalOrder M a) (cb : CausalOrder M b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    result M valid p P (derive (merged M valid p P (fun _ _ => none) a)) values inputs a =
      result M valid p P (derive (merged M valid p P (fun _ _ => none) b)) values inputs b := by
  rw [converges M valid p P (fun _ _ => none) ca cb same]
  exact result_converges M valid p P _ values inputs ca cb same

end CovenMerge.Ownership
