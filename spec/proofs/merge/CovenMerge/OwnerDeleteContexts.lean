import CovenMerge.OwnerEvaluation

/-! The all-contexts comparison model through the complete removal passes. Testing every
surviving foreign-delete context prevents concurrent deletes from masking
each other, including effects of null substitution on unique claims. -/
namespace CovenMerge.Ownership

variable {Row K Member Op : Type}
  [DecidableEq Row] [DecidableEq K] [DecidableEq Member] [DecidableEq Op]

/-- Enumerate every subsequence in a fixed order. No context is guessed from
an already removed view. The enumeration has exponential cardinality. -/
def deletionContexts : List Op → List (List Op)
  | [] => [[]]
  | op :: rest => let contexts := deletionContexts rest
    contexts ++ contexts.map (op :: ·)

theorem context_complete (active context : List Op) :
    context ∈ deletionContexts active ↔ context.Sublist active := by
  induction active generalizing context with
  | nil => simp [deletionContexts]
  | cons op rest ih =>
    simp only [deletionContexts, List.mem_append, List.mem_map, ih]
    constructor
    · rintro (h | ⟨tail, h, rfl⟩)
      · exact h.cons op
      · exact h.cons_cons op
    · intro h
      cases h with
      | cons _ h => exact Or.inl h
      | cons_cons _ h => exact Or.inr ⟨_, h, rfl⟩

structure DeleteContexts (Row K Member Op : Type) where
  rows : List Row
  owner : Row → Option Member
  actor : Op → Member
  isDelete : Op → Bool
  inputs : List Op → Inputs Row K

/-- Only foreign deletions vary. Non-delete operations and an owner's own
deletions remain present in every trial protecting that owner. Otherwise a
trial could resurrect a row the owner explicitly deleted. -/
def ownerDeletesFixed (s : DeleteContexts Row K Member Op) (active context : List Op)
    (member : Member) : Bool :=
  active.all fun op => (s.isDelete op && decide (s.actor op ≠ member)) || decide (op ∈ context)

/-- Reference protection ignores independent CHECK/unique losses; the full
view additionally detects deletion-induced substitutions that hide rivals.
Both views retain the rebuilt reference generations. -/
inductive ProtectionView where
  | references | allRules
  deriving DecidableEq

def protectionShown (mode : ProtectionView) (inputs : Inputs Row K) : Row → Bool :=
  match mode with
  | .references => (view ({ inputs with checkFails := fun _ => false, claims := fun _ => [] } : Inputs Row K)).shown
  | .allRules => (view inputs).shown

def contextHarms (s : DeleteContexts Row K Member Op) (active : List Op) (op : Op)
    (r : Row) (member : Member) (context : List Op) : Bool :=
  !decide (op ∈ context) && ownerDeletesFixed s active context member &&
    [ProtectionView.references, .allRules].any fun mode =>
      protectionShown mode (s.inputs context) r && !protectionShown mode (s.inputs (op :: context)) r

def contextBlocker (s : DeleteContexts Row K Member Op) (active : List Op) (op : Op) :
    Option (Row × Member) :=
  (s.rows.filterMap fun r => (s.owner r).map (r, ·)).find? fun (r, member) =>
    decide (member ≠ s.actor op) && (deletionContexts active).any (contextHarms s active op r member)

def contextDeleteReason {W : Type} (s : DeleteContexts Row K Member Op)
    (active : List Op) (op : Op) : Option (Cause W Member Row) :=
  (contextBlocker s active op).map fun (r, member) => .delete r member

theorem context_delete_exact (s : DeleteContexts Row K Member Op) (active : List Op) (op : Op) :
    (contextBlocker s active op).isSome = true ↔
      ∃ r ∈ s.rows, ∃ member, s.owner r = some member ∧ member ≠ s.actor op ∧
        ∃ context, context.Sublist active ∧ contextHarms s active op r member context = true := by
  simp only [contextBlocker, List.find?_isSome, List.mem_filterMap, Option.map_eq_some_iff,
    Bool.and_eq_true, decide_eq_true_eq, List.any_eq_true, context_complete]
  constructor
  · rintro ⟨⟨r, member⟩, ⟨row, mem, m, owned, eq⟩, other, context, sub, harm⟩
    cases eq
    exact ⟨r, mem, member, owned, other, context, sub, harm⟩
  · rintro ⟨r, mem, member, owned, other, context, sub, harm⟩
    exact ⟨(r, member), ⟨r, mem, member, owned, rfl⟩, other, context, sub, harm⟩

theorem context_delete_safe {W : Type} (s : DeleteContexts Row K Member Op)
    (active : List Op) (op : Op) (mode : ProtectionView) (safe : contextDeleteReason (W := W) s active op = none)
    (r : Row) (member : Member) (mem : r ∈ s.rows) (owned : s.owner r = some member)
    (other : member ≠ s.actor op) (context : List Op) (sub : context.Sublist active)
    (without : op ∉ context) (fixed : ownerDeletesFixed s active context member = true)
    (before : protectionShown mode (s.inputs context) r = true) :
    protectionShown mode (s.inputs (op :: context)) r = true := by
  cases after : protectionShown mode (s.inputs (op :: context)) r
  · have bad := (context_delete_exact s active op).mpr
      ⟨r, mem, member, owned, other, context, sub, by
        simp only [contextHarms, without, decide_false, Bool.not_false, fixed, Bool.true_and]
        exact List.any_eq_true.mpr ⟨mode, by cases mode <;> simp, by simp [before, after]⟩⟩
    simp only [contextDeleteReason, Option.map_eq_none_iff] at safe
    simp [safe] at bad
  · rfl

/-- Every reported delete cause has a real before/after witness in one of the two views,
with the protected owner's own deletions held fixed. -/
theorem context_reason_witness {W : Type} (s : DeleteContexts Row K Member Op)
    (active : List Op) (op : Op) (r : Row) (member : Member)
    (reason : contextDeleteReason (W := W) s active op = some (.delete r member)) :
    r ∈ s.rows ∧ s.owner r = some member ∧ member ≠ s.actor op ∧
      ∃ context, context.Sublist active ∧ contextHarms s active op r member context = true := by
  obtain ⟨⟨row, m⟩, found, eq⟩ := Option.map_eq_some_iff.mp reason
  cases eq
  have mem := List.mem_of_find?_eq_some (show _ = some (r, member) from found)
  have hit := List.find?_some (show _ = some (r, member) from found)
  simp only [List.mem_filterMap, Option.map_eq_some_iff] at mem
  obtain ⟨row', present, m', owned, eq⟩ := mem
  cases eq
  simp only [Bool.and_eq_true, decide_eq_true_eq, List.any_eq_true, context_complete] at hit
  exact ⟨present, owned, hit⟩

variable {W : Type}

private theorem context_safe_subset (s : DeleteContexts Row K Member Op)
    (active : List Op) (op : Op) (mode : ProtectionView) (safe : contextDeleteReason (W := W) s active op = none)
    (setOnly : ∀ a b, (∀ d, d ∈ a ↔ d ∈ b) → s.inputs a = s.inputs b)
    (r : Row) (m : Member) (mem : r ∈ s.rows) (owned : s.owner r = some m)
    (other : m ≠ s.actor op) (context : List Op) (sub : ∀ d ∈ context, d ∈ active)
    (without : op ∉ context) (fixed : ownerDeletesFixed s active context m = true)
    (before : protectionShown mode (s.inputs context) r = true) :
    protectionShown mode (s.inputs (op :: context)) r = true := by
  let canonical := active.filter (fun d => decide (d ∈ context))
  have same : ∀ d, d ∈ canonical ↔ d ∈ context := by
    intro d
    simp only [canonical, List.mem_filter, decide_eq_true_eq]
    exact ⟨fun h => h.2, fun h => ⟨sub d h, h⟩⟩
  have canonicalFixed : ownerDeletesFixed s active canonical m = true := by
    simpa only [ownerDeletesFixed, same] using fixed
  have h := context_delete_safe s active op mode safe r m mem owned other canonical
    (List.filter_sublist) (by simpa only [same] using without) canonicalFixed
    (by rw [setOnly canonical context same]; exact before)
  rw [setOnly (op :: canonical) (op :: context) (by intro d; simp only [List.mem_cons, same])] at h
  exact h

/-- If every active delete passes the full-context guard, their combination
cannot hide an owned row visible with only that owner's deletes and the
non-delete candidates. The input builder depends on the active set. -/
theorem all_deletes_preserve (s : DeleteContexts Row K Member Op) (active : List Op) (mode : ProtectionView)
    (setOnly : ∀ a b, (∀ d, d ∈ a ↔ d ∈ b) → s.inputs a = s.inputs b)
    (safe : ∀ op ∈ active, contextDeleteReason (W := W) s active op = none)
    (r : Row) (m : Member) (mem : r ∈ s.rows) (owned : s.owner r = some m)
    (before : protectionShown mode (s.inputs (active.filter
      (fun op => !s.isDelete op || decide (s.actor op = m)))) r = true) :
    protectionShown mode (s.inputs active) r = true := by
  let base := active.filter (fun op => !s.isDelete op || decide (s.actor op = m))
  have baseSub : ∀ d ∈ base, d ∈ active := fun _ h => (List.mem_filter.mp h).1
  have start : ownerDeletesFixed s active base m = true := by
    apply List.all_eq_true.mpr
    intro d present
    by_cases foreign : s.isDelete d = true ∧ s.actor d ≠ m
    · simp [foreign.1, foreign.2]
    · have kept : d ∈ base := by
        simp only [base, List.mem_filter, Bool.or_eq_true, Bool.not_eq_true', decide_eq_true_eq]
        exact ⟨present, by cases h : s.isDelete d <;> simp_all⟩
      simp [kept]
  have extend (context : List Op) (contextSub : ∀ d ∈ context, d ∈ active)
      (fixed : ownerDeletesFixed s active context m = true)
      (shown : protectionShown mode (s.inputs context) r = true)
      (remaining : List Op) (remainingSub : ∀ d ∈ remaining, d ∈ active) :
      protectionShown mode (s.inputs (remaining ++ context)) r = true := by
    induction remaining with
    | nil => exact shown
    | cons op rest ih =>
      have restSub : ∀ d ∈ rest, d ∈ active := fun d h => remainingSub d (by simp [h])
      have shownRest := ih restSub
      have sub : ∀ d ∈ rest ++ context, d ∈ active := by
        intro d h
        exact (List.mem_append.mp h).elim (restSub d) (contextSub d)
      have fixedRest : ownerDeletesFixed s active (rest ++ context) m = true := by
        apply List.all_eq_true.mpr
        intro d hd
        have h := List.all_eq_true.mp fixed d hd
        simp only [Bool.or_eq_true, decide_eq_true_eq] at h ⊢
        exact h.elim Or.inl (fun hc => Or.inr (List.mem_append_right _ hc))
      by_cases already : op ∈ rest ++ context
      · rw [setOnly (op :: rest ++ context) (rest ++ context)
          (by intro d; simp only [List.cons_append, List.mem_cons]; exact ⟨fun h => h.elim (fun e => e ▸ already) id, Or.inr⟩)]
        exact shownRest
      · have present := remainingSub op (by simp)
        have required := List.all_eq_true.mp fixedRest op present
        simp only [Bool.or_eq_true, decide_eq_true_eq, already, or_false, Bool.and_eq_true] at required
        exact context_safe_subset s active op mode (safe op present) setOnly r m mem owned
          (Ne.symm required.2) (rest ++ context) sub already fixedRest shownRest
  have shown := extend base baseSub start before active (fun _ h => h)
  rw [setOnly (active ++ base) active (by
    intro d
    exact ⟨fun h => (List.mem_append.mp h).elim id (baseSub d), List.mem_append_left _⟩)] at shown
  exact shown

end CovenMerge.Ownership
