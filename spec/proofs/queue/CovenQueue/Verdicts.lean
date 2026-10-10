import CovenQueue.Model

namespace CovenQueue

abbrev History := Nat → CheckedWrite

/-- All earlier timestamp indices actually read by this write, including its
own earlier numbers. Valid metadata has no read at a later index (§7). -/
def causes (h : History) (v : View) (w : Nat) : List Nat :=
  (List.range w).filter fun a => reads (h w).record.identity (h a).record.identity &&
    !discardedBefore v (h w).record.identity (h a).record.identity

def Causal (h : History) : Prop :=
  ∀ w a, reads (h w).record.identity (h a).record.identity = true → a < w

theorem cause_earlier {h : History} {v : View} {w a : Nat} (ha : a ∈ causes h v w) : a < w := by
  exact List.mem_range.mp (List.mem_filter.mp ha).1

theorem own_cause (h : History) (v : View) (valid : Causal h) (w a : Nat)
    (hd : (h w).record.identity.id.device = (h a).record.identity.id.device)
    (hn : (h a).record.identity.id.number < (h w).record.identity.id.number)
    (kept : discardedBefore v (h w).record.identity (h a).record.identity = false) :
    a ∈ causes h v w := by
  have hr := own_earlier_read _ _ hd hn
  exact List.mem_filter.mpr ⟨List.mem_range.mpr (valid _ _ hr), by simp [hr, kept]⟩

/-- A complete device evaluation, in any causal arrival order. Missing or
refused causes leave a write waiting, so they cannot produce this certificate.
The actual verdict is computed by `classify`, not supplied by a guard. -/
inductive Judged (h : History) (v : View) : Nat → Verdict → Prop where
  | receive (w : Nat) (answers : Nat → Verdict) :
      (∀ a ∈ causes h v w, Judged h v a (answers a)) →
      (∀ a ∈ causes h v w, ∀ r, answers a ≠ .refused r) →
      Judged h v w (classify v (h w) ((causes h v w).map answers))

/-- Two devices can process independent causes in different orders. Their
checked answers still agree, by induction through the actual dependency graph. -/
theorem verdict_agreement {h : History} {v : View} {w : Nat} {x y : Verdict}
    (hx : Judged h v w x) (hy : Judged h v w y) : x = y := by
  induction hx generalizing y with
  | receive w answers deps _ ih =>
    cases hy with
    | receive _ other deps' _ =>
      congr 1
      apply List.map_congr_left
      intro a ha
      exact ih a ha (deps' a ha)

theorem applied_has_no_excluded_input (v : View) (w : CheckedWrite) (cs : List Verdict)
    (ha : classify v w cs = .applied) : excludedInput w.record cs = none := by
  unfold classify classify.schema classify.inherited at ha
  split at ha
  · contradiction
  · split at ha
    · split at ha
      · contradiction
      · split at ha
        · split at ha
          · contradiction
          · split at ha <;> simp_all
        · split at ha <;> simp_all
    · split at ha
      · split at ha
        · contradiction
        · split at ha <;> simp_all
      · split at ha <;> simp_all

theorem excluded_cause_adopted (v : View) (w : CheckedWrite) (cs : List Verdict)
    (ha : classify v w cs = .applied) (he : Verdict.excluded e ∈ cs) :
    e ∈ w.record.identity.storeRead := by
  have hn := applied_has_no_excluded_input v w cs ha
  have := (List.findSome?_eq_none_iff.mp hn) (.excluded e) he
  by_cases hm : e ∈ w.record.identity.storeRead
  · exact hm
  · simp [hm] at this

/-- An applied write cannot still use an excluded value. Adoption passes the
number while replacing its state inputs, rather than inheriting the loss. -/
theorem no_applied_excluded_dependency {h : History} {v : View} {w a e : Nat}
    (hw : Judged h v w .applied) (ha : a ∈ causes h v w)
    (he : Judged h v a (.excluded e)) :
    e ∈ (h w).record.identity.storeRead := by
  generalize hv : Verdict.applied = result at hw
  cases hw with
  | receive _ answers deps _ =>
    have eq := verdict_agreement (deps a ha) he
    apply excluded_cause_adopted v (h w) _ hv.symm
    exact List.mem_map.mpr ⟨a, ha, eq⟩

/-- Reset adoption is needed only outside S: S itself supplies its own state.
An old write covered by S is not reconstructed from discarded outside inputs. -/
theorem applied_outside_reset_adopted (v : View) (w : CheckedWrite) (cs : List Verdict)
    (b : Boundary) (hb : v.reset = some b) (hc : w.record.identity.id ∉ b.covers)
    (ha : classify v w cs = .applied) : b.entry ∈ w.record.identity.storeRead := by
  unfold classify at ha
  split at ha
  · contradiction
  · rw [hb] at ha
    by_cases hn : b.entry ∈ w.record.identity.storeRead
    · exact hn
    · simp [beforeReset, hc, hn] at ha

theorem schema_not_ignored (v : View) (w : CheckedWrite) (cs : List Verdict) (e : Nat) :
    classify.schema v w cs ≠ .ignored e := by
  unfold classify.schema classify.inherited
  split
  · split
    · simp
    · split <;> simp
  · split <;> simp

theorem ignored_origin (v : View) (w : CheckedWrite) (cs : List Verdict) (e : Nat)
    (he : classify v w cs = .ignored e) :
    ∃ b, v.reset = some b ∧ b.entry = e ∧ w.record.identity.id ∉ b.covers := by
  unfold classify at he
  split at he
  · contradiction
  · split at he
    · rename_i b hb
      split at he
      · rename_i hi
        cases he
        exact ⟨b, hb, rfl, by
          simp only [beforeReset, Bool.and_eq_true] at hi
          simpa using hi.1⟩
      · exact False.elim (schema_not_ignored v w cs e he)
    · exact False.elim (schema_not_ignored v w cs e he)

/-- A chosen snapshot's consumed history is causally closed (§6, §15).
This is a condition on authenticated snapshot inputs, not an upload receipt. -/
def SnapshotClosed (h : History) (v : View) (b : Boundary) : Prop :=
  ∀ w a, (h w).record.identity.id ∈ b.covers → a ∈ causes h v w →
    (h a).record.identity.id ∈ b.covers

theorem no_applied_ignored_dependency {h : History} {v : View} {w a e : Nat}
    (closed : ∀ b, v.reset = some b → SnapshotClosed h v b)
    (hw : Judged h v w .applied) (ha : a ∈ causes h v w)
    (he : Judged h v a (.ignored e)) :
    e ∈ (h w).record.identity.storeRead := by
  generalize hv : Verdict.ignored e = ignored at he
  cases he with
  | receive _ answers _ _ =>
    obtain ⟨b, hb, be, absent⟩ := ignored_origin v (h a) _ e hv.symm
    have outside : (h w).record.identity.id ∉ b.covers := by
      intro inside
      exact absent (closed b hb w a inside ha)
    generalize happ : Verdict.applied = result at hw
    cases hw with
    | receive _ answers' _ _ =>
      rw [← be]
      exact applied_outside_reset_adopted v (h w) _ b hb outside happ.symm

theorem no_applied_discarded_input {h : History} {v : View} {w a : Nat} {answer : Verdict}
    (closed : ∀ b, v.reset = some b → SnapshotClosed h v b)
    (hw : Judged h v w .applied) (ha : a ∈ causes h v w)
    (dep : Judged h v a answer) : usesDiscarded (h w).record answer = false := by
  cases answer with
  | applied | refused _ => rfl
  | excluded e =>
    simp [usesDiscarded, no_applied_excluded_dependency hw ha dep]
  | ignored e =>
    simp [usesDiscarded, no_applied_ignored_dependency closed hw ha dep]

/-- Every recorded read, including an implicit own predecessor, either had
already lost its effects at adoption or cannot carry a discarded input now. -/
theorem no_applied_discarded_read {h : History} {v : View} {w a : Nat} {answer : Verdict}
    (valid : Causal h) (closed : ∀ b, v.reset = some b → SnapshotClosed h v b)
    (hw : Judged h v w .applied)
    (read : reads (h w).record.identity (h a).record.identity = true)
    (dep : Judged h v a answer) :
    discardedBefore v (h w).record.identity (h a).record.identity = true ∨
      usesDiscarded (h w).record answer = false := by
  by_cases cut : discardedBefore v (h w).record.identity (h a).record.identity = true
  · exact Or.inl cut
  · apply Or.inr
    apply no_applied_discarded_input closed hw _ dep
    exact List.mem_filter.mpr ⟨List.mem_range.mpr (valid w a read), by simp [read, cut]⟩

theorem adopted_discard_not_input (h : History) (v : View) (w a : Nat)
    (b : Adoption) (hb : b ∈ v.adoptions) (read : b.entry ∈ (h w).record.identity.storeRead)
    (discarded : (h a).record.identity.id ∈ b.discarded) : a ∉ causes h v w := by
  intro ha
  have found : discardedBefore v (h w).record.identity (h a).record.identity = true := by
    apply List.any_eq_true.mpr
    exact ⟨b, hb, by simp [read, discarded]⟩
  have hm := (List.mem_filter.mp ha).2
  simp [found] at hm

/-- Executable evaluation used by the checked histories. Its recursion follows
strictly earlier timestamps, not a guessed fuel bound. -/
def evaluate (h : History) (v : View) (w : Nat) : Verdict :=
  classify v (h w) ((causes h v w).attach.map fun a => evaluate h v a.val)
termination_by w
decreasing_by exact cause_earlier a.property

theorem evaluate_eq (h : History) (v : View) (w : Nat) :
    evaluate h v w = classify v (h w) ((causes h v w).map (evaluate h v)) := by
  rw [evaluate]
  simp

theorem classify_not_refused (v : View) (w : CheckedWrite) (cs : List Verdict)
    (hw : w.refusal = none) (r : Refusal) : classify v w cs ≠ .refused r := by
  simp only [classify, hw, classify.schema, classify.inherited]
  split
  · split
    · simp
    · split
      · split
        · simp
        · split <;> simp
      · split <;> simp
  · split
    · split
      · simp
      · split <;> simp
    · split <;> simp

/-- Once an unrefused causal history is available, every write has an answer.
This constructs the certificates used by the agreement theorem. -/
theorem every_write_judged (h : History) (v : View)
    (good : ∀ w, (h w).refusal = none) (w : Nat) :
    Judged h v w (evaluate h v w) := by
  induction w using Nat.strongRecOn with
  | ind w ih =>
    rw [evaluate_eq]
    apply Judged.receive
    · intro a ha
      exact ih a (cause_earlier ha)
    · intro a _ r
      rw [evaluate_eq]
      exact classify_not_refused v (h a) _ (good a) r

/-- Build retained verdicts by evaluating the earlier adopted view. The list
is independent of whether the app later dismisses any frozen loss values. -/
def adoptionFrom (h : History) (v : View) (entry limit : Nat) : Adoption :=
  ⟨entry, (List.range limit).filterMap fun w => match evaluate h v w with
    | .excluded _ | .ignored _ => some (h w).record.identity.id
    | _ => none⟩

theorem retained_verdicts_justified (h : History) (v : View) (entry limit : Nat)
    (good : ∀ w, (h w).refusal = none) (id : WriteId)
    (hi : id ∈ (adoptionFrom h v entry limit).discarded) :
    ∃ w e, (h w).record.identity.id = id ∧
      (Judged h v w (.excluded e) ∨ Judged h v w (.ignored e)) := by
  obtain ⟨w, _, hw⟩ := List.mem_filterMap.mp hi
  have judged := every_write_judged h v good w
  cases he : evaluate h v w with
  | applied | refused _ => simp [he] at hw
  | excluded e =>
    simp only [he, Option.some.injEq] at hw
    exact ⟨w, e, hw, Or.inl (he ▸ judged)⟩
  | ignored e =>
    simp only [he, Option.some.injEq] at hw
    exact ⟨w, e, hw, Or.inr (he ▸ judged)⟩

/-- A materialized audience after reload. Snapshot identities begin passed;
eligible waiting and arriving writes are consumed by the same function. -/
structure Materialized where
  passed : List Nat
  applied : List Nat
  deriving DecidableEq, Repr

def Materialized.load (included : List Nat) : Materialized := ⟨included, included⟩

def Materialized.consume (s : Materialized) (w : Nat) (v : Verdict) : Materialized :=
  if w ∈ s.passed then s else
    match v with
    | .refused _ => s
    | .applied => ⟨w :: s.passed, w :: s.applied⟩
    | _ => { s with passed := w :: s.passed }

def Materialized.Valid (s : Materialized) : Prop :=
  s.applied.Nodup ∧ ∀ w ∈ s.applied, w ∈ s.passed

theorem load_valid (included : List Nat) (hn : included.Nodup) :
    (Materialized.load included).Valid := ⟨hn, fun _ h => h⟩

theorem consume_valid (s : Materialized) (w : Nat) (v : Verdict) (h : s.Valid) :
    (s.consume w v).Valid := by
  unfold Materialized.consume
  split
  · exact h
  · rename_i hn
    cases v with
    | refused r => exact h
    | applied =>
      constructor
      · exact List.nodup_cons.mpr ⟨fun hw => hn (h.2 w hw), h.1⟩
      · intro a ha
        rcases List.mem_cons.mp ha with rfl | ha
        · exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ (h.2 a ha)
    | excluded e | ignored e =>
      exact ⟨h.1, fun a ha => List.mem_cons_of_mem _ (h.2 a ha)⟩

theorem consume_idempotent (s : Materialized) (w : Nat) (v : Verdict) :
    (s.consume w v).consume w v = s.consume w v := by
  unfold Materialized.consume
  split
  · simp_all
  · cases v <;> simp_all

theorem covered_not_applied_again (included : List Nat) (w : Nat) (hw : w ∈ included)
    (v : Verdict) : (Materialized.load included).consume w v = .load included := by
  simp [Materialized.consume, Materialized.load, hw]

theorem nothing_applied_twice (s : Materialized) (h : s.Valid) (arrivals : List (Nat × Verdict)) :
    (arrivals.foldl (fun s w => s.consume w.1 w.2) s).applied.Nodup := by
  have hv : (arrivals.foldl (fun s w => s.consume w.1 w.2) s).Valid := by
    induction arrivals generalizing s with
    | nil => exact h
    | cons x xs ih => exact ih _ (consume_valid _ _ _ h)
  exact hv.1

inductive AudienceState where
  | ready (adopted : View) (data : Materialized)
  | reloading (target : View) (previous : Materialized)
  deriving DecidableEq, Repr

inductive CommitResult where
  | accepted (view : View)
  | audienceReloading
  deriving DecidableEq, Repr

def commitInto : AudienceState → CommitResult
  | .ready v _ => .accepted v
  | .reloading _ _ => .audienceReloading

/-- All audiences touched by the commit are supplied by its row records. -/
def AudiencesReady (audiences : List AudienceState) : Prop :=
  ∀ audience ∈ audiences, ∃ view, commitInto audience = .accepted view

theorem reloading_blocks_commit (audiences : List AudienceState) (v : View)
    (old : Materialized) (h : AudiencesReady audiences) :
    .reloading v old ∉ audiences := by
  intro member
  obtain ⟨view, ready⟩ := h _ member
  cases ready

/-- Download or rebuilding failure retains the previous state. Only successful
atomic replacement changes which boundary a new write may claim (§15, E3). -/
def finishReload (s : AudienceState) (loaded : Option Materialized) : AudienceState :=
  match s, loaded with
  | .reloading v _, some data => .ready v data
  | _, _ => s

theorem reloading_refused (v : View) (old : Materialized) :
    commitInto (.reloading v old) = .audienceReloading := rfl

theorem failed_reload_unchanged (s : AudienceState) : finishReload s none = s := by
  cases s <;> rfl

end CovenQueue
