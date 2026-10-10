import CovenStorelogData.Pending

/-! §9, §15, §16.5. Retention decisions carry the entries they depend on.
A condition says when an original is required; a covered replacement is a
separate check. Finality must cover every dependency, not just the last entry. -/
namespace CovenStorelogData.RetentionSafety

/-- A finite decision about entry fate. This covers alternatives as well as
joint causes: a dropped boundary can return, or a deletion can disappear. -/
inductive Need where
  | always | never | kept (entry : Nat)
  | not (condition : Need) | both (left right : Need) | either (left right : Need)
  deriving DecidableEq, Repr

def Need.entries : Need → List Nat
  | .always | .never => []
  | .kept e => [e]
  | .not p => p.entries
  | .both p q | .either p q => p.entries ++ q.entries

def Need.eval (kept : Nat → Bool) : Need → Bool
  | .always => true
  | .never => false
  | .kept e => kept e
  | .not p => !p.eval kept
  | .both p q => p.eval kept && q.eval kept
  | .either p q => p.eval kept || q.eval kept

theorem dependency_agreement (need : Need) (before after : Nat → Bool)
    (same : ∀ e ∈ need.entries, before e = after e) :
    need.eval before = need.eval after := by
  induction need with
  | always | never => rfl
  | kept e => exact same e List.mem_cons_self
  | not p ih => exact congrArg Bool.not (ih same)
  | both p q ihp ihq | either p q ihp ihq =>
      have hp := ihp (fun e he => same e (List.mem_append_left _ he))
      have hq := ihq (fun e he => same e (List.mem_append_right _ he))
      simp only [Need.eval, hp, hq]

inductive Kind where
  | deviceLog | snapshot | row | mergeRecord | loss | fileReference
  | localSource | localChild | boundary | preMigration | pendingObservation
  deriving DecidableEq, Repr

structure Input where
  id : Nat
  kind : Kind
  needed : Need
  /-- Snapshot coverage, ownership, age/posted positions and authenticated
  absence of references must all succeed. No device clock appears here. -/
  checks : List CovenStorelogData.Pending.Condition
  deriving DecidableEq, Repr

/-- The first non-final dependency is observable maintenance work (E5). -/
def conditions (final : Nat → Bool) (input : Input) : List CovenStorelogData.Pending.Condition :=
  (input.needed.entries.map fun e => ⟨final e, .waits (.entryFinality e)⟩) ++ input.checks

def work (path : String) (reporter : Nat) (final : Nat → Bool) (input : Input) :
    CovenStorelogData.Pending.Work :=
  ⟨.retention path, reporter, conditions final input⟩

theorem first_finality_wait_visible (path : String) (reporter : Nat)
    (final : Nat → Bool) (input : Input) (before after : List Nat) (entry : Nat)
    (dependencies : input.needed.entries = before ++ entry :: after)
    (earlier : ∀ e ∈ before, final e = true) (waiting : final entry = false) :
    ⟨.retention path, reporter, .waits (.entryFinality entry)⟩ ∈
      CovenStorelogData.Pending.records [work path reporter final input] := by
  apply CovenStorelogData.Pending.every_pending_subject _ _ _ List.mem_cons_self
  simp only [work, conditions, dependencies, List.map_append, List.map_cons,
    List.append_assoc, List.cons_append]
  apply CovenStorelogData.Pending.first_unmet
  · intro condition hc
    obtain ⟨e, he, rfl⟩ := List.mem_map.mp hc
    exact earlier e he
  · exact waiting

def mayDelete (kept final : Nat → Bool) (input : Input) : Bool :=
  !input.needed.eval kept && (conditions final input).all (·.met)

def retain (kept final : Nat → Bool) (inputs : List Input) : List Input :=
  inputs.filter (fun i => !mayDelete kept final i)

theorem deletion_requires_final (kept final : Nat → Bool) (input : Input)
    (delete : mayDelete kept final input = true) :
    input.needed.eval kept = false ∧ ∀ e ∈ input.needed.entries, final e = true := by
  have h : input.needed.eval kept = false ∧
      (conditions final input).all (·.met) = true := by
    simpa only [mayDelete, Bool.and_eq_true, Bool.not_eq_true'] using delete
  refine ⟨h.1, ?_⟩
  intro e he
  exact List.all_eq_true.mp h.2 ⟨final e, .waits (.entryFinality e)⟩
    (List.mem_append_left _ (List.mem_map.mpr ⟨e, he, rfl⟩))

/-- The finality premise: later replay cannot change any certified entry.
This module does not establish it from storage time; no physical deletion
assumes that current kept entries alone remain kept. -/
def Stable (before after final : Nat → Bool) : Prop :=
  ∀ e, final e = true → before e = after e

theorem deleted_input_never_needed (before after final : Nat → Bool)
    (stable : Stable before after final) (input : Input)
    (delete : mayDelete before final input = true) : input.needed.eval after = false := by
  have h := deletion_requires_final before final input delete
  rw [← dependency_agreement input.needed before after (fun e he => stable e (h.2 e he))]
  exact h.1

theorem retention_preserves_possible_replay (before after final : Nat → Bool)
    (stable : Stable before after final) (inputs : List Input) (input : Input)
    (present : input ∈ inputs) (needed : input.needed.eval after = true) :
    input ∈ retain before final inputs := by
  apply List.mem_filter.mpr
  refine ⟨present, ?_⟩
  cases hd : mayDelete before final input
  · rfl
  · have := deleted_input_never_needed before after final stable input hd
    simp [needed] at this

/-- Once dependencies are final and the other checks pass, precisely the
currently required originals remain. This gives a bound without an absent
reader's acknowledgement; storage age can satisfy the §15 alternative. -/
theorem bounded_after_finality (kept final : Nat → Bool) (inputs : List Input)
    (ready : ∀ input ∈ inputs, (conditions final input).all (·.met) = true) :
    retain kept final inputs = inputs.filter (fun input => input.needed.eval kept) := by
  apply List.filter_congr
  intro input hi
  simp [mayDelete, ready input hi]

theorem retained_count_bound (kept final : Nat → Bool) (inputs : List Input)
    (ready : ∀ input ∈ inputs, (conditions final input).all (·.met) = true) :
    (retain kept final inputs).length ≤ inputs.countP (fun input => input.needed.eval kept) := by
  rw [bounded_after_finality kept final inputs ready, List.countP_eq_length_filter]
  exact Nat.le_refl _

/-- All references protect files, including frozen losses and inputs retained
for possible replay. Unreadable retained data prevents an absence proof. -/
structure References where
  rows : List Nat
  losses : List Nat
  snapshots : List Nat
  logs : List Nat
  localRows : List Nat
  waitingWrites : List Nat
  uploadQueues : List Nat
  reversible : List Nat
  deriving DecidableEq, Repr

def References.all (refs : References) : List Nat :=
  refs.rows ++ refs.losses ++ refs.snapshots ++ refs.logs ++ refs.localRows ++
    refs.waitingWrites ++ refs.uploadQueues ++ refs.reversible

def fileDeletable (file : Nat) (references : Option References) : Bool :=
  match references with
  | none => false
  | some refs => file ∉ refs.all

theorem loss_protects_file (file : Nat) (refs : References) (h : file ∈ refs.losses) :
    fileDeletable file (some refs) = false := by
  simp [fileDeletable, References.all, h]

theorem reversible_input_protects_file (file : Nat) (refs : References)
    (h : file ∈ refs.reversible) : fileDeletable file (some refs) = false := by
  simp [fileDeletable, References.all, h]

theorem unreadable_prevents_deletion (file : Nat) : fileDeletable file none = false := rfl

/-- §15's alternative waits for posts or 30 storage days, never a local clock. -/
def agedOrPosted (stored observed window : Nat) (allPosted : Bool) : Bool :=
  allPosted || stored + window ≤ observed

theorem absent_device_does_not_prevent_age (stored observed window : Nat)
    (aged : stored + window ≤ observed) : agedOrPosted stored observed window false = true := by
  simp [agedOrPosted, aged]

end CovenStorelogData.RetentionSafety
