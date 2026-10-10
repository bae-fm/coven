import CovenMerge.Reset

/-! Value accounting (§3, §8, §17.1, §19.3). A value is identified by its
setter and cell. These results quantify over every such value, not only the
currently displayed winner. -/

namespace CovenMerge

section
variable {W Row Col K : Type} [DecidableEq W] [DecidableEq Row] [DecidableEq Col]
  [DecidableEq K] {M : Writes W Row Col} {S : W → Prop} {st : St W Row Col}

/-- The four permitted outcomes for a value in the effective merge. -/
def ValueAccounted (M : Writes W Row Col) (S : W → Prop) (st : St W Row Col)
    (I : EntryInputs Row K) (r : Row) (c : Col) (a : W) (k : Nat) : Prop :=
    (st.cell r c = some a ∧ (entryView I).shown r = true) ∨
    (∃ w, Replacer M S r c a k w ∧ M.past w a = true) ∨
    (∃ w, st.lost r c a = some (k, w)) ∨
    (st.cell r c = some a ∧ (entryView I).removed r = true ∧
      (entryView I).rules r ≠ [] ∧
      ∃ record, lossRecord st (entryView I) r none = some record ∧ record.values c = some a)

/-- Every effective value is displayed, knowingly replaced, recorded as a
lost cell, or held in a removed row with a rule that still holds. -/
theorem value_accounted (valid : Valid M) (closed : Closed M S) (correct : IsSpec M S st)
    (I : EntryInputs Row K) (present : ∀ r, I.present r = decide (st.gen r % 2 = 1))
    {r : Row} {c : Col} {a : W} {k : Nat} (setter : Setter M S r c a k) :
    ValueAccounted M S st I r c a k := by
  classical
  unfold ValueAccounted
  by_cases replaced : ∃ w, Replacer M S r c a k w
  · by_cases read : ∃ w, Replacer M S r c a k w ∧ M.past w a = true
    · exact Or.inr (Or.inl read)
    · obtain ⟨w, canon⟩ := canon_exists valid closed correct setter
      have unread : ∀ y, Replacer M S r c a k y → M.past y a = false := by
        intro y hy
        cases hp : M.past y a
        · rfl
        · exact (read ⟨y, hy, hp⟩).elim
      exact Or.inr (Or.inr (Or.inl ⟨w,
        (correct.lost r c a k w).mpr ⟨setter, replaced, unread, canon⟩⟩))
  · obtain ⟨gen, cell⟩ := unreplaced valid closed correct setter replaced
    have hp : I.present r = true := by
      rw [present, ← gen]
      simp [setter_inc_odd valid setter]
    cases hd : removal I.erase r
    · exact Or.inl ⟨cell, by
        change (I.present r && !removal I.erase r) = true
        simp [hp, hd]⟩
    · have removed : (entryView I).removed r = true := by
        change (I.present r && removal I.erase r) = true
        simp [hp, hd]
      refine Or.inr (Or.inr (Or.inr ⟨cell, removed, named_rules_nonempty I removed, ?_⟩))
      exact ⟨⟨r, st.gen r, none, st.cell r, .rules ((entryView I).rules r)⟩,
        by simp [lossRecord, removed], cell⟩

/-- A migration deletion cannot erase a captured hidden value, including one
that an ordinary delete would omit because it had read its setter. -/
theorem hidden_value_frozen (I : EntryInputs Row K) (final : Nat → Bool)
    (migration : W) (version : Nat) {r : Row} {c : Col} {a : W}
    (hidden : migrationRemoves I final r = true) (cell : st.cell r c = some a) :
    ∃ loss, captureHidden st I final migration version r = some loss ∧
      loss.record.values c = some a ∧ loss.record.column = none ∧
      loss.record.cause = .schemaChange version migration := by
  refine ⟨freeze ⟨r, st.gen r, none, st.cell r, .schemaChange version migration⟩,
    ?_, cell, rfl, rfl⟩
  simp [captureHidden, hidden]

end

/-! Schema exclusions and the two explicit exceptions. Reset eligibility is
tested before schema exclusion, so a reset-ignored part never gains a loss.
The schema verdict is supplied by version/conversion processing (§17.1). -/

inductive SchemaDisposition where
  | eligible
  | excluded (version : Nat)
  | losingMigration
  deriving DecidableEq, Repr

inductive BoundaryDisposition where
  | snapshot
  | apply
  | schemaLoss (version : Nat)
  | resetIgnored
  | losingMigration
  deriving DecidableEq, Repr

/-- With no kept reset there is no reset eligibility restriction. -/
def resetAllows {W : Type} (reset : Option Nat) (read : W → Nat → Bool) (w : W) : Bool :=
  reset.all (fun entry => read w entry)

def disposition {W : Type} (B : SnapshotBoundary W) (reset : Option Nat) (read : W → Nat → Bool)
    (schema : W → SchemaDisposition) (w : W) : BoundaryDisposition :=
  if B.covered w then .snapshot
  else if !resetAllows reset read w then .resetIgnored
  else match schema w with
    | .eligible => .apply
    | .excluded v => .schemaLoss v
    | .losingMigration => .losingMigration

theorem no_reset_cannot_ignore {W : Type} (B : SnapshotBoundary W)
    (read : W → Nat → Bool) (schema : W → SchemaDisposition) (w : W) :
    disposition B none read schema w ≠ .resetIgnored := by
  cases hc : B.covered w <;> cases hs : schema w <;> simp [disposition, resetAllows, hc, hs]

/-- Row values are the write's own setters. Even an excluded deletion with
no values retains its identity in the schema cause (format D7). -/
def excludedLoss {W Row Col : Type} (w : W) (r : Row) (generation : Nat)
    (values : Col → Option W) (version : Nat) : FrozenLoss W Row Col :=
  freeze ⟨r, generation, none, values, .schemaChange version w⟩

def boundaryLoss {W Row Col : Type} (B : SnapshotBoundary W) (reset : Option Nat) (read : W → Nat → Bool)
    (schema : W → SchemaDisposition) (w : W) (r : Row) (generation : Nat)
    (values : Col → Option W) : Option (FrozenLoss W Row Col) :=
  match disposition B reset read schema w with
  | .schemaLoss v => some (excludedLoss w r generation values v)
  | _ => none

theorem reset_ignored_never_schema_loss {W Row Col : Type}
    (B : SnapshotBoundary W) (reset : Option Nat) (read : W → Nat → Bool) (schema : W → SchemaDisposition)
    (w : W) (r : Row) (g : Nat) (values : Col → Option W)
    (outside : B.covered w = false) (unread : resetAllows reset read w = false) :
    disposition B reset read schema w = .resetIgnored ∧
      boundaryLoss B reset read schema w r g values = none := by
  simp [disposition, boundaryLoss, outside, unread]

/-- Classification cannot silently discard an ordinary admitted value. It
either goes into merge/snapshot accounting, has a frozen schema loss, or is
one of precisely the two stated exceptions. -/
theorem boundary_accounted {W Row Col : Type}
    (B : SnapshotBoundary W) (reset : Option Nat) (read : W → Nat → Bool) (schema : W → SchemaDisposition)
    (w : W) (r : Row) (g : Nat) (values : Col → Option W) :
    disposition B reset read schema w = .snapshot ∨ disposition B reset read schema w = .apply ∨
    (∃ loss, boundaryLoss B reset read schema w r g values = some loss ∧ loss.record.values = values) ∨
    (B.covered w = false ∧ resetAllows reset read w = false) ∨ schema w = .losingMigration := by
  cases covered : B.covered w
  · cases readEntry : resetAllows reset read w
    · exact Or.inr (Or.inr (Or.inr (Or.inl ⟨rfl, rfl⟩)))
    · cases hs : schema w with
      | eligible => exact Or.inr (Or.inl (by simp [disposition, covered, readEntry, hs]))
      | excluded v =>
        exact Or.inr (Or.inr (Or.inl ⟨excludedLoss w r g values v,
          by simp [boundaryLoss, disposition, covered, readEntry, hs], rfl⟩))
      | losingMigration => exact Or.inr (Or.inr (Or.inr (Or.inr rfl)))
  · exact Or.inl (by simp [disposition, covered])

end CovenMerge
