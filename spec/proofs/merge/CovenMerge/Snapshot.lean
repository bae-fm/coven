import CovenMerge.Migration

/-! Loading a boundary snapshot (§15, §17.1, §19.3). Its consumed positions
and effective inputs are distinct. A caller supplies the eligibility test;
reset and schema processing compose their tests before using this merge. -/

namespace CovenMerge

/-- Covered positions include ignored/excluded writes (format D7). `inputs`
names only the effective writes represented by the merge snapshot. -/
structure SnapshotBoundary (W : Type) where
  covered : W → Bool
  inputs : W → Bool
  inputs_covered : ∀ w, inputs w = true → covered w = true

section
variable {W Row Col : Type} [DecidableEq W]

def snapshotRetains (B : SnapshotBoundary W) (eligible : W → Bool) (w : W) : Bool :=
  B.inputs w || (!B.covered w && eligible w)

def afterSnapshot (B : SnapshotBoundary W) (eligible : W → Bool) (w : W) : Bool :=
  !B.covered w && eligible w

/-- Remove discarded parts and their input edges. The transport still consumes
their identities; only the row merge uses these effective records (§7.1, §19.3). -/
def snapshotWrites (M : Writes W Row Col) (B : SnapshotBoundary W)
    (eligible : W → Bool) : Writes W Row Col :=
  { M with
    chg := fun w r => if snapshotRetains B eligible w then M.chg w r else none
    past := fun w a => M.past w a && snapshotRetains B eligible a }

/-- Authoring after a boundary reload uses generations in the selected snapshot or
eligible history, not generations from discarded local rows. This is the
generation witness the ordinary merge already requires, restricted to inputs
that this audience retains. -/
def SnapshotGenerations (M : Writes W Row Col) (B : SnapshotBoundary W)
    (eligible : W → Bool) : Prop :=
  ∀ w r ch, snapshotRetains B eligible w = true → M.chg w r = some ch →
    ch.gen = 0 ∨ ∃ x ch', M.past w x = true ∧ snapshotRetains B eligible x = true ∧
      M.chg x r = some ch' ∧ ch'.kind ≠ .upd ∧ ch'.gen + 1 = ch.gen

theorem snapshot_valid {M : Writes W Row Col} (valid : Valid M)
    (B : SnapshotBoundary W) (eligible : W → Bool) (generations : SnapshotGenerations M B eligible) :
    Valid (snapshotWrites M B eligible) where
  ts_inj := valid.ts_inj
  past_ts w a h := by
    simp only [snapshotWrites, Bool.and_eq_true] at h
    exact valid.past_ts w a h.1
  gen_seen w r ch h := by
    have hw : snapshotRetains B eligible w = true := by
      cases he : snapshotRetains B eligible w <;> simp_all [snapshotWrites]
    have hc : M.chg w r = some ch := by simpa [snapshotWrites, hw] using h
    rcases generations w r ch hw hc with h0 | ⟨x, ch', hp, hx, hc', hk, hg⟩
    · exact Or.inl h0
    · exact Or.inr ⟨x, ch', by simp [snapshotWrites, hp, hx],
        by simp [snapshotWrites, hx, hc'], hk, hg⟩
  parity w r ch h := by
    by_cases hw : snapshotRetains B eligible w = true
    · exact valid.parity w r ch (by simpa [snapshotWrites, hw] using h)
    · simp [snapshotWrites, hw] at h

/-- The old log order can include discarded writes. Filtering it yields a causal
order of the effective changes; ignored dependencies do not block it. -/
theorem snapshot_causal {M : Writes W Row Col} (B : SnapshotBoundary W)
    (eligible : W → Bool) {L : List W}
    (h : CausalFrom M (fun w => B.covered w = true) L) :
    CausalFrom (snapshotWrites M B eligible) (fun w => B.inputs w = true)
      (L.filter (afterSnapshot B eligible)) := by
  induction h with
  | nil => exact .nil
  | @snoc L w _ uncovered fresh past ih =>
    by_cases hw : afterSnapshot B eligible w = true
    · simp only [List.filter_append, List.filter_cons, hw, List.filter_nil, ↓reduceIte]
      refine CausalFrom.snoc ih (fun hi => uncovered (B.inputs_covered w hi))
        (fun hm => fresh (List.mem_filter.mp hm).1) ?_
      intro a ha
      simp only [snapshotWrites, Bool.and_eq_true] at ha
      obtain ⟨hp, retained⟩ := ha
      rcases past a hp with covered | mem
      · exact Or.inl (by simpa [snapshotRetains, covered] using retained)
      · by_cases covered : B.covered a = true
        · exact Or.inl (by simpa [snapshotRetains, covered] using retained)
        · right
          refine List.mem_filter.mpr ⟨mem, ?_⟩
          have notInput : B.inputs a = false := by
            cases hi : B.inputs a
            · rfl
            · exact (covered (B.inputs_covered a hi)).elim
          simp [snapshotRetains, covered, notInput] at retained
          simp [afterSnapshot, covered, retained]
    · simpa [List.filter_append, hw] using ih

def reloadSnapshot (M : Writes W Row Col) (B : SnapshotBoundary W) (eligible : W → Bool)
    (snapshot : St W Row Col) (received : List W) : St W Row Col :=
  (received.filter (afterSnapshot B eligible)).foldl (step (snapshotWrites M B eligible)) snapshot

/-- Exact state equality includes generations, cells and all merge losses. -/
theorem snapshot_exact (M : Writes W Row Col) (B : SnapshotBoundary W) (eligible : W → Bool)
    (snapshot : St W Row Col) (received : List W) :
    reloadSnapshot M B eligible snapshot received =
      (received.filter (fun w => !B.covered w && eligible w)).foldl
        (step (snapshotWrites M B eligible)) snapshot := rfl

theorem snapshot_ignored_no_effect (M : Writes W Row Col) (B : SnapshotBoundary W)
    (eligible : W → Bool) (snapshot : St W Row Col) (received : List W) (w : W)
    (unread : eligible w = false) :
    reloadSnapshot M B eligible snapshot (w :: received) = reloadSnapshot M B eligible snapshot received := by
  simp [reloadSnapshot, afterSnapshot, unread]

theorem snapshot_covered_not_reapplied (M : Writes W Row Col) (B : SnapshotBoundary W)
    (eligible : W → Bool) (snapshot : St W Row Col) (received : List W) (w : W)
    (covered : B.covered w = true) :
    reloadSnapshot M B eligible snapshot (w :: received) = reloadSnapshot M B eligible snapshot received := by
  simp [reloadSnapshot, afterSnapshot, covered]

theorem snapshot_ignores_old_dependency (M : Writes W Row Col) (B : SnapshotBoundary W)
    (eligible : W → Bool) (w a : W)
    (uncovered : B.covered w = false) (adopted : eligible w = true)
    (old : snapshotRetains B eligible a = false) :
    afterSnapshot B eligible w = true ∧ (snapshotWrites M B eligible).past w a = false := by
  simp [afterSnapshot, uncovered, adopted, snapshotWrites, old]

theorem snapshot_inputs_converge {M : Writes W Row Col} (valid : Valid M)
    (B : SnapshotBoundary W) (eligible : W → Bool) (generations : SnapshotGenerations M B eligible)
    {snapshot : St W Row Col}
    (closed : Closed (snapshotWrites M B eligible) (fun w => B.inputs w = true))
    (correct : IsSpec (snapshotWrites M B eligible) (fun w => B.inputs w = true) snapshot)
    {a b : List W} (ca : CausalFrom M (fun w => B.covered w = true) a)
    (cb : CausalFrom M (fun w => B.covered w = true) b)
    (same : ∀ w, afterSnapshot B eligible w = true → (w ∈ a ↔ w ∈ b)) :
    reloadSnapshot M B eligible snapshot a = reloadSnapshot M B eligible snapshot b := by
  apply isSpec_unique (foldl_isSpec (snapshot_valid valid B eligible generations) closed correct
    (snapshot_causal B eligible ca)).1
  apply isSpec_congr _ (foldl_isSpec (snapshot_valid valid B eligible generations) closed correct
    (snapshot_causal B eligible cb)).1
  intro w
  simp only [List.mem_filter]
  by_cases hp : afterSnapshot B eligible w = true
  · rw [same w hp]
  · simp [hp]

end
end CovenMerge
