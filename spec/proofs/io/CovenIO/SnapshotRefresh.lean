import CovenIO.Reload

namespace CovenIO.SnapshotRefresh

/-- Encoded positions name each writer once. This equality states precisely
what the count order needs, including zero positions for omitted writers. -/
def Normalized (writers : List Nat) (s : Snapshot) : Prop :=
  s.writeCount = (writers.map s.position).sum

theorem sum_positions_le (writers : List Nat) (a b : Nat → Nat)
    (le : ∀ w ∈ writers, a w ≤ b w) : (writers.map a).sum ≤ (writers.map b).sum := by
  induction writers with
  | nil => simp
  | cons w ws ih =>
      have head := le w (by simp)
      have tail := ih (fun v hv => le v (by simp [hv]))
      simp only [List.map_cons, List.sum_cons]
      omega

theorem sum_positions_lt (writers : List Nat) (a b : Nat → Nat) (writer : Nat)
    (member : writer ∈ writers) (le : ∀ w ∈ writers, a w ≤ b w)
    (strict : a writer < b writer) : (writers.map a).sum < (writers.map b).sum := by
  induction writers with
  | nil => simp at member
  | cons w ws ih =>
      have head := le w (by simp)
      have tailLe := sum_positions_le ws a b (fun v hv => le v (by simp [hv]))
      simp only [List.map_cons, List.sum_cons]
      rcases List.mem_cons.mp member with rfl | tail
      · omega
      · have := ih tail (fun v hv => le v (by simp [hv])); omega

theorem newest_rank (order : SnapshotOrder) (ss : List Snapshot) (chosen : Snapshot)
    (selected : newest order ss = some chosen) (s : Snapshot) (member : s ∈ ss) :
    s.rank order ≤ chosen.rank order := by
  induction ss generalizing chosen with
  | nil => simp at member
  | cons head tail ih =>
      simp only [newest] at selected
      cases h : newest order tail with
      | none =>
          simp only [h] at selected
          cases selected
          rcases List.mem_cons.mp member with rfl | ht
          · exact Nat.le_refl _
          · have empty : tail = [] := by
              cases tail with
              | nil => rfl
              | cons x xs => simp only [newest] at h; split at h <;> contradiction
            simp [empty] at ht
      | some other =>
          simp only [h] at selected
          split at selected
          · rename_i wins
            have rank : other.rank order ≤ head.rank order := by
              simp only [newer, decide_eq_true_eq] at wins
              omega
            cases selected
            rcases List.mem_cons.mp member with rfl | ht
            · exact Nat.le_refl _
            · exact Nat.le_trans (ih _ h ht) rank
          · rename_i loses
            have rank : head.rank order ≤ other.rank order := by
              simp only [newer, decide_eq_true_eq] at loses
              omega
            cases selected
            rcases List.mem_cons.mp member with rfl | ht
            · exact rank
            · exact ih _ h ht

theorem current_rank (order : SnapshotOrder) (ss : List Snapshot) (a : Nat) (chosen s : Snapshot)
    (selected : catalogCurrent order ss a = some chosen)
    (member : s ∈ ss) (audience : s.audience = a) (counts : s.counts = true) :
    s.rank order ≤ chosen.rank order :=
  newest_rank order _ chosen selected s (List.mem_filter.mpr ⟨member, by simp [audience, counts]⟩)

theorem current_exists (order : SnapshotOrder) (ss : List Snapshot) (a : Nat) (s : Snapshot)
    (member : s ∈ ss) (audience : s.audience = a) (counts : s.counts = true) :
    ∃ chosen, catalogCurrent order ss a = some chosen := by
  have hm : s ∈ ss.filter (fun v => v.audience == a && v.counts) := by simp [member, audience, counts]
  generalize he : ss.filter (fun v => v.audience == a && v.counts) = entries at hm ⊢
  cases entries with
  | nil => simp at hm
  | cons v vs =>
      unfold catalogCurrent
      rw [he]
      simp only [newest]
      cases newest order vs <;> simp

/-- Once the historical witness is mature, every subsequently landing
counting snapshot of a part audience contains the write, in every order. -/
theorem fresh_covers (published : Write → Prop) (w : Write) (pub : published w)
    (snap : Snapshot) (sound : SnapshotHistory.Sound published snap)
    (audience : snap.audience ∈ w.audiences) (counts : snap.counts = true)
    (bound ready : Nat) (old : w.storedAt ≤ bound) (mature : bound + day < ready)
    (later : ready ≤ snap.storedAt) : snap.covers w snap.audience := by
  have admitted : snap.storedAt ≤ snap.caughtAt + day := by simpa [Snapshot.counts] using counts
  exact sound.caught w pub audience (by omega)

/-- A refresh under a) beats a selected snapshot missing the old write.
For count order this uses real additional coverage, not time as a tie-break. -/
theorem refresh_outranks (order : SnapshotOrder) (writers : List Nat) (published : Write → Prop)
    (w : Write) (pub : published w) (writer : w.writer ∈ writers)
    (old fresh : Snapshot) (ha : old.audience ∈ w.audiences)
    (sound : SnapshotHistory.Sound published old) (counts : old.counts = true)
    (oldForm : Normalized writers old) (freshForm : Normalized writers fresh)
    (dominates : Dominates fresh old) (missing : ¬ old.covers w old.audience)
    (covers : fresh.covers w fresh.audience) (bound ready : Nat)
    (stored : w.storedAt ≤ bound) (mature : bound + day < ready)
    (caught : ready ≤ fresh.caughtAt) (clock : fresh.caughtAt ≤ fresh.storedAt) :
    old.rank order < fresh.rank order := by
  have absent : old.position w.writer < w.number := by
    simp only [Snapshot.covers, true_and] at missing
    omega
  have oldTime : old.caughtAt ≤ w.storedAt := by
    apply Nat.le_of_not_gt
    intro before
    exact missing (sound.caught w pub ha before)
  have oldStored : old.storedAt ≤ old.caughtAt + day := by simpa [Snapshot.counts] using counts
  cases order with
  | writes =>
      change old.writeCount < fresh.writeCount
      rw [oldForm, freshForm]
      exact sum_positions_lt writers old.position fresh.position w.writer writer
        (fun v _ => dominates.2 v) (by have := covers.2; omega)
  | catchup => change old.caughtAt < fresh.caughtAt; omega
  | storage => change old.storedAt < fresh.storedAt; omega

/-- The selection after a refresh includes either a later covering snapshot
or the refresh itself; an older uncovered contender cannot win. -/
theorem refresh_selected (order : SnapshotOrder) (old added : List Snapshot) (a : Nat)
    (previous fresh : Snapshot) (w : Write)
    (selected : catalogCurrent order old a = some previous)
    (freshMember : fresh ∈ added) (freshAudience : fresh.audience = a) (freshCounts : fresh.counts = true)
    (newCoverage : ∀ s ∈ added, s.audience = a → s.counts = true → s.covers w a)
    (wins : previous.rank order < fresh.rank order) :
    ∃ chosen, catalogCurrent order (added ++ old) a = some chosen ∧ chosen.covers w a := by
  obtain ⟨chosen, hc⟩ := current_exists order (added ++ old) a fresh
    (List.mem_append_left _ freshMember) freshAudience freshCounts
  refine ⟨chosen, hc, ?_⟩
  obtain ⟨member, ha, counted⟩ := current_member order (added ++ old) a chosen hc
  rcases List.mem_append.mp member with newer | older
  · exact newCoverage chosen newer ha counted
  · have le := current_rank order old a previous chosen selected older ha counted
    have ge := current_rank order (added ++ old) a chosen fresh hc
      (List.mem_append_left _ freshMember) freshAudience freshCounts
    omega

theorem covered_selection_preserved (order : SnapshotOrder) (old added : List Snapshot) (a : Nat)
    (w : Write) (covered : ∃ s, catalogCurrent order old a = some s ∧ s.covers w a)
    (newCoverage : ∀ s ∈ added, s.audience = a → s.counts = true → s.covers w a) :
    ∃ s, catalogCurrent order (added ++ old) a = some s ∧ s.covers w a := by
  let single : Write := { w with audiences := [a] }
  have current : Covered (catalogCurrent order old) single := by
    intro b hb
    have eq : b = a := by simpa [single] using hb
    subst b
    obtain ⟨s, hs, cov⟩ := covered
    simpa [hs, single, Snapshot.covers] using cov
  have preserved : Covered (catalogCurrent order (added ++ old)) single := by
    induction added with
    | nil => exact current
    | cons head tail ih =>
        apply add_preserves_coverage order (tail ++ old) head single
          (ih (by intro s hs; exact newCoverage s (by simp [hs])))
        intro counted ha
        have eq : head.audience = a := by simpa [single] using ha
        simpa [single, Snapshot.covers, eq] using newCoverage head (by simp) eq counted
  have result := preserved a (by simp [single])
  cases hs : catalogCurrent order (added ++ old) a with
  | none => simp [hs] at result
  | some s => exact ⟨s, rfl, by simpa [hs, single, Snapshot.covers] using result⟩

end CovenIO.SnapshotRefresh
