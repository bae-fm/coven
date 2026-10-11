import CovenQueue.Operations

namespace CovenQueue.Operations

structure Run (codec : Codec) (kept : Kept) (id : Nat) (d : Data) where
  state : Nat → State
  start : state 0 = initial id d
  step : ∀ t, Step codec kept (state t) (state (t + 1))

theorem Run.reachable {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    (run : Run codec kept id d) (t : Nat) : Reachable codec kept id d (run.state t) := by
  induction t with
  | zero => rw [run.start]; exact .initial
  | succ t ih => exact .step ih (run.step t)

theorem Run.monotone {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    (run : Run codec kept id d) {t u : Nat} (h : t ≤ u) :
    (run.state t).journal.position codec ≤ (run.state u).journal.position codec := by
  induction h with
  | refl => exact Nat.le_refl _
  | @step u _ ih => exact Nat.le_trans ih (step_monotone (run.step u))

theorem Run.finished_stays {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    (run : Run codec kept id d) {t u : Nat} (h : t ≤ u)
    (done : (run.state t).journal.row = none) : (run.state u).journal.row = none := by
  induction h with
  | refl => exact done
  | @step u _ ih => exact deleted_row_stays_deleted (run.step u) ih

/-- Environmental assumptions for a single publication attempt, after the
last crash. Local checks and transactions eventually succeed, storage accepts
the fixed objects, comparison/checkpoint transactions eventually commit, and
the final verification or kept-entry check eventually completes. A dropped
rotation is not covered: §18 restarts it with a new entry and may block it. -/
structure Fair {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    (run : Run codec kept id d) (after : Nat) : Prop where
  localCommit : ∀ t r, after ≤ t → (run.state t).journal = .pending r → r.last_step = 0 →
    ∃ u, t ≤ u ∧ (run.state t).journal.position codec < (run.state u).journal.position codec
  storageAccepts : ∀ t r n o, after ≤ t → (run.state t).journal = .pending r →
    r.last_step = n + 1 → (r.data.objects codec)[n]? = some o →
    ∃ u, t ≤ u ∧ (run.state u).stored o.path = some o.bytes
  checkpoint : ∀ t r n o, after ≤ t → (run.state t).journal = .pending r →
    r.last_step = n + 1 → (r.data.objects codec)[n]? = some o →
    (run.state t).stored o.path = some o.bytes →
    ∃ u, t ≤ u ∧ (run.state t).journal.position codec < (run.state u).journal.position codec
  finalize : ∀ t r, after ≤ t → (run.state t).journal = .pending r →
    r.last_step = (r.data.objects codec).length + 1 →
    ∃ u, t ≤ u ∧ (run.state u).journal.row = none

private theorem same_checkpoint {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    (run : Run codec kept id d) (t u : Nat) (r : Row)
    (hj : (run.state t).journal = .pending r)
    (same : (run.state t).journal.position codec = (run.state u).journal.position codec) :
    (run.state u).journal = .pending r := by
  have vt := reachable_valid (run.reachable t)
  have ft := fixed_data (run.reachable t)
  have fu := fixed_data (run.reachable u)
  have bound := vt.pendingBound r hj
  cases hu : (run.state u).journal with
  | pending row =>
    have hd : row.data = r.data := by
      have := fu.1.trans ft.1.symm
      simpa [hj, hu, Journal.data] using this
    have hi : row.id = r.id := by
      have := fu.2.trans ft.2.symm
      simpa [hj, hu, Journal.id] using this
    have hp : row.last_step = r.last_step := by simpa [hj, hu, Journal.position] using same.symm
    cases row; cases r; simp_all
  | finished i data =>
    have hd : data = r.data := by
      have := fu.1.trans ft.1.symm
      simpa [hj, hu, Journal.data] using this
    simp [hj, hu, Journal.position, hd] at same
    omega

theorem pending_advances {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    (run : Run codec kept id d) {after : Nat} (fair : Fair run after)
    (t : Nat) (ht : after ≤ t) (r : Row) (hj : (run.state t).journal = .pending r) :
    ∃ u, t ≤ u ∧ (run.state t).journal.position codec < (run.state u).journal.position codec := by
  by_cases zero : r.last_step = 0
  · exact fair.localCommit t r ht hj zero
  · have bound := (reachable_valid (run.reachable t)).pendingBound r hj
    by_cases terminal : r.last_step = (r.data.objects codec).length + 1
    · obtain ⟨u, hu, done⟩ := fair.finalize t r ht hj terminal
      have finished := (finished_publication (run.reachable u) done).1
      have data := (fixed_data (run.reachable t)).1
      simp only [hj, Journal.data] at data
      exact ⟨u, hu, by simp [hj, finished, Journal.position, terminal, data]⟩
    · have hn : r.last_step - 1 < (r.data.objects codec).length := by omega
      let o := (r.data.objects codec)[r.last_step - 1]
      have ho : (r.data.objects codec)[r.last_step - 1]? = some o := List.getElem?_eq_getElem hn
      obtain ⟨u, hu, stored⟩ := fair.storageAccepts t r (r.last_step - 1) o ht hj (by omega) ho
      by_cases advanced : (run.state t).journal.position codec < (run.state u).journal.position codec
      · exact ⟨u, hu, advanced⟩
      · have mono := run.monotone hu
        have same : (run.state t).journal.position codec = (run.state u).journal.position codec := by omega
        have row := same_checkpoint run t u r hj same
        obtain ⟨v, hv, progressed⟩ := fair.checkpoint u r (r.last_step - 1) o (by omega) row (by omega) ho stored
        exact ⟨v, by omega, by omega⟩

theorem eventually_finishes {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    (run : Run codec kept id d) {after : Nat} (fair : Fair run after) (t : Nat) :
    ∃ u, t ≤ u ∧ (run.state u).journal.row = none := by
  suffices ∀ start, after ≤ start → ∃ u, start ≤ u ∧ (run.state u).journal.row = none by
    obtain ⟨u, hu, done⟩ := this (max t after) (Nat.le_max_right _ _)
    exact ⟨u, Nat.le_trans (Nat.le_max_left _ _) hu, done⟩
  intro start hs
  generalize remainingEq : (d.objects codec).length + 2 -
    (run.state start).journal.position codec = remaining
  induction remaining using Nat.strongRecOn generalizing start with
  | ind remaining ih =>
    cases hj : (run.state start).journal with
    | finished i data => exact ⟨start, Nat.le_refl _, by simp [hj, Journal.row]⟩
    | pending row =>
      obtain ⟨u, hu, advanced⟩ := pending_advances run fair start hs row hj
      have bound := (reachable_valid (run.reachable u)).bound
      have fixed := (fixed_data (run.reachable u)).1
      rw [fixed] at bound
      have less : (d.objects codec).length + 2 - (run.state u).journal.position codec < remaining := by omega
      obtain ⟨v, hv, done⟩ := ih _ less u (by omega) rfl
      exact ⟨v, by omega, done⟩

def FinishesAt {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    (run : Run codec kept id d) (t : Nat) : Prop :=
  (run.state t).journal.row ≠ none ∧ (run.state (t + 1)).journal.row = none

theorem finishes_at_most_once {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    (run : Run codec kept id d) {t u : Nat} (a : FinishesAt run t) (b : FinishesAt run u) : t = u := by
  by_cases eq : t = u
  · exact eq
  exfalso
  rcases Nat.lt_or_gt_of_ne eq with lt | gt
  · exact b.1 (run.finished_stays (by omega) a.2)
  · exact a.1 (run.finished_stays (by omega) b.2)

theorem finishes_exactly_once {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    (run : Run codec kept id d) {after : Nat} (fair : Fair run after) :
    ∃ t, FinishesAt run t ∧ ∀ u, FinishesAt run u → u = t := by
  have event : ∀ u, (run.state u).journal.row = none → ∃ t, FinishesAt run t := by
    intro u
    induction u with
    | zero => simp [run.start, initial, Journal.row]
    | succ u ih =>
      intro done
      by_cases earlier : (run.state u).journal.row = none
      · exact ih earlier
      · exact ⟨u, earlier, done⟩
  obtain ⟨u, _, done⟩ := eventually_finishes run fair 0
  obtain ⟨t, finish⟩ := event u done
  exact ⟨t, finish, fun u hu => finishes_at_most_once run hu finish⟩

theorem rotation_resumes_and_finishes {codec : Codec} {kept : Kept} {id : Nat} {r : Rotation}
    (run : Run codec kept id (.rotation r)) {after : Nat} (fair : Fair run after) :
    ∃ t, FinishesAt run t ∧ (∀ u, FinishesAt run u → u = t) ∧
      (run.state (t + 1)).journal = .finished id (.rotation r) ∧
      (run.state (t + 1)).retained = some r.keyBytes ∧
      (run.state (t + 1)).stored (.entry r.entryNumber) = some (codec.entry r) ∧
      (∀ c ∈ r.copies, (run.state (t + 1)).stored (.copy c.number) = some c.sealed) := by
  obtain ⟨t, event, unique⟩ := finishes_exactly_once run fair
  have completed := finished_publication (run.reachable (t + 1)) event.2
  have entry : (run.state (t + 1)).stored (.entry r.entryNumber) = some (codec.entry r) :=
    completed.2.2.2 ⟨.entry r.entryNumber, codec.entry r⟩ (by simp [Data.objects, Rotation.objects])
  exact ⟨t, event, unique, completed.1, completed.2.1, entry,
    (rotation_entry_after_copies (run.reachable (t + 1)) entry).2⟩

theorem snapshot_resumes_and_finishes {codec : Codec} {kept : Kept} {id : Nat} {s : Snapshot}
    (run : Run codec kept id (.snapshot s)) {after : Nat} (fair : Fair run after) :
    ∃ t, FinishesAt run t ∧ (∀ u, FinishesAt run u → u = t) ∧
      (run.state (t + 1)).journal = .finished id (.snapshot s) ∧
      (run.state (t + 1)).retained = some s.sealed ∧
      (run.state (t + 1)).stored (.snapshot s.number) = some s.sealed ∧
      codec.openSnapshot s.key s.sealed = some s.captured := by
  obtain ⟨t, event, unique⟩ := finishes_exactly_once run fair
  have completed := finished_publication (run.reachable (t + 1)) event.2
  exact ⟨t, event, unique, completed.1, completed.2.1,
    completed.2.2.2 ⟨.snapshot s.number, s.sealed⟩ (by simp [Data.objects]), completed.2.2.1⟩

end CovenQueue.Operations
