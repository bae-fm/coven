import CovenStorelog.Replay

namespace CovenStorelog

/-- The bound cannot be exhausted: each restart removes a received entry. -/
def settle (M : Log) (views : Nat → State) (entries : List Nat) : Result :=
  (settleN M views entries (entries.length + 1) []).get (by
    obtain ⟨r, hr⟩ := settleN_total M views entries (entries.length + 1) [] (by
      simp [remaining])
    simp [hr])

theorem settle_eq_some (M : Log) (views : Nat → State) (entries : List Nat) :
    settleN M views entries (entries.length + 1) [] = some (settle M views entries) := by
  exact (Option.some_get _).symm

def materialize (M : Log) (n : Nat) (S : EntrySet) (views : Nat → State) : Result :=
  settle M views ((List.range n).filter S)

/-- Each recorded past is smaller than its entry. Construct its replay using
only earlier author views, with the same restart algorithm as the receiver. -/
def authorViews (M : Log) : Nat → Nat → State
  | 0 => fun _ => State.empty
  | n + 1 =>
      let prior := authorViews M n
      fun w => if w = n then (materialize M n (hadRead M n) prior).state else prior w

/-- Replay using recorded author views for authority and for whether an
entry deletes a circle. -/
def resolve (M : Log) (n : Nat) (S : EntrySet) : Result :=
  materialize M n S (authorViews M n)

def authorView (M : Log) (w : Nat) : State := (resolve M w (hadRead M w)).state

theorem authorViews_at (M : Log) {n w : Nat} (h : w < n) :
    authorViews M n w = authorView M w := by
  induction n with
  | zero => omega
  | succ n ih =>
      by_cases he : w = n
      · subst w; simp [authorViews, authorView, resolve]
      · simp only [authorViews, he, ite_false]
        exact ih (by omega)

def reports (M : Log) (r : Result) (author : Nat) : List Nat :=
  r.dropped.filter (fun w => (M w).author == author)

theorem reported_to_author (M : Log) (r : Result) (w : Nat) :
    w ∈ reports M r (M w).author ↔ w ∈ r.dropped := by simp [reports]

theorem report_only_author (M : Log) (r : Result) (w a : Nat)
    (h : w ∈ reports M r a) : (M w).author = a := by
  simpa [reports] using (List.mem_filter.mp h).2

end CovenStorelog
