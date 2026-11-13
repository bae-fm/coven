import CovenSequence.JoinScan

/-!
# What a waiting join reports

A join checks for approval again and again (§13.2), and not always in one
call: the app may close while it waits, and a later call resumes the join.
Before each check the join reports its wait. Its scan for its copies is part
of that wait. A check whose copies open its keys reports loading, and each
check ends in `JoinScan.outcome`: a check whose copies open has found its
admission, and the join publishes the store, which loads on in the same
call; once published it is a store, no longer a join (`FirstInstallation`).

The first check whose copies open the keys records that in the join's saved
request before it reports loading. A join whose keys opened, in this call or
an earlier one, reports no wait again (api.md E10). A call stops between
finding its admission and publishing only by a crash or a custody write that
fails; the resumed call finds the admission again.

`phases_forward` proves the reported phases never go back, whatever each
check finds and wherever a call stops. Each rule is needed:
`resumed_waits_again` keeps the opened keys only in the call, and the resumed
call reports waiting after the call before it loaded; `scan_shown_as_loading`
reports a scan that finds no copies as loading, and the next check reports
waiting again.
-/
namespace CovenSequence.JoinStatus
open JoinScan

inductive Phase where
  | waiting
  | loading
  | done
  deriving DecidableEq, Repr

def Phase.rank : Phase → Nat
  | .waiting => 0
  | .loading => 1
  | .done => 2

/-- What one check finds: its keys open, the request is still in storage, the
shared location is reachable with the join's proven access. Keys open only
from the admission's slot, so a check whose keys open ends the join
(`JoinScan.approved_never_declined`); one that cannot reach the location
before they open ends it too (`JoinScan.lost_access_ends`). -/
abbrev Check := Bool × Bool × Bool

/-- What happens to a join, in order. -/
inductive Event where
  /-- A check, with what it finds. -/
  | check (found : Check)
  /-- The call stops between checks; a later call resumes the join. -/
  | stop
  /-- The call stops in the check whose copies opened its keys, after that
  check reported loading and before it published the store. -/
  | stopLoading

structure Rules where
  /-- The saved request keeps, across a stop, that the keys opened. -/
  kept : Bool
  /-- A check reports its scan for the copies as loading. -/
  scanLoads : Bool

def Rules.correct : Rules := ⟨true, false⟩

@[simp] theorem correct_kept : Rules.correct.kept = true := rfl
@[simp] theorem correct_scanLoads : Rules.correct.scanLoads = false := rfl

/-- The phases the join reports for `events`, until a check ends it.
`opened` says its keys opened in an earlier check. -/
def run (R : Rules) (opened : Bool) : List Event → List Phase
  | [] => []
  | .stop :: rest => run R (R.kept && opened) rest
  | .stopLoading :: rest =>
    (if opened then [] else [Phase.waiting]) ++ [Phase.loading] ++ run R R.kept rest
  | .check (key, pending, reach) :: rest =>
    let before := if opened then [] else [Phase.waiting]
    let during := if key || R.scanLoads then [Phase.loading] else []
    match outcome key pending reach with
    | .admitted | .declined => before ++ during ++ [.done]
    | _ => before ++ during ++ run R (opened || key) rest

/-- Once the keys opened, the join reports no wait, in this call or a
resumed one. -/
theorem opened_never_waits : ∀ events, Phase.waiting ∉ run Rules.correct true events
  | [] => by simp [run]
  | .stop :: rest => by
    simpa [run] using opened_never_waits rest
  | .stopLoading :: rest => by
    simpa [run] using opened_never_waits rest
  | .check (key, pending, reach) :: rest => by
    have := opened_never_waits rest
    simp only [run, correct_scanLoads, Bool.true_or, Bool.or_false, ite_true]
    split <;> cases key <;> simp_all

theorem waiting_lowest (p : Phase) : Phase.waiting.rank ≤ p.rank := by
  cases p <;> simp [Phase.rank]

theorem loading_below_opened (events : List Event) :
    ∀ p ∈ run Rules.correct true events, Phase.loading.rank ≤ p.rank := by
  intro p mem
  have := opened_never_waits events
  cases p <;> simp_all [Phase.rank]

/-- The reported phases never go back: waiting, then loading, then done,
across every check and every call. -/
theorem phases_forward : ∀ (opened : Bool) (events : List Event),
    (run Rules.correct opened events).Pairwise (fun a b => a.rank ≤ b.rank)
  | opened, [] => by simp [run]
  | opened, .stop :: rest => by
    simpa [run] using phases_forward opened rest
  | opened, .stopLoading :: rest => by
    have tail := phases_forward true rest
    have above := loading_below_opened rest
    simp only [run, correct_kept]
    cases opened <;>
      simp_all [Phase.rank] <;>
      intro p mem <;> cases p <;> simp_all
  | opened, .check (key, pending, reach) :: rest => by
    have tail := phases_forward (opened || key) rest
    have above := loading_below_opened rest
    have keyless := opened_never_waits rest
    simp only [run, correct_scanLoads, Bool.or_false]
    split
    all_goals
      cases opened <;> cases key <;>
        simp_all [outcome, Phase.rank] <;>
        first
        | (intro p mem; cases p <;> simp_all)
        | (constructor <;> intro p mem <;> cases p <;> simp_all [Phase.rank])
        | skip

/-- A first check that finds the join's access lost reports its wait and then
that the join is done: it ends as an ended invite does, with no loading
between and nothing after. -/
theorem lost_access_reports_done (pending : Bool) (rest : List Event) :
    run Rules.correct false (.check (false, pending, false) :: rest) = [.waiting, .done] := by
  simp [run, outcome]

/-- Kept only in the call: the call stops while it loads, and the resumed
call reports waiting after the call before it loaded. -/
theorem resumed_waits_again :
    run ⟨false, false⟩ false [.stopLoading, .check (true, false, true)] =
        [.waiting, .loading, .waiting, .loading, .done] ∧
      ¬ (run ⟨false, false⟩ false [.stopLoading, .check (true, false, true)]).Pairwise
        (fun a b => a.rank ≤ b.rank) := by
  decide

/-- A scan that finds no copies, reported as loading: the next check reports
waiting again. -/
theorem scan_shown_as_loading :
    run ⟨true, true⟩ false [.check (false, true, true), .check (false, true, true)] =
        [.waiting, .loading, .waiting, .loading] ∧
      ¬ (run ⟨true, true⟩ false [.check (false, true, true), .check (false, true, true)]).Pairwise
        (fun a b => a.rank ≤ b.rank) := by
  decide

end CovenSequence.JoinStatus
