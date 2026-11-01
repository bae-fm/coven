import CovenSequence.JoinScan

/-!
# What a waiting join reports

A join's call checks for approval again and again (§13.2). Before each check
it reports its wait, and a check that opens its keys reports loading. Each
check ends in `JoinScan.outcome`. Once one check has found approval begun,
the call reports no wait again (`approving`, api.md E10).

`phases_forward` proves the reported phases never go back, whatever each
check finds. Without the `approving` outcome, a check between the copies
and the entry reported loading, found no entry, and the next check reported
waiting again (`waited_after_loading`).
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

/-- What one check finds: its keys open, the approving entry binds the
request's pair, the request is still in storage. -/
abbrev Check := Bool × Bool × Bool

/-- The phases a call reports for its checks, in order, until one ends it.
`approving` says an earlier check found approval begun. -/
def run (approving : Bool) : List Check → List Phase
  | [] => []
  | (key, binding, pending) :: rest =>
    let before := if approving then [] else [Phase.waiting]
    let during := if key then [Phase.loading] else []
    match outcome key binding pending with
    | .admitted | .declined => before ++ during ++ [.done]
    | found => before ++ during ++ run (approving || found == .approving) rest

theorem approving_never_waits : ∀ checks, Phase.waiting ∉ run true checks
  | [] => by simp [run]
  | (key, binding, pending) :: rest => by
    have := approving_never_waits rest
    simp only [run, Bool.true_or]
    split <;> cases key <;> simp_all

theorem waiting_lowest (p : Phase) : Phase.waiting.rank ≤ p.rank := by
  cases p <;> simp [Phase.rank]

/-- The reported phases never go back: waiting, then loading, then done. -/
theorem phases_forward : ∀ (approving : Bool) (checks : List Check),
    (run approving checks).Pairwise (fun a b => a.rank ≤ b.rank)
  | _, [] => by simp [run]
  | approving, (key, binding, pending) :: rest => by
    have tail := phases_forward (approving || outcome key binding pending == .approving) rest
    have keyless := approving_never_waits rest
    simp only [run]
    split
    all_goals
      cases approving <;> cases key <;>
        simp_all [outcome, Phase.rank] <;>
        first
        | (intro p mem; cases p <;> simp_all)
        | (constructor <;> intro p mem <;> cases p <;> simp_all [Phase.rank])
        | skip

/-- The outcome before a check could find approval begun. -/
def oldOutcome (key binding pending : Bool) : Outcome :=
  if key && binding then .admitted else if pending then .waiting else .declined

def runOld : List Check → List Phase
  | [] => []
  | (key, binding, pending) :: rest =>
    let during := if key then [Phase.loading] else []
    match oldOutcome key binding pending with
    | .admitted | .declined => [Phase.waiting] ++ during ++ [.done]
    | _ => [Phase.waiting] ++ during ++ runOld rest

/-- Two checks between the copies and the entry: the second reported waiting
after the first had loaded. -/
theorem waited_after_loading :
    runOld [(true, false, true), (true, false, true)] = [.waiting, .loading, .waiting, .loading] ∧
      ¬ (runOld [(true, false, true), (true, false, true)]).Pairwise
        (fun a b => a.rank ≤ b.rank) := by
  decide

end CovenSequence.JoinStatus
