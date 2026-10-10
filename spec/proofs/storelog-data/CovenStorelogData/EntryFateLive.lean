import CovenStorelogData.EntryFate

namespace CovenStorelogData.EntryFate

open CovenStorelog

/-- Stop-all models §10 and sync_loop.rs. Observe-entries is the proposed
repair: pause data work while removed, but keep fetching replay evidence. -/
inductive RemovalPolicy where
  | stopAll | observeEntries
  deriving DecidableEq, Repr

def poll (policy : RemovalPolicy) (log : Log) (bound member device : Nat)
    (readable : Bool) (s : Device) (entry : Nat) : Device :=
  if readable && (policy == .observeEntries || running s.result.state member device)
  then CovenStorelog.step log bound s entry else s

theorem stopped_forever (log : Log) (bound member device : Nat) (s : Device)
    (stopped : running s.result.state member device = false) (arrivals : List Nat) :
    arrivals.foldl (poll .stopAll log bound member device true) s = s := by
  induction arrivals with
  | nil => rfl
  | cons e es ih => simpa [poll, stopped] using ih

theorem unreadable_forever (policy : RemovalPolicy) (log : Log) (bound member device : Nat)
    (s : Device) (arrivals : List Nat) :
    arrivals.foldl (poll policy log bound member device false) s = s := by
  induction arrivals with
  | nil => rfl
  | cons e es ih => simpa [poll] using ih

/-- With readable evidence and successful polling, removal cannot disable
receipt. Fair eventual delivery and eventual quiescence remain assumptions for
an eventual *stable* verdict; an infinite stream can keep changing replay. -/
theorem observer_receives (log : Log) (bound member device : Nat) (s : Device)
    (arrivals : List Nat) :
    arrivals.foldl (poll .observeEntries log bound member device true) s =
      arrivals.foldl (CovenStorelog.step log bound) s := by
  induction arrivals generalizing s with
  | nil => rfl
  | cons e es ih => simpa [poll] using ih (CovenStorelog.step log bound s e)

/-- Once both observers have received the finite history, they agree on every
kept/dropped entry and whether data work is permitted, even after removal. -/
theorem observers_converge (log : Log) (bound member device : Nat)
    {a b : List Nat} (ha : CausalOrder log a) (hb : CausalOrder log b)
    (same : ∀ e, e ∈ a ↔ e ∈ b) :
    a.foldl (poll .observeEntries log bound member device true) (CovenStorelog.initial log bound) =
      b.foldl (poll .observeEntries log bound member device true) (CovenStorelog.initial log bound) := by
  rw [observer_receives, observer_receives]
  exact storelog_converges log bound ha hb same

/-- The app's current result is replaceable; what it already learned is not.
Delivery can occur between any two receipts. A subscription may coalesce
changes, but an app allowed to await each one can observe this entire trace. -/
structure AppValue where
  lost : Bool
  stuck : List Nat
  removed : Bool
  deriving DecidableEq, Repr

def removalStatus (member device : Nat) (s : Device) : Bool :=
  !running s.result.state member device

def tell (trace : List AppValue) (current : AppValue) : List AppValue := trace ++ [current]

theorem told_persists (trace : List AppValue) (current earlier : AppValue)
    (told : earlier ∈ trace) : earlier ∈ tell trace current := List.mem_append_left _ told

/-- Loss permanence is appropriate only when the API has promised it. §8
rule losses explicitly may return; §17's frozen losses say they stay out. -/
def PermanentLoss (trace : List AppValue) : Prop :=
  trace.Pairwise fun before after => before.lost = true → after.lost = true

end CovenStorelogData.EntryFate
