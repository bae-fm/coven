/-! §12.2's approval of an invite whose access is an S3 key `K` the invite
made, with §13's deletion obligations as every admin derives them from
replay: a key an entry records stops being intended once its member does not
use it, whatever that entry's fate, and a deletion is never cancelled. The
joiner reads its approver's bindings and treats a dropped one as final. The
question: may the approver publish the binding again after a drop? -/

namespace CovenStorage.InviteApproval

inductive Fate where
  | kept
  | dropped
  deriving DecidableEq, Repr

/-- `bindings` are the approver's published entries binding the new member
with `K`, oldest first; `live` says the invite can still publish one. -/
structure World where
  live : Bool
  bindings : List Fate
  deriving DecidableEq, Repr

def start : World := ⟨true, []⟩

/-- The new member uses `K`: some binding is kept. -/
def uses (w : World) : Bool := w.bindings.contains .kept

/-- `K` is pending deletion: a binding dropped while no kept one bound the
member, which made `K` unintended, and nothing cancels that. -/
def pending (w : World) : Bool := (w.bindings.takeWhile (· != .kept)).contains .dropped

/-- The joiner is admitted only if the first binding it reads is kept; a
dropped one reaches the app as a rejection (§12.2). -/
def admitted (w : World) : Bool := w.bindings.head? == some .kept

/-- §18's general rule: a dropped entry starts the operation over, so the
invite stays open and publishes again. -/
def restart (w : World) (f : Fate) : World :=
  if w.live then ⟨f == .dropped, w.bindings ++ [f]⟩ else w

/-- T243: a published binding settles the invite whatever its fate; a drop
ends the invite, which takes back its grants. -/
def settle (w : World) (f : Fate) : World :=
  if w.live then ⟨false, w.bindings ++ [f]⟩ else w

def run (step : World → Fate → World) (fates : List Fate) : World := fates.foldl step start

theorem settled_stays (fs : List Fate) (w : World) (hw : w.live = false) :
    fs.foldl settle w = w := by
  induction fs with
  | nil => rfl
  | cons f fs ih =>
    rw [List.foldl_cons, show settle w f = w by simp [settle, hw]]
    exact ih

/-- The first published binding settles the invite. -/
theorem settle_once (f : Fate) (fs : List Fate) : run settle (f :: fs) = ⟨false, [f]⟩ := by
  simp only [run, List.foldl_cons]
  exact settled_stays fs _ rfl

/-- Under T243 the member never uses a key that is pending deletion, and the
approver binds the member exactly when the joiner is admitted. -/
theorem settled_key_never_pending (fates : List Fate) :
    ¬ (uses (run settle fates) = true ∧ pending (run settle fates) = true) ∧
      uses (run settle fates) = admitted (run settle fates) := by
  cases fates with
  | nil => decide
  | cons f fs => rw [settle_once]; cases f <;> decide

/-- Checked counterexample to restarting: Ana's approval of Carol drops (it
landed too late), so every admin makes `K` pending deletion and Carol's
installation reports the rejection. Ana publishes the binding again and it
is kept: Carol is a member whose key is pending deletion, and her
installation has stopped. -/
theorem restart_binds_pending_key :
    let w := run restart [.dropped, .kept]
    uses w = true ∧ pending w = true ∧ admitted w = false := by
  decide

end CovenStorage.InviteApproval
