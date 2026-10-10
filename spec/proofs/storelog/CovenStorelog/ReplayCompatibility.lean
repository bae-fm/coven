import CovenStorelog.CurrentModel
import CovenStorelog.ReplayPolicy

/-! The data package's historical Finality.History input retains its original
key-list gate. D6's CurrentReplay.History has no such gate or removal payload.
Dispatch is determined by the input type, not by probing its contents. -/
namespace CovenStorelog.CurrentReplay

/-- Historical removal validation, retained for the data package's model. -/
def keysMatch (view : State) (entry : CovenStorelog.Entry) : Bool :=
  match entry.action with
  | .removeMember m keys =>
      let expected := view.circles.filterMap fun (c, circle) =>
        if m ∈ circle.members && circle.members.any (· != m) then some c else none
      keys.all (· ∈ expected) && expected.all (· ∈ keys)
  | _ => true

/-- Historical input compatibility. Current D6 admission is ReplayPolicy's
storage-time check with no optional predicate. -/
def admitted (H : Finality.History) (W n : Nat) (views : Nat → State)
    (S : EntrySet) : EntrySet := fun e =>
  S e && !Finality.tooLate H W n (fun _ => true) e && keysMatch (views e) (H.log e)

abbrev conflict := ReplayPolicy.conflict
abbrev prefer := ReplayPolicy.prefer
abbrev realize := ReplayPolicy.realize
abbrev read_no_conflict := ReplayPolicy.read_no_conflict

class ReplayInput (α : Type) where
  membership : α → Finality.History
  admission : Option (State → CovenStorelog.Entry → Bool)

instance : ReplayInput History where
  membership := History.membership
  admission := none

instance : ReplayInput Finality.History where
  membership := id
  admission := some keysMatch

end CovenStorelog.CurrentReplay
