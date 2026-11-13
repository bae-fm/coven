/-!
# A member's devices write their profile as their registration applies

The new person's name and the circle their admission made are facts of the
admission (§13.2). The app registers a joining write, which runs in the
transaction that first applies a device's registration and reads the store as
that transaction leaves it: the profile is committed with the registration,
and queued like any local write (§13.1, §13.2, §19.3). This module follows one
member's devices through a join, a restore or pairing's new device, a lost
phone and a landed write.

`Rules.inState` says where the facts live. In the member's state in history,
every device of the member can write the profile when it registers. Kept only
on the joining phone, as a device record until the app acts on it, they go
with the phone.

- `every_device_has_profile`: with the facts in the member's state, every
  device whose registration applied has the profile, whichever devices were
  lost, whether or not the first write landed. A device whose history already
  holds the row finds it and writes nothing.
- `device_record_lost`: with the facts on the joining phone only, the phone
  is lost before its write lands, and a device of the member restored from
  their code registers with the member and no profile, in every extension.
-/
namespace CovenSequence.JoiningWrite

structure World where
  /-- The member's admission applied. -/
  admitted : Bool
  /-- The joining phone still holds its record of the join's facts. -/
  phone : Bool
  /-- A profile write is queued on some device, not yet landed. -/
  owed : Bool
  /-- The profile row is in history. -/
  landed : Bool
  /-- The devices of the member whose registration applied. -/
  devices : List Nat
  /-- The devices whose store holds the profile. -/
  has : List Nat
  deriving DecidableEq, Repr

def start : World := ⟨false, false, false, false, [], []⟩

structure Rules where
  /-- The join's facts live in the member's state in history. -/
  inState : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true⟩

inductive Act where
  /-- The admission applies on the joining phone, device 0. -/
  | admission
  /-- Where the facts are on the phone only, its app writes the profile. -/
  | phoneWrites
  /-- A queued profile write lands in history. -/
  | land
  /-- The joining phone is lost: its queue and its record go with it. -/
  | lose
  /-- Another device of the member registers (a restore, a pairing, a reset). -/
  | register (device : Nat)
  deriving DecidableEq, Repr

def enabled (R : Rules) (w : World) : Act → Bool
  | .admission => !w.admitted
  | .phoneWrites => !R.inState && w.phone && w.devices.contains 0 && !w.has.contains 0
  | .land => w.owed
  | .lose => w.devices.contains 0
  | .register d => w.admitted && d != 0 && !w.devices.contains d

def act (R : Rules) (w : World) : Act → World
  | .admission =>
    if R.inState then
      -- The joining write runs with the registration and queues its row.
      { w with admitted := true, phone := true, owed := true, devices := [0], has := [0] }
    else { w with admitted := true, phone := true, devices := [0] }
  | .phoneWrites => { w with phone := false, owed := true, has := 0 :: w.has }
  | .land => { w with owed := false, landed := true }
  | .lose =>
    { w with phone := false, owed := false, devices := w.devices.filter (· != 0),
             has := w.has.filter (· != 0) }
  | .register d =>
    if R.inState then
      -- The joining write reads the store as the registration leaves it: a
      -- row in history is found, and none is written then.
      { w with devices := d :: w.devices, has := d :: w.has, owed := w.owed || !w.landed }
    else { w with devices := d :: w.devices, has := if w.landed then d :: w.has else w.has }

/-- Take each act in turn while it is enabled. -/
def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act R w a) rest else none

/-- Every device whose registration applied has the profile. -/
structure Inv (w : World) : Prop where
  profile : ∀ d ∈ w.devices, d ∈ w.has

theorem inv_start : Inv start := ⟨by simp [start]⟩

theorem inv_step (w : World) (a : Act) (inv : Inv w) :
    Inv (act Rules.correct w a) := by
  obtain ⟨profile⟩ := inv
  constructor
  cases a with
  | admission => simp [act, Rules.correct]
  | phoneWrites =>
    intro d hd
    exact List.mem_cons_of_mem _ (profile d (by simpa [act] using hd))
  | land => simpa [act] using profile
  | lose =>
    intro d hd
    simp only [act, List.mem_filter] at hd ⊢
    exact ⟨profile d hd.1, hd.2⟩
  | register e =>
    intro d hd
    simp only [act, Rules.correct, ite_true, List.mem_cons] at hd ⊢
    rcases hd with rfl | hd
    · exact Or.inl rfl
    · exact Or.inr (profile d hd)

theorem trace_inv : ∀ (rest : List Act) (w w' : World), Inv w →
    trace Rules.correct w rest = some w' → Inv w'
  | [], w, w', inv, h => by simp [trace] at h; subst h; exact inv
  | a :: rest, w, w', inv, h => by
    simp only [trace] at h
    split at h
    · exact trace_inv rest _ _ (inv_step w a inv) h
    · cases h

/-- **Every device of the member has the profile.** With the facts in the
member's state, each device whose registration applied has the profile in its
store, in every history of joins, restores, new devices, lost phones and
landed writes. -/
theorem every_device_has_profile (rest : List Act) (w : World)
    (h : trace Rules.correct start rest = some w) : ∀ d ∈ w.devices, d ∈ w.has :=
  (trace_inv rest start w inv_start h).profile

/-- Nothing for a restored device to find: the join's facts went with the
phone, no profile row landed, and nothing is queued. -/
structure Lost (w : World) : Prop where
  phone : w.phone = false
  owed : w.owed = false
  landed : w.landed = false
  has : w.has = []
  admitted : w.admitted = true
  one : 1 ∈ w.devices

theorem lost_stays (w : World) (a : Act) (lost : Lost w)
    (e : enabled ⟨false⟩ w a = true) : Lost (act ⟨false⟩ w a) := by
  obtain ⟨phone, owed, landed, has, admitted, one⟩ := lost
  cases a with
  | admission => simp [enabled, admitted] at e
  | phoneWrites => simp [enabled, phone] at e
  | land => simp [enabled, owed] at e
  | lose =>
    refine ⟨by simp [act], by simp [act], by simp [act, landed], by simp [act, has],
      by simp [act, admitted], ?_⟩
    show 1 ∈ w.devices.filter (· != 0)
    exact List.mem_filter.mpr ⟨one, by decide⟩
  | register d =>
    simp only [enabled, Bool.and_eq_true, bne_iff_ne, ne_eq, Bool.not_eq_true'] at e
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩ <;> simp [act, phone, owed, landed, has, admitted, one]

theorem trace_lost : ∀ (rest : List Act) (w w' : World), Lost w →
    trace ⟨false⟩ w rest = some w' → Lost w'
  | [], w, w', lost, h => by simp [trace] at h; subst h; exact lost
  | a :: rest, w, w', lost, h => by
    simp only [trace] at h
    split at h
    · rename_i e
      exact trace_lost rest _ _ (lost_stays w a lost e) h
    · cases h

/-- **Counterexample: the facts on the joining phone alone.** Dan's phone
applies his admission and is lost before the app writes his profile. His
laptop, restored from his code, registers with his member id and finds no
profile: the facts went with the phone. In every extension no profile row ever
lands and no device of his has one. -/
theorem device_record_lost :
    ∃ w, trace ⟨false⟩ start [.admission, .lose, .register 1] = some w ∧
      1 ∈ w.devices ∧
      ∀ rest w', trace ⟨false⟩ w rest = some w' →
        1 ∈ w'.devices ∧ w'.landed = false ∧ w'.has = [] := by
  refine ⟨_, rfl, by decide, ?_⟩
  intro rest w' h
  have lost := trace_lost rest _ w' ⟨rfl, rfl, rfl, rfl, rfl, by decide⟩ h
  exact ⟨lost.one, lost.landed, lost.has⟩

end CovenSequence.JoiningWrite
