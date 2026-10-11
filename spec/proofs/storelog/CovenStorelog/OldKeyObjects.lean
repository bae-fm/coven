import CovenStorelog.MemberKeyExamples

/-! Old-key writes count only within every unread kept replacement's inclusive
deadline. Readers cannot change admission. Entry finality fixes the earlier
replacement prefix; the send gate bounds remaining publication. -/
namespace CovenStorelog.CurrentReplay.Keyed.OldKeyObjects

structure Write where
  author : Nat
  signer : Nat
  device : Nat
  storeLogRead : List Nat
  stored : Nat
  deriving DecidableEq, Repr

abbrev Writes := Nat → Write

/-- The same owner and signing-key check is used in the recorded author view
for admission and the completed current view before every provider request. -/
def writerCurrent (view : State) (obj : Write) : Bool :=
  member view.membership obj.author &&
    lookup view.membership.devices obj.device == some obj.author &&
    (lookup view.keys obj.author).any (fun keys => keys.signing == obj.signer)

/-- Ordinary schema/reset/content checks are a supplied predicate. This
function additionally checks the actual replay of the signed recorded past. -/
def authorized (H : History) (W n : Nat) (otherChecks : Nat → Bool) (writes : Writes) (w : Nat) : Bool :=
  otherChecks w && writerCurrent (resolve H W n (entrySet (writes w).storeLogRead)).state (writes w)

/-- An unread replacement bounds every other credential for that member,
including a credential installed only in a losing concurrent branch. A write
under the winning keys is not an old-key write. Read earlier replacements do
not retire keys installed after them in the write's recorded view. -/
def retires (H : History) (obj : Write) (e : Nat) : Bool :=
  match (H.log e).action with
  | .replaceMemberKeys m keys =>
      m == obj.author && keys.signing != obj.signer && e ∉ obj.storeLogRead
  | _ => false

def timely (H : History) (W : Nat) (r : Result) (obj : Write) : Bool :=
  r.kept.all (fun e => !retires H obj e || obj.stored ≤ H.stored e + W)

theorem unread_replacement_deadline (H : History) (W e m : Nat) (r : Result)
    (obj : Write) (keys : MemberKeys) (kept : e ∈ r.kept)
    (action : (H.log e).action = .replaceMemberKeys m keys)
    (author : obj.author = m) (different : keys.signing ≠ obj.signer)
    (unread : e ∉ obj.storeLogRead) (within : timely H W r obj = true) :
    obj.stored ≤ H.stored e + W := by
  have h := List.all_eq_true.mp within e kept
  simpa [retires, action, author, different, unread] using h

theorem timely_iff (H : History) (W : Nat) (r : Result) (obj : Write) :
    timely H W r obj = true ↔
      ∀ e ∈ r.kept, retires H obj e = true → obj.stored ≤ H.stored e + W := by
  simp only [timely, List.all_eq_true, Bool.or_eq_true, Bool.not_eq_true', decide_eq_true_eq]
  constructor
  · intro h e he hr
    simpa [hr] using h e he
  · intro h e he
    cases hr : retires H obj e
    · exact Or.inl rfl
    · exact Or.inr (h e he hr)

/-- Write reads and other received writes are not admission inputs. -/
def counts (valid timely : Nat → Bool) (received : EntrySet) (w : Nat) : Bool :=
  received w && valid w && timely w

def countInReplay (H : History) (W n : Nat) (entries : EntrySet) (otherChecks : Nat → Bool)
    (writes : Writes) (received : EntrySet) (w : Nat) : Bool :=
  let r := resolve H W n entries
  counts (authorized H W n otherChecks writes) (fun w => timely H W r (writes w)) received w

theorem counts_iff (valid timely : Nat → Bool) (received : EntrySet) (w : Nat) :
    counts valid timely received w = true ↔
      received w = true ∧ valid w = true ∧ timely w = true := by
  simp [counts, and_assoc]

theorem late_never_counts (valid timely : Nat → Bool) (received : EntrySet)
    (w : Nat) (late : timely w = false) : counts valid timely received w = false := by
  simp [counts, late]

theorem other_writes_do_not_change_verdict (H : History) (W n : Nat) (entries : EntrySet)
    (otherChecks : Nat → Bool) (writes : Writes) (A B : EntrySet) (w : Nat)
    (present : A w = B w) :
    countInReplay H W n entries otherChecks writes A w =
      countInReplay H W n entries otherChecks writes B w := by
  simp only [countInReplay, counts, present]

theorem counting_converges (H : History) (W n : Nat) (otherChecks : Nat → Bool)
    (writes : Writes) (w : Nat) (A B X Y : List Nat)
    (entries : ∀ e, e ∈ A ↔ e ∈ B) (objects : ∀ e, e ∈ X ↔ e ∈ Y) :
    countInReplay H W n (entrySet A) otherChecks writes (entrySet X) w =
      countInReplay H W n (entrySet B) otherChecks writes (entrySet Y) w := by
  have eq : entrySet X = entrySet Y := by funext e; simp [entrySet, objects e]
  simp only [countInReplay, equal_received H W n A B entries, eq]

theorem counting_requires_signature (H : History) (W n : Nat) (entries : EntrySet)
    (otherChecks : Nat → Bool) (writes : Writes) (received : EntrySet) (w : Nat)
    (counted : countInReplay H W n entries otherChecks writes received w = true) :
    authorized H W n otherChecks writes w = true := by
  unfold countInReplay at counted
  exact ((counts_iff _ _ _ _).mp counted).2.1

theorem counting_requires_window (H : History) (W n : Nat) (entries : EntrySet)
    (otherChecks : Nat → Bool) (writes : Writes) (received : EntrySet) (w e : Nat)
    (counted : countInReplay H W n entries otherChecks writes received w = true)
    (kept : e ∈ (resolve H W n entries).kept) (retired : retires H (writes w) e = true) :
    (writes w).stored ≤ H.stored e + W := by
  unfold countInReplay at counted
  exact (timely_iff _ _ _ _).mp ((counts_iff _ _ _ _).mp counted).2.2 e kept retired

/-- A final kept replacement fixes the prefix through its storage time,
including ties. Any later replacement has a weaker deadline. This preserves
both verdicts without assuming that the later kept set only grows. -/
theorem replacement_window_final (H : History) (W e : Nat) (r later : Result) (obj : Write)
    (kept : e ∈ r.kept) (retired : retires H obj e = true)
    (finalPrefix : ∀ a, H.stored a ≤ H.stored e → (a ∈ r.kept ↔ a ∈ later.kept)) :
    timely H W later obj = timely H W r obj := by
  have laterKept := (finalPrefix e (Nat.le_refl _)).mp kept
  have transfer (source target : Result) (anchor : e ∈ source.kept)
      (prefixKept : ∀ a ∈ target.kept, H.stored a ≤ H.stored e → a ∈ source.kept)
      (h : timely H W source obj = true) : timely H W target obj = true := by
    apply (timely_iff _ _ _ _).mpr
    intro a ha hr
    by_cases old : H.stored a ≤ H.stored e
    · exact (timely_iff _ _ _ _).mp h a (prefixKept a ha old) hr
    · have bound := (timely_iff _ _ _ _).mp h e anchor retired
      omega
  apply Bool.eq_iff_iff.mpr
  exact ⟨transfer later r laterKept (fun a ha ht => (finalPrefix a ht).mp ha),
    transfer r later kept (fun a ha ht => (finalPrefix a ht).mpr ha)⟩

/-- Admission is final once the retiring replacement and its preceding entry
prefix are final. The object's presence and ordinary validation are fixed;
other writes and later entry decisions may differ arbitrarily. -/
theorem final_replacement_finalizes_write (H : History) (W n e w : Nat)
    (A B X Y : EntrySet) (otherChecks : Nat → Bool) (writes : Writes)
    (kept : e ∈ (resolve H W n A).kept) (retired : retires H (writes w) e = true)
    (finalPrefix : ∀ a, H.stored a ≤ H.stored e →
      (a ∈ (resolve H W n A).kept ↔ a ∈ (resolve H W n B).kept))
    (present : X w = Y w) :
    countInReplay H W n B otherChecks writes Y w =
      countInReplay H W n A otherChecks writes X w := by
  simp only [countInReplay, counts, present,
    replacement_window_final H W e _ _ (writes w) kept retired finalPrefix]

/-- Observing a different current signing key blocks even an identical retry
of an object whose recorded past still authenticates its old signature. -/
theorem replaced_signer_cannot_send (view : State) (obj : Write) (keys : MemberKeys)
    (binding : lookup view.keys obj.author = some keys) (different : keys.signing ≠ obj.signer) :
    writerCurrent view obj = false := by
  simp [writerCurrent, binding, different]

/-- Time here is monotonic seconds, separate from storage-time deadlines.
Complete catch-up begun after finality refuses the old signer. Earlier
catch-up expires before five minutes; its request settles within one day.
This bounds discovery, not admission, and covers retries and lost replies. -/
theorem send_gate_publication_bound (view : State) (obj : Write)
    (finalAt catchupStart sent published : Nat)
    (seesFinal : finalAt ≤ catchupStart → writerCurrent view obj = false)
    (sendGate : writerCurrent view obj = true)
    (fresh : sent < catchupStart + 300) (duration : published ≤ sent + 86400) :
    published < finalAt + 300 + 86400 := by
  have before : catchupStart < finalAt := by
    by_cases h : finalAt ≤ catchupStart
    · have stopped := seesFinal h
      simp [sendGate] at stopped
    · omega
  omega

namespace Examples
open Keyed.Examples
set_option maxRecDepth 8192
set_option maxHeartbeats 4000000

/-- One member, creation on day 0 and self-replacement on day 1. -/
def H : History := onlyAdmin

def writes : Writes
  | 0 => ⟨0, 100, 0, [0], 32⟩
  | _ => ⟨0, 102, 0, [0, 1], 34⟩

def counted (received : List Nat) (w : Nat) : Bool :=
  countInReplay H 30 2 (entrySet [0, 1]) (fun _ => true) writes (entrySet received) w

/-- With no entries in days 3–33, day 33 qualifies under §9's strict
horizon rule. Writes do not participate in that entry-only quiet test. -/
theorem replacement_final_window :
    H.stored 1 + 30 < 33 ∧
    (List.range 2).all (fun e => !(3 ≤ H.stored e && H.stored e ≤ 33)) = true ∧
    1 ∈ (resolve H 30 2 (entrySet [0, 1])).kept := by decide

/-- The old write is already stored after its deadline when replacement is
final. A later current-key write cannot change that verdict. -/
theorem final_replacement_keeps_late_write_dropped :
    counted [0] 0 = false ∧ counted [0, 1] 1 = true ∧ counted [0, 1] 0 = false := by decide

example : counted [0] 0 = counted [0, 1] 0 := by decide

theorem boundary_inclusive :
    timely H 30 (resolve H 30 2 (entrySet [0, 1])) { writes 0 with stored := 31 } = true ∧
    timely H 30 (resolve H 30 2 (entrySet [0, 1])) (writes 0) = false := by decide

def competingWrite : Write := ⟨1, 102, 1, [0, 1, 2, 3], 35⟩

/-- The admin winner retires the self-replacement's losing keys too. -/
theorem losing_binding_is_retired :
    countInReplay Keyed.Examples.H 30 5 (entrySet [0, 1, 2, 3, 4])
      (fun _ => true) (fun _ => competingWrite) (entrySet [0]) 0 = false := by decide

/-- An equal-payload winner installs the write's own key, so it is current. -/
theorem equal_payload_is_current :
    countInReplay sameKeys 30 5 (entrySet [0, 1, 2, 3, 4])
      (fun _ => true) (fun _ => competingWrite) (entrySet [0]) 0 = true := by decide

/-- A second replacement can enter or leave the non-final suffix without
changing the first replacement's window verdict, for every publication time. -/
def successive : History := { H with log := fun e =>
  if e = 2 then ⟨0, 102, 0, [0, 1], .replaceMemberKeys 0 ⟨104, 204⟩⟩ else H.log e }

theorem later_replacements_preserve_verdicts (stored : Nat) :
    timely successive 30 (resolve successive 30 3 (entrySet [0, 1, 2]))
      { writes 0 with stored } =
    timely successive 30 (resolve successive 30 3 (entrySet [0, 1]))
      { writes 0 with stored } := by
  apply replacement_window_final successive 30 1
  · decide
  · rfl
  · intro a old
    have bound : a ≤ 1 := old
    have cases : a = 0 ∨ a = 1 := by omega
    rcases cases with rfl | rfl <;> decide

/-- The exact deadline is an admission decision, not only a time check. -/
theorem admission_boundary_inclusive :
    countInReplay H 30 2 (entrySet [0, 1]) (fun _ => true)
      (fun w => { writes w with stored := 31 }) (entrySet [0]) 0 = true ∧
    counted [0, 1] 0 = false := by decide

/-- Catch-up rejects the old signer but permits the replacement signer;
admission still authenticates the old object's original recorded view. -/
theorem send_gate_checks_current_view :
    authorized H 30 2 (fun _ => true) writes 0 = true ∧
    writerCurrent (resolve H 30 2 (entrySet [0, 1])).state (writes 0) = false ∧
    writerCurrent (resolve H 30 2 (entrySet [0, 1])).state (writes 1) = true := by decide

/-- Missing a replacement permits a write that its receipt excludes. A
finality decision therefore needs the complete earlier entry prefix. -/
theorem unreceived_replacement_changes_admission :
    countInReplay H 30 2 (entrySet [0]) (fun _ => true) writes (entrySet [0]) 0 = true ∧
    counted [0] 0 = false := by decide

end Examples
end CovenStorelog.CurrentReplay.Keyed.OldKeyObjects
