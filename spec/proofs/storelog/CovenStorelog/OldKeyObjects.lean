import CovenStorelog.MemberKeyExamples

/-! The retired-write reading: an otherwise valid old-key write counts if it
lands within the inclusive deadline, or a counting write has read it. The
least grounded reading prevents writes from vouching for each other without
any timely source. Entries retain §9's permanent deadline. -/
namespace CovenStorelog.CurrentReplay.Keyed.OldKeyObjects

structure Write where
  author : Nat
  signer : Nat
  device : Nat
  storeLogRead : List Nat
  writeRead : List Nat
  stored : Nat
  deriving DecidableEq, Repr

abbrev Writes := Nat → Write

/-- Ordinary schema/reset/content checks are a supplied predicate. This
function additionally checks the actual replay of the signed recorded past. -/
def authorized (H : History) (W n : Nat) (otherChecks : Nat → Bool) (writes : Writes) (w : Nat) : Bool :=
  let obj := writes w
  let view := (resolve H W n (entrySet obj.storeLogRead)).state
  otherChecks w && member view.membership obj.author &&
    lookup view.membership.devices obj.device == some obj.author &&
    (lookup view.keys obj.author).any (fun keys => keys.signing == obj.signer)

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

/-- Recorded write pasts are transitive. A chain of counting readers therefore
has a timely last witness that directly read every rescued predecessor. -/
def counts (valid timely : Nat → Bool) (read : Nat → Nat → Bool)
    (bound : Nat) (received : EntrySet) (w : Nat) : Bool :=
  received w && valid w && (timely w ||
    (List.range bound).any (fun v => received v && valid v && timely v && read v w))

def countInReplay (H : History) (W n : Nat) (entries : EntrySet) (otherChecks : Nat → Bool)
    (writes : Writes) (bound : Nat) (received : EntrySet) (w : Nat) : Bool :=
  let r := resolve H W n entries
  counts (authorized H W n otherChecks writes) (fun w => timely H W r (writes w))
    (fun v w => w ∈ (writes v).writeRead) bound received w

theorem counts_iff (valid timely : Nat → Bool) (read : Nat → Nat → Bool)
    (bound : Nat) (received : EntrySet) (w : Nat) :
    counts valid timely read bound received w = true ↔
      received w = true ∧ valid w = true ∧
      (timely w = true ∨ ∃ v, v < bound ∧ received v = true ∧ valid v = true ∧
        timely v = true ∧ read v w = true) := by
  simp [counts, List.any_eq_true, and_assoc]

theorem counting_reader (valid timely : Nat → Bool) (read : Nat → Nat → Bool)
    (bound : Nat) (received : EntrySet)
    (transitive : ∀ a b c, read a b = true → read b c = true → read a c = true)
    (v w : Nat) (vb : v < bound) (rw : received w = true) (vw : valid w = true)
    (cv : counts valid timely read bound received v = true) (readIt : read v w = true) :
    counts valid timely read bound received w = true := by
  obtain ⟨rv, vv, tv | ⟨seed, sb, rs, vs, ts, sv⟩⟩ := (counts_iff _ _ _ _ _ _).mp cv
  · exact (counts_iff _ _ _ _ _ _).mpr ⟨rw, vw, Or.inr ⟨v, vb, rv, vv, tv, readIt⟩⟩
  · exact (counts_iff _ _ _ _ _ _).mpr
      ⟨rw, vw, Or.inr ⟨seed, sb, rs, vs, ts, transitive seed v w sv readIt⟩⟩

/-- Every closed counting policy that counts timely valid writes must contain
this result. No circular or mutually invalid witness can create acceptance. -/
theorem least_counting_rule (valid timely : Nat → Bool) (read : Nat → Nat → Bool)
    (bound : Nat) (received candidate : EntrySet)
    (seed : ∀ w, received w = true → valid w = true → timely w = true → candidate w = true)
    (closed : ∀ v w, candidate v = true → received w = true → valid w = true →
      read v w = true → candidate w = true) (w : Nat)
    (counted : counts valid timely read bound received w = true) : candidate w = true := by
  obtain ⟨rw, vw, tw | ⟨v, _, rv, vv, tv, read⟩⟩ := (counts_iff _ _ _ _ _ _).mp counted
  · exact seed w rw vw tw
  · exact closed v w (seed v rv vv tv) rw vw read

theorem counting_converges (H : History) (W n : Nat) (otherChecks : Nat → Bool)
    (writes : Writes) (bound w : Nat) (A B X Y : List Nat)
    (entries : ∀ e, e ∈ A ↔ e ∈ B) (objects : ∀ e, e ∈ X ↔ e ∈ Y) :
    countInReplay H W n (entrySet A) otherChecks writes bound (entrySet X) w =
      countInReplay H W n (entrySet B) otherChecks writes bound (entrySet Y) w := by
  have eq : entrySet X = entrySet Y := by funext e; simp [entrySet, objects e]
  simp only [countInReplay, equal_received H W n A B entries, eq]

theorem rescue_never_bypasses_signature (H : History) (W n : Nat) (entries : EntrySet)
    (otherChecks : Nat → Bool) (writes : Writes) (bound : Nat) (received : EntrySet) (w : Nat)
    (counted : countInReplay H W n entries otherChecks writes bound received w = true) :
    authorized H W n otherChecks writes w = true := by
  unfold countInReplay at counted
  exact ((counts_iff _ _ _ _ _ _).mp counted).2.1

namespace Counterexample
open Examples
set_option maxRecDepth 8192
set_option maxHeartbeats 4000000

/-- One member, creation on day 0 and self-replacement on day 1. -/
def H : History := onlyAdmin

def writes : Writes
  | 0 => ⟨0, 100, 0, [0], [], 32⟩
  | _ => ⟨0, 102, 0, [0, 1], [0], 34⟩

def counted (received : List Nat) (w : Nat) : Bool :=
  countInReplay H 30 2 (entrySet [0, 1]) (fun _ => true) writes 2 (entrySet received) w

/-- With no entries in days 3–33, day 33 qualifies under §9's strict
horizon rule. Writes do not participate in that entry-only quiet test. -/
theorem replacement_final_window :
    H.stored 1 + 30 < 33 ∧
    (List.range 2).all (fun e => !(3 ≤ H.stored e && H.stored e ≤ 33)) = true ∧
    1 ∈ (resolve H 30 2 (entrySet [0, 1])).kept := by decide

/-- The old write is already stored after its deadline when replacement is
final. A future current-key reader makes it count without changing any entry. -/
theorem final_replacement_does_not_finalize_write :
    counted [0] 0 = false ∧ counted [0, 1] 1 = true ∧ counted [0, 1] 0 = true := by decide

example : counted [0] 0 ≠ counted [0, 1] 0 := by decide

theorem boundary_inclusive :
    timely H 30 (resolve H 30 2 (entrySet [0, 1])) { writes 0 with stored := 31 } = true ∧
    timely H 30 (resolve H 30 2 (entrySet [0, 1])) (writes 0) = false := by decide

def competingWrite : Write := ⟨1, 102, 1, [0, 1, 2, 3], [], 35⟩

/-- The admin winner retires the self-replacement's losing keys too. -/
theorem losing_binding_is_retired :
    countInReplay Examples.H 30 5 (entrySet [0, 1, 2, 3, 4])
      (fun _ => true) (fun _ => competingWrite) 1 (entrySet [0]) 0 = false := by decide

/-- An equal-payload winner installs the write's own key, so it is current. -/
theorem equal_payload_is_current :
    countInReplay sameKeys 30 5 (entrySet [0, 1, 2, 3, 4])
      (fun _ => true) (fun _ => competingWrite) 1 (entrySet [0]) 0 = true := by decide

/-- Without another write to read it, a late object cannot rescue itself. -/
theorem no_reader_no_rescue (valid timely : Nat → Bool) (read : Nat → Nat → Bool)
    (bound w : Nat) (late : timely w = false) :
    counts valid timely read bound (entrySet [w]) w = false := by
  have noWitness : (List.range bound).any
      (fun v => entrySet [w] v && valid v && timely v && read v w) = false := by
    apply List.any_eq_false.mpr
    intro v _
    by_cases eq : v = w
    · simp [eq, late]
    · simp [entrySet, eq]
  simp [counts, late, noWitness]

end Counterexample
end CovenStorelog.CurrentReplay.Keyed.OldKeyObjects
