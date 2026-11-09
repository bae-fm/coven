import CovenSequence.CopyOrder

/-! Histories of `CopyOrder`. The first is the counterexample to
`CopyPrecedesNeed`: §12.1's own Gifts recovery, followed to the letter. The
second is the races §12.1's order has to survive, which all hold. -/
namespace CovenSequence.CopyOrderExamples
open Keys CopyOrder

/-- The store's audience, and Gifts'. -/
def household : Nat := 0
def gifts : Nat := 1

def ana : Nat := 1
def ben : Nat := 2
def carol : Nat := 3
def dan : Nat := 4

def storeKey : Key Nat := ⟨household, 1⟩
def giftsKey : Key Nat := ⟨gifts, 2⟩

def nextStoreKey : Key Nat := ⟨household, 3⟩
def nextGiftsKey : Key Nat := ⟨gifts, 4⟩

/-- Ana is an admin outside Gifts. Carol made Gifts, added Ben, and wrote to it
at slot 11. Ben loses his devices, and Ana recovers him at slot 13, sending what
she holds: the store key. Carol's next write chose the Gifts key and lost slot
13 to the recovery, and her device stops before it reads why. Ana's next pass
replaces the store key the recovery exposed, at slots 14 to 17. Carol's device
comes back: a pass sends its writes first, so her write lands at 18, in the new
store key's slot but with the Gifts key of its first attempt (§6.3). The same
pass then replaces the Gifts key, at slots 19 to 21, and sends what she owes Ben
of the older one, at slot 22 (§12.1). -/
def recovery : Nat → CopyOrder.Slot Nat Nat
  | 1 => .entry ana storeKey (.create storeKey 100 10)
  | 2 => .copy storeKey ⟨storeKey, ana, 10⟩
  | 3 => .copy storeKey ⟨storeKey, ben, 20⟩
  | 4 => .entry ana storeKey (.addMember ben false 20)
  | 5 => .copy storeKey ⟨storeKey, carol, 30⟩
  | 6 => .entry ana storeKey (.addMember carol false 30)
  | 7 => .copy storeKey ⟨giftsKey, carol, 30⟩
  | 8 => .entry carol storeKey (.createCircle giftsKey 200)
  | 9 => .copy storeKey ⟨giftsKey, ben, 20⟩
  | 10 => .entry carol storeKey (.addToCircle gifts ben)
  | 11 => .writes storeKey [giftsKey]
  | 12 => .copy storeKey ⟨storeKey, ben, 21⟩
  | 13 => .entry ana storeKey (.replaceKeys ben 21)
  | 14 => .copy storeKey ⟨nextStoreKey, ana, 10⟩
  | 15 => .copy storeKey ⟨nextStoreKey, ben, 21⟩
  | 16 => .copy storeKey ⟨nextStoreKey, carol, 30⟩
  | 17 => .entry ana storeKey (.rotate nextStoreKey 101)
  | 18 => .writes nextStoreKey [giftsKey]
  | 19 => .copy nextStoreKey ⟨nextGiftsKey, ben, 21⟩
  | 20 => .copy nextStoreKey ⟨nextGiftsKey, carol, 30⟩
  | 21 => .entry carol nextStoreKey (.rotate nextGiftsKey 201)
  | 22 => .copy nextStoreKey ⟨giftsKey, ben, 21⟩
  | _ => .writes nextStoreKey []

theorem recovery_follows : Follows household recovery 22 := by
  refine ⟨⟨ana, storeKey, 100, 10, rfl, rfl, rfl⟩, ?_⟩
  have all : ∀ k, k < 23 → 3 ≤ k → legal household (keysAt recovery (k - 1))
      (rosterAt household recovery (k - 1)) (recovery k) = true := by decide +kernel
  exact fun k three below => all k (by omega) three

/-- **A member following the rules waits on a key.** Every slot of `recovery`
follows §12.1, yet at slot 18 Ben reads Gifts with sealing key 21, and the copy
of the Gifts key to it is at slot 22, above the slot that needs it. -/
theorem copy_precedes_need_fails :
    ¬ ∀ (seq : Nat → CopyOrder.Slot Nat Nat) (n : Nat), Follows household seq n →
      CopyPrecedesNeed household seq n := by
  intro claim
  obtain ⟨t, positive, below, copied⟩ := claim recovery 22 recovery_follows 18 (by omega)
    (by omega) giftsKey (by decide) ben 21 (by decide +kernel)
  have none : ∀ t, t < 18 → 0 < t →
      (recovery t).event ≠ .copy ⟨giftsKey, ben, 21⟩ := by decide
  exact none t below positive copied

/-- A device with pair `c` of member `m`, whose key scan ended at slot `scan`,
holds `key` when it reaches slot `s` exactly if a copy to `c` sits in the slots
it scanned or the slots it has read. -/
def holdsAt (seq : Nat → CopyOrder.Slot Nat Nat) (m c scan s : Nat) (key : Key Nat) : Bool :=
  decide ((⟨key, m, c⟩ : Route Nat Nat) ∈ (keysAt seq (max scan (s - 1))).copies)

/-- **The wait reaches below the replacement too.** Ben's new phone reads the
store's history with his new pair. If its key scan ends before Carol's copy
lands, it can't open her write at slot 11, whose copy to Ben's pair of the time
sits below it; a phone that scans after slot 22 can. A reader that can't open a
slot stops there (`keyless_stops`), so reading in order never brings it the
copy at slot 22. Scanning again from its stop once slot 22 has landed does
(`stuck_scan_finds_copy`): the scan's end moves from 13 to 22. -/
theorem early_scan_stops_below_replacement :
    holdsAt recovery ben 21 13 11 giftsKey = false ∧
    holdsAt recovery ben 21 21 18 giftsKey = false ∧
    holdsAt recovery ben 21 22 11 giftsKey = true ∧
    holdsAt recovery ben 20 0 11 giftsKey = true := by decide +kernel

def secondKey : Key Nat := ⟨household, 5⟩
def thirdKey : Key Nat := ⟨household, 6⟩

/-- Ben rotates the store key while Ana adds Dan: the addition lands among the
rotation's copies, and Ben, reading it before his next claim, sends Dan his too.
Then Dan replaces his keys among the next rotation's copies, after one of them
went to his old sealing key, and Ben sends one to his new one. -/
def races : Nat → CopyOrder.Slot Nat Nat
  | 1 => .entry ana storeKey (.create storeKey 100 10)
  | 2 => .copy storeKey ⟨storeKey, ana, 10⟩
  | 3 => .copy storeKey ⟨storeKey, ben, 20⟩
  | 4 => .entry ana storeKey (.addMember ben true 20)
  | 5 => .copy storeKey ⟨secondKey, ana, 10⟩
  | 6 => .copy storeKey ⟨storeKey, dan, 40⟩
  | 7 => .entry ana storeKey (.addMember dan false 40)
  | 8 => .copy storeKey ⟨secondKey, ben, 20⟩
  | 9 => .copy storeKey ⟨secondKey, dan, 40⟩
  | 10 => .entry ben storeKey (.rotate secondKey 201)
  | 11 => .copy storeKey ⟨thirdKey, ana, 10⟩
  | 12 => .copy storeKey ⟨thirdKey, dan, 40⟩
  | 13 => .copy storeKey ⟨storeKey, dan, 41⟩
  | 14 => .copy storeKey ⟨secondKey, dan, 41⟩
  | 15 => .entry dan storeKey (.replaceKeys dan 41)
  | 16 => .copy storeKey ⟨thirdKey, ben, 20⟩
  | 17 => .copy storeKey ⟨thirdKey, dan, 41⟩
  | 18 => .entry ben storeKey (.rotate thirdKey 202)
  | _ => .copy thirdKey ⟨thirdKey, ana, 10⟩

theorem races_follow : Follows household races 19 := by
  refine ⟨⟨ana, storeKey, 100, 10, rfl, rfl, rfl⟩, ?_⟩
  have all : ∀ k, k < 20 → 3 ≤ k → legal household (keysAt races (k - 1))
      (rosterAt household races (k - 1)) (races k) = true := by decide +kernel
  exact fun k three below => all k (by omega) three

/-- Dan's copy of the newest key sits below the first slot it seals. -/
theorem races_keep_order : ∃ t, 0 < t ∧ t < 19 ∧ (races t).event = .copy ⟨thirdKey, dan, 41⟩ :=
  store_copy_precedes_need races_follow (by omega) (by omega) (by decide) rfl (by decide +kernel)

/-- A rotation that kept the routes it first read lands without Dan's copy, and
isn't legal: the claim loop's rereading is what the order needs. -/
theorem stale_rotation_illegal :
    legal household (keysAt races 8) (rosterAt household races 8)
      (.entry ben storeKey (.rotate secondKey 201)) = false := by decide +kernel

end CovenSequence.CopyOrderExamples
