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

/-- The circles an admission makes for the member it admits: each member's
own, and the new one a recovery gives Ben. -/
def benCircleKey : Key Nat := ⟨2, 7⟩
def carolCircleKey : Key Nat := ⟨3, 8⟩
def danCircleKey : Key Nat := ⟨4, 9⟩
def benNewCircleKey : Key Nat := ⟨5, 10⟩

/-- Ana is an admin outside Gifts. Carol made Gifts, added Ben, and wrote to it
at slot 8. Ben loses his devices, and Ana recovers him at slot 9, her
admission carrying what she holds: the store key, and the new circle's key,
which only Ben reads. Carol's next write chose the
Gifts key and lost slot 9 to the recovery, and her device stops before it
reads why. Ana's next pass replaces the store key the recovery exposed, at
slots 10 to 13. Carol's device comes back: a pass sends its writes first, so
her write lands at 14, in the new store key's slot but with the Gifts key of
its first attempt (§6.3). The same pass then replaces the Gifts key, at slots
15 to 17, and sends what she owes Ben of the older one, at slot 18 (§12.1). -/
def recovery : Nat → CopyOrder.Slot Nat Nat
  | 1 => .entry ana storeKey (.create storeKey 100 10 [])
  | 2 => .entry ana storeKey (.admit ben false 20 benCircleKey 300)
  | 3 => .entry ana storeKey (.admit carol false 30 carolCircleKey 301)
  | 4 => .copy storeKey ⟨giftsKey, carol, 30⟩
  | 5 => .entry carol storeKey (.createCircle giftsKey 200)
  | 6 => .copy storeKey ⟨giftsKey, ben, 20⟩
  | 7 => .entry carol storeKey (.addToCircle gifts ben)
  | 8 => .writes storeKey [giftsKey]
  | 9 => .entry ana storeKey (.recover ben 21 benNewCircleKey 302)
  | 10 => .copy storeKey ⟨nextStoreKey, ana, 10⟩
  | 11 => .copy storeKey ⟨nextStoreKey, ben, 21⟩
  | 12 => .copy storeKey ⟨nextStoreKey, carol, 30⟩
  | 13 => .entry ana storeKey (.rotate nextStoreKey 101)
  | 14 => .writes nextStoreKey [giftsKey]
  | 15 => .copy nextStoreKey ⟨nextGiftsKey, ben, 21⟩
  | 16 => .copy nextStoreKey ⟨nextGiftsKey, carol, 30⟩
  | 17 => .entry carol nextStoreKey (.rotate nextGiftsKey 201)
  | 18 => .copy nextStoreKey ⟨giftsKey, ben, 21⟩
  | _ => .writes nextStoreKey []

theorem recovery_follows : Follows household recovery 18 := by
  refine ⟨⟨ana, storeKey, 100, 10, [], rfl, rfl⟩, ?_⟩
  have all : ∀ k, k < 19 → 2 ≤ k → legal household (keysAt household recovery (k - 1))
      (rosterAt household recovery (k - 1)) (recovery k) = true := by decide +kernel
  exact fun k two below => all k (by omega) two

/-- **A member following the rules waits on a key.** Every slot of `recovery`
follows §12.1, yet at slot 14 Ben reads Gifts with sealing key 21, and the copy
of the Gifts key to it is at slot 18, above the slot that needs it. -/
theorem copy_precedes_need_fails :
    ¬ ∀ (seq : Nat → CopyOrder.Slot Nat Nat) (n : Nat), Follows household seq n →
      CopyPrecedesNeed household seq n := by
  intro claim
  obtain ⟨t, positive, below, copied⟩ := claim recovery 18 recovery_follows 14 (by omega)
    (by omega) giftsKey (by decide) ben 21 (by decide +kernel)
  have none : ∀ t, t < 14 → 0 < t →
      (⟨giftsKey, ben, 21⟩ : Route Nat Nat) ∉ carriedAt household recovery t := by
    decide +kernel
  exact none t below positive copied

/-- A device with pair `c` of member `m`, whose key scan ended at slot `scan`,
holds `key` when it reaches slot `s` exactly if a copy to `c` sits in the slots
it scanned or the slots it has read. -/
def holdsAt (seq : Nat → CopyOrder.Slot Nat Nat) (m c scan s : Nat) (key : Key Nat) : Bool :=
  decide ((⟨key, m, c⟩ : Route Nat Nat) ∈ (keysAt household seq (max scan (s - 1))).copies)

/-- **The wait reaches below the recovery too.** Ben's new phone reads the
store's history with his new pair. If its key scan ends before Carol's copy
lands, it can't open her write at slot 8, whose copy to Ben's pair of the time
sits below it; a phone that scans after slot 18 can. A reader that can't open a
slot stops there (`keyless_stops`), so reading in order never brings it the
copy at slot 18. Scanning again from its stop once slot 18 has landed does
(`stuck_scan_finds_copy`): the scan's end moves from 9 to 18. -/
theorem early_scan_stops_below_replacement :
    holdsAt recovery ben 21 9 8 giftsKey = false ∧
    holdsAt recovery ben 21 17 14 giftsKey = false ∧
    holdsAt recovery ben 21 18 8 giftsKey = true ∧
    holdsAt recovery ben 20 0 8 giftsKey = true := by decide +kernel

def secondKey : Key Nat := ⟨household, 5⟩
def thirdKey : Key Nat := ⟨household, 6⟩

/-- Ben rotates the store key while Ana admits Dan: the admission lands among
the rotation's copies, and Ben, reading it before his next claim, sends Dan his
too. Then Dan replaces his keys among the next rotation's copies, after one of
them went to his old sealing key, and Ben sends one to his new one. His
replacement carries his own circle's key too, which only he holds. -/
def races : Nat → CopyOrder.Slot Nat Nat
  | 1 => .entry ana storeKey (.create storeKey 100 10 [])
  | 2 => .entry ana storeKey (.admit ben true 20 benCircleKey 300)
  | 3 => .copy storeKey ⟨secondKey, ana, 10⟩
  | 4 => .entry ana storeKey (.admit dan false 40 danCircleKey 303)
  | 5 => .copy storeKey ⟨secondKey, ben, 20⟩
  | 6 => .copy storeKey ⟨secondKey, dan, 40⟩
  | 7 => .entry ben storeKey (.rotate secondKey 201)
  | 8 => .copy storeKey ⟨thirdKey, ana, 10⟩
  | 9 => .copy storeKey ⟨thirdKey, dan, 40⟩
  | 10 => .copy storeKey ⟨storeKey, dan, 41⟩
  | 11 => .copy storeKey ⟨secondKey, dan, 41⟩
  | 12 => .copy storeKey ⟨danCircleKey, dan, 41⟩
  | 13 => .entry dan storeKey (.replaceKeys dan 41)
  | 14 => .copy storeKey ⟨thirdKey, ben, 20⟩
  | 15 => .copy storeKey ⟨thirdKey, dan, 41⟩
  | 16 => .entry ben storeKey (.rotate thirdKey 202)
  | _ => .copy thirdKey ⟨thirdKey, ana, 10⟩

theorem races_follow : Follows household races 17 := by
  refine ⟨⟨ana, storeKey, 100, 10, [], rfl, rfl⟩, ?_⟩
  have all : ∀ k, k < 18 → 2 ≤ k → legal household (keysAt household races (k - 1))
      (rosterAt household races (k - 1)) (races k) = true := by decide +kernel
  exact fun k two below => all k (by omega) two

/-- Dan's copy of the newest key sits below the first slot it seals. -/
theorem races_keep_order :
    ∃ t, 0 < t ∧ t < 17 ∧ (⟨thirdKey, dan, 41⟩ : Route Nat Nat) ∈ carriedAt household races t :=
  store_copy_precedes_need races_follow (by omega) (by omega) (by decide) rfl (by decide +kernel)

/-- A rotation that kept the routes it first read lands without Dan's copy, and
isn't legal: the claim loop's rereading is what the order needs. -/
theorem stale_rotation_illegal :
    legal household (keysAt household races 5) (rosterAt household races 5)
      (.entry ben storeKey (.rotate secondKey 201)) = false := by decide +kernel

end CovenSequence.CopyOrderExamples
