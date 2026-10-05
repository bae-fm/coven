import CovenMerge.Audience

/-!
# The spec's examples, run through the proven step and rules

Each theorem evaluates the model on the writes of one of the spec's examples,
in the arrival orders the example names, and checks the outcome the spec
states. Where removal rules are involved, their inputs are computed from the
merged state the step builds.
-/

namespace CovenMerge

/-! ## §8.1 and §8.2: note 42's title

Writes are named by number: 1 Ana's write 1, 4 Ana's write 4, 9 Ben's write
9, 2 Carol's write 2. Row 42, cell 0 is the title. -/

namespace Title

def M : Writes Nat Nat Nat where
  ts w := if w = 1 then 1 else if w = 4 then 13010 else if w = 9 then 13011 else 14000
  past w a := (w = 4 && a = 1) || (w = 9 && (a = 1 || a = 4)) || (w = 2 && a = 1)
  chg w r :=
    if r = 42 then
      if w = 1 then some ⟨.ins, 0, fun c => c = 0⟩
      else if w ∈ [4, 9, 2] then some ⟨.upd, 1, fun c => c = 0⟩ else none
    else none

def bensPhone : St Nat Nat Nat := [1, 4, 9, 2].foldl (step M) St.init
def carolsTablet : St Nat Nat Nat := [1, 2, 4, 9].foldl (step M) St.init

/-- Both orders end with Carol's title and one `coven_lost` row: Ben's
"Weekly groceries", replaced by Carol's write 2. -/
theorem example_8_1 :
    bensPhone.cell 42 0 = some 2 ∧ carolsTablet.cell 42 0 = some 2 ∧
    bensPhone.lost 42 0 9 = some (1, 2) ∧ carolsTablet.lost 42 0 9 = some (1, 2) ∧
    bensPhone.lost 42 0 4 = none ∧ carolsTablet.lost 42 0 4 = none ∧
    bensPhone.lost 42 0 1 = none ∧ carolsTablet.lost 42 0 1 = none := by decide

/-- On Carol's tablet "Groceries" is lost at first; Ben's write 9, which had
read it, takes it back out (§8.2). -/
theorem example_8_2 :
    ([1, 2, 4].foldl (step M) St.init).lost 42 0 4 = some (1, 2) ∧
    carolsTablet.lost 42 0 4 = none := by decide

end Title

/-! ## §8.3: note 43 deleted, edited and re-added

16 Ana's write 5 creates note 43; 18 Ana's write 7 deletes it; 19 Ana's
write 8 re-adds it; 10 Ben's write 10 edits its title at generation 1,
stamped after the re-add. -/

namespace Delete

def M : Writes Nat Nat Nat where
  ts w := if w = 16 then 1445 else if w = 18 then 1600 else if w = 19 then 1700 else 1800
  past w a := (w = 18 && a = 16) || (w = 19 && (a = 16 || a = 18)) || (w = 10 && a = 16)
  chg w r :=
    if r ≠ 43 then none
    else if w = 16 then some ⟨.ins, 0, fun c => c = 0⟩
    else if w = 18 then some ⟨.del, 1, fun _ => false⟩
    else if w = 19 then some ⟨.ins, 2, fun c => c = 0⟩
    else if w = 10 then some ⟨.upd, 1, fun c => c = 0⟩
    else none

/-- Whether Ben's edit arrives first or last: note 43 ends at generation 3
with Ana's title, generation 2's row names write 7, and Ben's value is lost,
replaced by write 7. -/
theorem example_8_3 :
    let late := [16, 18, 19, 10].foldl (step M) (St.init : St Nat Nat Nat)
    let early := [16, 10, 18, 19].foldl (step M) (St.init : St Nat Nat Nat)
    late.gen 43 = 3 ∧ early.gen 43 = 3 ∧
    late.cell 43 0 = some 19 ∧ early.cell 43 0 = some 19 ∧
    late.genWrite 43 2 = some 18 ∧ early.genWrite 43 2 = some 18 ∧
    late.lost 43 0 10 = some (1, 18) ∧ early.lost 43 0 10 = some (1, 18) := by decide

end Delete

/-! ## §8: todo 7 removed for two reasons

List 3 is deleted, and todo 7, in it under cascade, fails `start <= end`. -/

namespace TwoReasons

def I : Inputs Nat Nat where
  rows := [3, 7]
  present x := x = 7
  refs x := if x = 7 then [⟨3, true⟩] else []
  checkFails x := x = 7
  isAncestor _ := false
  keepRef _ := none
  sharedBase _ := true
  audienceFrom _ := none
  claims _ := []
  rank x := x

/-- Todo 7's `coven_lost` row names both rules. -/
theorem example_8 : (view I).removed 7 = true ∧
    (view I).rules 7 = [Rule.foreignKey, Rule.check] := by decide

end TwoReasons

/-! ## §8.4: a removed child comes back when re-pointed; set null

Rows 43 and 44 are notes, row 9 tag 9, row 6 link 6. Cell 0 of tag 9 is its
`note_id`, under cascade; cell 0 of link 6 is its `note_id`, under set null.
Write 1 creates both notes; Ana's write 7 deletes note 43; Ben's write 10 adds
tag 9 and link 6 on note 43; his write 11 moves tag 9 to note 44. -/

namespace FK

def M : Writes Nat Nat Nat where
  ts w := if w = 1 then 1 else if w = 7 then 1600 else if w = 10 then 1601 else 1605
  past w a := (w = 7 && a = 1) || (w = 10 && a = 1) || (w = 11 && (a = 1 || a = 10))
  chg w r :=
    if w = 1 ∧ (r = 43 ∨ r = 44) then some ⟨.ins, 0, fun _ => false⟩
    else if w = 7 ∧ r = 43 then some ⟨.del, 1, fun _ => false⟩
    else if w = 10 ∧ (r = 9 ∨ r = 6) then some ⟨.ins, 0, fun c => c = 0⟩
    else if w = 11 ∧ r = 9 then some ⟨.upd, 1, fun c => c = 0⟩
    else none

/-- The note and its generation each write points tag 9 or link 6 at. -/
def target (w : Nat) : Nat × Nat := if w = 10 then (43, 1) else (44, 1)

/-- A reference's merged value: the note it points at, or null under set null
once that note's generation was deleted. -/
def refValue (st : St Nat Nat Nat) (r : Nat) (setNull : Bool) : Option (Nat × Nat) :=
  match st.cell r 0 with
  | some w =>
    if setNull && decide (st.gen (target w).1 ≠ (target w).2) then none else some (target w)
  | none => none

/-- The rules' inputs, read from the merged state. -/
def inputs (st : St Nat Nat Nat) : Inputs Nat Nat where
  rows := [43, 44, 9, 6]
  present r := st.gen r % 2 == 1
  refs r :=
    if r = 9 ∨ r = 6 then
      match refValue st r (r = 6) with
      | some (p, g) => [⟨p, decide (st.gen p ≠ g)⟩]
      | none => []
    else []
  checkFails _ := false
  isAncestor _ := false
  keepRef _ := none
  sharedBase _ := true
  audienceFrom _ := none
  claims _ := []
  rank r := r

/-- Carol's tablet removes tag 9 once write 10 arrives, and puts it back when
write 11 arrives; Ben's phone never removes it. Link 6 stays, its reference
null, whichever order the writes arrived in. -/
theorem example_8_4 :
    (device M inputs [1, 7, 10]).view.removed 9 = true ∧
    (device M inputs [1, 7, 10]).removedLost 9 0 = some (10, [Rule.foreignKey]) ∧
    (device M inputs [1, 7, 10, 11]).view.shown 9 = true ∧
    (device M inputs [1, 10, 11, 7]).view.shown 9 = true ∧
    (device M inputs [1, 7, 10, 11]).view.removed 9 = false ∧
    (device M inputs [1, 10, 11, 7]).view.removed 9 = false ∧
    (device M inputs [1, 7, 10, 11]).view.shown 6 = true ∧
    (device M inputs [1, 10, 11, 7]).view.shown 6 = true ∧
    refValue ([1, 7, 10, 11].foldl (step M) St.init) 6 true = none ∧
    refValue ([1, 10, 11, 7].foldl (step M) St.init) 6 true = none := by decide

end FK

/-! ## §8.5: a key change is a delete plus an insert

Row 10 is the tag "urgent", row 11 "important"; rows 20, 21 and 22 are the
`note_tags` rows (42, "urgent"), (42, "important") and (44, "urgent"), each
pointing at its tag under cascade.

* write 1: insert "urgent" and (42, "urgent").
* write 2, Ana, had read write 1: rename "urgent" to "important": delete rows
  10 and 20, insert rows 11 and 21.
* write 3, Ben, had read write 1 only: insert (44, "urgent"). -/

namespace KeyChange

def M : Writes Nat Nat Nat where
  ts w := if w = 1 then 1 else if w = 2 then 1600 else 1601
  past w a := (w = 2 && a = 1) || (w = 3 && a = 1)
  chg w r :=
    if w = 1 ∧ (r = 10 ∨ r = 20) then some ⟨.ins, 0, fun c => c = 0⟩
    else if w = 2 ∧ (r = 10 ∨ r = 20) then some ⟨.del, 1, fun _ => false⟩
    else if w = 2 ∧ (r = 11 ∨ r = 21) then some ⟨.ins, 0, fun c => c = 0⟩
    else if w = 3 ∧ r = 22 then some ⟨.ins, 0, fun c => c = 0⟩
    else none

/-- The tag each `note_tags` row points at, at generation 1. -/
def tagOf (r : Nat) : Nat := if r = 21 then 11 else 10

def inputs (st : St Nat Nat Nat) : Inputs Nat Nat where
  rows := [10, 11, 20, 21, 22]
  present r := st.gen r % 2 == 1
  refs r := if r ≥ 20 then [⟨tagOf r, decide (st.gen (tagOf r) ≠ 1)⟩] else []
  checkFails _ := false
  isAncestor _ := false
  keepRef _ := none
  sharedBase _ := true
  audienceFrom _ := none
  claims _ := []
  rank r := r

/-- Every order ends with note 42 tagged "important", and Ben's (44,
"urgent") removed and recorded in `coven_lost`. -/
theorem example_8_5_key :
    (device M inputs [1, 2, 3]).view.shown 21 = true ∧
    (device M inputs [1, 3, 2]).view.shown 21 = true ∧
    (device M inputs [1, 2, 3]).view.rules 22 = [Rule.foreignKey] ∧
    (device M inputs [1, 3, 2]).view.rules 22 = [Rule.foreignKey] := by decide

end KeyChange

/-! ## §8.5: a claim dates from the latest write that set its columns

Notes are unique by `(folder, title)`. Cell 0 is the folder, cell 1 the title.
Folders: 1 Home, 2 Work. Titles: 5 "Draft", 7 "Plan".

* write 0, 09:00: note 2 is "Draft" in Home.
* write 1, Ana, 10:00: add note 1, "Plan" in Work.
* write 2, Ben, 11:00, had read write 0: rename note 2 to "Plan".
* write 3, Carol, 12:00, had read write 0: move note 2 to Work. -/

namespace Stamp

def M : Writes Nat Nat Nat where
  ts w := if w = 0 then 900 else if w = 1 then 1000 else if w = 2 then 1100 else 1200
  past w a := (w = 2 || w = 3) && a = 0
  chg w r :=
    if w = 0 ∧ r = 2 then some ⟨.ins, 0, fun _ => true⟩
    else if w = 1 ∧ r = 1 then some ⟨.ins, 0, fun _ => true⟩
    else if w = 2 ∧ r = 2 then some ⟨.upd, 1, fun c => c = 1⟩
    else if w = 3 ∧ r = 2 then some ⟨.upd, 1, fun c => c = 0⟩
    else none

/-- The value each write sets in each cell. -/
def val (w r c : Nat) : Nat :=
  if r = 2 ∧ w = 0 then (if c = 0 then 1 else 5)
  else if r = 2 ∧ w = 2 then 7
  else if r = 2 ∧ w = 3 then 2
  else if c = 0 then 2 else 7

def claimOf (st : St Nat Nat Nat) (r : Nat) : List (Claim Nat) :=
  match st.cell r 0, st.cell r 1 with
  | some a, some b => [⟨0, val a r 0 * 100 + val b r 1, max (M.ts a) (M.ts b)⟩]
  | _, _ => []

def inputs (st : St Nat Nat Nat) : Inputs Nat Nat where
  rows := [1, 2]
  present r := st.gen r % 2 == 1
  refs _ := []
  checkFails _ := false
  isAncestor _ := false
  keepRef _ := none
  sharedBase _ := true
  audienceFrom _ := none
  claims := claimOf st
  rank r := r

/-- Note 2's claim dates from 12:00, note 1's from 10:00: note 1 keeps "Plan"
in Work, in either arrival order. -/
theorem example_8_5_stamp :
    (device M inputs [0, 1, 2, 3]).view.shown 1 = true ∧
    (device M inputs [0, 1, 2, 3]).view.rules 2 = [Rule.unique] ∧
    (device M inputs [0, 3, 2, 1]).view.shown 1 = true ∧
    (device M inputs [0, 3, 2, 1]).view.rules 2 = [Rule.unique] := by decide

end Stamp

/-! ## §8.5: when a unique loser comes back

Notes are unique by title, and a sub-note points at its parent under
cascade. -/

namespace Comeback

/-- Note 2, "Plan" from 10:00, is a sub-note of note 1, "Plan" from 11:00. -/
def subNote : Inputs Nat Nat where
  rows := [1, 2]
  present _ := true
  refs x := if x = 2 then [⟨1, false⟩] else []
  checkFails _ := false
  isAncestor _ := false
  keepRef _ := none
  sharedBase _ := true
  audienceFrom _ := none
  claims x := if x = 1 then [⟨0, 7, 1100⟩] else [⟨0, 7, 1000⟩]
  rank x := x

/-- Note 1 loses "Plan" and is removed; note 2 goes with it by cascade; note
1 stays removed. -/
theorem example_8_5_subnote :
    (view subNote).rules 1 = [Rule.unique] ∧ (view subNote).rules 2 = [Rule.foreignKey] := by
  decide

/-- Note 2, "Plan" from 10:00, is a sub-note of note 3, "Ideas" from 12:00;
note 4 is "Ideas" from 09:00; note 1 is "Plan" from 11:00. Note 3 loses to
note 4, note 2 goes with it, and note 1, which lost to note 2 in the same
judgment, stays removed. -/
def otherLoser : Inputs Nat Nat where
  rows := [1, 2, 3, 4]
  present _ := true
  refs x := if x = 2 then [⟨3, false⟩] else []
  checkFails _ := false
  isAncestor _ := false
  keepRef _ := none
  sharedBase _ := true
  audienceFrom _ := none
  claims x :=
    if x = 1 then [⟨0, 7, 1100⟩] else if x = 2 then [⟨0, 7, 1000⟩]
    else if x = 3 then [⟨0, 8, 1200⟩] else [⟨0, 8, 900⟩]
  rank x := x

theorem example_8_5_step3 :
    (view otherLoser).rules 1 = [Rule.unique] ∧ (view otherLoser).rules 2 = [Rule.foreignKey] ∧
    (view otherLoser).rules 3 = [Rule.unique] ∧ (view otherLoser).shown 4 = true := by decide

end Comeback

/-! ## §8.6: a CHECK failure clears

Row 50 checks `start <= end`: cell 0 is start, cell 1 end.

* write 0: insert row 50, start 5, end 12.
* write 1, Ana, had read write 0: start 10.
* write 2, Ben, had read write 0: end 8.
* write 3, Ben, had read writes 0 and 2: end 20. -/

namespace Check

def M : Writes Nat Nat Nat where
  ts w := w + 1
  past w a := (w = 1 && a = 0) || (w = 2 && a = 0) || (w = 3 && (a = 0 || a = 2))
  chg w r :=
    if r ≠ 50 then none
    else if w = 0 then some ⟨.ins, 0, fun _ => true⟩
    else if w = 1 then some ⟨.upd, 1, fun c => c = 0⟩
    else some ⟨.upd, 1, fun c => c = 1⟩

def val (w c : Nat) : Nat :=
  if w = 0 then (if c = 0 then 5 else 12) else if w = 1 then 10 else if w = 2 then 8 else 20

def inputs (st : St Nat Nat Nat) : Inputs Nat Nat where
  rows := [50]
  present r := st.gen r % 2 == 1
  refs _ := []
  checkFails r :=
    match st.cell r 0, st.cell r 1 with
    | some a, some b => decide (val b 1 < val a 0)
    | _, _ => false
  isAncestor _ := false
  keepRef _ := none
  sharedBase _ := true
  audienceFrom _ := none
  claims _ := []
  rank r := r

/-- After Ana's start and Ben's end the row is removed; Ben's end 20 puts it
back, whichever order the writes arrived in. -/
theorem example_8_6 :
    (device M inputs [0, 1, 2]).view.rules 50 = [Rule.check] ∧
    (device M inputs [0, 2, 1]).view.rules 50 = [Rule.check] ∧
    (device M inputs [0, 1, 2, 3]).view.shown 50 = true ∧
    (device M inputs [0, 2, 3, 1]).view.shown 50 = true := by decide

end Check

/-! ## §14.2: an ancestor kept by a new child

Label 0 is an ancestor. Todo 1 was its last shared keeper, and Ana moved it
to her device only; Ben, concurrently, added shared todo 2 wearing the label.
Dan's device no longer has todo 1; Ana's still has it, on her device only. -/

namespace Kept

def G (todo1Present : Bool) : Inputs Nat Nat where
  rows := [0, 1, 2]
  present x := if x = 1 then todo1Present else true
  refs _ := []
  checkFails _ := false
  isAncestor x := x = 0
  keepRef x := if x = 1 ∨ x = 2 then some ⟨0, false⟩ else none
  sharedBase x := x ≠ 1
  audienceFrom _ := none
  claims _ := []
  rank x := x

/-- Both devices show the label. -/
theorem example_14_2 : (view (G false)).shown 0 = true ∧ (view (G true)).shown 0 = true := by
  decide

end Kept

end CovenMerge
