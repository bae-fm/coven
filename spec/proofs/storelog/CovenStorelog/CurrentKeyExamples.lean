import CovenStorelog.CurrentKeys
import CovenStorelog.CurrentExamples

namespace CovenStorelog.CurrentKeyExamples
open CurrentKeys
open CurrentExamples (entry key)

set_option maxRecDepth 32768
set_option maxHeartbeats 1600000

/-- Ana and Ben are admins; Dan and Erin are members. Ana is outside Gifts.
Both removers read the same past. Ben then rotates Gifts, and Ana the store. -/
def removals : CurrentReplay.Log
  | 0 => entry 0 0 [] (.create "ana" (key 0))
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "dan")
  | 3 => entry 0 0 (List.range 3) (.addMember 3 .member "erin")
  | 4 => entry 1 1 (List.range 4) (.addDevice 1 1)
  | 5 => entry 2 2 (List.range 5) (.addDevice 2 2)
  | 6 => entry 3 3 (List.range 6) (.addDevice 3 3)
  | 7 => entry 1 1 (List.range 7) (.makeCircle 0 "Gifts" (key 1))
  | 8 => entry 1 1 (List.range 8) (.addToCircle 0 2)
  | 9 => entry 1 1 (List.range 9) (.addToCircle 0 3)
  | 10 => entry 0 0 (List.range 10) (.removeMember 2)
  | 11 => entry 1 1 (List.range 10) (.removeMember 3)
  | 12 => entry 1 1 (List.range 12) (.rotateKey (.circle 0) (key 2))
  | _ => entry 0 0 (List.range 13) (.rotateKey .store (key 3))

def history : CurrentReplay.History := ⟨removals, id, fun e => min e 10⟩
def received (n : Nat) := entrySet (List.range n)
def replay (n : Nat) := CurrentReplay.resolve history 30 14 (received n)
def intros (n : Nat) := receivedIntroductions history 30 14 (received n)

def originalStore : Introduction := ⟨0, .store, key 0⟩
def originalCircle : Introduction := ⟨7, .circle 0, key 1⟩
def newCircle : Introduction := ⟨12, .circle 0, key 2⟩
def newStore : Introduction := ⟨13, .store, key 3⟩

/-- Identity is an injective hash instance for executable examples. The
universal byte theorems quantify over the actual hash and its hypothesis. -/
def payload (i : Introduction) : Payload := ⟨i.identity, i.key.keyHash⟩
def copy (i : Introduction) (recipient : Nat) : Copy :=
  ⟨i.identity, recipient, some (payload i)⟩

def oldCopies : List Copy :=
  [copy originalStore 0, copy originalStore 1, copy originalStore 2, copy originalStore 3,
   copy originalCircle 1, copy originalCircle 2, copy originalCircle 3]

def allCopies : List Copy := oldCopies ++
  [copy newCircle 1, copy newStore 0, copy newStore 1]

def oldHeld := [payload originalStore, payload originalCircle]
def allHeld := oldHeld ++ [payload newCircle, payload newStore]

theorem concurrent_removals_apply :
    CurrentReplay.Valid removals 14 ∧
    concurrent (CurrentReplay.membershipLog removals) 10 11 = true ∧
    10 ∈ (replay 12).kept ∧ 11 ∈ (replay 12).kept ∧
    member (replay 12).state 2 = false ∧ member (replay 12).state 3 = false ∧
    admin (replay 12).state 0 = true ∧ admin (replay 12).state 1 = true ∧
    inCircle (replay 12).state 0 1 = true ∧
    inCircle (replay 12).state 0 0 = false := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

theorem both_removals_mint_nothing :
    (removals 10).action.introduction = none ∧
    (removals 11).action.introduction = none ∧
    keptIntroductions history 30 14 (received 12) = [originalCircle, originalStore] := by decide

theorem excluded_dan_or_erin_retires_each_audience :
    ∀ i ∈ [originalStore, originalCircle], ∀ excluded ∈ [2, 3],
      usable id (intros 12) (replay 12).state 1 [copy i excluded] oldHeld i = false := by
  simp only [List.mem_cons, List.not_mem_nil, or_false]
  intro i hi excluded he
  rcases hi with rfl | rfl <;> rcases he with rfl | rfl <;> decide

theorem removals_wait_without_fallback :
    select id (intros 12) (replay 12).state 1 (.complete oldCopies) oldHeld .store = .pending .key ∧
    select id (intros 12) (replay 12).state 1 (.complete oldCopies) oldHeld (.circle 0) = .pending .key ∧
    select id (intros 12) (replay 12).state 1 .incomplete oldHeld .store = .pending .listing := by decide

theorem remaining_members_rotate :
    12 ∈ (replay 14).kept ∧ 13 ∈ (replay 14).kept ∧
    usable id (intros 14) (replay 14).state 1 allCopies allHeld newCircle = true ∧
    usable id (intros 14) (replay 14).state 1 allCopies allHeld newStore = true ∧
    select id (intros 14) (replay 14).state 1 (.complete allCopies) allHeld (.circle 0) = .use newCircle ∧
    select id (intros 14) (replay 14).state 1 (.complete allCopies) allHeld .store = .use newStore := by decide

/-- A circle's first key cannot require membership before the circle exists:
creation followed by circle creation is already a counterexample. -/
def creation : CurrentReplay.History :=
  ⟨fun e => if e = 0 then entry 0 0 [] (.create "ana" (key 0))
    else entry 0 0 [0] (.makeCircle 0 "Gifts" (key 1)), id, id⟩

theorem creation_has_no_prior_circle_membership :
    CurrentReplay.Valid creation.log 2 ∧
    1 ∈ (CurrentReplay.resolve creation 30 2 (received 2)).kept ∧
    (creation.log 1).action.introduction = some (.circle 0, key 1) ∧
    inCircle (CurrentReplay.authorViews creation 30 2 2 1) 0 0 = false ∧
    inCircle (CurrentReplay.resolve creation 30 2 (received 2)).state 0 0 = true := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

example :
    1 ∈ (CurrentReplay.resolve creation 30 2 (received 2)).kept ∧
    inCircle (CurrentReplay.authorViews creation 30 2 2 1) 0 0 = false := by decide

def lastAdmins : CurrentReplay.History :=
  ⟨fun e => match e with
    | 0 => entry 0 0 [] (.create "ana" (key 0))
    | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
    | 2 => entry 0 0 [0, 1] (.removeMember 1)
    | _ => entry 1 1 [0, 1] (.removeMember 0), id, fun e => min e 2⟩

theorem last_admin_removal_drops :
    CurrentReplay.Valid lastAdmins.log 4 ∧
    2 ∈ (CurrentReplay.resolve lastAdmins 30 4 (received 4)).kept ∧
    3 ∈ (CurrentReplay.resolve lastAdmins 30 4 (received 4)).dropped ∧
    admin (CurrentReplay.resolve lastAdmins 30 4 (received 4)).state 0 = true := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

/-- Dan's forged box names the current store key but contains other bytes.
Ben cannot open it, even though he holds the real store key. -/
def forged : Copy := ⟨newStore.identity, 2, some ⟨newStore.identity, (key 99).keyHash⟩⟩

theorem recipient_rejects_forgery :
    acceptedCopy id (intros 14) 2 forged = none ∧ exposure id (intros 14) 2 forged = false ∧
    acceptedCopy id (intros 14) 1 forged = none ∧ exposure id (intros 14) 1 forged = true := by decide

def afterForgery : CurrentReplay.History :=
  { history with log := fun e =>
      if e = 14 then entry 1 1 (List.range 14) (.rotateKey .store (key 4))
      else removals e }

def fresh : Introduction := ⟨14, .store, key 4⟩

theorem forged_foreign_box_costs_a_rotation :
    CurrentReplay.Valid afterForgery.log 15 ∧
    select id (intros 14) (replay 14).state 1 (.complete allCopies) allHeld .store = .use newStore ∧
    select id (intros 14) (replay 14).state 1 (.complete (forged :: allCopies)) allHeld .store =
      .pending .key ∧
    (CurrentReplay.resolve afterForgery 30 15 (received 15)).state = (replay 14).state ∧
    14 ∈ (CurrentReplay.resolve afterForgery 30 15 (received 15)).kept ∧
    select id (receivedIntroductions afterForgery 30 15 (received 15))
      (CurrentReplay.resolve afterForgery 30 15 (received 15)).state 1
      (.complete (forged :: allCopies ++ [copy fresh 0, copy fresh 1]))
      (allHeld ++ [payload fresh]) .store = .use fresh ∧
    acceptedCopy id (intros 14) 1 (copy newStore 1) = some (payload newStore) := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

/-- A caller cannot smuggle mismatching local bytes into first-attempt custody. -/
theorem forged_custody_never_selected :
    select id [newStore] (replay 14).state 1 (.complete [])
      [⟨newStore.identity, (key 99).keyHash⟩] .store = .pending .key := by decide

end CovenStorelog.CurrentKeyExamples
