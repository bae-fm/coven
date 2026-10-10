import CovenStorelogData.Delivery

namespace CovenStorelogData.DeliveryExamples

open CovenStorelog
open CovenStorelog.Examples (entry)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- Ana has a phone and a tablet. Ben removes Ana from Gifts, then deletes
Gifts without having downloaded Carol's intervening row. Ana's tablet's earlier
concurrent circle removal of Ben makes both Ben entries drop. -/
def log : Log
  | 0 => entry 0 0 [] (.create "S3")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "S3")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .admin "S3")
  | 3 => entry 1 1 [0, 1, 2] (.addDevice 1 1)
  | 4 => entry 2 2 [0, 1, 2, 3] (.addDevice 2 2)
  | 5 => entry 0 0 (List.range 5) (.addDevice 0 3)
  | 6 => entry 0 0 (List.range 6) (.makeCircle 0 "Gifts")
  | 7 => entry 0 0 (List.range 7) (.addToCircle 0 1)
  | 8 => entry 0 0 (List.range 8) (.addToCircle 0 2)
  | 9 => entry 0 3 (List.range 9) (.removeFromCircle 0 1)
  | 10 => entry 1 1 (List.range 9) (.removeFromCircle 0 0)
  | 11 => entry 1 1 (List.range 9 ++ [10]) (.rotateKey (.circle 0) 2)
  | _ => entry 1 1 (List.range 9 ++ [10, 11]) (.deleteCircle 0)

theorem valid_log : Valid log 13 := validCheck_sound _ _ (by decide)
theorem causal_orders : CausalOrder log (List.range 9 ++ [10, 11, 12, 9]) ∧
    CausalOrder log (List.range 13) := by
  constructor
  · apply causalCheck_sound _ .nil; decide
  · exact full_history_causal log 13 valid_log

def row : Row := ⟨0, 0, .circle 0⟩
def writes : CovenMerge.Writes (Fin 1) Row Unit where
  ts _ := 1
  past _ _ := false
  chg _ r := if r = row then some ⟨.ins, 0, fun _ => true⟩ else none

theorem valid_writes : CovenMerge.Valid writes where
  ts_inj a b _ := Subsingleton.elim a b
  past_ts _ _ h := by cases h
  gen_seen w r ch h := by
    simp only [writes] at h
    split at h <;> cases h
    exact Or.inl rfl
  parity w r ch h := by
    simp only [writes] at h
    split at h <;> cases h
    decide

def schema : Schema (Fin 1) Unit Nat := ⟨[row], fun _ _ => [], fun _ _ => false, fun _ _ => []⟩
def header : Key := ⟨0, .store⟩
def key : Key := ⟨11, .circle 0⟩
def copies : Copies := [(header, 1), (header, 2), (⟨6, .circle 0⟩, 1), (⟨6, .circle 0⟩, 2)] ++
  introductionCopies log (List.range 9 ++ [10, 11])
def result := resolve log 13 (entrySet (List.range 13))
def shared := redistribute log result (copies ++ introductionCopies log [9])

def common : Reader (Fin 1) Unit :=
  ⟨⟨⟨entrySet (List.range 9), resolve log 13 (entrySet (List.range 9))⟩,
    CovenMerge.St.init⟩, [], false⟩
def outside := [10, 11].foldl (receiveEntry writes log 13 0) common
def skipped := receiveWrite writes log 0 0 copies header [key] outside 0
def deleted := receiveEntry writes log 13 0 skipped 12
def restored := receiveEntry writes log 13 0 deleted 9
def phone := receiveWrite writes log 0 0 shared header [key] restored 0
def tabletEntries := [9, 10, 11, 12].foldl (receiveEntry writes log 13 0) common
def tablet := receiveWrite writes log 0 3 shared header [key] tabletEntries 0

theorem same_received_entries : phone.state.log.received = tablet.state.log.received := by
  funext e
  change (decide (e = 9) || (decide (e = 12) || (decide (e = 11) ||
    (decide (e = 10) || decide (e ∈ List.range 9))))) =
    (decide (e = 12) || (decide (e = 11) || (decide (e = 10) ||
    (decide (e = 9) || decide (e ∈ List.range 9)))))
  simp [Bool.or_assoc, Bool.or_comm]

theorem reachable_skip_and_return :
    writeAuthority log 13 ⟨2, 2, List.range 9 ++ [10, 11]⟩ = true ∧
    part log outside.state.log.result copies key 0 = .skip ∧
    skipped.consumed = [0] ∧
    deletedCircle log deleted.state.log.result 0 = true ∧
    result.dropped = [12, 10] ∧
    deletedCircle log phone.state.log.result 0 = false ∧
    inCircle phone.state.log.result.state 0 0 = true ∧
    (key, 0) ∈ shared ∧ phone.pendingReload = false := by decide

/-- Same entries, same consumed write, same readable audience and keys, but
different rows. The lost value is not recorded on the phone. -/
theorem convergence_counterexample :
    phone.state.log.result = tablet.state.log.result ∧
    phone.consumed = tablet.consumed ∧
    (observe schema writes log phone.state).view.shown row = false ∧
    (observe schema writes log tablet.state).view.shown row = true ∧
    (observe schema writes log phone.state).losses row none = none := by decide

example : (observe schema writes log phone.state).view.shown row ≠
    (observe schema writes log tablet.state).view.shown row := by decide

theorem another_download_changes_nothing :
    receiveWrite writes log 0 0 shared header [key] phone 0 = phone :=
  consumed_write_stays_skipped writes log 0 0 shared header [key] phone 0 (by decide)

end CovenStorelogData.DeliveryExamples
