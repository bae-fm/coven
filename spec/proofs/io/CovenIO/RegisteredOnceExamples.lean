import CovenIO.RegisteredOnce
import CovenStorelog.ExamplesMembers

/-! The histories behind §10's register-once rule, checked by evaluation.
Storage times are the entry numbers unless a history gives its own. -/

namespace CovenIO.Writers

open CovenStorelog (resolve entrySet)
open CovenStorelog.Examples (carol entry)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- Without the rule: Carol's device registered once (6), the registration
dropped with her addition and retires the device. Ana adds Carol again (7) and
the same device registers again (8), having read everything. Replay keeps the
second registration, so the device is registered again and nothing retires it:
a reader that had already drained the device on the first drop never reads
entry 8, which lives in that device's own folder, while every other reader
keeps it. The history registers the id twice, which the rule forbids. -/
def carolAgain : CovenStorelog.Log
  | 7 => entry 0 0 (List.range 7) (.addMember 2 .member "initial")
  | 8 => entry 2 2 (List.range 8) (.addDevice 2 2)
  | w => carol w

theorem reregistration_lifts_retirement :
    CovenStorelog.validCheck carolAgain 9 = true ∧
    droppedRetirements carol (resolve carol 7 (entrySet (List.range 7))) id = [⟨6, 6, [2]⟩] ∧
    6 ∈ (resolve carolAgain 9 (entrySet (List.range 9))).dropped ∧
    keptRegisters carolAgain (resolve carolAgain 9 (entrySet (List.range 9))) 2 = true ∧
    (retirements carolAgain (resolve carolAgain 9 (entrySet (List.range 9))) id).all
      (fun r => !r.devices.contains 2) = true := by decide

theorem carol_again_registers_twice : ¬ RegistersOnce carolAgain 9 := by
  intro once
  have := once 6 8 2 (by decide) (by decide) (by decide) (by decide)
  omega

/-- A bounded check of `RegistersOnce`, for evaluating examples. -/
def registersOnceCheck (M : CovenStorelog.Log) (n : Nat) : Bool :=
  (List.range n).all fun a => (List.range n).all fun b =>
    (registered (M a)).isNone || registered (M a) != registered (M b) || a == b

theorem registersOnceCheck_sound (M : CovenStorelog.Log) (n : Nat)
    (h : registersOnceCheck M n = true) : RegistersOnce M n := by
  intro a b d ha hb ra rb
  have := List.all_eq_true.mp (List.all_eq_true.mp h a (List.mem_range.mpr ha)) b
    (List.mem_range.mpr hb)
  simpa [ra, rb] using this

/-- Carol's history itself registers each id once, so `final_registration_retires`
applies to it: device 2 stays retired by entry 6. -/
theorem carol_registers_once : RegistersOnce carol 7 :=
  registersOnceCheck_sound carol 7 (by decide)

/-- §9's late landing (T213): Ben's phone (device 4) attempts its registration
after reading creation and Ben's addition, but it lands at 33, more than 30
after Ana's circle (2), which it never read. The entry is permanently late, so
replay drops it, no kept entry registers the phone, and the late registration
retires it at its own storage time. Under the register-once rule the phone
never registers that id again, so it drains like any retired device. -/
def latePhone : CovenStorelog.Finality.History :=
  ⟨fun w => match w with
    | 0 => entry 0 0 [] (.create "initial")
    | 1 => entry 0 0 [0] (.addMember 1 .member "initial")
    | 2 => entry 0 0 [0, 1] (.makeCircle 5 "trip")
    | _ => entry 1 4 [0, 1] (.addDevice 1 4),
   fun w => if w = 3 then 33 else w,
   fun w => if w = 3 then 2 else w⟩

theorem late_phone_retired :
    CovenStorelog.Finality.tooLate latePhone 30 4 (fun _ => true) 3 = true ∧
    3 ∈ (CovenStorelog.ReplayPolicy.resolve latePhone 30 4 (entrySet (List.range 4))).dropped ∧
    keptRegisters latePhone.log
      (CovenStorelog.ReplayPolicy.resolve latePhone 30 4 (entrySet (List.range 4))) 4 = false ∧
    retirements latePhone.log
      (CovenStorelog.ReplayPolicy.resolve latePhone 30 4 (entrySet (List.range 4)))
      latePhone.stored = [⟨3, 33, [4]⟩] ∧
    registersOnceCheck latePhone.log 4 = true := by decide

end CovenIO.Writers
