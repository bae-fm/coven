import CovenIO.DroppedRegistrations
import CovenStorelog.ExamplesMembers

/-! The two histories behind the dropped-registration rule, checked by
evaluation. Storage times are the entry numbers. -/

namespace CovenIO.Writers

open CovenStorelog (resolve entrySet)
open CovenStorelog.Examples (carol phoneAndRemoval)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- Carol's device (2) registered after her addition (4), which Ben's
concurrent removal of member 3 makes drop; her registration (6) drops with it.
No kept entry retires device 2, so under kept retirements alone it is known,
polled and never drained. The dropped registration retires it. -/
theorem carol_device_needs_dropped_registration :
    2 ∈ known carol (List.range 7) [] ∧
    keptRegisters carol (resolve carol 7 (entrySet (List.range 7))) 2 = false ∧
    (keptRetirements carol (resolve carol 7 (entrySet (List.range 7))) id).all
      (fun r => !r.devices.contains 2) = true ∧
    (droppedRetirements carol (resolve carol 7 (entrySet (List.range 7))) id) =
      [⟨6, 6, [2]⟩] := by decide

/-- Ben's phone (4) registers itself while Ana removes Ben. The kept removal
joins through the devices kept registrations give Ben, which do not include
the phone, so it too was never drained; its dropped registration retires it. -/
theorem phone_needs_dropped_registration :
    4 ∈ known phoneAndRemoval (List.range 5) [] ∧
    keptRegisters phoneAndRemoval (resolve phoneAndRemoval 5 (entrySet (List.range 5))) 4 = false ∧
    (keptRetirements phoneAndRemoval (resolve phoneAndRemoval 5 (entrySet (List.range 5))) id).all
      (fun r => !r.devices.contains 4) = true ∧
    (droppedRetirements phoneAndRemoval (resolve phoneAndRemoval 5 (entrySet (List.range 5))) id) =
      [⟨3, 3, [4]⟩] := by decide

/-- A phone write that its own view counts (§7.1) and that lands 30 days after
the phone's registration still counts; one second more is late. -/
theorem phone_write_window :
    Retired.writeAllowed
      (retirements phoneAndRemoval (resolve phoneAndRemoval 5 (entrySet (List.range 5))) id)
      false ⟨4, 1, 3 + month, []⟩ = true ∧
    Retired.writeAllowed
      (retirements phoneAndRemoval (resolve phoneAndRemoval 5 (entrySet (List.range 5))) id)
      false ⟨4, 1, 3 + month + 1, []⟩ = false := by decide

end CovenIO.Writers
