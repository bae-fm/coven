import CovenIO.Writers
import CovenStorelog.ExamplesMembers

/-! §7.1's dropped-addition history against the discovery set. -/

namespace CovenIO.Writers

open CovenStorelog (resolve entrySet lookup)
open CovenStorelog.Examples (phoneAndRemoval)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- Ben's phone (device 4) registers itself while Ana removes Ben
(`Examples.phoneAndRemoval`). The phone's write reads its own
registration, which the replay of that view keeps, so the write counts (§7.1). A reader
that received the write before the removal probes the phone under the
replaced rule; one that received the removal first does not, and never
discovers that write. The discovery set holds the phone in both orders. -/
theorem replay_devices_miss_counting_write :
    lookup (resolve phoneAndRemoval 4 (entrySet (List.range 4))).state.devices 4 =
      some 1 ∧
    4 ∈ replayDevices (resolve phoneAndRemoval 4 (entrySet (List.range 4))) ∧
    4 ∉ replayDevices (resolve phoneAndRemoval 5 (entrySet (List.range 5))) ∧
    4 ∈ known phoneAndRemoval (List.range 4) [] ∧
    4 ∈ known phoneAndRemoval (List.range 5) [] := by decide

/-- That reader's idle pass: Ana's device 0, Ben's laptop 1 and the phone,
whose addition dropped, give two listings and nine GETs. -/
theorem dropped_registration_polled :
    known phoneAndRemoval (List.range 5) [] = [0, 1, 4] ∧
    (idle (fun _ _ => 1)
      (Retired.polling (fun _ => .active) (known phoneAndRemoval (List.range 5) []))).length =
      11 := by decide

end CovenIO.Writers
