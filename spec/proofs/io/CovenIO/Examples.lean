import CovenIO.Waiting
import CovenIO.Downloads

namespace CovenIO.Examples

def downloaded : Downloads.State := .unfinished [⟨0, [1, 2]⟩, ⟨1, [3, 4]⟩] .network
def diskFull : Downloads.Attempt :=
  ⟨.file 0 7, .ok (), true, true, false, true, ⟨2, [5, 6]⟩, 3, true, true⟩

theorem disk_failure_keeps_progress :
    Downloads.progress (Downloads.apply downloaded diskFull) = 4 ∧
    Downloads.pending (Downloads.apply downloaded diskFull) = some .disk ∧
    Downloads.pinned (Downloads.apply downloaded diskFull) = false ∧
    Downloads.completed (Downloads.apply downloaded diskFull) = false := by decide

example : Waiting.restart 100 1000 600 = 0 := by decide
example : Waiting.restart 100 (-1000) 600 = 600 := by decide

end CovenIO.Examples
