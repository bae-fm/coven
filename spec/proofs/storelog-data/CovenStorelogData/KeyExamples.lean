import CovenStorelogData.Keys

namespace CovenStorelogData.KeyExamples

open CovenStorelog
open CovenStorelog.Examples (entry)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- Two admins on S3, where either can remove the other. Ana's removal has
the earlier stamp; Ben has already used his own removal's key for a write. -/
def log : Log
  | 0 => entry 0 0 [] (.create "ana-S3")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben-S3")
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 0 0 [0, 1, 2] (.removeMember 1 [])
  | _ => entry 1 1 [0, 1, 2] (.removeMember 0 [])

theorem valid_log : Valid log 5 := validCheck_sound _ _ (by decide)

def result : Result := resolve log 5 (entrySet (List.range 5))
def key : Key := ⟨4, .store⟩

/-- Initial-key sharing to Ben accompanies his addition. All fresh removal
keys have precisely the pre-publication recipients computed from their views. -/
def copies : Copies := (⟨0, .store⟩, 1) :: introductionCopies log (List.range 5)

theorem addition_shares_initial_key :
    shareAddition log 1 (introductionCopies log [0]) =
      some [(⟨0, .store⟩, 0), (⟨0, .store⟩, 1)] := by decide

theorem both_removals_initially_kept :
    (resolve log 5 (entrySet [0, 1, 2, 3])).dropped = [] ∧
    (resolve log 5 (entrySet [0, 1, 2, 4])).dropped = [] := by decide

/-- The write is authorized in Ben's past, although the receiver's current
replay removes him. Its header and store part can both use key 4. -/
theorem write_counts :
    writeAuthority log 5 ⟨1, 1, [0, 1, 2, 4]⟩ = true ∧
    result.dropped = [4] ∧ member result.state 0 = true ∧
    member result.state 1 = false ∧ (key, 1) ∈ copies ∧ (key, 0) ∉ copies := by decide

theorem no_running_holder :
    redistribute log result copies = copies ∧
    running result.state 1 1 = false ∧ mustRead log result key 0 = true := by decide

/-- Counterexample to unconditional readability: even an unbounded number
of successful redistribution rounds cannot give Ana this required key. -/
theorem readability_counterexample : ∀ n,
    (key, 0) ∉ rounds log result n copies :=
  unavailable_forever log result copies no_running_holder.1 key 0 (by decide)

example : part log result copies key 0 = .wait := by decide

end CovenStorelogData.KeyExamples
