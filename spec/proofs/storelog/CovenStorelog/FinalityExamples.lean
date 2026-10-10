import CovenStorelog.Finality
import CovenStorelog.ExamplesSupport

namespace CovenStorelog.Finality.Examples

set_option maxRecDepth 32768
set_option maxHeartbeats 16000000

/-- Turn bounded, kernel-decidable timing checks into the history assumptions. -/
theorem finite_valid (H : History) (n : Nat) (hc : CovenStorelog.Valid H.log n)
    (unique : ∀ a b : Fin n, H.stored a = H.stored b → a = b)
    (attempt : ∀ w : Fin n, H.attempted w ≤ H.stored w)
    (online : ∀ w a : Fin n, hadRead H.log w a = true ↔ H.stored a < H.attempted w) :
    Valid H n := by
  refine ⟨hc, ?_, ?_, ?_⟩
  · intro a b ha hb he
    exact congrArg Fin.val (unique ⟨a, ha⟩ ⟨b, hb⟩ he)
  · intro w hw; exact attempt ⟨w, hw⟩
  · intro w a hw ha; exact online ⟨w, hw⟩ ⟨a, ha⟩

/-- One member, two devices, two different store resets. Both attempts
read creation before either reset landed. The earlier author stamp wins. -/
def resets : Log
  | 0 => ⟨0, 0, [], .create "initial"⟩
  | 1 => ⟨0, 1, [0], .reset ⟨.store, 1⟩⟩
  | _ => ⟨0, 2, [0], .reset ⟨.store, 2⟩⟩

def history (lastLanding : Nat) : History :=
  ⟨resets, (fun w => if w = 0 then 0 else if w = 1 then 1 else lastLanding),
    (fun w => if w = 0 then 0 else 1)⟩

theorem resets_valid : CovenStorelog.Valid resets 3 :=
  validCheck_sound _ _ (by decide)

theorem history_valid (lastLanding : Nat) (hl : 1 < lastLanding) : Valid (history lastLanding) 3 := by
  refine ⟨resets_valid, ?_, ?_, ?_⟩
  · intro a b ha hb he
    have ha' : a = 0 ∨ a = 1 ∨ a = 2 := by omega
    have hb' : b = 0 ∨ b = 1 ∨ b = 2 := by omega
    rcases ha' with rfl | rfl | rfl <;> rcases hb' with rfl | rfl | rfl <;>
      simp [history] at he ⊢ <;> omega
  · intro w hw
    have hw' : w = 0 ∨ w = 1 ∨ w = 2 := by omega
    rcases hw' with rfl | rfl | rfl <;> simp [history] <;> omega
  · intro w a hw ha
    have hw' : w = 0 ∨ w = 1 ∨ w = 2 := by omega
    have ha' : a = 0 ∨ a = 1 ∨ a = 2 := by omega
    rcases hw' with rfl | rfl | rfl <;> rcases ha' with rfl | rfl | rfl <;>
      simp [hadRead, history, resets] <;> omega

theorem causal_deliveries : CausalOrder resets [0, 2] ∧
    CausalOrder resets [0, 2, 1] ∧ CausalOrder resets [0, 1, 2] := by
  constructor
  · exact causalCheck_sound _ .nil (by decide)
  · constructor <;> exact causalCheck_sound _ .nil (by decide)

/-- The storage history is quiet at day 32. Device receipt of A can still be
delayed beyond day 32; B flips even though both landings are already old. -/
theorem delayed_delivery_counterexample :
    Valid (history 2) 3 ∧
    finalSet (history 2) 30 3 (atTime (history 2) 32) 32 2 = true ∧
    finalSet (history 2) 30 3 (entrySet [0, 2]) 32 2 = true ∧
    tooLate (history 2) 30 3 (entrySet [0, 1, 2]) 1 = false ∧
    tooLate (history 2) 30 3 (entrySet [0, 1, 2]) 2 = false ∧
    2 ∈ (resolve (history 2) 30 3 (entrySet [0, 2])).kept ∧
    2 ∉ (resolve (history 2) 30 3 (entrySet [0, 2, 1])).kept ∧
    2 ∈ (resolve (history 2) 30 3 (entrySet [0, 2, 1])).dropped := by
  refine ⟨history_valid 2 (by decide), ?_⟩
  decide

/-- Incomplete evidence cannot decide that an entry passes rule 1. Storage
already contains A at day 1; B's retry at day 32 missed it. -/
theorem missing_drop_witness :
    Valid (history 32) 3 ∧
    tooLate (history 32) 30 3 (entrySet [0, 2]) 2 = false ∧
    tooLate (history 32) 30 3 (entrySet [0, 2, 1]) 2 = true := by
  refine ⟨history_valid 32 (by decide), ?_⟩
  decide

/-- Strictly more than W is dropped; exactly W survives. A late entry at the
window's open lower boundary does not prevent finality. -/
theorem exact_boundaries :
    tooLate (history 31) 30 3 (entrySet [0, 1, 2]) 2 = false ∧
    tooLate (history 32) 30 3 (entrySet [0, 1, 2]) 2 = true ∧
    quiet (history 2) 30 3 (fun _ => true) 31 = false ∧
    quiet (history 2) 30 3 (fun _ => true) 32 = true ∧
    finalSet (history 2) 30 3 (fun _ => true) 32 2 = true := by decide

/-- Arrival may reverse author timestamps, and storage may differ from both.
After receiving the cutoff prefix, both dispositions survive every future
entry in every finite valid continuation, by `stability`. -/
theorem arrived_orders_agree :
    [0, 2, 1].foldl (receive (history 2) 30 3 32) (initial (history 2) 30 3 32) =
      [0, 1, 2].foldl (receive (history 2) 30 3 32) (initial (history 2) 30 3 32) := by
  apply agreement
  intro w
  simp only [List.mem_cons, List.not_mem_nil, or_false]
  omega

/-- Ana and Ben are admins. E changes Ben's access; B removes Ben. C read E
but missed B, and removes Ana. Author order E<C<B differs from landing E<B<C. -/
def chain : History :=
  { log := fun w => match w with
      | 0 => ⟨0, 0, [], .create "initial"⟩
      | 1 => ⟨0, 0, [0], .addMember 1 .admin "initial"⟩
      | 2 => ⟨1, 1, [0, 1], .setAccess 1 "replacement"⟩
      | 3 => ⟨1, 2, [0, 1, 2], .removeMember 0 []⟩
      | _ => ⟨0, 0, [0, 1], .removeMember 1 []⟩
    stored := fun w => match w with | 0 => 0 | 1 => 1 | 2 => 2 | 3 => 40 | _ => 20
    attempted := fun w => match w with | 0 => 0 | 1 => 1 | 3 => 3 | _ => 2 }

theorem chain_valid : Valid chain 5 :=
  finite_valid _ _ (validCheck_sound _ _ (by decide)) (by decide) (by decide) (by decide)

/-- Waiting W after E alone is insufficient. Every late landing here survives
rule 1, but the quiet-window test correctly refuses to finalize E. -/
theorem quiet_window_needed :
    old chain 30 32 2 = true ∧ quiet chain 30 5 (atTime chain 32) 32 = false ∧
    tooLate chain 30 5 (fun _ => true) 3 = false ∧
    tooLate chain 30 5 (fun _ => true) 4 = false ∧
    2 ∈ (resolve chain 30 5 (atTime chain 32)).dropped ∧
    2 ∈ (resolve chain 30 5 (atTime chain 40)).kept := by decide

/-- A future retry has the earlier author timestamp, but its storage delay
exceeds W. Rule 1 prevents it from defeating the already-final later stamp. -/
def backdated : History :=
  { history 2 with stored := fun w => if w = 0 then 0 else if w = 1 then 32 else 1 }

theorem backdated_valid : Valid backdated 3 :=
  finite_valid _ _ resets_valid (by decide) (by decide) (by decide)

theorem drop_rule_needed :
    finalSet backdated 30 3 (atTime backdated 31) 31 2 = true ∧
    tooLate backdated 30 3 (atTime backdated 32) 1 = true ∧
    2 ∈ (resolve backdated 30 3 (atTime backdated 32)).kept ∧
    2 ∈ (settle backdated.log (authorViews backdated 30 3) [0, 1, 2]
      (conflict backdated.log) (prefer backdated.log)).dropped := by decide

/-- A missing entry exactly at T-W can still defeat an entry already received.
Thus a uniformly earlier receipt cutoff cannot replace the full old prefix. -/
def boundaryDelivery : History :=
  { history 2 with stored := fun w => if w = 0 then 0 else if w = 1 then 2 else 1 }

theorem cutoff_receipt_needed :
    Valid boundaryDelivery 3 ∧
    (∀ w : Fin 3, boundaryDelivery.stored w ≤ 1 → entrySet [0, 2] w = true) ∧
    finalSet boundaryDelivery 30 3 (atTime boundaryDelivery 32) 32 2 = true ∧
    2 ∈ (resolve boundaryDelivery 30 3 (entrySet [0, 2])).kept ∧
    2 ∈ (resolve boundaryDelivery 30 3 (entrySet [0, 2, 1])).dropped := by
  refine ⟨finite_valid _ _ resets_valid (by decide) (by decide) (by decide), ?_⟩
  decide

/-- With only creation and one other entry, a closed received set containing
that other entry has no remaining old entry to receive. Thus the delayed-conflict
witness needs two non-creation entries, in addition to the required creation. -/
theorem one_change_no_delayed_flip (H : History) (W : Nat) (hv : Valid H 2)
    (S U : EntrySet) (hs : Closed H.log S) (hu : Closed H.log U)
    (hS : S 1 = true) (hU : U 1 = true)
    (bs : ∀ w, S w = true → w < 2) (bu : ∀ w, U w = true → w < 2) :
    resolve H W 2 S = resolve H W 2 U := by
  have hr := hv.causal.read_root 1 (by decide) (by decide)
  have h0s := hs 1 hS 0 hr
  have h0u := hu 1 hU 0 hr
  have eq : S = U := by
    funext w
    by_cases h0 : w = 0
    · subst w; rw [h0s, h0u]
    · by_cases h1 : w = 1
      · subst w; rw [hS, hU]
      · have sf : S w = false := by
          cases he : S w
          · rfl
          · have := bs w he; omega
        have uf : U w = false := by
          cases he : U w
          · rfl
          · have := bu w he; omega
        rw [sf, uf]
  rw [eq]

/-- The contradiction predicate discards key-only conflicts. Member targets
and snapshot conflicts still apply, with the five explicit preference tiers. -/
theorem decided_conflicts :
    contradiction ⟨0, 0, [], .addMember 2 .member "new"⟩
      ⟨1, 1, [], .removeMember 3 [0]⟩ = false ∧
    contradiction ⟨0, 0, [], .addToCircle 0 2⟩
      ⟨1, 1, [], .removeFromCircle 0 3⟩ = false ∧
    contradiction ⟨0, 0, [], .removeMember 2 [0]⟩
      ⟨1, 1, [], .removeMember 3 [0]⟩ = false ∧
    contradiction ⟨0, 0, [], .removeFromCircle 0 2⟩
      ⟨1, 1, [], .removeFromCircle 1 2⟩ = false ∧
    contradiction ⟨0, 0, [], .removeMember 2 []⟩
      ⟨1, 1, [], .removeFromCircle 0 2⟩ = false ∧
    contradiction ⟨0, 0, [], .deleteCircle 0⟩
      ⟨1, 1, [], .renameCircle 0 "new"⟩ = false ∧
    contradiction ⟨0, 0, [], .changeRole 2 .member⟩
      ⟨1, 1, [], .changeRole 2 .admin⟩ = true ∧
    contradiction ⟨0, 0, [], .reset ⟨.store, 1⟩⟩
      ⟨1, 1, [], .reset ⟨.store, 2⟩⟩ = true ∧
    [tier (.removeMember 2 []), tier (.deleteCircle 0), tier (.removeFromCircle 0 2),
      tier (.setAccess 0 "new"), tier (.changeRole 2 .admin)] = [0, 1, 2, 3, 4] := by decide

end CovenStorelog.Finality.Examples
