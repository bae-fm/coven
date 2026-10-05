import CovenStorelog.Converge

namespace CovenStorelog

def causalCheck (M : Log) : List Nat → List Nat → Bool
  | _, [] => true
  | seen, w :: ws => w ∉ seen && (M w).past.all (fun a => a ∈ seen) &&
      causalCheck M (seen ++ [w]) ws

theorem causalCheck_sound (M : Log) {seen L : List Nat}
    (hs : CausalOrder M seen) (h : causalCheck M seen L = true) :
    CausalOrder M (seen ++ L) := by
  induction L generalizing seen with
  | nil => simpa using hs
  | cons w ws ih =>
      simp only [causalCheck, Bool.and_eq_true, decide_eq_true_eq, List.all_eq_true] at h
      have hp : ∀ a, hadRead M w a = true → a ∈ seen := by
        simpa [hadRead] using h.1.2
      simpa [List.append_assoc] using ih (CausalOrder.snoc hs h.1.1 hp) h.2

def validCheck (M : Log) (n : Nat) : Bool :=
  let all := List.range n
  (M 0).action == .create && all.all (fun w =>
    (M w).past.all (fun a => a < w && (M a).past.all (fun b => b ∈ (M w).past)) &&
    (all.all (fun a => !(a < w && (M a).device == (M w).device) || a ∈ (M w).past)) &&
    ((M w).action != .create || w == 0) && (w == 0 || 0 ∈ (M w).past))

theorem validCheck_sound (M : Log) (n : Nat) (h : validCheck M n = true) : Valid M n := by
  simp only [validCheck, Bool.and_eq_true, beq_iff_eq, List.all_eq_true,
    List.mem_range, Bool.or_eq_true, Bool.not_eq_true', Bool.and_eq_false_iff,
    decide_eq_false_iff_not, decide_eq_true_eq, beq_eq_false_iff_ne, bne_iff_ne] at h
  refine ⟨?_, ?_, ?_, h.1, ?_, ?_⟩
  · intro w hw a ha
    exact ((h.2 w hw).1.1.1 a (by simpa [hadRead] using ha)).1
  · intro w hw a ha b hb
    have hh := ((h.2 w hw).1.1.1 a (by simpa [hadRead] using ha)).2
    simpa [hadRead] using hh b (by simpa [hadRead] using hb)
  · intro a b hab hb hd
    have hh := (h.2 b hb).1.1.2 a (by omega)
    simp_all [hadRead]
  · intro w hw hc
    exact ((h.2 w hw).1.2).resolve_left (by simpa using hc)
  · intro w hw hn
    simpa [hadRead] using ((h.2 w hn).2.resolve_left (by omega))

def EveryOrder (M : Log) (n : Nat) (entries : List Nat) (P : Result → Prop) : Prop :=
  ∀ arrival, CausalOrder M arrival → (∀ w, w ∈ arrival ↔ w ∈ entries) →
    P (arrival.foldl (step M n) (initial M n)).result

theorem every_order (M : Log) (n : Nat) (entries : List Nat) {P : Result → Prop}
    (h : P (resolve M n (entrySet entries))) : EveryOrder M n entries P := by
  intro arrival hc hs
  have he : entrySet arrival = entrySet entries := by funext w; simp [entrySet, hs w]
  have hh := (run_isSpec M n hc).1.2
  rw [hh, he]
  exact h

namespace Examples

def entry (author device : Nat) (past : List Nat) (action : Action) : Entry :=
  ⟨author, device, past, action⟩

end Examples
end CovenStorelog
