import CovenMerge.ReceiveMerge

/-! Retirement admission and finality. Store-log finality supplies the stable
old entry prefix. The send gate supplies the stop and publication bound.
Neither conclusion uses a later write's reads or data. -/

namespace CovenMerge.Receive

variable {W : Type}

def beforeRemoval (P : Policy W) (w : W) : Bool :=
  P.retirements.all fun e => !(e.device == P.logs.device w && P.readEntry w e.entry)

theorem beforeRemoval_iff (P : Policy W) (w : W) :
    beforeRemoval P w = true ↔ ∀ e ∈ P.retirements, e.device = P.logs.device w →
      P.readEntry w e.entry = false := by
  simp only [beforeRemoval, List.all_eq_true, Bool.not_eq_true', Bool.and_eq_false_iff]
  grind

theorem post_read_refused (P : Policy W) (w : W) (e : Retirement)
    (kept : e ∈ P.retirements) (own : e.device = P.logs.device w)
    (read : P.readEntry w e.entry = true) : withinWindow P w = false := by
  cases h : withinWindow P w
  · rfl
  · have := ((window_exact P w).mp h e kept own).1
    simp [read] at this

section
variable [DecidableEq W]

theorem step_before_removal (P : Policy W) (past : W → List W)
    {gate : W → Bool} {action : Action W} {s s' : State W}
    (next : Step P.boundary P.logs past gate action s s')
    (refuses : ∀ w, gate w = true → beforeRemoval P w = true)
    (prior : ∀ w ∈ s.passed, beforeRemoval P w = true) :
    ∀ w ∈ s'.passed, beforeRemoval P w = true := by
  cases next with
  | download => exact prior
  | idle => exact prior
  | attempt s w =>
    unfold Receive.attempt
    split
    · rename_i enabled
      intro a mem
      rcases List.mem_append.mp mem with old | new
      · exact prior a old
      · have eq := List.mem_singleton.mp new
        subst a
        simp only [Receive.enabled, Bool.and_eq_true] at enabled
        exact refuses w enabled.1.2
    · exact prior

theorem passed_before_removal (P : Policy W) (past : W → List W)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate)
    (refuses : ∀ t w, gate t w = true → beforeRemoval P w = true) (t : Nat) :
    ∀ w ∈ (run.state t).passed, beforeRemoval P w = true := by
  induction t with
  | zero => simp [run.start, initial]
  | succ t ih => exact step_before_removal P past (run.next t) (refuses t) ih

end

/-- Finality fixes the kept entry prefix through the retiring entry's
storage time, including ties. Later retirements may appear or disappear;
none can tighten its earlier deadline. The send gate rules out reads of any
own retirement while it is kept in either view. -/
theorem retirement_verdict_final (P : Policy W) (later : List Retirement)
    (anchor : Retirement) (w : W)
    (kept : anchor ∈ P.retirements) (own : anchor.device = P.logs.device w)
    (finalPrefix : ∀ e, e.stored ≤ anchor.stored → (e ∈ P.retirements ↔ e ∈ later))
    (sendGate : ∀ e, e ∈ P.retirements ∨ e ∈ later →
      e.device = P.logs.device w → P.readEntry w e.entry = false) :
    withinWindow { P with retirements := later } w = withinWindow P w := by
  have nextAnchor := (finalPrefix anchor (Nat.le_refl _)).mp kept
  have transfer (source target : List Retirement)
      (anchorPresent : anchor ∈ source)
      (prefixKept : ∀ e ∈ target, e.stored ≤ anchor.stored → e ∈ source)
      (gate : ∀ e ∈ target, e.device = P.logs.device w → P.readEntry w e.entry = false)
      (h : ∀ e ∈ source, e.device = P.logs.device w →
        P.readEntry w e.entry = false ∧ P.stored w ≤ e.stored + 30 * 86400) :
      ∀ e ∈ target, e.device = P.logs.device w →
        P.readEntry w e.entry = false ∧ P.stored w ≤ e.stored + 30 * 86400 := by
    intro e he hd
    by_cases old : e.stored ≤ anchor.stored
    · exact h e (prefixKept e he old) hd
    · have bound := (h anchor anchorPresent own).2
      exact ⟨gate e he hd, by omega⟩
  apply Bool.eq_iff_iff.mpr
  rw [window_exact, window_exact]
  constructor
  · exact transfer later P.retirements nextAnchor
      (fun e he old => (finalPrefix e old).mp he)
      (fun e he hd => sendGate e (Or.inl he) hd)
  · exact transfer P.retirements later kept
      (fun e he old => (finalPrefix e old).mpr he)
      (fun e he hd => sendGate e (Or.inr he) hd)

/-- Every provider request uses completed catch-up less than five minutes
old, measured from its start, and settles within one day, including lost replies.
An observation started after finality sees the retirement and blocks sending. -/
theorem send_gate_publication_bound (finalAt catchupStart sent stored : Nat)
    (observed : Bool)
    (seesFinal : finalAt ≤ catchupStart → observed = true)
    (sendGate : observed = false)
    (fresh : sent < catchupStart + 300) (duration : stored ≤ sent + 86400) :
    stored < finalAt + 300 + 86400 := by
  have before : ¬ finalAt ≤ catchupStart := by
    intro h
    have := seesFinal h
    simp [sendGate] at this
  omega

end CovenMerge.Receive
