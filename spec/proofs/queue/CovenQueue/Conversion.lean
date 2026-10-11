import CovenQueue.Upload

namespace CovenQueue

structure Conversion where
  version : Nat
  /-- A supplied function stands for a successful validated conversion.
  Failure aborts reload; absence excludes the old write with its old values. -/
  convert : Option (List Nat → List Nat)

/-- Only a received interpretation changes. The original log record and
upload attempt remain immutable, including writes not yet attempted. -/
def convertRecord (c : Conversion) (inputsExcluded : Bool) (r : Record) : Record :=
  if r.version < c.version then
    let converted := if inputsExcluded then none else
      match r.disposition with
      | .ordinary => c.convert.map (fun f => f r.body)
      | _ => none
    match converted with
    | some body => { r with version := c.version, body := body }
    | none => match r.disposition with
      | .ordinary => { r with disposition := .lost c.version }
      | _ => r
  else r

theorem conversion_preserves_identity (c : Conversion) (bad : Bool) (r : Record) :
    (convertRecord c bad r).identity = r.identity := by
  unfold convertRecord
  split
  · dsimp only
    generalize (if bad then none else match r.disposition with
      | .ordinary => c.convert.map (fun f => f r.body) | _ => none) = result
    cases result with
    | some _ => rfl
    | none => cases r.disposition <;> rfl
  · rfl

theorem excluded_input_not_converted (c : Conversion) (r : Record) :
    (convertRecord c true r).body = r.body ∧
      (convertRecord c true r).version = r.version := by
  simp only [convertRecord, ↓reduceIte]
  split
  · cases r.disposition <;> exact ⟨rfl, rfl⟩
  · exact ⟨rfl, rfl⟩

theorem lost_never_reconverted (c : Conversion) (bad : Bool) (r : Record) (version : Nat)
    (lost : r.disposition = .lost version) : convertRecord c bad r = r := by
  simp [convertRecord, lost]

def resetIgnores (v : View) (r : Record) : Bool :=
  match v.reset with
  | none => false
  | some b => beforeReset b r

/-- Covered effects come from the snapshot, reset-ignored effects create no
schema loss, and a conversion is used only for its selected breaking version.
The author uses this same receive path after a competing raise wins. -/
def interpret (c : Conversion) (v : View) (w : CheckedWrite) (answers : List Verdict) : Record :=
  if w.refusal.isSome || resetIgnores v w.record then w.record else
    match v.raise with
    | none => w.record
    | some b => if w.record.identity.id ∈ b.covers ∨ c.version ≠ b.version then w.record
      else convertRecord c (excludedInput w.record answers).isSome w.record

theorem interpretation_identity (c : Conversion) (v : View) (w : CheckedWrite) (answers : List Verdict) :
    (interpret c v w answers).identity = w.record.identity := by
  unfold interpret
  split
  · rfl
  · split
    · rfl
    · split
      · rfl
      · exact conversion_preserves_identity _ _ _

/-- Receipt follows causal order, converting dependencies before their users.
Attempt status is deliberately absent from this shared classification. -/
def receive (c : Conversion) (v : View) (h : History) (w : Nat) : Record × Verdict :=
  let answers := (causes h v w).attach.map fun a => (receive c v h a.val).2
  let record := interpret c v (h w) answers
  (record, classify v ⟨record, (h w).refusal⟩ answers)
termination_by w
decreasing_by exact cause_earlier a.property

theorem receive_eq (c : Conversion) (v : View) (h : History) (w : Nat) :
    receive c v h w =
      let answers := (causes h v w).map fun a => (receive c v h a).2
      let record := interpret c v (h w) answers
      (record, classify v ⟨record, (h w).refusal⟩ answers) := by
  rw [receive]; simp

def receivedHistory (c : Conversion) (v : View) (h : History) : History :=
  fun w => ⟨(receive c v h w).1, (h w).refusal⟩

theorem received_identity (c : Conversion) (v : View) (h : History) (w : Nat) :
    (receivedHistory c v h w).record.identity = (h w).record.identity := by
  unfold receivedHistory
  rw [receive_eq]
  exact interpretation_identity _ _ _ _

theorem received_causes (c : Conversion) (v : View) (h : History) (w : Nat) :
    causes (receivedHistory c v h) v w = causes h v w := by
  simp only [causes, received_identity]

theorem receive_matches_evaluation (c : Conversion) (v : View) (h : History) (w : Nat) :
    (receive c v h w).2 = evaluate (receivedHistory c v h) v w := by
  induction w using Nat.strongRecOn with
  | ind w ih =>
    rw [evaluate_eq, received_causes]
    have eq : ((causes h v w).map fun a => (receive c v h a).2) =
        (causes h v w).map (evaluate (receivedHistory c v h) v) := by
      apply List.map_congr_left
      intro a ha
      exact ih a (cause_earlier ha)
    change (receive c v h w).2 = classify v ⟨(receive c v h w).1, (h w).refusal⟩ _
    rw [← eq, receive_eq c v h w]

/-- Independent receivers interpret each write only after judging its
causes. They share the conversion function and selected boundary, not a
precomputed converted history or a common arrival order. -/
inductive Received (c : Conversion) (v : View) (h : History) : Nat → (Record × Verdict) → Prop
  | apply (w : Nat) (answers : Nat → Record × Verdict) :
      (∀ a ∈ causes h v w, Received c v h a (answers a)) →
      (∀ a ∈ causes h v w, ∀ reason, (answers a).2 ≠ .refused reason) →
      Received c v h w
        (let previous := (causes h v w).map fun a => (answers a).2
         let record := interpret c v (h w) previous
         (record, classify v ⟨record, (h w).refusal⟩ previous))

theorem receive_agreement {c : Conversion} {v : View} {h : History} {w : Nat}
    {x y : Record × Verdict} (first : Received c v h w x) (second : Received c v h w y) : x = y := by
  induction first generalizing y with
  | apply w answers deps ready ih =>
      cases second with
      | apply _ other deps' ready' =>
          have eq : ((causes h v w).map fun a => (answers a).2) =
              (causes h v w).map fun a => (other a).2 := by
            apply List.map_congr_left
            intro a ha
            exact congrArg Prod.snd (ih a ha (deps' a ha))
          dsimp only
          rw [eq]

theorem every_write_received (c : Conversion) (v : View) (h : History)
    (good : ∀ w, (h w).refusal = none) (w : Nat) :
    Received c v h w (receive c v h w) := by
  induction w using Nat.strongRecOn with
  | ind w ih =>
      rw [receive_eq]
      apply Received.apply w (receive c v h)
      · intro a ha
        exact ih a (cause_earlier ha)
      · intro a _ reason
        rw [receive_eq]
        dsimp only
        apply classify_not_refused
        exact good a

/-- Original values, names and metadata are the retained loss, never a
partially converted claim. Reset-ignored and migration markers have no loss. -/
def receivedLoss (h : History) (w : Nat) (answer : Verdict) : Option (Nat × Record) :=
  match answer, (h w).record.disposition with
  | .excluded e, .ordinary => some (e, (h w).record)
  | _, _ => none

theorem excluded_records_loss (h : History) (w e : Nat)
    (ordinary : (h w).record.disposition = .ordinary) :
    receivedLoss h w (.excluded e) = some (e, (h w).record) := by
  simp [receivedLoss, ordinary]

abbrev PendingHistory := Nat → Pending × Option Refusal

def checkedHistory (h : PendingHistory) : History :=
  fun w => ⟨(h w).1.record, (h w).2⟩

structure Reloaded where
  uploads : PendingHistory
  data : History

def rebuild (c : Conversion) (v : View) (s : Reloaded) : Reloaded :=
  ⟨s.uploads, receivedHistory c v s.data⟩

def runConversions : List (Conversion × View) → Reloaded → Reloaded
  | [], s => s
  | (c, v) :: cs, s => runConversions cs (rebuild c v s)

theorem conversions_preserve_uploads (cs : List (Conversion × View)) (s : Reloaded) :
    (runConversions cs s).uploads = s.uploads := by
  induction cs generalizing s with
  | nil => rfl
  | cons c cs ih => exact ih _

theorem conversions_preserve_attempt (cs : List (Conversion × View)) (s : Reloaded)
    (w : Nat) (a : Attempt) (ha : (s.uploads w).1 = .tried a) :
    ((runConversions cs s).uploads w).1 = .tried a := by
  rw [conversions_preserve_uploads]; exact ha

theorem conversions_preserve_untried (cs : List (Conversion × View)) (s : Reloaded)
    (w : Nat) (r : Record) (ha : (s.uploads w).1 = .untried r) :
    ((runConversions cs s).uploads w).1 = .untried r := by
  rw [conversions_preserve_uploads]; exact ha

/-- An author's reload and a peer's receipt agree on both the interpreted
record and its verdict, even when their local upload-attempt facts differ. -/
theorem reload_matches_receiver (c : Conversion) (v : View) (own : PendingHistory)
    (remote : History) (same : checkedHistory own = remote) (w : Nat) :
    receive c v (checkedHistory own) w = receive c v remote w := by rw [same]

/-- Independent legal causal evaluations agree after receive conversion.
This covers converted bodies as well as excluded original-value losses. -/
theorem converted_verdicts_converge (c : Conversion) (v : View) (h : History)
    (w : Nat) (x y : Verdict)
    (first : Judged (receivedHistory c v h) v w x)
    (second : Judged (receivedHistory c v h) v w y) :
    x = y ∧ receivedLoss h w x = receivedLoss h w y := by
  have eq := verdict_agreement first second
  exact ⟨eq, congrArg (receivedLoss h w) eq⟩

/-- Two independently computed conversions retain identical results and
loss records, including when the author reloads after another raise wins. -/
theorem receive_losses_converge {c : Conversion} {v : View} {h : History} {w : Nat}
    {x y : Record × Verdict} (first : Received c v h w x) (second : Received c v h w y) :
    x = y ∧ receivedLoss h w x.2 = receivedLoss h w y.2 := by
  have eq := receive_agreement first second
  exact ⟨eq, by rw [eq]⟩

end CovenQueue
