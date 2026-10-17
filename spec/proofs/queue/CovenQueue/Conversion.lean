import CovenQueue.Pending

namespace CovenQueue

structure Conversion where
  version : Nat
  /-- An absent callback marks the record lost. Present callbacks model
  successful, validated results; a failing migration never commits its reload. -/
  convert : Option (List Nat → List Nat)

inductive ConversionPlan where
  | keep
  | lost (version : Nat)
  | convert (version : Nat) (values : List Nat → List Nat)

def conversionPlan (c : Conversion) (r : Record) : ConversionPlan :=
  if r.version < c.version then
    match r.disposition with
    | .lost _ | .migration => .keep
    | .ordinary => match c.convert with
      | none => .lost c.version
      | some f => .convert c.version f
  else .keep

/-- Determine direct schema admission before running any value converter.
This carries the original change evidence and does not examine any reads. -/
def ConversionPlan.prepare (plan : ConversionPlan) (r : Record) : Record :=
  match plan with
  | .keep => r
  | .lost version => { r with disposition := .lost version }
  | .convert version _ => { r with version := version }

def convertChange (f : List Nat → List Nat)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (ch : RowChange) : RowChange :=
  match ch.content with
  | .frozen _ _ => ch
  | .values values =>
    { ch with content := match losses ch.row with
      | some cause => .frozen cause values
      | none => .values (f values) }

def ConversionPlan.change (losses : Nat → Option (CovenMerge.LossCause Nat)) : ConversionPlan → RowChange → RowChange
  | .keep | .lost _ => id
  | .convert _ f => convertChange f losses

def ConversionPlan.record (plan : ConversionPlan)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (r : Record) : Record :=
  { plan.prepare r with body := r.body.map (plan.change losses) }

/-- Conversion changes only a received interpretation, never upload bytes. -/
def convertRecord (c : Conversion) (losses : Nat → Option (CovenMerge.LossCause Nat))
    (r : Record) : Record := (conversionPlan c r).record losses r

theorem conversion_preserves_identity (c : Conversion)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (r : Record) :
    (convertRecord c losses r).identity = r.identity := by
  simp only [convertRecord, ConversionPlan.record, ConversionPlan.prepare]; split <;> rfl

theorem missing_converter_preserves_values (version : Nat) (r : Record)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (old : r.version < version)
    (ordinary : r.disposition = .ordinary) :
    convertRecord ⟨version, none⟩ losses r = { r with disposition := .lost version } := by
  simp [convertRecord, ConversionPlan.record, ConversionPlan.prepare, conversionPlan, old, ordinary, ConversionPlan.change]

theorem lost_never_reconverted (c : Conversion)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (r : Record) (version : Nat)
    (lost : r.disposition = .lost version) : convertRecord c losses r = r := by
  simp [convertRecord, ConversionPlan.record, ConversionPlan.prepare, conversionPlan, lost, ConversionPlan.change]
  cases r
  simp_all

theorem dependent_change_not_converted (f : List Nat → List Nat)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (ch : RowChange) (values : List Nat)
    (cause : CovenMerge.LossCause Nat) (original : ch.content = .values values)
    (lost : losses ch.row = some cause) :
    (convertChange f losses ch).content = .frozen cause values := by
  simp [convertChange, original, lost]

theorem independent_change_converted (f : List Nat → List Nat)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (ch : RowChange) (values : List Nat)
    (original : ch.content = .values values) (kept : losses ch.row = none) :
    (convertChange f losses ch).content = .values (f values) := by
  simp [convertChange, original, kept]

theorem frozen_change_preserved (f : List Nat → List Nat)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (ch : RowChange)
    (cause : CovenMerge.LossCause Nat) (values : List Nat) (frozen : ch.content = .frozen cause values) :
    convertChange f losses ch = ch := by simp [convertChange, frozen]

theorem convertChange_metadata (f : List Nat → List Nat)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (ch : RowChange) :
    (convertChange f losses ch).row = ch.row ∧
    (convertChange f losses ch).generation = ch.generation ∧
    (convertChange f losses ch).operation = ch.operation ∧
    (convertChange f losses ch).provenance = ch.provenance := by
  cases content : ch.content <;> simp [convertChange, content]

abbrev PendingHistory := Nat → Pending × Option Refusal

def checkedHistory (h : PendingHistory) : History :=
  fun w => ⟨(h w).1.record, (h w).2⟩

def resetIgnores (v : View) (r : Record) : Bool :=
  match v.reset with
  | none => false
  | some b => beforeReset b r

/-- Only uncovered late records use the selected breaking conversion.
Covered effects come from the snapshot. Refused and reset-ignored records
and migration markers never invoke an app converter. -/
def interpretationPlan (c : Conversion) (v : View) (w : CheckedWrite) : ConversionPlan :=
  if w.refusal.isSome || resetIgnores v w.record then .keep else
    match v.raise with
    | none => .keep
    | some b => if w.record.identity.id ∈ b.covers ∨ c.version ≠ b.version then .keep
      else conversionPlan c w.record

/-- Candidate schema admission changes no values and invokes no callback. -/
def prepareInterpretation (c : Conversion) (v : View) (w : CheckedWrite) : Record :=
  (interpretationPlan c v w).prepare w.record

def preparedHistory (c : Conversion) (v : View) (h : History) : History :=
  fun w => ⟨prepareInterpretation c v (h w), (h w).refusal⟩

theorem prepare_body (c : Conversion) (v : View) (w : CheckedWrite) :
    (prepareInterpretation c v w).body = w.record.body := by
  unfold prepareInterpretation ConversionPlan.prepare
  split <;> rfl

theorem prepared_row (c : Conversion) (v : View) (h : History) (w r : Nat) :
    rowChange (preparedHistory c v h) w r = rowChange h w r := by
  simp [rowChange, preparedHistory, prepare_body]

theorem preparedValid (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) : DataValid (preparedHistory c v h) where
  uniqueRows w := by simpa [preparedHistory, prepare_body] using valid.uniqueRows w
  earlier w r a h := valid.earlier w r a (by simpa [provenance, prepared_row] using h)
  sourcePresent w r a h := by
    rw [prepared_row]
    apply valid.sourcePresent w r a
    simpa [provenance, prepared_row] using h

def preparedState (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) :=
  dataState (preparedHistory c v h) (preparedValid c v h valid) v

/-- The same per-change decision as receipt. An excluded base freezes its own
values; an admitted change freezes only when its data source did not survive. -/
def conversionLosses (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w r : Nat) : Option (CovenMerge.LossCause Nat) :=
  freshLoss (preparedHistory c v h) (preparedValid c v h valid) v w r

def rowConversion (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) : RowChange → RowChange :=
  (interpretationPlan c v w).change losses

def interpret (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) : Record :=
  (interpretationPlan c v w).record losses w.record

theorem covered_not_converted (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (b : Raise)
    (selected : v.raise = some b) (covered : w.record.identity.id ∈ b.covers) :
    interpret c v w losses = w.record := by
  have plan : interpretationPlan c v w = .keep := by
    simp [interpretationPlan, selected, covered]
  simp [interpret, ConversionPlan.record, ConversionPlan.prepare, plan, ConversionPlan.change]

theorem reset_ignored_not_converted (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat))
    (ignored : resetIgnores v w.record = true) : interpret c v w losses = w.record := by
  simp [interpret, ConversionPlan.record, ConversionPlan.prepare, interpretationPlan, ignored, ConversionPlan.change]

theorem refused_not_converted (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (reason : Refusal)
    (refused : w.refusal = some reason) : interpret c v w losses = w.record := by
  simp [interpret, ConversionPlan.record, ConversionPlan.prepare, interpretationPlan, refused, ConversionPlan.change]

theorem wrong_version_not_converted (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (b : Raise)
    (selected : v.raise = some b) (different : c.version ≠ b.version) :
    interpret c v w losses = w.record := by
  have plan : interpretationPlan c v w = .keep := by
    simp [interpretationPlan, selected, different]
  simp [interpret, ConversionPlan.record, ConversionPlan.prepare, plan, ConversionPlan.change]

def receive (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w : Nat) : Record × Verdict :=
  let record := interpret c v (h w) (conversionLosses c v h valid w)
  (record, classify v ⟨record, (h w).refusal⟩)

theorem interpretation_identity (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) :
    (interpret c v w losses).identity = w.record.identity := by
  simp only [interpret, ConversionPlan.record, ConversionPlan.prepare]
  split <;> rfl

def receivedHistory (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) : History :=
  fun w => ⟨(receive c v h valid w).1, (h w).refusal⟩

theorem received_identity (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w : Nat) :
    (receivedHistory c v h valid w).record.identity = (h w).record.identity :=
  interpretation_identity _ _ _ _

theorem received_prerequisites (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w : Nat) :
    prerequisites (receivedHistory c v h valid) w = prerequisites h w := by
  simp only [prerequisites, received_identity]

theorem receive_matches_evaluation (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w : Nat) :
    (receive c v h valid w).2 = evaluate (receivedHistory c v h valid) v w := rfl

theorem rowConversion_metadata (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (ch : RowChange) :
    (rowConversion c v w losses ch).row = ch.row ∧
    (rowConversion c v w losses ch).generation = ch.generation ∧
    (rowConversion c v w losses ch).operation = ch.operation ∧
    (rowConversion c v w losses ch).provenance = ch.provenance := by
  unfold rowConversion
  cases interpretationPlan c v w with
  | keep | lost _ => exact ⟨rfl, rfl, rfl, rfl⟩
  | convert _ f => exact convertChange_metadata f losses ch

theorem rowConversion_frozen (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (ch : RowChange)
    (frozen : ch.frozen = true) : rowConversion c v w losses ch = ch := by
  unfold rowConversion
  cases interpretationPlan c v w with
  | keep | lost _ => rfl
  | convert _ f =>
    cases content : ch.content with
    | values _ => simp [RowChange.frozen, content] at frozen
    | frozen cause values => exact frozen_change_preserved _ _ _ _ _ content

theorem rowConversion_kept (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (ch : RowChange)
    (unfrozen : ch.frozen = false) (none : losses ch.row = none) :
    (rowConversion c v w losses ch).frozen = false := by
  unfold rowConversion
  cases interpretationPlan c v w with
  | keep | lost _ => exact unfrozen
  | convert _ f =>
    cases content : ch.content with
    | values values => simp [ConversionPlan.change, convertChange, content, none, RowChange.frozen]
    | frozen _ _ => simp [RowChange.frozen, content] at unfrozen

theorem received_body (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w : Nat) :
    (receivedHistory c v h valid w).record.body =
      (h w).record.body.map (rowConversion c v (h w) (conversionLosses c v h valid w)) := rfl

theorem received_row (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w r : Nat) :
    rowChange (receivedHistory c v h valid) w r =
      (rowChange h w r).map (rowConversion c v (h w) (conversionLosses c v h valid w)) := by
  simp only [rowChange, received_body, List.find?_map, Function.comp_def]
  simp only [(rowConversion_metadata c v (h w) (conversionLosses c v h valid w) _).1]

theorem received_provenance (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w r : Nat) :
    provenance (receivedHistory c v h valid) w r = provenance h w r := by
  simp only [provenance, received_row]
  cases found : rowChange h w r
  · rfl
  · exact (rowConversion_metadata _ _ _ _ _).2.2.2

theorem receivedValid (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) : DataValid (receivedHistory c v h valid) where
  uniqueRows w := by
    simpa only [received_body, List.map_map, Function.comp_def,
      (rowConversion_metadata c v (h w) (conversionLosses c v h valid w) _).1] using valid.uniqueRows w
  earlier w r a ha := valid.earlier w r a (by simpa [received_provenance] using ha)
  sourcePresent w r a ha := by
    simpa only [received_row, Option.isSome_map] using
      valid.sourcePresent w r a (by simpa [received_provenance] using ha)

theorem received_admission (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w : Nat) :
    evaluate (receivedHistory c v h valid) v w = evaluate (preparedHistory c v h) v w := rfl

theorem received_admitted (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w : Nat) :
    decide (evaluate (receivedHistory c v h valid) v w = .applied) =
      decide (evaluate (preparedHistory c v h) v w = .applied) := rfl

def receivedState (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) :=
  dataState (receivedHistory c v h valid) (receivedValid c v h valid) v

theorem received_sources (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w r : Nat) :
    ((preparedState c v h valid).provenance w r).sources =
      ((receivedState c v h valid).provenance w r).sources := by
  change (provenance (preparedHistory c v h) w r).sources =
    (provenance (receivedHistory c v h valid) w r).sources
  rw [received_provenance]
  simp only [provenance, prepared_row]

theorem received_present (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) :
    (preparedState c v h valid).present = (receivedState c v h valid).present := by
  funext w r
  simp [preparedState, receivedState, dataState, prepared_row, received_row]

theorem conversion_only_drops (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w r : Nat)
    (kept : (receivedState c v h valid).base w r = true) :
    (preparedState c v h valid).base w r = true := by
  simp only [receivedState, dataState, received_admitted, received_row] at kept
  simp only [preparedState, dataState, prepared_row]
  cases found : rowChange h w r with
  | none => simp [found] at kept
  | some ch =>
    have parts : (evaluate (preparedHistory c v h) v w = .applied ∧ snapshotLoss v w r = none) ∧
        (rowConversion c v (h w) (conversionLosses c v h valid w) ch).frozen = false := by
      simpa [found] using kept
    have old : ch.frozen = false := by
      cases frozen : ch.frozen
      · rfl
      · have same := rowConversion_frozen c v (h w) (conversionLosses c v h valid w) ch frozen
        rw [same, frozen] at parts
        exact parts.2
    simp [parts.1.1, parts.1.2, old]

theorem conversion_keeps_retained (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w r : Nat)
    (kept : CovenMerge.DataDependency.retained (preparedState c v h valid) w r = true) :
    (receivedState c v h valid).base w r = true := by
  have base := ((CovenMerge.DataDependency.retained_iff _ w r).mp kept).1
  simp only [preparedState, dataState, prepared_row] at base
  simp only [receivedState, dataState, received_admitted, received_row]
  cases found : rowChange h w r with
  | none => simp [found] at base
  | some ch =>
    have parts : (evaluate (preparedHistory c v h) v w = .applied ∧ snapshotLoss v w r = none) ∧ ch.frozen = false := by
      simpa [found, decide_eq_true_eq] using base
    have row : ch.row = r := by simpa using List.find?_some found
    have none : conversionLosses c v h valid w ch.row = none := by
      rw [row]
      simp [conversionLosses, freshLoss, parts.1.1, parts.1.2, show CovenMerge.DataDependency.retained
        (dataState (preparedHistory c v h) (preparedValid c v h valid) v) w r = true from kept]
    have output := rowConversion_kept c v (h w) (conversionLosses c v h valid w) ch parts.2 none
    simp [parts.1.1, parts.1.2, output]

/-- Receive conversion and per-change classification have identical data
retention, including chains through a partially lost write. -/
theorem conversion_retention_agrees (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w r : Nat) :
    CovenMerge.DataDependency.retained (preparedState c v h valid) w r =
      CovenMerge.DataDependency.retained (receivedState c v h valid) w r :=
  CovenMerge.DataDependency.retained_after_recording_losses _ _
    (received_present c v h valid) (received_sources c v h valid)
    (conversion_only_drops c v h valid) (conversion_keeps_retained c v h valid) w r

theorem conversion_dependencies_agree (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w r : Nat) :
    CovenMerge.DataDependency.droppedSources (preparedState c v h valid) w r =
      CovenMerge.DataDependency.droppedSources (receivedState c v h valid) w r := by
  simp only [CovenMerge.DataDependency.droppedSources, received_sources, conversion_retention_agrees]

theorem conversion_loss_agrees (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w r : Nat) :
    conversionLosses c v h valid w r =
      freshLoss (receivedHistory c v h valid) (receivedValid c v h valid) v w r := by
  simp only [conversionLosses, freshLoss, received_admission]
  change (match evaluate (preparedHistory c v h) v w with
    | .excluded _ version => (snapshotLoss v w r).orElse fun _ => some (CovenMerge.LossCause.schemaChange version w)
    | .applied => (snapshotLoss v w r).orElse fun _ => if CovenMerge.DataDependency.retained (preparedState c v h valid) w r then none
        else some (.dependency w (CovenMerge.DataDependency.droppedSources (preparedState c v h valid) w r))
    | .ignored _ | .refused _ => none) = _
  rw [conversion_retention_agrees, conversion_dependencies_agree]
  rfl

theorem rowConversion_answer (c : Conversion) (v : View) (w : CheckedWrite)
    (losses : Nat → Option (CovenMerge.LossCause Nat)) (ch : RowChange) :
    preserveFrozen (rowConversion c v w losses ch) ((losses ch.row).elim .applied .frozen) =
      preserveFrozen ch ((losses ch.row).elim .applied .frozen) := by
  unfold rowConversion
  cases interpretationPlan c v w with
  | keep | lost _ => rfl
  | convert _ f =>
    cases content : ch.content <;> cases loss : losses ch.row <;>
      simp [ConversionPlan.change, convertChange, preserveFrozen, content, loss]

/-- Complete per-change verdict agreement, not only equal header admission.
Frozen values are preserved by `dependent_change_not_converted`. -/
theorem converted_change_matches_receiver (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w : Nat) (ch : RowChange) :
    classifyChange (receivedHistory c v h valid) (receivedValid c v h valid) v w
      (rowConversion c v (h w) (conversionLosses c v h valid w) ch) =
    classifyChange (preparedHistory c v h) (preparedValid c v h valid) v w ch := by
  simp only [classifyChange, received_admission,
    (rowConversion_metadata c v (h w) (conversionLosses c v h valid w) ch).1,
    ← conversion_loss_agrees, rowConversion_answer]
  rfl

/-- Receipt certificates require the recorded-read prerequisites for readiness.
The result itself follows only the checked record and its row data sources;
no predecessor verdict is passed to the converter. -/
inductive Received (c : Conversion) (v : View) (h : History) (valid : DataValid h) :
    Nat → (Record × Verdict) → Prop
  | apply (w : Nat) (answers : Nat → Record × Verdict) :
      (∀ a ∈ prerequisites h w, Received c v h valid a (answers a)) →
      (∀ a ∈ prerequisites h w, ∀ reason, (answers a).2 ≠ .refused reason) →
      Received c v h valid w (receive c v h valid w)

theorem receive_agreement {c : Conversion} {v : View} {h : History} {valid : DataValid h}
    {w : Nat} {x y : Record × Verdict}
    (first : Received c v h valid w x) (second : Received c v h valid w y) : x = y := by
  cases first; cases second; rfl

theorem every_write_received (c : Conversion) (v : View) (h : History) (valid : DataValid h)
    (good : ∀ w, (h w).refusal = none) (w : Nat) :
    Received c v h valid w (receive c v h valid w) := by
  induction w using Nat.strongRecOn with
  | ind w ih =>
    apply Received.apply w (receive c v h valid)
    · intro a ha; exact ih a (prerequisite_earlier ha)
    · intro a _ reason
      apply classify_not_refused
      exact good a

/-- A direct schema exclusion retains the whole original ordinary record.
Mixed writes instead have per-change losses from `classifyChange`. -/
def receivedLoss (h : History) (w : Nat) (answer : Verdict) : Option (Nat × Record) :=
  match answer, (h w).record.disposition with
  | .excluded entry _, .ordinary => some (entry, (h w).record)
  | _, _ => none

theorem excluded_records_loss (h : History) (w entry version : Nat)
    (ordinary : (h w).record.disposition = .ordinary) :
    receivedLoss h w (.excluded entry version) = some (entry, (h w).record) := by
  simp [receivedLoss, ordinary]

theorem converted_verdicts_converge (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w : Nat) (x y : Verdict)
    (first : Judged (receivedHistory c v h valid) v w x)
    (second : Judged (receivedHistory c v h valid) v w y) :
    x = y ∧ receivedLoss h w x = receivedLoss h w y := by
  have eq := verdict_agreement first second
  exact ⟨eq, congrArg (receivedLoss h w) eq⟩

theorem receive_losses_converge {c : Conversion} {v : View} {h : History} {valid : DataValid h}
    {w : Nat} {x y : Record × Verdict}
    (first : Received c v h valid w x) (second : Received c v h valid w y) :
    x = y ∧ receivedLoss h w x.2 = receivedLoss h w y.2 := by
  have eq := receive_agreement first second
  exact ⟨eq, by rw [eq]⟩

/-- Upload originals and received interpretations are separate durable facts. -/
structure Reloaded where
  uploads : PendingHistory
  data : History
  valid : DataValid data

def rebuild (c : Conversion) (v : View) (s : Reloaded) : Reloaded :=
  ⟨s.uploads, receivedHistory c v s.data s.valid, receivedValid c v s.data s.valid⟩

def runConversions : List (Conversion × View) → Reloaded → Reloaded
  | [], s => s
  | (c, v) :: cs, s => runConversions cs (rebuild c v s)

theorem conversions_preserve_uploads (cs : List (Conversion × View)) (s : Reloaded) :
    (runConversions cs s).uploads = s.uploads := by
  induction cs generalizing s with
  | nil => rfl
  | cons cv cs ih => exact ih _

theorem conversions_preserve_attempt (cs : List (Conversion × View)) (s : Reloaded)
    (w : Nat) (a : Attempt) (ha : (s.uploads w).1 = .tried a) :
    ((runConversions cs s).uploads w).1 = .tried a := by
  rw [conversions_preserve_uploads]; exact ha

theorem conversions_preserve_untried (cs : List (Conversion × View)) (s : Reloaded)
    (w : Nat) (r : Record) (ha : (s.uploads w).1 = .untried r) :
    ((runConversions cs s).uploads w).1 = .untried r := by
  rw [conversions_preserve_uploads]; exact ha

/-- Attempt status is absent from the shared receive path, including the
original author's reload after another device's raise wins. -/
theorem reload_matches_receiver (c : Conversion) (v : View) (own : PendingHistory)
    (remote : History) (same : checkedHistory own = remote)
    (valid : DataValid (checkedHistory own)) (remoteValid : DataValid remote) (w : Nat) :
    receive c v (checkedHistory own) valid w = receive c v remote remoteValid w := by
  subst remote; rfl

theorem conversion_preserves_frozen (c : Conversion) (v : View) (h : History)
    (valid : DataValid h) (w r : Nat) (ch : RowChange)
    (found : rowChange h w r = some ch) (frozen : ch.frozen = true) :
    rowChange (receivedHistory c v h valid) w r = some ch := by
  rw [received_row, found]
  simp [rowConversion_frozen c v (h w) (conversionLosses c v h valid w) ch frozen]

theorem conversions_preserve_frozen (cs : List (Conversion × View)) (s : Reloaded)
    (w r : Nat) (ch : RowChange)
    (found : rowChange s.data w r = some ch) (frozen : ch.frozen = true) :
    rowChange (runConversions cs s).data w r = some ch := by
  induction cs generalizing s with
  | nil => exact found
  | cons cv cs ih =>
    exact ih _ (conversion_preserves_frozen _ _ _ _ w r ch found frozen)

end CovenQueue
