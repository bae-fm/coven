/-! The rows a wait names (§3.1 Waiting, SP "Waiting without repeated work"),
as `prerequisite_rows.rs` and `operation_evidence.rs` choose them.
`WaitingEvidence` takes a wait's prerequisite as given; this file checks the
choice of committed rows that stands for it.

A wait names prerequisites (`Prereq`, Rust `PrerequisiteRows`), and each
prerequisite names whole rows of the local schema, keyed by primary key
(`Prereq.rows`). A commit changing one of those rows is evidence for the
wait; no other commit is. The waiting work reads its prerequisite through the
same rows (`reading`): every row as it is, except a write's applied position,
of which it reads only whether it reaches the write.

Proved here: the rows are exactly what the waiting work reads. A commit that
changes what it reads changes one of the rows, so the wait wakes for it and
never stalls (`reading_frame`, `unblocking_commit_named`), and every row
named can by itself change what it reads, so none is watched for nothing
(`named_row_matters`). A commit changing a named row without changing the
reading is always a writer's position moving without crossing the write
(`only_partial_advances`): a partial advance counts as evidence. Each one is
a waking commit paid for in `evidence_rate`, and while positions only rise
there are fewer of them than the writes still missing
(`partial_advances_bounded`). -/

namespace CovenIO.PrerequisiteRows

/-- `_coven_log_inputs.kind`. -/
inductive InputKind
  | entry
  | key
  deriving DecidableEq, Repr

/-- A row of the local schema a wait can name, by table and primary key. -/
inductive Row
  /-- `_coven_store_log`: an applied entry. -/
  | storeLog (device number : Nat)
  /-- `_coven_log_inputs`: received entry or key-copy bytes. -/
  | input (kind : InputKind) (writer number : Nat)
  /-- `_coven_store_log_uploads`: this device's queued entry. -/
  | entryUpload (device number : Nat)
  /-- `_coven_key_uploads`: this device's queued key copy. -/
  | keyUpload (writer number : Nat)
  /-- `_coven_positions`: a writer's applied position. -/
  | position (device : Nat)
  /-- `_coven_uploads`: this device's queued write. -/
  | upload (device number : Nat)
  /-- `_coven_write_landings`: a write's landing. -/
  | landing (device number : Nat)
  /-- `_coven_devices`: a device's registration. -/
  | registration (device : Nat)
  /-- `_coven_peer_positions`: the positions a peer posted. -/
  | peerPositions (device : Nat)
  /-- `_coven_data_reloads`: an audience's pending reload. -/
  | reload (audience : String)
  /-- `_coven_finality`: the finality horizon. -/
  | finality
  /-- `_coven_boundaries`: one reset or schema boundary. -/
  | boundary (key : Nat)
  /-- `_coven_provider_access`: a provider account's access record. -/
  | access (account : String)
  /-- `_coven_operations`: another operation's journal row. -/
  | operation (id : Nat)
  deriving DecidableEq, Repr

/-- Committed state: each row's non-key values, `none` when absent. A commit
changes a row when it changes its value (SQLite sessions record no unchanged
update). -/
abbrev State := Row → Option Nat

/-- What a wait can name (Rust `PrerequisiteRows`). -/
inductive Prereq
  | entry (device number : Nat)
  | keyCopy (writer number : Nat)
  | write (device number : Nat)
  | device (device : Nat)
  | peerPositions (device : Nat)
  | reload (audience : String)
  | finality
  | boundaries
  | providerAccess (account : String)
  | operation (id : Nat)
  deriving DecidableEq, Repr

/-- One row, or every row of `_coven_boundaries`. -/
inductive Pattern
  | row (r : Row)
  | boundaries
  deriving DecidableEq, Repr

def Pattern.covers : Pattern → Row → Bool
  | .row r, x => decide (r = x)
  | .boundaries, .boundary _ => true
  | .boundaries, _ => false

/-- `PrerequisiteRows::reads`. -/
def Prereq.rows : Prereq → List Pattern
  | .entry d n => [.row (.storeLog d n), .row (.input .entry d n), .row (.entryUpload d n)]
  | .keyCopy w n => [.row (.input .key w n), .row (.keyUpload w n)]
  | .write d n => [.row (.position d), .row (.upload d n), .row (.landing d n)]
  | .device d => [.row (.registration d)]
  | .peerPositions d => [.row (.peerPositions d)]
  | .reload a => [.row (.reload a)]
  | .finality => [.row .finality]
  | .boundaries => [.boundaries]
  | .providerAccess a => [.row (.access a)]
  | .operation id => [.row (.operation id)]

/-- Whether a commit changing row `r` is evidence for a wait on `p`. -/
def names (p : Prereq) (r : Row) : Bool := p.rows.any (·.covers r)

/-- The writer position a write's reading reduces to whether it is applied. -/
def Prereq.writePosition : Prereq → Row → Bool
  | .write d _, .position d' => decide (d = d')
  | _, _ => false

/-- Writes are numbered from 1: a write is applied once its writer's
position reaches its number, and an absent position is 0. -/
def Prereq.applied : Prereq → Option Nat → Nat
  | .write _ n, v => if n ≤ v.getD 0 then 1 else 0
  | _, _ => 0

/-- What the waiting work reads of its prerequisite: nothing outside its
rows; each row as it is, except the writer's position of a write, read only
as whether it reaches the write. -/
def reading (p : Prereq) (s : State) (r : Row) : Option Nat :=
  if names p r then
    if p.writePosition r then some (p.applied (s r)) else s r
  else none

/-- What the waiting work reads depends only on the rows the wait names. -/
theorem reading_frame (p : Prereq) {s s' : State} (same : ∀ r, names p r = true → s r = s' r) :
    reading p s = reading p s' := by
  funext r
  unfold reading
  by_cases hn : names p r = true
  · rw [same r hn]
  · simp [hn]

/-- No stall: a commit that changes what the waiting work reads changes a row
the wait names, so it is evidence and wakes the wait. -/
theorem unblocking_commit_named (p : Prereq) {s s' : State}
    (changed : reading p s ≠ reading p s') : ∃ r, names p r = true ∧ s r ≠ s' r := by
  apply Classical.byContradiction
  intro none
  apply changed
  apply reading_frame
  intro r hr
  apply Classical.byContradiction
  intro differs
  exact none ⟨r, hr, differs⟩

/-- Writes are numbered from 1. -/
def Prereq.numbered : Prereq → Prop
  | .write _ n => 0 < n
  | _ => True

/-- A value that changes what is read at a named row. -/
def Prereq.reaching : Prereq → Nat
  | .write _ n => n
  | _ => 1

theorem write_position {p : Prereq} {r : Row} (h : p.writePosition r = true) :
    ∃ d n, p = .write d n ∧ r = .position d := by
  cases p <;> cases r <;> simp_all [Prereq.writePosition]

/-- No row is watched for nothing: every row a wait names can by itself
change what the waiting work reads. -/
theorem named_row_matters (p : Prereq) (r : Row) (numbered : p.numbered)
    (named : names p r = true) :
    ∃ s s' : State, (∀ x, x ≠ r → s x = s' x) ∧ reading p s ≠ reading p s' := by
  refine ⟨fun _ => none, fun x => if x = r then some p.reaching else none,
    fun x hx => by simp [hx], fun h => ?_⟩
  have at_r := congrFun h r
  simp only [reading, named, ite_true] at at_r
  by_cases wp : p.writePosition r = true
  · obtain ⟨d, n, rfl, rfl⟩ := write_position wp
    simp only [Prereq.numbered] at numbered
    simp only [wp, ite_true, Prereq.applied, Prereq.reaching] at at_r
    simp at at_r; omega
  · simp [wp] at at_r

/-- Evidence that changes nothing read is a partial advance: the wait is on a
write, the commit changed no named row but the writer's position, and that
position stayed on the same side of the write. -/
theorem only_partial_advances (p : Prereq) {s s' : State}
    (evidence : ∃ r, names p r = true ∧ s r ≠ s' r) (unread : reading p s = reading p s') :
    ∃ d n, p = .write d n ∧
      (∀ r, names p r = true → r ≠ .position d → s r = s' r) ∧
      (n ≤ (s (.position d)).getD 0 ↔ n ≤ (s' (.position d)).getD 0) := by
  obtain ⟨r, named, differs⟩ := evidence
  have at_r := congrFun unread r
  simp only [reading, named, ite_true] at at_r
  by_cases wp : p.writePosition r = true
  · obtain ⟨d, n, rfl, rfl⟩ := write_position wp
    refine ⟨d, n, rfl, fun x hx other => ?_, ?_⟩
    · have at_x := congrFun unread x
      have notPosition : (Prereq.write d n).writePosition x = false := by
        cases x with
        | position e =>
            simp only [Prereq.writePosition, decide_eq_false_iff_not]
            intro same; exact other (by rw [same])
        | _ => rfl
      simpa [reading, hx, notPosition] using at_x
    · simp only [wp, ite_true, Prereq.applied, Option.some.injEq] at at_r
      by_cases h1 : n ≤ (s (.position d)).getD 0 <;>
        by_cases h2 : n ≤ (s' (.position d)).getD 0 <;> simp_all <;> omega
  · simp only [wp, Bool.false_eq_true, ite_false] at at_r
    exact absurd at_r differs

/-- Strictly rising positions, each above the one before. -/
def Rising : Nat → List Nat → Prop
  | _, [] => True
  | p, x :: xs => p < x ∧ Rising x xs

/-- While positions only rise, a wait on write `n` whose writer stands at `p`
sees fewer partial advances than the writes still missing before it. -/
theorem partial_advances_bounded {p n : Nat} {ps : List Nat} (rising : Rising p ps)
    (below : ∀ x ∈ ps, x < n) : ps.length ≤ n - 1 - p := by
  induction ps generalizing p with
  | nil => simp
  | cons x xs ih =>
      obtain ⟨lt, rest⟩ := rising
      have hx := below x (by simp)
      have := ih rest fun y hy => below y (List.mem_cons_of_mem _ hy)
      simp only [List.length_cons]
      omega

/-- A write's partial advance in numbers: Ana's reload waits on Ben's write 5
while Ben's position moves from 3 to 4. The commit changes a named row, and
the reading is unchanged. -/
theorem partial_advance_counts (s : State)
    (three : s (.position 2) = some 3) :
    names (.write 2 5) (.position 2) = true ∧
    reading (.write 2 5) s =
      reading (.write 2 5) (fun r => if r = .position 2 then some 4 else s r) := by
  refine ⟨by decide, ?_⟩
  funext r
  by_cases hr : r = .position 2
  · subst hr; simp [reading, names, Prereq.rows, Pattern.covers, Prereq.writePosition,
      Prereq.applied, three]
  · simp [reading, hr]

/-! ## What each recorded condition names -/

/-- A stored object a condition can name: an entry, a key copy, a write, or
one only storage supplies (a snapshot, file, request or other object). -/
inductive Object
  | entry (device number : Nat)
  | keyCopy (writer number : Nat)
  | write (device number : Nat)
  | stored
  deriving DecidableEq, Repr

/-- A recorded first unmet condition (`PendingReason`), as far as what ends
its wait goes. -/
inductive Reason
  /-- `Missing { path }` or `Waits(Object(path))`. -/
  | object (o : Object)
  | catchUp
  | registration (device : Nat)
  | schemaPublication
  | reload (audience : String)
  | positions (device : Nat)
  | entryFinality
  /-- `KeyUnavailable`: the key itself, not a row. -/
  | keyUnavailable
  /-- `ProviderPending`, `PendingOwner`, `AccountInUse`. -/
  | providerAccess (account : String)
  /-- `Storage` or `NoStorage`: the connection. -/
  | storage
  /-- Every other condition waits for time, the app or an update. -/
  | other
  deriving DecidableEq, Repr

/-- What a wait for a reason watches: its prerequisites' rows, the key it
lacks, or the storage connection. -/
structure Needs where
  prereqs : List Prereq
  key : Bool
  connection : Bool
  deriving DecidableEq, Repr

def Object.prereqs : Object → List Prereq
  | .entry d n => [.entry d n]
  | .keyCopy w n => [.keyCopy w n]
  | .write d n => [.write d n]
  | .stored => []

/-- `operation_evidence.rs` `needs`, plus the reload operation a reload wait
names (`arm_evidence`'s `reload`). -/
def needs (reason : Reason) (reloadOperation : Option Nat) : Needs :=
  match reason with
  | .object o => ⟨o.prereqs, false, false⟩
  | .catchUp => ⟨[], false, false⟩
  | .registration d => ⟨[.device d], false, false⟩
  | .schemaPublication => ⟨[.boundaries], false, false⟩
  | .reload a => ⟨[.reload a] ++ reloadOperation.toList.map .operation, false, false⟩
  | .positions d => ⟨[.peerPositions d], false, false⟩
  | .entryFinality => ⟨[.finality], false, false⟩
  | .keyUnavailable => ⟨[], true, false⟩
  | .providerAccess a => ⟨[.providerAccess a], false, false⟩
  | .storage => ⟨[], false, true⟩
  | .other => ⟨[], false, false⟩

/-- A condition naming an object only storage supplies, or none of these,
names nothing a commit can change: its wait ends by time alone. -/
theorem time_only (reason : Reason) (op : Option Nat)
    (h : reason = .object .stored ∨ reason = .catchUp ∨ reason = .other) :
    needs reason op = ⟨[], false, false⟩ := by
  rcases h with rfl | rfl | rfl <;> rfl

end CovenIO.PrerequisiteRows
