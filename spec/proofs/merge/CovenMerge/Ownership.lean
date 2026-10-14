import CovenMerge.Boundary
import CovenStorage.RowAuthorship

/-! Opt-in owner tables. The creating write is retained identity evidence,
shared by every incarnation and audience copy of one logical row. Authentication
and conflict selection of that identity are admission obligations. -/
namespace CovenMerge.Ownership

structure Policy (W Row Table Member : Type) where
  table : Row → Table
  owned : Table → Bool
  creator : Row → W
  author : W → Member

variable {W Row Col Table Member K : Type}
  [DecidableEq W] [DecidableEq Member]

def owner (P : Policy W Row Table Member) (r : Row) : Member := P.author (P.creator r)

def allowed (P : Policy W Row Table Member) (w : W) (r : Row) : Bool :=
  !P.owned (P.table r) || decide (P.author w = owner P r)

theorem opt_out (P : Policy W Row Table Member) (w : W) (r : Row)
    (h : P.owned (P.table r) = false) : allowed P w r = true := by simp [allowed, h]

theorem creator_allowed (P : Policy W Row Table Member) (r : Row) :
    allowed P (P.creator r) r = true := by simp [allowed, owner]

/-- Moves and reinsertions cannot replace the creator. Different merge states
are deliberately absent from the owner calculation. -/
theorem owner_fixed (P : Policy W Row Table Member) (a b : Row)
    (same : P.creator a = P.creator b) : owner P a = owner P b := by simp [owner, same]

theorem non_owner_denied (P : Policy W Row Table Member) (w : W) (r : Row)
    (owned : P.owned (P.table r) = true) (other : P.author w ≠ owner P r) :
    allowed P w r = false := by simp [allowed, owned, other]

/-- Use the first generation's canonical creating write, not first receipt.
The existing merge selects its smallest timestamp before the owner filter.
An audience transfer carries this original identity instead of creating one. -/
theorem selected_creation {S : W → Prop} {M : Writes W Row Col}
    (valid : Valid M) {st : St W Row Col} (correct : IsSpec M S st)
    (r : Row) (creator : W) (selected : st.genWrite r 1 = some creator) :
    S creator ∧ ∃ ch, M.chg creator r = some ch ∧ ch.kind = .ins ∧ ch.gen = 0 ∧
      ∀ other, GenChange M S r other 1 → M.ts creator ≤ M.ts other := by
  obtain ⟨⟨mem, ch, change, _, generation⟩, earliest⟩ := (correct.gw_some r 1 creator).mp selected
  have zero : ch.gen = 0 := by omega
  exact ⟨mem, ch, change, (valid.parity creator r ch change).mpr (by simp [zero]), zero, earliest⟩

theorem creation_selection_converges (M : Writes W Row Col) (valid : Valid M)
    {a b : List W} (ca : CausalOrder M a) (cb : CausalOrder M b)
    (same : ∀ w, w ∈ a ↔ w ∈ b) (r : Row) :
    (a.foldl (step M) St.init).genWrite r 1 = (b.foldl (step M) St.init).genWrite r 1 := by
  rw [merge_converges valid ca cb same]

/-- Updates, deletes and reinsertions at later generations cannot change the
initial creator record, including after a row stops being displayed. -/
theorem later_change_preserves_creation (M : Writes W Row Col) (st : St W Row Col)
    (w : W) (r : Row) (later : ∀ ch, M.chg w r = some ch → ch.gen ≠ 0) :
    (step M st w).genWrite r 1 = st.genWrite r 1 := by
  cases hc : M.chg w r with
  | none => simp [step, genWriteStep, hc]
  | some ch =>
    have positive := later ch hc
    simp [step, genWriteStep, hc, positive]

/-- Snapshot verification supplies the same owner enforced by the filter.
The policy's authors come from authenticated write identities; creation
selection remains a separate, permanent identity obligation. -/
theorem accepted_owner_matches_filter {Key Payload Digest Signature : Type}
    (h : CovenStorage.RowAuthorship.Hashing (CovenStorage.RowAuthorship.Change Key Payload) Digest)
    (s : CovenStorage.RowAuthorship.Signatures Member W
      (CovenStorage.RowAuthorship.Change Key Payload) Digest Signature h)
    (P : Policy W Row Table Member) (key : Row → Key) (r : Row)
    (claimed : Member) (signature : Signature)
    (proof : CovenStorage.RowAuthorship.Proof (CovenStorage.RowAuthorship.Change Key Payload) Digest)
    (authors : ∀ w, P.author w = s.author w)
    (accepted : CovenStorage.RowAuthorship.AcceptsCreator h s (key r) claimed (P.creator r) signature proof) :
    claimed = owner P r := by
  rw [owner, authors]
  exact (CovenStorage.RowAuthorship.owned_row_true_author h s (key r) claimed (P.creator r)
    signature proof accepted).1

/-- Ownership refusals use the same row-change identity as other losses. -/
inductive Cause (W Member Row : Type) where
  | notOwner (owner : Member)
  | foreignKey
  | move (protectedRow : Row) (owner : Member)
  | dependency (sources : List (W × Row))
  deriving DecidableEq, Repr

structure Loss (W Row Col Member : Type) extends LossRecord W Row Col (Cause W Member Row) where
  write : W

def changeLoss (w : W) (r : Row) (generation : Nat) (values : Col → Option W)
    (cause : Cause W Member Row) : Loss W Row Col Member :=
  ⟨⟨r, generation, none, values, cause⟩, w⟩

/-- Admission is a per-change verdict. The FK verifier supplies it after
checking direct permissions and recomputing every derived payload. -/
def base (guard : W → Row → Option (Cause W Member Row)) (w : W) (r : Row) : Bool :=
  (guard w r).isNone

def loss (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (guard : W → Row → Option (Cause W Member Row))
    (values : W → Row → Col → Option W) (received : List W) (w : W) (r : Row) :
    Option (Loss W Row Col Member) :=
  if w ∈ received then
    (M.chg w r).bind fun ch =>
      let record := fun cause => some (changeLoss w r ch.gen (values w r) cause)
      match guard w r with
      | some cause => record cause
      | none =>
        let s := DataDependency.authoredState M valid p (base guard)
        if DataDependency.dependsOnDropped s w r then
          record (.dependency ((DataDependency.droppedSources s w r).map (·, r)))
        else none
  else none

def merged (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (guard : W → Row → Option (Cause W Member Row))
    (received : List W) : St W Row Col :=
  Data.merged M valid p (base guard) St.init received

theorem guard_recorded (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M)
    (guard : W → Row → Option (Cause W Member Row)) (values : W → Row → Col → Option W)
    (received : List W) (w : W) (r : Row) (ch : Change Col) (cause : Cause W Member Row)
    (mem : w ∈ received) (change : M.chg w r = some ch) (denied : guard w r = some cause) :
    loss M valid p guard values received w r = some (changeLoss w r ch.gen (values w r) cause) := by
  simp [loss, mem, change, denied]

/-- Empty-valued deletions are recorded too. -/
theorem excluded_recorded (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M)
    (guard : W → Row → Option (Cause W Member Row)) (values : W → Row → Col → Option W)
    (received : List W) (w : W) (r : Row) (ch : Change Col)
    (mem : w ∈ received) (change : M.chg w r = some ch)
    (excluded : DataDependency.retained (DataDependency.authoredState M valid p (base guard)) w r = false) :
    ∃ record, loss M valid p guard values received w r = some record ∧
      record.write = w ∧ record.row = r ∧ record.generation = ch.gen ∧ record.values = values w r := by
  cases hg : guard w r with
  | some cause => exact ⟨_, guard_recorded M valid p guard values received w r ch cause mem change hg,
      rfl, rfl, rfl, rfl⟩
  | none =>
    have dep : DataDependency.dependsOnDropped (DataDependency.authoredState M valid p (base guard)) w r = true := by
      cases hd : DataDependency.dependsOnDropped (DataDependency.authoredState M valid p (base guard)) w r
      · have kept := DataDependency.independent_retained
          (DataDependency.authoredState M valid p (base guard)) w r
          (by simp [DataDependency.authoredState, base, hg])
          (by simp [DataDependency.authoredState, change]) hd
        simp [excluded] at kept
      · rfl
    exact ⟨changeLoss w r ch.gen (values w r) (.dependency
      ((DataDependency.droppedSources (DataDependency.authoredState M valid p (base guard)) w r).map (·, r))),
      by simp [loss, mem, change, hg, dep], rfl, rfl, rfl, rfl⟩

theorem converges (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M)
    (guard : W → Row → Option (Cause W Member Row)) {a b : List W}
    (ca : CausalOrder M a) (cb : CausalOrder M b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    merged M valid p guard a = merged M valid p guard b :=
  Data.converges M valid p (base guard) _ closed_empty St.init isSpec_init
    (causalFrom_of_causalOrder ca) (causalFrom_of_causalOrder cb) same

theorem loss_converges (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M)
    (guard : W → Row → Option (Cause W Member Row)) (values : W → Row → Col → Option W)
    {a b : List W} (same : ∀ w, w ∈ a ↔ w ∈ b) :
    loss M valid p guard values a = loss M valid p guard values b := by
  funext w r
  simp only [loss, same w]

variable [DecidableEq Row] [DecidableEq Col] [DecidableEq K]

theorem value_accounted (M : Writes W Row Col) (valid : Valid M)
    (p : DataDependency.Authored M)
    (guard : W → Row → Option (Cause W Member Row)) (values : W → Row → Col → Option W)
    (received : List W) (causal : CausalOrder M received) (I : EntryInputs Row K)
    (present : ∀ r, I.present r = decide ((merged M valid p guard received).gen r % 2 = 1))
    (w : W) (r : Row) (c : Col) (k : Nat)
    (setter : Setter M (fun a => a ∈ received) r c w k) (written : values w r c = some w) :
    CovenMerge.ValueAccounted (DataDependency.effective M valid p (base guard)) (fun a => a ∈ received)
      (merged M valid p guard received) I r c w k ∨
    ∃ record, loss M valid p guard values received w r = some record ∧ record.values c = some w := by
  obtain ⟨mem, ch, change, kind, sets, inc⟩ := setter
  cases kept : DataDependency.retained (DataDependency.authoredState M valid p (base guard)) w r
  · obtain ⟨record, hl, _, _, _, hv⟩ := excluded_recorded M valid p guard values received w r ch mem change kept
    exact Or.inr ⟨record, hl, by rw [hv]; exact written⟩
  · have result := Data.isSpec M valid p (base guard) _ closed_empty St.init isSpec_init received
      (causalFrom_of_causalOrder causal)
    have correct : IsSpec (DataDependency.effective M valid p (base guard))
        (fun a => a ∈ received) (merged M valid p guard received) :=
      isSpec_congr (fun a => by simp) result.1
    have closed : Closed (DataDependency.effective M valid p (base guard)) (fun a => a ∈ received) :=
      closed_congr (fun a => by simp) result.2
    exact Or.inl (CovenMerge.value_accounted (DataDependency.effective_valid M valid p (base guard)) closed correct
      I present ⟨mem, ch, by simp [DataDependency.effective, kept, change], kind, sets, inc⟩)

def result (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (guard : W → Row → Option (Cause W Member Row))
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    (received : List W) : Device W Row Col × (W → Row → Option (Loss W Row Col Member)) :=
  let st := merged M valid p guard received
  let v := entryView (inputs st)
  (⟨st, v, lossRecord st v⟩, loss M valid p guard values received)

theorem result_converges (M : Writes W Row Col) (valid : Valid M) (p : DataDependency.Authored M)
    (guard : W → Row → Option (Cause W Member Row))
    (values : W → Row → Col → Option W) (inputs : St W Row Col → EntryInputs Row K)
    {a b : List W} (ca : CausalOrder M a) (cb : CausalOrder M b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    result M valid p guard values inputs a = result M valid p guard values inputs b := by
  simp only [result, converges M valid p guard ca cb same, loss_converges M valid p guard values same]

/-- Owned rows use the ordinary removal recorder: every hidden row retains
its values and every applicable cause. No delete permission trial is involved. -/
theorem constraint_effect_recorded (st : St W Row Col) (I : EntryInputs Row K) (r : Row)
    (hidden : (entryView I).removed r = true) :
    (entryView I).rules r ≠ [] ∧
    lossRecord st (entryView I) r none =
      some ⟨r, st.gen r, none, st.cell r, .rules ((entryView I).rules r)⟩ :=
  ⟨named_rules_nonempty I hidden, by simp [lossRecord, hidden]⟩

/-- Recompute after a cause drops. An independent remaining cause can still
hide the row; when none remains its original merged values are displayed. -/
theorem constraint_effect_reversed (st : St W Row Col) (I : EntryInputs Row K) (r : Row)
    (present : I.present r = true) (cleared : (entryView I).rules r = []) :
    (entryView I).shown r = true ∧ lossRecord st (entryView I) r none = none := by
  have shown := row_returns I present cleared
  refine ⟨shown, ?_⟩
  have absent : (entryView I).removed r = false := by
    change (I.present r && removal I.erase r) = false
    change (I.present r && !removal I.erase r) = true at shown
    simp_all
  simp [lossRecord, absent]

end CovenMerge.Ownership
