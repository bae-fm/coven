import CovenMerge.ReceiveMerge

/-! The proposed retirement rule. Seeds pass the landing window; a counting
reader retains its retired causes. Neither rule can override refusal after
reading one's own retirement, or a reset/schema/snapshot exclusion. The old
window-only functions remain in `ReceiveMerge` for comparison. -/

namespace CovenMerge.Receive.RetiredClosure

variable {W Row Col : Type}

def beforeRemoval (P : Policy W) (w : W) : Bool :=
  P.retirements.all fun e => !(e.device == P.logs.device w && P.readEntry w e.entry)

def retired (P : Policy W) (w : W) : Bool :=
  P.retirements.any fun e => e.device == P.logs.device w

def permitted (P : Policy W) (w : W) : Bool :=
  snapshotRetains P.boundary (boundaryEligible P.reset P.readEntry P.schema) w &&
    beforeRemoval P w

/-- `received` includes the represented snapshot inputs as well as retained
log writes. Their immutable recorded pasts are supplied separately. -/
inductive Counts (P : Policy W) (past : W → W → Bool) (received : List W) : W → Prop where
  | seed {w} : w ∈ received → permitted P w = true → withinWindow P w = true →
      Counts P past received w
  | reader {w a} : Counts P past received w → a ∈ received → permitted P a = true →
      retired P a = true → past w a = true → Counts P past received a

theorem counts_received {P : Policy W} {past : W → W → Bool} {received : List W} {w : W}
    (h : Counts P past received w) : w ∈ received := by
  cases h <;> assumption

theorem counts_permitted {P : Policy W} {past : W → W → Bool} {received : List W} {w : W}
    (h : Counts P past received w) : permitted P w = true := by
  cases h <;> assumption

/-- The defining operator; the inductive set below is its least fixed point,
not any larger set sustained by a cycle of readers. -/
def extend (P : Policy W) (past : W → W → Bool) (received : List W)
    (S : W → Prop) (a : W) : Prop :=
  a ∈ received ∧ permitted P a = true ∧
    (withinWindow P a = true ∨
      (retired P a = true ∧ ∃ w, S w ∧ past w a = true))

theorem counts_fixed (P : Policy W) (past : W → W → Bool) (received : List W) (a : W) :
    Counts P past received a ↔ extend P past received (Counts P past received) a := by
  constructor
  · intro h
    cases h with
    | seed mem permit window => exact ⟨mem, permit, Or.inl window⟩
    | reader reader mem permit retired read =>
      exact ⟨mem, permit, Or.inr ⟨retired, _, reader, read⟩⟩
  · rintro ⟨mem, permit, window | ⟨retired, w, reader, read⟩⟩
    · exact .seed mem permit window
    · exact .reader reader mem permit retired read

theorem counts_least (P : Policy W) (past : W → W → Bool) (received : List W)
    (S : W → Prop) (closed : ∀ w, extend P past received S w → S w) :
    ∀ w, Counts P past received w → S w := by
  intro w h
  induction h with
  | seed mem permit window => exact closed _ ⟨mem, permit, Or.inl window⟩
  | reader _ mem permit retired read ih =>
    exact closed _ ⟨mem, permit, Or.inr ⟨retired, _, ih, read⟩⟩

theorem counts_have_seed {P : Policy W} {past : W → W → Bool} {received : List W} {w : W}
    (h : Counts P past received w) :
    ∃ root ∈ received, permitted P root = true ∧ withinWindow P root = true := by
  induction h with
  | seed mem permit window => exact ⟨_, mem, permit, window⟩
  | reader _ _ _ _ _ ih => exact ih

/-- A finite path from a window-admitted root is an exact certificate. Only
that path and its root need survive a policy change to preserve this verdict. -/
theorem counts_path (P : Policy W) (past : W → W → Bool) (received : List W) (a : W) :
    Counts P past received a ↔ ∃ root ∈ received,
      permitted P root = true ∧ withinWindow P root = true ∧
      Star (fun w x => x ∈ received ∧ permitted P x = true ∧
        retired P x = true ∧ past w x = true) root a := by
  constructor
  · intro h
    induction h with
    | seed mem permit window => exact ⟨_, mem, permit, window, .refl _⟩
    | reader _ mem permit retired read ih =>
      obtain ⟨root, hr, hp, hw, path⟩ := ih
      exact ⟨root, hr, hp, hw, path.trans (.single ⟨mem, permit, retired, read⟩)⟩
  · rintro ⟨root, mem, permit, window, path⟩
    have seed : Counts P past received root := .seed mem permit window
    clear mem permit window
    induction path with
    | refl => exact seed
    | head edge _ ih => exact ih (.reader seed edge.1 edge.2.1 edge.2.2.1 edge.2.2.2)

theorem counts_transfer {P Q : Policy W} {past nextPast : W → W → Bool}
    {xs ys : List W}
    (received : ∀ w, w ∈ xs → w ∈ ys)
    (permits : ∀ w, permitted P w = true → permitted Q w = true)
    (windows : ∀ w, withinWindow P w = true → withinWindow Q w = true)
    (edges : ∀ w a, past w a = true → nextPast w a = true)
    (retirements : ∀ w, retired P w = true →
      retired Q w = true ∨ withinWindow Q w = true)
    {w : W} (h : Counts P past xs w) : Counts Q nextPast ys w := by
  induction h with
  | seed mem permit window => exact .seed (received _ mem) (permits _ permit) (windows _ window)
  | reader _ mem permit retired read ih =>
    rcases retirements _ retired with hr | hw
    · exact .reader ih (received _ mem) (permits _ permit) hr (edges _ _ read)
    · exact .seed (received _ mem) (permits _ permit) hw

theorem counts_monotone (P : Policy W) (past : W → W → Bool) {xs ys : List W}
    (grows : ∀ w, w ∈ xs → w ∈ ys) {w : W} :
    Counts P past xs w → Counts P past ys w :=
  counts_transfer grows (fun _ h => h) (fun _ h => h) (fun _ _ h => h)
    (fun _ h => Or.inl h)

/-- With fixed policy and immutable pasts, receiving more writes can change
excluded to counting, never counting to excluded. No age threshold is used. -/
theorem counting_permanent (P : Policy W) (past : W → W → Bool)
    (received : Nat → List W) (t : Nat)
    (grows : ∀ u, t ≤ u → ∀ w, w ∈ received t → w ∈ received u)
    (w : W) (counts : Counts P past (received t) w) :
    ∀ u, t ≤ u → Counts P past (received u) w := by
  intro u later
  exact counts_monotone P past (grows u later) counts

/-- A negative verdict is permanent if closure over every possible future
receipt still excludes it. Completeness of that upper bound is a separate
discovery obligation; draining the retired author's log alone is insufficient. -/
theorem exclusion_permanent (P : Policy W) (past : W → W → Bool)
    (possible : List W) (received : Nat → List W) (t : Nat)
    (bounded : ∀ u, t ≤ u → ∀ w, w ∈ received u → w ∈ possible)
    (w : W) (excluded : ¬ Counts P past possible w) :
    ∀ u, t ≤ u → ¬ Counts P past (received u) w := by
  intro u later counts
  exact excluded (counts_monotone P past (bounded u later) counts)

/-- An exclusion can also be certified without a finite future bound: every
possible path of readers into `blocked` stays inside it, and it has no seed. -/
theorem exclusion_barrier (P : Policy W) (past : W → W → Bool)
    (blocked : W → Prop)
    (noSeed : ∀ w, blocked w → permitted P w = true → withinWindow P w = false)
    (noReader : ∀ w a, blocked a → permitted P a = true → retired P a = true →
      past w a = true → blocked w)
    (received : List W) (a : W) (ha : blocked a) : ¬ Counts P past received a := by
  intro counts
  apply counts_least P past received (fun w => ¬ blocked w) ?_ a counts ha
  intro w hw blocked
  rcases hw with ⟨_, permit, window | ⟨retired, reader, outside, read⟩⟩
  · have := noSeed w blocked permit
    simp [window] at this
  · exact outside (noReader reader w blocked permit retired read)

theorem revival_needs_other_write {P : Policy W} {past : W → W → Bool} {received : List W}
    {w : W} (counts : Counts P past received w) (late : withinWindow P w = false) :
    ∃ root ∈ received, root ≠ w ∧ permitted P root = true ∧ withinWindow P root = true := by
  obtain ⟨root, mem, permit, window⟩ := counts_have_seed counts
  refine ⟨root, mem, ?_, permit, window⟩
  intro eq
  subst root
  simp [late] at window

/-- Equal immutable facts, entry sets and received sets give equal counting
sets. Repeated entries and repeated receipts have no effect. -/
theorem counts_deterministic (P Q : Policy W) (past nextPast : W → W → Bool)
    (xs ys : List W)
    (facts : P.boundary = Q.boundary ∧ P.logs = Q.logs ∧ P.reset = Q.reset ∧
      P.readEntry = Q.readEntry ∧ P.schema = Q.schema ∧ P.stored = Q.stored)
    (entries : ∀ e, e ∈ P.retirements ↔ e ∈ Q.retirements)
    (pasts : past = nextPast) (received : ∀ w, w ∈ xs ↔ w ∈ ys) :
    Counts P past xs = Counts Q nextPast ys := by
  have window : ∀ w, withinWindow P w = withinWindow Q w := by
    intro w
    apply Bool.eq_iff_iff.mpr
    simp only [window_exact, entries, facts.2.1, facts.2.2.2.1, facts.2.2.2.2.2]
  have retire : ∀ w, retired P w = retired Q w := by
    intro w
    apply Bool.eq_iff_iff.mpr
    simp only [retired, List.any_eq_true, entries, facts.2.1]
  have permit : ∀ w, permitted P w = permitted Q w := by
    intro w
    have before : beforeRemoval P w = beforeRemoval Q w := by
      apply Bool.eq_iff_iff.mpr
      simp only [beforeRemoval, List.all_eq_true, entries, facts.2.1, facts.2.2.2.1]
    simp only [permitted, before, facts.1, facts.2.2.1, facts.2.2.2.1, facts.2.2.2.2.1]
  subst nextPast
  funext w
  apply propext
  constructor
  · exact counts_transfer (fun w => (received w).mp) (fun w h => (permit w) ▸ h)
      (fun w h => (window w) ▸ h) (fun _ _ h => h) (fun w h => Or.inl ((retire w) ▸ h))
  · exact counts_transfer (fun w => (received w).mpr) (fun w h => (permit w).symm ▸ h)
      (fun w h => (window w).symm ▸ h) (fun _ _ h => h) (fun w h => Or.inl ((retire w).symm ▸ h))

theorem post_read_refused (P : Policy W) (past : W → W → Bool) (received : List W)
    (w : W) (e : Retirement) (kept : e ∈ P.retirements)
    (own : e.device = P.logs.device w) (read : P.readEntry w e.entry = true) :
    ¬ Counts P past received w := by
  intro h
  have permit := counts_permitted h
  simp only [permitted, Bool.and_eq_true] at permit
  have before := permit.2
  have := (List.all_eq_true.mp before) e kept
  simp [own, read] at this

theorem not_retired_window (P : Policy W) (w : W) (active : retired P w = false) :
    withinWindow P w = true := by
  apply (window_exact P w).mpr
  intro e mem own
  have hr : retired P w = true := List.any_eq_true.mpr ⟨e, mem, by simp [own]⟩
  simp [active] at hr

noncomputable def counting (P : Policy W) (past : W → W → Bool) (received : List W) : W → Bool :=
  fun w => @decide (Counts P past received w) (Classical.propDecidable _)

@[simp] theorem counting_iff (P : Policy W) (past : W → W → Bool) (received : List W) (w : W) :
    counting P past received w = true ↔ Counts P past received w := by
  simp [counting]

theorem counts_cause {P : Policy W} {past : W → W → Bool} {received : List W} {w a : W}
    (reader : Counts P past received w) (mem : a ∈ received)
    (permit : permitted P a = true) (read : past w a = true) : Counts P past received a := by
  cases hr : retired P a
  · exact .seed mem permit (not_retired_window P a hr)
  · exact .reader reader mem permit hr read

/-- A snapshot cannot freeze a different counting verdict. Rebuilding the
boundary must also rebuild any snapshot whose represented inputs changed. -/
theorem retains_iff (P : Policy W) (past : W → W → Bool) (received : List W)
    (snapshotCounts : ∀ w, P.boundary.inputs w = true → Counts P past received w) (w : W) :
    snapshotRetains P.boundary (counting P past received) w = true ↔
      Counts P past received w := by
  constructor
  · intro h
    simp only [snapshotRetains, Bool.or_eq_true, Bool.and_eq_true, Bool.not_eq_true',
      counting_iff] at h
    exact h.elim (snapshotCounts w) And.right
  · intro h
    have hp := counts_permitted h
    simp only [permitted, snapshotRetains, Bool.and_eq_true, Bool.or_eq_true,
      Bool.not_eq_true'] at hp
    simp only [snapshotRetains, Bool.or_eq_true, Bool.and_eq_true, Bool.not_eq_true',
      counting_iff]
    rcases hp.1 with snapshot | later
    · exact Or.inl snapshot
    · exact Or.inr ⟨later.1, h⟩

/-- Only boundary exclusions still require a generation witness. Retirement
adds none: every permitted cause of a counting write is retained by closure.
`safePast` states an authoring/admission obligation absent from `Valid`: a
write admitted before its own removal cannot rely on a refused cause. -/
theorem generations {M : Writes W Row Col} (P : Policy W) (received : List W)
    (boundaryGenerations : SnapshotGenerations M P.boundary
      (boundaryEligible P.reset P.readEntry P.schema))
    (complete : Closed M (fun w => w ∈ received))
    (safePast : ∀ w ∈ received, beforeRemoval P w = true →
      ∀ a, M.past w a = true → beforeRemoval P a = true)
    (snapshotCounts : ∀ w, P.boundary.inputs w = true → Counts P M.past received w) :
    SnapshotGenerations M P.boundary (counting P M.past received) := by
  intro w r ch kept change
  have hw := (retains_iff P M.past received snapshotCounts w).mp kept
  have permit := counts_permitted hw
  simp only [permitted, Bool.and_eq_true] at permit
  rcases boundaryGenerations w r ch permit.1 change with zero | ⟨a, ca, read, retained, ha, hk, hg⟩
  · exact Or.inl zero
  · have pa : permitted P a = true := by
      simp only [permitted, Bool.and_eq_true]
      exact ⟨retained, safePast w (counts_received hw) permit.2 a read⟩
    have counts := counts_cause hw (complete w (counts_received hw) a read) pa read
    exact Or.inr ⟨a, ca, read, (retains_iff P M.past received snapshotCounts a).mpr counts, ha, hk, hg⟩

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

/-- The ordinary refusal gate proves that no consumed log write was made
after reading its own removal. Window exclusions still pass this gate. -/
theorem passed_before_removal (P : Policy W) (past : W → List W)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate)
    (refuses : ∀ t w, gate t w = true → beforeRemoval P w = true) (t : Nat) :
    ∀ w ∈ (run.state t).passed, beforeRemoval P w = true := by
  induction t with
  | zero => simp [run.start, initial]
  | succ t ih => exact step_before_removal P past (run.next t) (refuses t) ih

noncomputable def applied (P : Policy W) (past : W → W → Bool) (received : List W)
    (s : State W) : List W :=
  s.passed.filter (afterSnapshot P.boundary (counting P past received))

noncomputable def effective (M : Writes W Row Col) (P : Policy W) (received : List W) :
    Writes W Row Col := snapshotWrites M P.boundary (counting P M.past received)

noncomputable def merged (M : Writes W Row Col) (P : Policy W) (received : List W)
    (snapshot : St W Row Col) (s : State W) : St W Row Col :=
  reloadSnapshot M P.boundary (counting P M.past received) snapshot s.passed

def inputs (P : Policy W) (past : W → W → Bool) (received : List W) (s : State W) : W → Prop :=
  fun w => P.boundary.inputs w = true ∨ w ∈ applied P past received s

theorem receive_isSpec {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (received : List W) (past : W → List W)
    (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate)
    (boundaryGenerations : SnapshotGenerations M P.boundary
      (boundaryEligible P.reset P.readEntry P.schema))
    (complete : Closed M (fun w => w ∈ received))
    (safePast : ∀ w ∈ received, beforeRemoval P w = true →
      ∀ a, M.past w a = true → beforeRemoval P a = true)
    (snapshotCounts : ∀ w, P.boundary.inputs w = true → Counts P M.past received w)
    (closed : Closed (effective M P received) (fun w => P.boundary.inputs w = true))
    (snapshot : St W Row Col)
    (correct : IsSpec (effective M P received) (fun w => P.boundary.inputs w = true) snapshot)
    (t : Nat) :
    IsSpec (effective M P received) (inputs P M.past received (run.state t))
      (merged M P received snapshot (run.state t)) ∧
    Closed (effective M P received) (inputs P M.past received (run.state t)) :=
  foldl_isSpec (snapshot_valid valid P.boundary (counting P M.past received)
    (generations P received boundaryGenerations complete safePast snapshotCounts)) closed correct
    (applied_causal P.boundary P.logs past exactPast (counting P M.past received) gate run t)

theorem receive_converges {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (received : List W) (past : W → List W)
    (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gateA gateB : Nat → W → Bool)
    (a : Run P.boundary P.logs past gateA) (b : Run P.boundary P.logs past gateB)
    (boundaryGenerations : SnapshotGenerations M P.boundary
      (boundaryEligible P.reset P.readEntry P.schema))
    (complete : Closed M (fun w => w ∈ received))
    (safePast : ∀ w ∈ received, beforeRemoval P w = true →
      ∀ x, M.past w x = true → beforeRemoval P x = true)
    (snapshotCounts : ∀ w, P.boundary.inputs w = true → Counts P M.past received w)
    (closed : Closed (effective M P received) (fun w => P.boundary.inputs w = true))
    (snapshot : St W Row Col)
    (correct : IsSpec (effective M P received) (fun w => P.boundary.inputs w = true) snapshot)
    (ta tb : Nat) (same : ∀ w, inputs P M.past received (a.state ta) w ↔
      inputs P M.past received (b.state tb) w) :
    merged M P received snapshot (a.state ta) = merged M P received snapshot (b.state tb) :=
  isSpec_unique
    (receive_isSpec valid P received past exactPast gateA a boundaryGenerations complete
      safePast snapshotCounts closed snapshot correct ta).1
    (isSpec_congr (fun w => (same w).symm)
      (receive_isSpec valid P received past exactPast gateB b boundaryGenerations complete
        safePast snapshotCounts closed snapshot correct tb).1)

/-- Without a snapshot, reset or schema exclusion, receive discharges both
complete pasts and hard-refusal safety. There is no generation hypothesis of
any kind beyond the ordinary authored `Valid` contract. -/
theorem receive_prefix_correct {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (empty : ∀ w, P.boundary.covered w = false)
    (ordinary : ∀ w, boundaryEligible P.reset P.readEntry P.schema w = true)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate)
    (refuses : ∀ t w, gate t w = true → beforeRemoval P w = true) (t : Nat) :
    Valid (effective M P (run.state t).passed) ∧
    IsSpec (effective M P (run.state t).passed)
      (inputs P M.past (run.state t).passed (run.state t))
      (merged M P (run.state t).passed St.init (run.state t)) ∧
    Closed (effective M P (run.state t).passed)
      (inputs P M.past (run.state t).passed (run.state t)) := by
  have noInput : ∀ w, P.boundary.inputs w = true → False := by
    intro w h
    have := P.boundary.inputs_covered w h
    simp [empty w] at this
  have complete : Closed M (fun w => w ∈ (run.state t).passed) := by
    simpa only [false_or] using
      (foldl_isSpec valid closed_empty isSpec_init (causalFrom_of_causalOrder
        (received_causalOrder P.boundary P.logs past exactPast empty gate run t))).2
  have safe := passed_before_removal P past gate run refuses t
  have boundaryGenerations : SnapshotGenerations M P.boundary
      (boundaryEligible P.reset P.readEntry P.schema) := by
    intro w r ch _ change
    rcases valid.gen_seen w r ch change with zero | ⟨a, ca, read, ha, hk, hg⟩
    · exact Or.inl zero
    · exact Or.inr ⟨a, ca, read, by simp [snapshotRetains, empty, ordinary], ha, hk, hg⟩
  have safePast := fun w hw (_ : beforeRemoval P w = true) a ha => safe a (complete w hw a ha)
  have snapshotCounts := fun w h => (noInput w h).elim (C := Counts P M.past (run.state t).passed w)
  have gen := generations P (run.state t).passed boundaryGenerations complete safePast snapshotCounts
  refine ⟨snapshot_valid valid P.boundary _ gen,
    receive_isSpec valid P (run.state t).passed past exactPast gate run boundaryGenerations
      complete safePast snapshotCounts (fun w h => (noInput w h).elim) St.init ?_ t⟩
  exact isSpec_congr (fun w => ⟨False.elim, noInput w⟩) isSpec_init

theorem receive_prefix_converges {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (empty : ∀ w, P.boundary.covered w = false)
    (ordinary : ∀ w, boundaryEligible P.reset P.readEntry P.schema w = true)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gateA gateB : Nat → W → Bool)
    (a : Run P.boundary P.logs past gateA) (b : Run P.boundary P.logs past gateB)
    (refusesA : ∀ t w, gateA t w = true → beforeRemoval P w = true)
    (refusesB : ∀ t w, gateB t w = true → beforeRemoval P w = true)
    (ta tb : Nat) (same : ∀ w, w ∈ (a.state ta).passed ↔ w ∈ (b.state tb).passed) :
    merged M P (a.state ta).passed St.init (a.state ta) =
      merged M P (b.state tb).passed St.init (b.state tb) := by
  have counts := counts_deterministic P P M.past M.past (a.state ta).passed (b.state tb).passed
    ⟨rfl, rfl, rfl, rfl, rfl, rfl⟩ (fun _ => Iff.rfl) rfl same
  have bools : counting P M.past (a.state ta).passed = counting P M.past (b.state tb).passed := by
    funext w
    apply Bool.eq_iff_iff.mpr
    simp only [counting_iff]
    exact Iff.of_eq (congrFun counts w)
  have sa := (receive_prefix_correct valid P empty ordinary past exactPast gateA a refusesA ta).2.1
  have sb := (receive_prefix_correct valid P empty ordinary past exactPast gateB b refusesB tb).2.1
  simp only [effective, merged, bools] at sa sb ⊢
  exact isSpec_unique sa (isSpec_congr (fun w => by
    simp only [inputs, applied, List.mem_filter, bools, same w]) sb)

variable {K : Type} [DecidableEq Row] [DecidableEq Col] [DecidableEq K]

theorem receive_value_accounted {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (received : List W) (past : W → List W)
    (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate)
    (boundaryGenerations : SnapshotGenerations M P.boundary
      (boundaryEligible P.reset P.readEntry P.schema))
    (complete : Closed M (fun w => w ∈ received))
    (safePast : ∀ w ∈ received, beforeRemoval P w = true →
      ∀ a, M.past w a = true → beforeRemoval P a = true)
    (snapshotCounts : ∀ w, P.boundary.inputs w = true → Counts P M.past received w)
    (closed : Closed (effective M P received) (fun w => P.boundary.inputs w = true))
    (snapshot : St W Row Col)
    (correct : IsSpec (effective M P received) (fun w => P.boundary.inputs w = true) snapshot)
    (t : Nat) (I : EntryInputs Row K)
    (present : ∀ r, I.present r = decide ((merged M P received snapshot (run.state t)).gen r % 2 = 1))
    {r : Row} {c : Col} {w : W} {k : Nat}
    (setter : Setter (effective M P received) (inputs P M.past received (run.state t)) r c w k) :
    ValueAccounted (effective M P received) (inputs P M.past received (run.state t))
      (merged M P received snapshot (run.state t)) I r c w k := by
  have spec := receive_isSpec valid P received past exactPast gate run boundaryGenerations
    complete safePast snapshotCounts closed snapshot correct t
  exact value_accounted (snapshot_valid valid P.boundary (counting P M.past received)
    (generations P received boundaryGenerations complete safePast snapshotCounts)) spec.2 spec.1 I present setter

theorem receive_prefix_value_accounted {M : Writes W Row Col} (valid : Valid M) (P : Policy W)
    (empty : ∀ w, P.boundary.covered w = false)
    (ordinary : ∀ w, boundaryEligible P.reset P.readEntry P.schema w = true)
    (past : W → List W) (exactPast : ∀ w a, a ∈ past w ↔ M.past w a = true)
    (gate : Nat → W → Bool) (run : Run P.boundary P.logs past gate)
    (refuses : ∀ t w, gate t w = true → beforeRemoval P w = true)
    (t : Nat) (I : EntryInputs Row K)
    (present : ∀ r, I.present r =
      decide ((merged M P (run.state t).passed St.init (run.state t)).gen r % 2 = 1))
    {r : Row} {c : Col} {w : W} {k : Nat}
    (setter : Setter (effective M P (run.state t).passed)
      (inputs P M.past (run.state t).passed (run.state t)) r c w k) :
    ValueAccounted (effective M P (run.state t).passed)
      (inputs P M.past (run.state t).passed (run.state t))
      (merged M P (run.state t).passed St.init (run.state t)) I r c w k := by
  have spec := receive_prefix_correct valid P empty ordinary past exactPast gate run refuses t
  exact value_accounted spec.1 spec.2.2 spec.2.1 I present setter

end
end CovenMerge.Receive.RetiredClosure
