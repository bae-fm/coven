import CovenStorelogData.ReloadWaitChecks
import CovenMerge.Retirement
import CovenStorelog.FinalityExamples
import CovenStorelog.Horizon

/-! §10, §15: a reload and the retirement-window verdicts a snapshot carries.

A snapshot carries, for each covered change it holds, its author's verdict,
including `dropped` by the write's own retirement window (D7 section 5). A
loader cannot judge that window again: the snapshot names no storage time
for covered writes. Rust's load transaction therefore compares each carried
verdict with the landing verdict this device recorded for the same write,
if it recorded one, and fails the load on a difference
(`write_landing::agrees`, `SnapshotError::RetirementVerdict`). The reload
records `Failed(Database)`, which only an app action retries; a retry
selects again (`chooseVerdict`, `verdictRecord`).

What holds:
- the check never adopts a verdict that disagrees with one this device
  recorded (`verdict_load_agrees`, `verdict_iff`);
- a window verdict reads only the kept retirements of the write's device
  stored more than thirty days before it (`late_depends_on_old`). So a
  snapshot whose author's kept retirements agree there with this device's
  never disagrees (`prepared_agrees`, `agreeing_never_waits`), and one
  prepared once that prefix was final carries every write's final verdict
  (`loaded_verdict_final`);
- once the reload runs again, a newer snapshot this device can load and
  whose verdicts agree ends the failure (`newer_agreeing_ends_wait`).

What fails (`unfinal_snapshot_counterexample`, checked): a snapshot
prepared while the retirement deciding a covered write can still drop.
`VerdictHistory` is such a store-log history in the storage-time replay of
§9: Ben's removal is kept when Ana prepares her snapshot a day after his
write's deadline, and dropped once a later entry lands. Ana's snapshot
carries the write as `dropped`; the final replay admits it.
- Ben recorded his unfixed admission: every reload fails the check on Ana's
  snapshot, its record waits for an app action, and in a store with no
  further writes nothing requests another snapshot, since Ana's covers
  every write. The reload never ends.
- Once Ben's admission is fixed Rust no longer records it, and Carol, who
  never received the write, has nothing to compare: both load Ana's
  snapshot and drop a write the final replay admits, while a device that
  received it keeps its effects.

So §15 prepares a snapshot only once every covered write is ready
(`Preparation.ready`, Rust `write_landing::unsettled`): a final kept
retirement excludes it, or the author received every entry stored by thirty
days before it and each received retirement of its device stored then is
final. Then:
- the snapshot carries each final verdict (`ready_agrees`,
  `prepared_final`), and the reload's check never fails for a loader that
  holds the entries stored before the author's horizon, as loading the
  snapshot's store-log view requires (`prepared_never_refused`);
- preparation waits only for a named non-final retirement or for receipt
  (`waiting_names`, `ready_of_final`), and every history reaches that
  finality: one quiet check after every entry's window (`every_history_prepares`);
- in `VerdictHistory`, Ana's snapshot waits on day 51 whatever checks she
  completed, and on day 91 carries the write admitted; Ben and Carol load it
  (`gated_snapshot_waits_then_loads`). -/

namespace CovenStorelogData.VerdictHistory
open CovenStorelog CovenStorelog.Finality

def day : Nat := 86400
def window : Nat := 30 * day

/-- Ana (0) and Ben (1) are admins, Carol (2) a member. Without reading one
another: Ana's tablet makes Carol an admin (3), Ben demotes Ana (4), and
Ana's phone removes Ben (5), which is safe only while Carol is an admin.
Carol then sets her own access (6), having read Ben's removal but not her
promotion. Author order is the index. -/
def log : Log
  | 0 => ⟨0, 0, [], .create "S3"⟩
  | 1 => ⟨0, 0, [0], .addMember 1 .admin "S3"⟩
  | 2 => ⟨0, 0, [0, 1], .addMember 2 .member "S3"⟩
  | 3 => ⟨0, 10, [0, 1, 2], .changeRole 2 .admin⟩
  | 4 => ⟨1, 1, [0, 1, 2], .changeRole 0 .member⟩
  | 5 => ⟨0, 0, [0, 1, 2], .removeMember 1 []⟩
  | _ => ⟨2, 2, [0, 1, 2, 5], .setAccess 2 "R2"⟩

/-- Ben's removal lands on day 20, Ben's demotion of Ana on day 40, Carol's
promotion on day 45 and Carol's access change on day 60. -/
def history : History :=
  { log
    stored := fun w => match w with
      | 0 => 0 | 1 => 1 | 2 => 2
      | 3 => 45 * day | 4 => 40 * day | 5 => 20 * day | _ => 60 * day
    attempted := fun w => match w with
      | 0 => 0 | 1 => 1 | 2 => 2
      | 6 => 25 * day | _ => 10 * day }

theorem valid : Valid history 7 :=
  Examples.finite_valid _ _ (validCheck_sound _ _ (by decide)) (by decide) (by decide)
    (by decide)

/-- Ben's write lands one second after his removal's thirty-day deadline;
Ana prepares her snapshot on day 51, having read every entry stored by then. -/
def prepared : Nat := 51 * day

/-- Thirty quiet days after Carol's late entry. -/
def settled : Nat := 91 * day

/-- Ana's replay keeps Ben's removal: Carol is an admin, so removing Ben is
safe. It is not final. Carol's access change contradicts her concurrent
promotion and wins, so the final replay drops the promotion, then Ben's
removal, which would leave no admin; Carol's change is not too late, and
the drop is final once thirty quiet days pass. -/
theorem removal_kept_then_dropped :
    5 ∈ (resolve history window 7 (atTime history prepared)).kept ∧
    finalSet history window 7 (atTime history prepared) prepared 5 = false ∧
    tooLate history window 7 (fun _ => true) 6 = false ∧
    5 ∈ (resolve history window 7 (fun _ => true)).dropped ∧
    finalSet history window 7 (atTime history settled) settled 5 = true := by
  decide

/-- No check Ana can complete by day 51 makes Ben's removal final, with
§9's horizon (`Horizon.horizon`, Rust `finality::horizon`). Ben's demotion of
Ana, stored on day 40, did not read the removal, so it is late, and it lies in
the recent window of every check from day 40 to day 70; an earlier quiet
check certifies only entries stored before day 10. -/
theorem removal_not_final_by_preparation (checks : List Nat)
    (byThen : ∀ T ∈ checks, T ≤ prepared) :
    Horizon.horizon history window 7 checks ≤ 20 * day := by
  apply Nat.le_of_not_lt
  intro lt
  obtain ⟨T, mem, quiet, after⟩ : ∃ T ∈ checks,
      Horizon.quiet history window 7 T = true ∧ 20 * day + window < T := by
    simpa only [Horizon.horizon, Horizon.accumulated_iff, Nat.not_lt_zero, false_or] using lt
  have bound := byThen T mem
  have late : Finality.late history 7 (fun _ => true) 4 = true := by decide
  have entry := List.all_eq_true.mp quiet 4 (by simp)
  have stored : history.stored 4 = 40 * day := rfl
  simp only [late, stored, Bool.not_true, Bool.or_false, Bool.not_eq_true',
    Bool.and_eq_false_iff, decide_eq_false_iff_not] at entry
  simp only [prepared, window, day] at bound after entry
  omega

/-- On day 91 the check is quiet: the last late entry, Carol's on day 60, has
left its window. The horizon then passes Ben's removal, whatever came before. -/
theorem removal_final_by_settled (checks : List Nat) :
    20 * day < Horizon.horizon history window 7 (checks ++ [settled]) := by
  have quiet : Horizon.quiet history window 7 settled = true := by decide
  simp only [Horizon.horizon, Horizon.accumulated_iff, Nat.not_lt_zero, false_or]
  exact ⟨settled, by simp, quiet, by decide⟩

/-- By day 91 every entry has landed, and the replay drops Ben's removal. -/
theorem settled_drops_removal :
    5 ∈ (resolve history window 7 (atTime history settled)).dropped ∧
      5 ∉ (resolve history window 7 (atTime history settled)).kept := by
  have all : atTime history settled = fun _ => true := by
    funext w
    simp only [atTime, decide_eq_true_eq]
    rcases w with _ | _ | _ | _ | _ | _ | _ | w <;> simp [history, settled, day]
  rw [all]
  have dropped := removal_kept_then_dropped.2.2.2.1
  exact ⟨dropped, fun kept => (partition history window 7 _ 5).2 kept dropped⟩

end CovenStorelogData.VerdictHistory

namespace CovenStorelogData.Pending
open CovenStorelog
open CovenMerge.Receive (Retirement retirementAllows)

/-! ## The window verdict -/

/-- Write ids are `(device, number)`. -/
def writeLogs : CovenMerge.Receive.Logs (Nat × Nat) := ⟨Prod.fst, Prod.snd⟩

def window : Nat := 30 * 86400

/-- Merge's `Receive.withinWindow` (Rust `write_landing::within_window`),
negated: storage published the write more than thirty days after a kept
retirement of its device. -/
def lateUnder (kept : List Retirement) (stored : Nat × Nat → Nat) (w : Nat × Nat) : Bool :=
  !retirementAllows writeLogs stored kept w

private theorem allows_iff (kept : List Retirement) (stored : Nat × Nat → Nat) (w : Nat × Nat) :
    retirementAllows writeLogs stored kept w = true ↔
      ∀ e ∈ kept, e.device = w.1 → stored w ≤ e.stored + window := by
  simp only [retirementAllows, List.all_eq_true, writeLogs, window]
  constructor
  · intro h e he hd
    simpa [hd] using h e he
  · intro h e he
    by_cases hd : e.device = w.1
    · simpa [hd] using h e he hd
    · simp [hd]

/-- A window verdict reads only the kept retirements of the write's device
stored more than thirty days before it. -/
theorem late_depends_on_old (K L : List Retirement) (stored : Nat × Nat → Nat) (w : Nat × Nat)
    (same : ∀ e : Retirement, e.device = w.1 → e.stored + window < stored w →
      (e ∈ K ↔ e ∈ L)) :
    lateUnder K stored w = lateUnder L stored w := by
  have allows : retirementAllows writeLogs stored K w = retirementAllows writeLogs stored L w := by
    apply Bool.eq_iff_iff.mpr
    rw [allows_iff, allows_iff]
    constructor
    · intro h e he hd
      by_cases old : e.stored + window < stored w
      · exact h e ((same e hd old).mpr he) hd
      · omega
    · intro h e he hd
      by_cases old : e.stored + window < stored w
      · exact h e ((same e hd old).mp he) hd
      · omega
  simp [lateUnder, allows]

/-! ## The reload's check -/

/-- What the reload compares for each listed snapshot. -/
structure Verdicts where
  /-- Covered writes whose changes the snapshot carries with its author's
  verdict (D7 section 5). -/
  carried : Listed → List (Nat × Nat)
  /-- The author's verdict: `true` for `dropped` by the write's own window;
  every other verdict, unjudged included, kept its effects. -/
  late : Listed → Nat × Nat → Bool
  /-- This device's recorded landing verdict (`_coven_write_landings`). -/
  held : Nat × Nat → Option Bool

/-- Rust `write_landing::agrees` over a snapshot's carried changes: the
first carried write whose recorded verdict differs. -/
def Verdicts.disagreement (v : Verdicts) (s : Listed) : Option (Nat × Nat) :=
  (v.carried s).find? fun w => match v.held w with
    | some late => late != v.late s w
    | none => false

theorem disagreement_none {v : Verdicts} {s : Listed} :
    v.disagreement s = none ↔
      ∀ w ∈ v.carried s, ∀ b, v.held w = some b → v.late s w = b := by
  unfold Verdicts.disagreement
  rw [List.find?_eq_none]
  constructor
  · intro h w hw b hb
    have := h w hw
    simp only [hb, bne_iff_ne, ne_eq, Decidable.not_not] at this
    exact this.symm
  · intro h w hw
    cases hb : v.held w with
    | none => simp
    | some b => simp [h w hw b hb]

inductive VerdictChoice where
  | choice (c : Choice)
  /-- The load transaction found a carried verdict that differs from this
  device's: nothing loads, and a retry selects again. -/
  | verdict (s : Listed) (w : Nat × Nat)
  deriving DecidableEq, Repr

/-- Rust's reload: `choose`, then the load transaction's check (T103). -/
def chooseVerdict (c : Checks) (v : Verdicts) (listed : List Listed) : VerdictChoice :=
  match choose c listed with
  | .load s => match v.disagreement s with
    | some w => .verdict s w
    | none => .choice (.load s)
  | other => .choice other

theorem verdict_iff (c : Checks) (v : Verdicts) (listed : List Listed) (s : Listed)
    (w : Nat × Nat) :
    chooseVerdict c v listed = .verdict s w ↔
      choose c listed = .load s ∧ v.disagreement s = some w := by
  constructor
  · intro h
    unfold chooseVerdict at h
    cases hc : choose c listed with
    | load t =>
      rw [hc] at h
      cases hd : v.disagreement t with
      | none => simp [hd] at h
      | some u =>
        simp only [hd, VerdictChoice.verdict.injEq] at h
        obtain ⟨rfl, rfl⟩ := h
        exact ⟨rfl, hd⟩
    | update f => rw [hc] at h; simp at h
    | waits t why => rw [hc] at h; simp at h
    | logs => rw [hc] at h; simp at h
  · rintro ⟨hc, hd⟩
    simp [chooseVerdict, hc, hd]

/-- The reload's own record for that failure: Rust `record_operation_error`
gives `SnapshotError::RetirementVerdict` the record `Failed(Database)`. -/
def verdictRecord : Reason := .failed .database

/-- Only an app action drives the reload again (`ReloadWaitChecks.drivenAgain`,
Rust `operation_can_retry`). -/
theorem verdict_needs_app_action :
    retry verdictRecord = .appAction ∧ drivenAgain verdictRecord = false := ⟨rfl, rfl⟩

/-- Loading never adopts a verdict that differs from one this device
recorded: it loads `choose`'s snapshot, whose carried verdicts all agree. -/
theorem verdict_load_agrees {c : Checks} {v : Verdicts} {listed : List Listed} {s : Listed}
    (h : chooseVerdict c v listed = .choice (.load s)) :
    choose c listed = .load s ∧
      ∀ w ∈ v.carried s, ∀ b, v.held w = some b → v.late s w = b := by
  unfold chooseVerdict at h
  cases hc : choose c listed with
  | load t =>
    rw [hc] at h
    cases hd : v.disagreement t with
    | some w => simp [hd] at h
    | none =>
      simp only [hd, VerdictChoice.choice.injEq, Choice.load.injEq] at h
      subst h
      exact ⟨rfl, disagreement_none.mp hd⟩
  | update f => rw [hc] at h; simp at h
  | waits t why => rw [hc] at h; simp at h
  | logs => rw [hc] at h; simp at h

/-- Without a disagreement on the snapshot `choose` loads, the check changes
nothing. -/
theorem agreeing_never_waits {c : Checks} {v : Verdicts} {listed : List Listed}
    (agrees : ∀ s, choose c listed = .load s → v.disagreement s = none) :
    chooseVerdict c v listed = .choice (choose c listed) := by
  unfold chooseVerdict
  cases hc : choose c listed with
  | load s => simp [agrees s hc]
  | update f => rfl
  | waits t why => rfl
  | logs => rfl

/-- The author judged every carried write under kept retirements `prep`,
and this device under `device`. Where they agree on each carried write's
old prefix, the snapshot never disagrees. -/
theorem prepared_agrees (v : Verdicts) (s : Listed) (stored : Nat × Nat → Nat)
    (prep device : List Retirement)
    (judged : ∀ w ∈ v.carried s, v.late s w = lateUnder prep stored w)
    (recorded : ∀ w b, v.held w = some b → b = lateUnder device stored w)
    (same : ∀ w ∈ v.carried s, ∀ e : Retirement, e.device = w.1 →
      e.stored + window < stored w → (e ∈ prep ↔ e ∈ device)) :
    v.disagreement s = none := by
  apply disagreement_none.mpr
  intro w hw b hb
  rw [judged w hw, recorded w b hb]
  exact late_depends_on_old prep device stored w (same w hw)

/-- A snapshot prepared once each carried write's old prefix of kept
retirements was final carries that write's final verdict: a device loading
it holds what a device judging each write itself under the final kept
retirements holds. -/
theorem loaded_verdict_final {c : Checks} {v : Verdicts} {listed : List Listed} {s : Listed}
    (stored : Nat × Nat → Nat) (prep final : List Retirement)
    (loads : chooseVerdict c v listed = .choice (.load s))
    (judged : ∀ w ∈ v.carried s, v.late s w = lateUnder prep stored w)
    (finalPrefix : ∀ w ∈ v.carried s, ∀ e : Retirement, e.device = w.1 →
      e.stored + window < stored w → (e ∈ prep ↔ e ∈ final)) :
    choose c listed = .load s ∧
      ∀ w ∈ v.carried s, v.late s w = lateUnder final stored w := by
  refine ⟨(verdict_load_agrees loads).1, fun w hw => ?_⟩
  rw [judged w hw]
  exact late_depends_on_old prep final stored w (finalPrefix w hw)

private theorem newer_selected {c : Checks} {listed : List Listed} {s n : Listed}
    (sel : selectChecked c listed = .ok (some s)) (newer : n.before s = true)
    (verified : c.verifies n = true) :
    selectChecked c (listed ++ [n]) = .ok (some n) := by
  obtain ⟨nowait, hs⟩ := selectChecked_ok sel
  have readable : c.waits n = false := by
    simp only [Checks.verifies, Bool.and_eq_true] at verified
    simp [Checks.waits, verified.1]
  have nowait' : (listed ++ [n]).find? c.waits = none := by
    simp [List.find?_append, nowait, readable]
  have one : [n].filter c.verifies = [n] := by simp [verified]
  have hn : newest ((listed ++ [n]).filter c.verifies) = some n := by
    rw [List.filter_append, one, newest_append, hs]
    simp [pick, newer]
  unfold selectChecked
  rw [nowait', hn]

/-- A newer snapshot this device can load, whose carried verdicts agree with
the ones it recorded, lands: the reload, once it runs again, loads it. -/
theorem newer_agreeing_ends_wait {c : Checks} {v : Verdicts} {listed : List Listed}
    {s n : Listed} {w : Nat × Nat} (waiting : chooseVerdict c v listed = .verdict s w)
    (newer : n.before s = true) (verified : c.verifies n = true)
    (loadable : c.loadWait n = none) (agrees : v.disagreement n = none) :
    chooseVerdict c v (listed ++ [n]) = .choice (.load n) := by
  have loads := ((verdict_iff c v listed s w).mp waiting).1
  have sel := (choose_loads_newest loads).1
  have chosen : choose c (listed ++ [n]) = .load n := by
    rw [choose_selected (newer_selected sel newer verified), loadable]
  simp [chooseVerdict, chosen, agrees]

/-! ## A snapshot prepared before the verdict was final (checked) -/

/-- Ben's removal, entry 5 of `VerdictHistory.log`, retires his device 1 on
day 20. -/
def benRemoval : Retirement := ⟨1, 5, 20 * 86400⟩

/-- Ben's device's write 1, stored one second after its deadline. -/
def benWrite : Nat × Nat := (1, 1)
def benStored : Nat × Nat → Nat := fun _ => 50 * 86400 + 1

/-- Ana's snapshot, prepared on day 51, covers Ben's write 1 and carries
its changes. -/
def anaPrepared : Listed := ⟨(0, 1), [(1, 1)], 1⟩

/-- Ana judged Ben's write under the retirement her replay kept. -/
def judgedBy (held : Nat × Nat → Option Bool) : Verdicts :=
  { carried := fun s => if s = anaPrepared then [benWrite] else []
    late := fun _ w => lateUnder [benRemoval] benStored w
    held }

/-- Rust `write_landing::land`: a verdict is recorded while it is late or
not yet fixed. -/
def landingRecord (late fixed : Bool) : Option Bool := if late || !fixed then some late else none

/-- §15's retention request: a write the current newest counting snapshot
does not cover. Growth also requests one, and needs new writes. -/
def requestsSnapshot (c : Checks) (listed : List Listed) (writes : List (Nat × Nat)) : Bool :=
  match selectChecked c listed with
  | .ok current => writes.any fun w => !coveredBy current w
  | .error _ => false

/-- Ana's snapshot carries Ben's write as dropped by his removal, which her
replay keeps when she prepares it and the final replay drops: that replay
admits the write. Ben, who recorded his unfixed admission, fails the check
on every reload, its record is not retried automatically, and nothing
requests a newer snapshot. A device recording
nothing, Ben once his admission is fixed or Carol who never received the
write, loads the dropped verdict. -/
theorem unfinal_snapshot_counterexample :
    5 ∈ (Finality.resolve VerdictHistory.history VerdictHistory.window 7
      (Finality.atTime VerdictHistory.history VerdictHistory.prepared)).kept ∧
    5 ∈ (Finality.resolve VerdictHistory.history VerdictHistory.window 7
      (fun _ => true)).dropped ∧
    lateUnder [benRemoval] benStored benWrite = true ∧
    lateUnder [] benStored benWrite = false ∧
    chooseVerdict passes (judgedBy fun _ => landingRecord false false) [anaPrepared] =
      .verdict anaPrepared benWrite ∧
    drivenAgain verdictRecord = false ∧
    requestsSnapshot passes [anaPrepared] [benWrite] = false ∧
    landingRecord false true = none ∧
    chooseVerdict passes (judgedBy fun _ => none) [anaPrepared] = .choice (.load anaPrepared) ∧
    (judgedBy fun _ => none).late anaPrepared benWrite ≠ lateUnder [] benStored benWrite := by
  refine ⟨VerdictHistory.removal_kept_then_dropped.1,
    VerdictHistory.removal_kept_then_dropped.2.2.2.1, ?_⟩
  decide

/-! ## Preparing only once each covered write's verdict is final (§15) -/

/-- What the author's device knows when it prepares a snapshot: every
retirement it received that is not permanently late, kept or dropped (Rust
`write_retirement::candidates`), its finality horizon (entries stored strictly
before it are final, §9) and the end of its complete store-log receipt: it
received every entry stored by then. -/
structure Preparation where
  received : List Retirement
  horizon : Nat
  through : Nat

/-- A retirement that can decide the write's window: one of its device, stored
more than thirty days before it. -/
def deciding (stored : Nat × Nat → Nat) (w : Nat × Nat) (e : Retirement) : Bool :=
  decide (e.device = w.1) && !decide (stored w ≤ e.stored + window)

/-- Rust `write_landing::unsettled`, negated: the write's verdict can no longer
change. Either a final kept retirement excludes it, or the author received
every entry stored by thirty days before it and each received retirement of
its device stored then is final. `prep` is the author's kept retirements. -/
def Preparation.ready (p : Preparation) (prep : List Retirement) (stored : Nat × Nat → Nat)
    (w : Nat × Nat) : Bool :=
  prep.any (fun e => deciding stored w e && decide (e.stored < p.horizon)) ||
    (decide (stored w ≤ p.through + window) &&
      p.received.all fun e => !deciding stored w e || decide (e.stored < p.horizon))

private theorem excluded_late (K : List Retirement) (stored : Nat × Nat → Nat) (w : Nat × Nat)
    (e : Retirement) (kept : e ∈ K) (decides : deciding stored w e = true) :
    lateUnder K stored w = true := by
  simp only [deciding, Bool.and_eq_true, decide_eq_true_eq, Bool.not_eq_true',
    decide_eq_false_iff_not] at decides
  have allows : retirementAllows writeLogs stored K w = false := by
    cases h : retirementAllows writeLogs stored K w with
    | false => rfl
    | true => exact absurd ((allows_iff K stored w).mp h e kept decides.1) decides.2
  simp [lateUnder, allows]

/-- Another replay judges a ready write as the author did when it agrees on
every retirement stored before the author's horizon and keeps no retirement
stored by the author's receipt end that the author did not receive. The final
replay is one: entries before the horizon are final (§9,
`Horizon.horizon_stability`), and receipt was complete. So is the replay of
any loader holding every entry stored before that horizon. -/
theorem ready_agrees (p : Preparation) (prep other : List Retirement)
    (stored : Nat × Nat → Nat) (w : Nat × Nat)
    (sub : ∀ e ∈ prep, e ∈ p.received)
    (stable : ∀ e : Retirement, e.stored < p.horizon → (e ∈ prep ↔ e ∈ other))
    (complete : ∀ e ∈ other, e.stored ≤ p.through → e ∈ p.received)
    (ready : p.ready prep stored w = true) :
    lateUnder prep stored w = lateUnder other stored w := by
  simp only [Preparation.ready, Bool.or_eq_true, List.any_eq_true, Bool.and_eq_true,
    decide_eq_true_eq, List.all_eq_true] at ready
  rcases ready with ⟨e, kept, decides, final⟩ | ⟨receipt, finals⟩
  · rw [excluded_late prep stored w e kept decides,
      excluded_late other stored w e ((stable e final).mp kept) decides]
  · apply late_depends_on_old
    intro e device old
    have decides : deciding stored w e = true := by
      simp only [deciding, Bool.and_eq_true, decide_eq_true_eq, Bool.not_eq_true',
        decide_eq_false_iff_not]
      exact ⟨device, by omega⟩
    have final (received : e ∈ p.received) : e.stored < p.horizon := by
      have := finals e received
      simpa [decides] using this
    constructor
    · intro kept
      exact (stable e (final (sub e kept))).mp kept
    · intro kept
      exact (stable e (final (complete e kept (by omega)))).mpr kept

/-- A snapshot prepared only once every covered write is ready carries each
write's final verdict. -/
theorem prepared_final (v : Verdicts) (s : Listed) (p : Preparation)
    (prep final : List Retirement) (stored : Nat × Nat → Nat)
    (gate : ∀ w ∈ v.carried s, p.ready prep stored w = true)
    (judged : ∀ w ∈ v.carried s, v.late s w = lateUnder prep stored w)
    (sub : ∀ e ∈ prep, e ∈ p.received)
    (stable : ∀ e : Retirement, e.stored < p.horizon → (e ∈ prep ↔ e ∈ final))
    (complete : ∀ e ∈ final, e.stored ≤ p.through → e ∈ p.received) :
    ∀ w ∈ v.carried s, v.late s w = lateUnder final stored w := fun w hw => by
  rw [judged w hw]
  exact ready_agrees p prep final stored w sub stable complete (gate w hw)

/-- The reload's verdict check never fails on such a snapshot: a loader holding
every entry stored before the author's horizon (loading first requires the
snapshot's store-log view, Rust `SnapshotError::StoreLog`) judges every
covered write as the author did, so `choose`'s snapshot loads. The check stays
as a corruption check. -/
theorem prepared_never_refused {c : Checks} {v : Verdicts} {listed : List Listed} {s : Listed}
    (p : Preparation) (prep device : List Retirement) (stored : Nat × Nat → Nat)
    (chosen : choose c listed = .load s)
    (gate : ∀ w ∈ v.carried s, p.ready prep stored w = true)
    (judged : ∀ w ∈ v.carried s, v.late s w = lateUnder prep stored w)
    (recorded : ∀ w b, v.held w = some b → b = lateUnder device stored w)
    (sub : ∀ e ∈ prep, e ∈ p.received)
    (stable : ∀ e : Retirement, e.stored < p.horizon → (e ∈ prep ↔ e ∈ device))
    (complete : ∀ e ∈ device, e.stored ≤ p.through → e ∈ p.received) :
    chooseVerdict c v listed = .choice (.load s) := by
  have agrees : v.disagreement s = none := by
    apply disagreement_none.mpr
    intro w hw b hb
    rw [prepared_final v s p prep device stored gate judged sub stable complete w hw,
      recorded w b hb]
  simp [chooseVerdict, chosen, agrees]

/-- Preparation waits only for finality: a write that is not ready names either
receipt short of thirty days before it, or a received retirement of its device
stored more than thirty days before it that is not yet final (Rust
`Waits(CatchUp)`, `Waits(EntryFinality)`). -/
theorem waiting_names (p : Preparation) (prep : List Retirement) (stored : Nat × Nat → Nat)
    (w : Nat × Nat) (waiting : p.ready prep stored w = false) :
    p.through + window < stored w ∨
      ∃ e ∈ p.received, e.device = w.1 ∧ e.stored + window < stored w ∧ p.horizon ≤ e.stored := by
  rcases Nat.lt_or_ge (p.through + window) (stored w) with short | receipt
  · exact Or.inl short
  · right
    have all : (p.received.all fun e => !deciding stored w e || decide (e.stored < p.horizon)) =
        false := by
      simp only [Preparation.ready, Bool.or_eq_false_iff, Bool.and_eq_false_iff,
        decide_eq_false_iff_not] at waiting
      exact waiting.2.resolve_left (by omega)
    obtain ⟨e, received, open_⟩ := List.all_eq_false.mp all
    simp only [deciding, Bool.or_eq_true, Bool.not_eq_true', Bool.and_eq_false_iff,
      decide_eq_false_iff_not, decide_eq_true_eq, Bool.not_eq_false, not_or, Decidable.not_not,
      Nat.not_le, Nat.not_lt] at open_
    exact ⟨e, received, open_.1.1, by omega, open_.2⟩

/-- Readiness needs nothing beyond finality: receipt through thirty days
before the write and a final horizon past every received retirement that can
decide it. -/
theorem ready_of_final (p : Preparation) (prep : List Retirement) (stored : Nat × Nat → Nat)
    (w : Nat × Nat) (receipt : stored w ≤ p.through + window)
    (final : ∀ e ∈ p.received, deciding stored w e = true → e.stored < p.horizon) :
    p.ready prep stored w = true := by
  simp only [Preparation.ready, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq,
    List.all_eq_true]
  refine Or.inr ⟨receipt, fun e received => ?_⟩
  cases decides : deciding stored w e
  · simp
  · simp [final e received decides]

/-- A received retirement that can decide the write and is not final, with no
kept one that is final and excludes it, keeps the write waiting. -/
theorem not_ready (p : Preparation) (prep : List Retirement) (stored : Nat × Nat → Nat)
    (w : Nat × Nat) (e : Retirement) (received : e ∈ p.received)
    (decides : deciding stored w e = true) (open_ : p.horizon ≤ e.stored)
    (anchors : ∀ k ∈ prep, deciding stored w k = true → p.horizon ≤ k.stored) :
    p.ready prep stored w = false := by
  simp only [Preparation.ready, Bool.or_eq_false_iff, List.any_eq_false, Bool.and_eq_false_iff,
    decide_eq_false_iff_not]
  refine ⟨fun k kept => ?_, Or.inr (List.all_eq_false.mpr ⟨e, received, ?_⟩)⟩
  · cases dk : deciding stored w k
    · simp
    · simp [anchors k kept dk]
  · simp [decides]; omega

/-- Every history reaches readiness: a check completed after every entry's
thirty days is quiet, so the horizon passes every received retirement, and
each write within thirty days of receipt is ready. -/
theorem every_history_prepares (H : Finality.History) (n : Nat) (checks : List Nat) (T : Nat)
    (p : Preparation) (prep : List Retirement) (stored : Nat × Nat → Nat) (w : Nat × Nat)
    (entries : ∀ e ∈ p.received, e.entry < n ∧ e.stored = H.stored e.entry)
    (beyond : ∀ e, e < n → H.stored e + window < T)
    (horizon : Horizon.horizon H window n (checks ++ [T]) ≤ p.horizon)
    (receipt : stored w ≤ p.through + window) :
    p.ready prep stored w = true := by
  have quiet : Horizon.quiet H window n T = true := by
    apply List.all_eq_true.mpr
    intro e he
    have := beyond e (List.mem_range.mp he)
    simp only [Bool.or_eq_true, Bool.not_eq_true', Bool.and_eq_false_iff,
      decide_eq_false_iff_not]
    exact Or.inl (Or.inl (by omega))
  have final (e : Retirement) (received : e ∈ p.received) : e.stored < p.horizon := by
    obtain ⟨lt, same⟩ := entries e received
    have : H.stored e.entry < Horizon.horizon H window n (checks ++ [T]) := by
      simp only [Horizon.horizon, Horizon.accumulated_iff, Nat.not_lt_zero, false_or]
      exact ⟨T, by simp, quiet, beyond e.entry lt⟩
    omega
  exact ready_of_final p prep stored w receipt fun e received _ => final e received

/-! ### The history of `unfinal_snapshot_counterexample`, gated -/

/-- Ana's device: it received Ben's removal, read every entry stored by the
given time, and has the horizon of the checks it completed by then. -/
def anaAt (horizon through : Nat) : Preparation := ⟨[benRemoval], horizon, through⟩

/-- The snapshot Ana prepares once Ben's write is ready, on day 91. -/
def anaGated : Listed := ⟨(0, 2), [(1, 1)], 1⟩

/-- Her replay then no longer keeps Ben's removal. -/
def gatedBy (held : Nat × Nat → Option Bool) : Verdicts :=
  { carried := fun s => if s = anaGated then [benWrite] else []
    late := fun _ w => lateUnder [] benStored w
    held }

theorem gated_ben_loads :
    chooseVerdict passes (gatedBy fun _ => landingRecord false false) [anaGated] =
      .choice (.load anaGated) := by decide

theorem gated_carol_loads :
    chooseVerdict passes (gatedBy fun _ => none) [anaGated] = .choice (.load anaGated) := by
  decide

theorem gated_admitted : (gatedBy fun _ => none).late anaGated benWrite = false := by decide

theorem ben_decides : deciding benStored benWrite benRemoval = true := by decide

theorem waits_on_day_51 (early : List Nat) (byThen : ∀ T ∈ early, T ≤ VerdictHistory.prepared) :
    (anaAt (Horizon.horizon VerdictHistory.history VerdictHistory.window 7 early)
        VerdictHistory.prepared).ready [benRemoval] benStored benWrite = false := by
  have bound := VerdictHistory.removal_not_final_by_preparation early byThen
  have stored : benRemoval.stored = 20 * VerdictHistory.day := rfl
  apply not_ready _ _ _ _ benRemoval (by simp [anaAt]) ben_decides (by simp only [anaAt]; omega)
  intro k kept _
  simp only [List.mem_singleton] at kept
  subst kept
  simp only [anaAt]
  omega

theorem ready_on_day_91 (later : List Nat) :
    (anaAt (Horizon.horizon VerdictHistory.history VerdictHistory.window 7
        (later ++ [VerdictHistory.settled])) VerdictHistory.settled).ready [] benStored
        benWrite = true := by
  have bound := VerdictHistory.removal_final_by_settled later
  have stored : benRemoval.stored = 20 * VerdictHistory.day := rfl
  have receipt : benStored benWrite ≤ VerdictHistory.settled + window := by decide
  apply ready_of_final _ _ _ _ receipt
  intro e received _
  simp only [anaAt, List.mem_singleton] at received
  subst received
  simp only [anaAt]
  omega

/-- On day 51 Ben's write is not ready, whatever checks Ana completed by then,
and the wait names Ben's removal. On day 91 it is ready, and Ana's replay drops
the removal, as the final one does: her snapshot carries the write admitted.
Ben, who recorded his unfixed admission, and Carol, who recorded nothing, both
load it and hold the final verdict. -/
theorem gated_snapshot_waits_then_loads (early : List Nat)
    (byThen : ∀ T ∈ early, T ≤ VerdictHistory.prepared) (later : List Nat) :
    (anaAt (Horizon.horizon VerdictHistory.history VerdictHistory.window 7 early)
        VerdictHistory.prepared).ready [benRemoval] benStored benWrite = false ∧
    (anaAt (Horizon.horizon VerdictHistory.history VerdictHistory.window 7
        (later ++ [VerdictHistory.settled])) VerdictHistory.settled).ready [] benStored
        benWrite = true ∧
    5 ∈ (Finality.resolve VerdictHistory.history VerdictHistory.window 7
      (Finality.atTime VerdictHistory.history VerdictHistory.settled)).dropped ∧
    5 ∉ (Finality.resolve VerdictHistory.history VerdictHistory.window 7
      (Finality.atTime VerdictHistory.history VerdictHistory.settled)).kept ∧
    chooseVerdict passes (gatedBy fun _ => landingRecord false false) [anaGated] =
      .choice (.load anaGated) ∧
    chooseVerdict passes (gatedBy fun _ => none) [anaGated] = .choice (.load anaGated) ∧
    (gatedBy fun _ => none).late anaGated benWrite = false :=
  ⟨waits_on_day_51 early byThen, ready_on_day_91 later, VerdictHistory.settled_drops_removal.1,
    VerdictHistory.settled_drops_removal.2, gated_ben_loads, gated_carol_loads, gated_admitted⟩

/-! ## Raise snapshots

A raise snapshot seals its breaking migration's capture, whose verdicts are
the ones its replay held when the capture was taken. The device sends a
capture only when nothing has applied since it was taken; otherwise it
captures again from the current state (io `RaiseHistory`). So at preparation
its replay keeps the capture's retirements, and the same gate on the current
state gives the capture's verdicts their final values. A capture reused after
its kept retirements changed would not: `stale_capture_counterexample`. -/

theorem capture_prepared_final (v : Verdicts) (s : Listed) (p : Preparation)
    (captured prep final : List Retirement) (stored : Nat × Nat → Nat)
    (unchanged : ∀ e, e ∈ captured ↔ e ∈ prep)
    (gate : ∀ w ∈ v.carried s, p.ready prep stored w = true)
    (judged : ∀ w ∈ v.carried s, v.late s w = lateUnder captured stored w)
    (sub : ∀ e ∈ prep, e ∈ p.received)
    (stable : ∀ e : Retirement, e.stored < p.horizon → (e ∈ prep ↔ e ∈ final))
    (complete : ∀ e ∈ final, e.stored ≤ p.through → e ∈ p.received) :
    ∀ w ∈ v.carried s, v.late s w = lateUnder final stored w :=
  prepared_final v s p prep final stored gate
    (fun w hw => by
      rw [judged w hw]
      exact late_depends_on_old captured prep stored w (fun e _ _ => unchanged e))
    sub stable complete

/-- Ana's capture of day 51, under Ben's then-kept removal, reused on day 91. -/
def staleCapture : Verdicts :=
  { carried := fun s => if s = anaGated then [benWrite] else []
    late := fun _ w => lateUnder [benRemoval] benStored w
    held := fun _ => none }

/-- On day 91 the gate on Ana's current state passes, but a capture taken on
day 51 carries Ben's write as dropped, which the final replay admits; its
kept retirements differ from the current ones, so the reuse rule captures
again instead. -/
theorem stale_capture_counterexample (later : List Nat) :
    (anaAt (Horizon.horizon VerdictHistory.history VerdictHistory.window 7
        (later ++ [VerdictHistory.settled])) VerdictHistory.settled).ready [] benStored
        benWrite = true ∧
    staleCapture.late anaGated benWrite ≠ lateUnder [] benStored benWrite ∧
    ¬ (∀ e, e ∈ [benRemoval] ↔ e ∈ ([] : List Retirement)) :=
  ⟨ready_on_day_91 later, by decide, fun same => by simpa using (same benRemoval).mp (by simp)⟩

end CovenStorelogData.Pending
