import CovenStorelogData.ReloadWaitChecks
import CovenMerge.Retirement
import CovenStorelog.FinalityExamples

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
  prepared once that prefix was final carries every write's final verdict:
  a device loading it holds what a device judging each write itself holds
  (`loaded_verdict_final`);
- once the reload runs again, a newer snapshot this device can load and
  whose verdicts agree ends the failure (`newer_agreeing_ends_wait`).

What fails (`unfinal_snapshot_counterexample`, checked): §15 lets a
snapshot be prepared while the retirement deciding a covered write can still
drop. `VerdictHistory` is such a store-log history in the storage-time
replay of §9: Ben's removal is kept when Ana prepares her snapshot a day
after his write's deadline, and dropped once a later entry lands. Ana's
snapshot carries the write as `dropped`; the final replay admits it.
- Ben recorded his unfixed admission: every reload fails the check on Ana's
  snapshot, its record waits for an app action, and in a store with no
  further writes nothing requests another snapshot, since Ana's covers
  every write. The reload never ends.
- Once Ben's admission is fixed Rust no longer records it, and Carol, who
  never received the write, has nothing to compare: both load Ana's
  snapshot and drop a write the final replay admits, while a device that
  received it keeps its effects. §10's "including contributions already in
  snapshots" is not met. -/

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

end CovenStorelogData.Pending
