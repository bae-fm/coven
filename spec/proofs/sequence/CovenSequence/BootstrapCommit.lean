/-!
# A new installation's commit, through crashes, failures and the facts

A restore or a join loads the store into a hidden installation, then commits
the device's custody for it, one write at a time: the storage location, the
store keys, the member's keys, the device id and the credentials (§13.4).
Only after the last write does it publish the installation. Every commit
runs past the call's point of no return (§11.6): the restore's record, or the
join's saved code, lists the installation, and the next call loads it again
and commits again. Each write sets a value the loaded installation fixes, so
writing it again changes nothing.

The commit's writes can't be one transaction: they go to the keychain and to
files. A rollback held in memory undoes them on a failure, but a crash never
runs it, and a commit run again after a crash would take the half-written
values for the ones to restore. So the commit only goes forward: a failure or
a crash keeps what it wrote, and the next call writes everything again.

What is written stays only while the installation does. The facts can still
end a restore past its commit, once its device is gone for good (§13.1): the
call then removes the installation, and the custody written for it would be
left for a store the device doesn't have. `Rules.owned`: ending an
installation deletes every device-only entry of its store, the ones a cut
commit wrote with them. A first installation is the only one of its store on
the device, so every such entry is its own. Without the rule, a crash in the
middle of the commit followed by the facts ending the restore leaves them, a
checked counterexample.

The model follows one installation from a call running on it, its device
registered. The call loads the store (`load`), the commit writes one value at
a time (`write`), and the call publishes once everything is written
(`publish`). The call may fail or the device crash at any point, and the app
resumes the listed installation. Another device may remove this one, after
which loading fails and the facts end the installation (`conclude`).
-/
namespace CovenSequence.BootstrapCommit

inductive Inst where
  /-- Removed, or never made. -/
  | none
  /-- Unpublished: hidden from the store listing, listed for its call. -/
  | hidden
  | published
  deriving DecidableEq, Repr

/-- How many writes the commit has. -/
def total : Nat := 5

structure World where
  inst : Inst
  /-- A call is running on the installation. -/
  call : Bool
  /-- The running call has loaded the store: its keys are in memory. -/
  loaded : Bool
  /-- How many of the commit's writes the device's custody holds. -/
  written : Nat
  /-- Another device removed this one: loading fails, and the facts end the
  installation. -/
  gone : Bool
  deriving DecidableEq, Repr

/-- A call has just begun on a hidden installation whose device is
registered. -/
def start : World := ⟨.hidden, true, false, 0, false⟩

structure Rules where
  /-- Ending the installation deletes every device-only entry of its store. -/
  owned : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true⟩

inductive Act where
  | load
  | write
  | publish
  /-- The facts end the installation: its device is gone for good. -/
  | conclude
  /-- The call fails and ends, keeping what the commit wrote. -/
  | fail
  /-- The device crashes, or the app drops the call. -/
  | crash
  /-- The app resumes the listed installation. -/
  | resume
  /-- Another device removes this one. -/
  | remove
  deriving DecidableEq, Repr

def enabled (w : World) : Act → Bool
  | .load => w.call && w.inst == .hidden && !w.loaded && !w.gone
  | .write => w.call && w.loaded && w.written < total
  | .publish => w.call && w.loaded && w.written == total
  | .conclude => w.call && w.inst == .hidden && !w.loaded && w.gone
  | .fail | .crash => w.call
  | .resume => !w.call && w.inst == .hidden
  | .remove => true

def act (R : Rules) (w : World) : Act → World
  | .load => { w with loaded := true }
  | .write => { w with written := w.written + 1 }
  | .publish => { w with inst := .published, call := false, loaded := false }
  | .conclude =>
    { w with inst := .none, call := false, written := if R.owned then 0 else w.written }
  | .fail | .crash => { w with call := false, loaded := false }
  | .resume => { w with call := true }
  | .remove => { w with gone := true }

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' : World} (a : Act) : enabled w a = true → Steps R (act R w a) w' → Steps R w w'

def Reachable (R : Rules) (w : World) : Prop := Steps R start w

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled w a then trace R (act R w a) rest else none

/-- Custody written for a store the device no longer has. -/
def Orphaned (w : World) : Prop := w.inst = .none ∧ 0 < w.written

/-! ## Counterexample: the facts end a restore a crash cut mid-commit -/

/-- Ana's laptop loads Household and commits two of its writes, its device
id among them, then crashes. Her phone removes the laptop. Resumed, the
laptop's restore fails to load, and the facts end it, removing the
installation. Without the rule the device id and keys it wrote stay in the
laptop's keychain, for a store the laptop doesn't have. -/
theorem cut_commit_orphans_custody :
    ∃ w, trace ⟨false⟩ start [.load, .write, .write, .crash, .remove, .resume, .conclude] =
        some w ∧ Orphaned w := by
  exact ⟨_, rfl, by simp [Orphaned, act, start]⟩

/-! ## Under the rule -/

structure Inv (w : World) : Prop where
  bound : w.written ≤ total
  call : w.call = true → w.inst = .hidden
  loaded : w.loaded = true → w.call = true
  none : w.inst = .none → w.written = 0
  published : w.inst = .published → w.written = total

theorem inv_start : Inv start := by
  constructor <;> simp [start, total]

theorem inv_step (w : World) (a : Act) (inv : Inv w) (e : enabled w a = true) :
    Inv (act Rules.correct w a) := by
  obtain ⟨bound, call, loaded, none, published⟩ := inv
  cases a <;> simp only [enabled, Bool.and_eq_true, Bool.not_eq_true', beq_iff_eq,
    decide_eq_true_eq] at e <;> constructor <;> simp_all [act, Rules.correct] <;> omega

theorem steps_keep_inv : ∀ {w w'}, Steps Rules.correct w w' → Inv w → Inv w' := by
  intro w w' h
  induction h with
  | refl => exact id
  | step a e _ ih => exact fun inv => ih (inv_step _ a inv e)

theorem reachable_inv {w : World} (h : Reachable Rules.correct w) : Inv w :=
  steps_keep_inv h inv_start

/-- No custody is left for a store the device doesn't have. -/
theorem never_orphaned {w : World} (h : Reachable Rules.correct w) : ¬Orphaned w := by
  rintro ⟨n, p⟩
  simp [(reachable_inv h).none n] at p

/-- A published installation's custody is whole. -/
theorem published_whole {w : World} (h : Reachable Rules.correct w)
    (p : w.inst = .published) : w.written = total :=
  (reachable_inv h).published p

/-! ### Every installation settles -/

/-- What coven does on its own, and the app resuming what is listed. -/
def coven : Act → Bool
  | .load | .write | .publish | .conclude | .resume => true
  | _ => false

inductive Moves : World → World → Prop
  | refl (w : World) : Moves w w
  | step {w w' : World} (a : Act) (own : coven a = true) (e : enabled w a = true)
      (rest : Moves (act Rules.correct w a) w') : Moves w w'

theorem Moves.steps {w w' : World} (h : Moves w w') : Steps Rules.correct w w' := by
  induction h with
  | refl => exact .refl _
  | step a _ e _ ih => exact .step a e ih

theorem Moves.trans {a b d : World} (h : Moves a b) (k : Moves b d) : Moves a d := by
  induction h with
  | refl => exact k
  | step x own e _ ih => exact .step x own e (ih k)

theorem Steps.trans {R : Rules} {a b d : World} :
    Steps R a b → Steps R b d → Steps R a d := by
  intro h k
  induction h with
  | refl => exact k
  | step x e _ ih => exact .step x e (ih k)

/-- Nothing left: published, or removed with no call running. -/
def quiet (w : World) : Bool := w.inst == .published || (w.inst == .none && !w.call)

def measure (w : World) : Nat :=
  if quiet w then 0
  else 1 + (if w.call then 0 else 100) + (if w.loaded then 0 else 10) + (total - w.written)

theorem progress (w : World) (inv : Inv w) (busy : quiet w = false) :
    ∃ w', Moves w w' ∧ measure w' < measure w := by
  have go : ∀ a, coven a = true → enabled w a = true →
      measure (act Rules.correct w a) < measure w →
      ∃ w', Moves w w' ∧ measure w' < measure w :=
    fun a own e less => ⟨_, .step a own e (.refl _), less⟩
  have bound := inv.bound
  rcases hi : w.inst
  · rcases hc : w.call
    · simp [quiet, hi, hc] at busy
    · simp [inv.call hc] at hi
  · rcases hc : w.call
    · refine go .resume rfl (by simp [enabled, hi, hc]) ?_
      have hl : w.loaded = false := by
        rcases h : w.loaded
        · rfl
        · simp [inv.loaded h] at hc
      simp [measure, quiet, act, hi, hc, hl]
    · rcases hl : w.loaded
      · rcases hg : w.gone
        · refine go .load rfl (by simp [enabled, hi, hc, hl, hg]) ?_
          simp [measure, quiet, act, hi, hc, hl]
        · refine go .conclude rfl (by simp [enabled, hi, hc, hl, hg]) ?_
          simp [measure, quiet, act, hi, hc, hl]
          omega
      · by_cases hw : w.written < total
        · refine go .write rfl (by simp [enabled, hc, hl, hw]) ?_
          simp [measure, quiet, act, hi, hc, hl]
          omega
        · refine go .publish rfl (by simp [enabled, hc, hl]; omega) ?_
          simp [measure, quiet, act, hi, hc, hl]
          omega
  · simp [quiet, hi] at busy

/-- Every installation settles, on every path: from any reachable state,
coven's own steps and the app resuming what is listed reach a state with
nothing left, its custody whole and published, or the installation gone and
none of its custody left. -/
theorem settles {w : World} (h : Reachable Rules.correct w) :
    ∃ w', Moves w w' ∧ quiet w' = true := by
  generalize hm : measure w = m
  induction m using Nat.strongRecOn generalizing w with
  | ind m ih =>
    by_cases q : quiet w = true
    · exact ⟨w, .refl w, q⟩
    · obtain ⟨w', moves, less⟩ := progress w (reachable_inv h) (by simpa using q)
      obtain ⟨w'', more, quiet⟩ := ih (measure w') (hm ▸ less) (Steps.trans h moves.steps) rfl
      exact ⟨w'', moves.trans more, quiet⟩

end CovenSequence.BootstrapCommit
