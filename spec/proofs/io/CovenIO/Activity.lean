import CovenIO.SyncReach

/-!
# What Syncing names (§19.1)

Work that clears by itself shows syncing, with the activity that moves it. This
names that activity from the same facts the state reads, in this order:

1. what the running pass moved last: a slot it read (`receiving`) or one it
   claimed for a write (`sending`);
2. a bound upload moving, or next in line with nothing holding it
   (`uploadingBound`), since the writes behind it wait on it;
3. a write next in line with nothing holding it (`sending`);
4. the next pass's turn, once nothing holds it (`receiving`).

What moves now comes before what is next in line, and a bound file before the
writes it holds.

- `syncing_iff`: syncing shows exactly when an activity is named and no
  candidate outranks it.
- `waiting_holds`: waiting to sync shows only while something holds the work: sync
  stopped, a cooldown, a slot that can't apply, or a retry. A turn that hasn't
  come is never a wait.
- `next_write_sends`: a write committed with nothing holding it shows syncing,
  sending, in the same step, with no pass between.
- `every_activity`: each activity is reached by a run of the model's events.
-/
namespace CovenIO.SyncState

/-- What Syncing names. -/
inductive Activity
  /-- The running pass reads slots it hadn't, or the next pass's turn has come. -/
  | receiving
  /-- A write goes out: a pass claimed a slot for one, or one waits with
  nothing holding it. -/
  | sending
  /-- A bound upload moves or waits its turn in the bound lane. -/
  | uploadingBound
  deriving DecidableEq, Repr

/-- The activities, in the order Syncing names them. -/
def Facts.activities (f : Facts) : List Activity :=
  (match f.moved with
    | some .read => [.receiving]
    | some .claimed => [.sending]
    | none => []) ++
  f.bound.filterMap (fun u => if u.candidate f.held = some .syncing then some .uploadingBound else none) ++
  (if f.unsent && f.writeWait.isNone then [.sending] else []) ++
  (if f.moved.isNone && f.ended == .syncing then [.receiving] else [])

theorem activities_nil (f : Facts) : f.activities = [] ↔
    f.moved = none ∧ (∀ u ∈ f.bound, u.candidate f.held ≠ some .syncing) ∧
      ¬ (f.unsent = true ∧ f.writeWait = none) ∧ f.ended ≠ .syncing := by
  unfold Facts.activities
  cases hm : f.moved with
  | none =>
      cases hu : f.unsent <;> cases hp : f.passWait <;>
        simp [List.filterMap_eq_nil_iff]
  | some m =>
      cases m <;> simp

/-- Syncing is a candidate exactly when an activity is named. -/
theorem syncing_candidate (f : Facts) : .syncing ∈ f.candidates ↔ f.activities ≠ [] := by
  rw [candidate_iff, ne_eq, activities_nil]
  have noRecord : ¬ ∃ r ∈ f.records, r.state = some .syncing := by
    rintro ⟨r, _, h⟩
    have := reason_named r _ h
    simp [State.named] at this
  have loopIff : f.loop = .syncing ↔ f.moved.isSome = true ∨ f.ended = .syncing := by
    unfold Facts.loop
    cases f.moved <;> simp
  have writeIff : (State.syncing = .work f.writeWait.isSome ∧ f.unsent = true) ↔
      (f.unsent = true ∧ f.writeWait = none) := by
    cases hp : f.writeWait <;> cases hu : f.unsent <;> simp [State.work]
  rw [loopIff, writeIff]
  cases hm : f.moved with
  | some m => simp
  | none =>
      by_cases a : f.ended = .syncing <;>
        by_cases b : ∃ u ∈ f.bound, u.candidate f.held = some .syncing <;>
        by_cases c : (f.unsent = true ∧ f.writeWait = none) <;>
        simp_all

/-- **Syncing shows exactly when an activity is named.** With a location
recorded, the state is syncing when an activity is named and no candidate
outranks syncing. -/
theorem syncing_iff (f : Facts) :
    state f = .syncing ↔ f.storage ≠ .none ∧ f.activities ≠ [] ∧
      ∀ c ∈ f.candidates, c.rank ≤ State.syncing.rank := by
  by_cases recorded : f.storage = .none
  · simp [state, recorded]
  · rw [state_iff f recorded, syncing_candidate]
    simp [recorded]

/-- **A turn that hasn't come is never a wait.** Waiting to sync shows only while
something holds the work: sync stopped, a cooldown, a slot that can't apply, or
a retry, the pass's or a bound upload's. -/
theorem waiting_holds (f : Facts) (h : state f = .waitingToSync) :
    f.heldAll ≠ none ∨ (f.lastRetries = true ∧ f.passRetrying = true) ∨
      (∃ u ∈ f.bound, u.retrying = true) ∨ f.writeKey = true := by
  obtain ⟨nonempty, _, _⟩ := waiting_not_moving f h
  obtain ⟨w, hw⟩ := List.exists_mem_of_ne_nil _ nonempty
  have := wait_facts f w hw
  simp only [Facts.waitHolds] at this
  cases w with
  | notSyncing => left; simp [Facts.heldAll, this]
  | coolingDown => left; simp [Facts.heldAll, this.1, this.2]
  | keyCopy =>
      rcases this with ⟨r1, r2, r, e, h⟩ | ⟨_, key, _⟩
      · left
        simp [Facts.heldAll, Facts.slotWait, r1, r2, e, h]
      · exact .inr (.inr (.inr key))
  | snapshot =>
      left
      obtain ⟨r1, r2, r, e, h⟩ := this
      simp [Facts.heldAll, Facts.slotWait, r1, r2, e, h]
  | retrying => exact this.2.elim (fun u => .inr (.inr (.inl u))) (fun l => .inr (.inl l))

/-- The pass's move comes first. -/
theorem move_first (f : Facts) (m : Moved) (h : f.moved = some m) :
    f.activities.head? = some (match m with | .read => .receiving | .claimed => .sending) := by
  cases m <;> simp [Facts.activities, h]

/-- A bound upload comes before the writes it holds: with nothing moving and a
bound upload moving or next in line, the first activity is uploading it. -/
theorem bound_before_write (f : Facts) (still : f.moved = none) (u : Upload) (mu : u ∈ f.bound)
    (syncs : u.candidate f.held = some .syncing) :
    f.activities.head? = some .uploadingBound := by
  have ne : f.bound.filterMap (fun u =>
      if u.candidate f.held = some .syncing then some Activity.uploadingBound else none) ≠ [] := by
    intro e
    have := List.filterMap_eq_nil_iff.mp e u mu
    simp [syncs] at this
  obtain ⟨x, xs, e⟩ := List.exists_cons_of_ne_nil ne
  have hx : x = .uploadingBound := by
    have : x ∈ f.bound.filterMap (fun u =>
        if u.candidate f.held = some .syncing then some Activity.uploadingBound else none) := by
      rw [e]; simp
    obtain ⟨v, _, hv⟩ := List.mem_filterMap.mp this
    split at hv
    · simpa using hv.symm
    · simp at hv
  simp [Facts.activities, still, e, hx]

/-- **The next write goes out at once.** A write committed to the send queue
of a device that is synced, with nothing moving, nothing queued, sync running
and no cooldown or retry, shows syncing, sending, in that step. -/
theorem next_write_sends (f : Facts) (connected : f.storage = .connected) (last : f.last = .synced)
    (still : f.moved = none) (noUploads : f.bound = []) (noRecords : f.records = [])
    (noKey : f.writeKey = false)
    (running : f.running = true) (cooling : f.cooling = false) :
    (step f (.queue true)).map (fun g => (state g, g.activities)) =
      some (.syncing, [.sending]) := by
  simp only [step, Option.map_some]
  generalize hg : ({ f with unsent := true } : Facts) = g
  have gs : g.storage = .connected := by rw [← hg]; exact connected
  have gl : g.last = .synced := by rw [← hg]; exact last
  have gm : g.moved = none := by rw [← hg]; exact still
  have gb : g.bound = [] := by rw [← hg]; exact noUploads
  have gr : g.records = [] := by rw [← hg]; exact noRecords
  have grun : g.running = true := by rw [← hg]; exact running
  have gc : g.cooling = false := by rw [← hg]; exact cooling
  have gu : g.unsent = true := by rw [← hg]
  have gk : g.writeKey = false := by rw [← hg]; exact noKey
  have gf : g.fileWait = none := by simp [Facts.fileWait, gb]
  simp [state, gs, top, Facts.rest, Facts.files, Facts.writes, Facts.others, Facts.loop,
    Facts.ended, gb, gf, Facts.writeWait, Facts.passWait, Facts.heldAll,
    Facts.lastRetries, Facts.slotWait, Facts.activities, Facts.held, State.work, pick,
    State.rank, gl, gm, grun, gc, gr, gu, gk]


namespace Reached

/-- The run of the model's events that reaches each activity from a synced
device: a slot read, a write committed, a bound upload queued. -/
def toActivity : Activity → List Event
  | .receiving => [.begin, .move .read]
  | .sending => [.queue true]
  | .uploadingBound => [.uploads [upload false true]]

/-- **No activity is dead.** Each activity is what Syncing names after some
run of the model's events. -/
theorem every_activity (a : Activity) :
    (run start (toActivity a)).map (fun f => (state f, f.activities.head?)) =
      some (.syncing, some a) := by
  cases a <;> decide

end Reached

namespace Examples

/-- Ben's pass reads a slot, then claims one for his write: receiving, then
sending. -/
example : (run connected [.queue true, .begin, .move .read]).map (fun f => f.activities) =
    some [.receiving, .sending] := by decide
example : (run connected [.queue true, .begin, .move .claimed]).map (fun f => f.activities) =
    some [.sending, .sending] := by decide

/-- Carol's cover waits behind a write: the bound file comes first. -/
example : (run connected [.uploads [cover .queued], .queue true]).map (fun f => f.activities) =
    some [.uploadingBound, .sending] := by decide

/-- The pass after a cooldown ends has its turn: receiving. During the cooldown
it waits. -/
example : (run connected [.begin, .finish (.failed (.storage .rateLimited)) 150]).map
    (fun f => (state f, f.activities)) = some (.syncing, [.receiving]) := by decide
example : (run connected [.settings true true false false, .begin,
    .finish (.failed (.storage .rateLimited)) 150]).map (fun f => (state f, f.activities)) =
    some (.waitingToSync, []) := by decide

/-- A pass that finds nothing on a synced device never shows syncing. -/
example : (run connected [.begin, .finish .synced 130]).map (fun f => (state f, f.activities)) =
    some (.synced, []) := by decide

/-- A write behind a bound cover that waits out a backoff waits with it: the
cover's own wait shows, and no activity is named. -/
example : (run connected [.uploads [⟨.queued, true, true, none⟩], .queue true]).map
    (fun f => (state f, f.activities, f.waits)) =
    some (.waitingToSync, [], [.retrying, .retrying]) := by decide

/-- A write behind a cooldown waits, and goes out when it ends. -/
example : (run connected [.settings true true false false, .queue true]).map
    (fun f => (state f, f.activities, f.waits)) = some (.waitingToSync, [], [.coolingDown]) := by decide
example : (run connected [.settings true true false false, .queue true,
    .settings true false false false]).map (fun f => (state f, f.activities)) =
    some (.syncing, [.sending]) := by decide

end Examples

end CovenIO.SyncState
