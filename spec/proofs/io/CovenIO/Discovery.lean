import CovenIO.Publication
import CovenIO.Retention

namespace CovenIO

/-- Events order requests independently of provider timestamps. Writers and
retention may act between any two observations. -/
abbrev World := Nat → Store

structure ReadEvent where
  time : Nat
  path : Path
  result : Option Object
  deriving DecidableEq, Repr

/-- A completed serial scan (transfer limit one). A hit commits received
bytes before advancing. Errors and permanent refusals have no terminal-miss
constructor, so no failed scan is silently certified as complete. -/
inductive Scan (world : World) (kind : Kind) (writer : Nat) :
    Nat → Nat → Nat → Nat → List ReadEvent → Prop
  | miss (time position : Nat) (absent : world time (.log kind writer (position + 1)) = none) :
      Scan world kind writer time position time position
        [⟨time, .log kind writer (position + 1), none⟩]
  | hit (time position : Nat) (o : Object) (found : world time (.log kind writer (position + 1)) = some o)
      {finish last : Nat} {tail : List ReadEvent}
      (rest : Scan world kind writer (time + 1) (position + 1) finish last tail) :
      Scan world kind writer time position finish last
        (⟨time, .log kind writer (position + 1), some o⟩ :: tail)

theorem scan_terminal {world : World} {kind : Kind} {writer start first finish last : Nat}
    {trace : List ReadEvent} (h : Scan world kind writer start first finish last trace) :
    start ≤ finish ∧ first ≤ last ∧ world finish (.log kind writer (last + 1)) = none := by
  induction h with
  | miss _ _ absent => exact ⟨Nat.le_refl _, Nat.le_refl _, absent⟩
  | hit _ _ _ _ _ ih => exact ⟨by omega, by omega, ih.2.2⟩

theorem scan_received {world : World} {kind : Kind} {writer start first finish last : Nat}
    {trace : List ReadEvent} (h : Scan world kind writer start first finish last trace)
    (n : Nat) (new : first < n) (through : n ≤ last) :
    ∃ time o, ⟨time, .log kind writer n, some o⟩ ∈ trace ∧
      world time (.log kind writer n) = some o := by
  induction h with
  | miss => omega
  | hit time position o found rest ih =>
      by_cases eq : n = position + 1
      · subst n; exact ⟨time, o, by simp, found⟩
      · obtain ⟨t, value, hm, hs⟩ := ih (by omega) through
        exact ⟨t, value, List.mem_cons_of_mem _ hm, hs⟩

theorem scan_event_bounds {world : World} {kind : Kind} {writer start first finish last : Nat}
    {trace : List ReadEvent} (scan : Scan world kind writer start first finish last trace)
    (event : ReadEvent) (member : event ∈ trace) : start ≤ event.time ∧ event.time ≤ finish := by
  induction scan with
  | miss => have eq : event = _ := List.mem_singleton.mp member; subst event; exact ⟨Nat.le_refl _, Nat.le_refl _⟩
  | hit time position object found rest ih =>
      rcases List.mem_cons.mp member with rfl | hm
      · have := (scan_terminal rest).1; exact ⟨by simp, by dsimp; omega⟩
      · have bounds := ih hm; exact ⟨by omega, bounds.2⟩

/-- Every still-needed number in the target prefix stays present while this
scan runs. Permanent logs derive this from create-once publication; recent
writes derive it from their checkpoint and the retention deadline. -/
def Available (world : World) (kind : Kind) (writer start finish first target : Nat) : Prop :=
  ∀ t, start ≤ t → t ≤ finish → ∀ n, first < n → n ≤ target →
    ∃ o, world t (.log kind writer n) = some o

theorem discovery_complete {world : World} {kind : Kind} {writer start first finish last : Nat}
    {trace : List ReadEvent} (h : Scan world kind writer start first finish last trace)
    (target : Nat) (available : Available world kind writer start finish first target) : target ≤ last := by
  obtain ⟨time, position, missing⟩ := scan_terminal h
  apply Nat.le_of_not_gt
  intro short
  obtain ⟨o, found⟩ := available finish time (Nat.le_refl _) (last + 1) (by omega) (by omega)
  rw [missing] at found
  cases found

theorem delivery_from_pass {world : World} {kind : Kind} {writer start first finish last : Nat}
    {trace : List ReadEvent} (h : Scan world kind writer start first finish last trace)
    (target : Nat) (available : Available world kind writer start finish first target)
    (n : Nat) (new : first < n) (needed : n ≤ target) :
    ∃ time o, ⟨time, .log kind writer n, some o⟩ ∈ trace ∧
      world time (.log kind writer n) = some o :=
  scan_received h n new (Nat.le_trans needed (discovery_complete h target available))

/-- Concurrent appends preserve the finite prefix sampled before the pass.
This representation also permits writes to have been deleted below `first`. -/
theorem permanent_prefix_available (world : World) (kind : Kind) (writer start finish first target : Nat)
    (present : ∀ n, first < n → n ≤ target → ∃ o, world start (.log kind writer n) = some o)
    (permanent : ∀ t, start ≤ t → ∀ p o, world start p = some o → world t p = some o) :
    Available world kind writer start finish first target := by
  intro time afterStart _ n hn ht
  obtain ⟨o, ho⟩ := present n hn ht
  exact ⟨o, permanent time afterStart _ _ ho⟩

theorem provider_prefix_available (states : Nat → Provider) (kind : Kind) (writer start finish first target : Nat)
    (fixed : ∀ n, permanent (.log kind writer n) = true)
    (present : ∀ n, first < n → n ≤ target → ∃ o,
      (states start).objects (.log kind writer n) = some o)
    (evolves : ∀ t, start ≤ t → ProviderRun (states start) (states t)) :
    Available (fun t => (states t).objects) kind writer start finish first target := by
  intro time afterStart _ n hn ht
  obtain ⟨o, ho⟩ := present n hn ht
  exact ⟨o, permanent_retained (evolves time afterStart) _ _ (fixed n) ho⟩

theorem permanent_receipt_exact (states : Nat → Provider) (kind : Kind)
    (writer start first finish last target n : Nat) (trace : List ReadEvent)
    (scan : Scan (fun t => (states t).objects) kind writer start first finish last trace)
    (available : Available (fun t => (states t).objects) kind writer start finish first target)
    (new : first < n) (needed : n ≤ target) (object : Object)
    (original : (states start).objects (.log kind writer n) = some object)
    (fixed : permanent (.log kind writer n) = true)
    (evolves : ∀ t, start ≤ t → ProviderRun (states start) (states t)) :
    ∃ time, ⟨time, .log kind writer n, some object⟩ ∈ trace := by
  obtain ⟨time, read, member, found⟩ := delivery_from_pass scan target available n new needed
  have afterStart := (scan_event_bounds scan _ member).1
  have immutable := permanent_retained (evolves time afterStart) _ object fixed original
  have eq : read = object := Option.some.inj (found.symm.trans immutable)
  subst read
  exact ⟨time, member⟩

/-- Deletion is the only way a published immutable write can disappear.
At every request instant, its retained metadata and posted-position evidence
rule out both deletion alternatives. -/
theorem recent_prefix_available (world : World) (writer start finish first target : Nat)
    (snapshots : Current) (active : List Nat) (posted : Nat → Nat → Nat)
    (checkpoint reader : Nat) (storageTime : Nat → Nat) (metadata : Nat → Write)
    (activeReader : reader ∈ active)
    (metadataNumber : ∀ n, (metadata n).number = n)
    (honest : ∀ n, posted reader (metadata n).writer ≤ first)
    (afterCheckpoint : ∀ n, first < n → n ≤ target → checkpoint ≤ (metadata n).storedAt)
    (recent : ∀ t, start ≤ t → t ≤ finish → storageTime t < checkpoint + month)
    (absenceOnlyByDeletion : ∀ t, start ≤ t → t ≤ finish → ∀ n, first < n → n ≤ target →
      (∃ o, world t (.log .write writer n) = some o) ∨
      Deletable snapshots active posted (storageTime t) True (metadata n)) :
    Available world .write writer start finish first target := by
  intro t ht he n hn hn'
  rcases absenceOnlyByDeletion t ht he n hn hn' with found | deleted
  · exact found
  · exact False.elim (recent_write_protected snapshots active posted (storageTime t)
      checkpoint reader first True (metadata n) activeReader (honest n)
      (by rw [metadataNumber]; exact hn) (afterCheckpoint n hn hn') (recent t ht he) deleted)

/-- At each observation use the current snapshots. A previously loaded
snapshot can become stale during a concurrent publication and deletion. -/
theorem selected_snapshot_suffix_available (world : World) (writer start finish first target : Nat)
    (history : Nat → Retention) (metadata : Nat → Write)
    (runs : ∀ t, RetainRun (history t))
    (uncovered : ∀ t, start ≤ t → t ≤ finish → ∀ n, first < n → n ≤ target →
      ¬ Covered (history t).current (metadata n))
    (storedOrDeleted : ∀ t, start ≤ t → t ≤ finish → ∀ n, first < n → n ≤ target →
      (∃ o, world t (.log .write writer n) = some o) ∨ (metadata n) ∈ (history t).deleted) :
    Available world .write writer start finish first target := by
  intro t ht he n hn hn'
  rcases storedOrDeleted t ht he n hn hn' with stored | deleted
  · exact stored
  · exact False.elim (uncovered t ht he n hn hn'
      (selection_covers_deleted (history t) (runs t) (metadata n) deleted))

/-- Only the terminal miss needs the recent-return test. The miss may occur
anywhere in the pass; no assumption about its start time occurs here. -/
theorem recent_return_complete {world : World} {writer start first finish last : Nat}
    {trace : List ReadEvent} (scan : Scan world .write writer start first finish last trace)
    (target checkpoint observed elapsed storageTime reader : Nat)
    (current : Current) (active : List Nat) (posted : Nat → Nat → Nat) (metadata : Nat → Write)
    (member : reader ∈ active)
    (numbers : ∀ n, (metadata n).number = n)
    (honest : ∀ n, posted reader (metadata n).writer ≤ first)
    (afterCheckpoint : ∀ n, first < n → n ≤ target → checkpoint ≤ (metadata n).storedAt)
    (recent : needsSnapshots (some checkpoint) observed elapsed = false)
    (clockBound : storageTime ≤ observed + elapsed + day)
    (storedOrDeleted : ∀ n, first < n → n ≤ target →
      (∃ o, world finish (.log .write writer n) = some o) ∨
      Deletable current active posted storageTime True (metadata n)) : target ≤ last := by
  obtain ⟨_, passed, absent⟩ := scan_terminal scan
  apply Nat.le_of_not_gt
  intro short
  have hn : first < last + 1 := by omega
  have ht : last + 1 ≤ target := by omega
  rcases storedOrDeleted (last + 1) hn ht with ⟨o, found⟩ | deleted
  · rw [absent] at found; cases found
  · exact recent_write_protected current active posted storageTime checkpoint reader first True
      (metadata (last + 1)) member (honest _) (by rw [numbers]; omega)
      (afterCheckpoint _ hn ht) (miss_before_retention _ _ _ _ recent clockBound) deleted

/-- Folder membership is derived from permanent entry 1, not posted numbers.
Every relevant writer must have a registration reachable in this observation. -/
def FolderComplete (world : World) (time : Nat) (writers : List Nat) : Prop :=
  ∀ writer o, world time (.log .entry writer 1) = some o → writer ∈ writers

theorem writer_discovered (world : World) (before listed : Nat) (writers : List Nat)
    (complete : FolderComplete world listed writers) (writer : Nat) (o : Object)
    (registered : world before (.log .entry writer 1) = some o)
    (permanent : world listed (.log .entry writer 1) = world before (.log .entry writer 1)) :
    writer ∈ writers := complete writer o (permanent.trans registered)

/-- Subtract a positive provider unit. With nonnegative times, a clock
before its first complete unit cannot certify a nonempty old prefix. -/
def observationTime (clockTime unit : Nat) : Nat := clockTime - unit

/-- Earlier stored times were already present at the clock publication.
This follows from provider transitions, without a publication-fence premise. -/
theorem through_time {clock future : Provider} (run : ProviderRun clock future)
    (unit : Nat) (positive : 0 < unit) (enough : unit ≤ clock.time)
    (p : Path) (o : Object) (fixed : permanent p = true)
    (published : future.objects p = some o)
    (old : o.value.storedAt ≤ observationTime clock.time unit) : clock.objects p = some o := by
  have immutable : replaceable p = false := by cases p <;> simp_all [permanent, replaceable]
  apply old_immutable_present run p o immutable published
  unfold observationTime at old
  omega

end CovenIO
