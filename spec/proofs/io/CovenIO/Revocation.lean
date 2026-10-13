import CovenIO.Execution
import CovenIO.Retired
import CovenStorelogData.KeyPhases
import CovenStorelogData.StorageFinality
import CovenStorelogData.Revocation

/-! §3 joins the request gate to §11's first-attempt selection. Permanent
receipts come from the completed scans, not from a second key-selection model.
Neither finality nor a request deadline rules out a copy after a writer's miss. -/
namespace CovenIO.Revocation
open CovenStorelog CovenStorelogData

/-- Decoding here includes authentication of entries and reading every copy's
public routing facts, including boxes this reader cannot open. -/
def values {α : Type} (decode : Object → List α) (reads : List ReadEvent) : List α :=
  reads.flatMap fun read => match read.result with
    | none => []
    | some object => decode object

def observed {α : Type} {world : World} (pass : CompletedPass world)
    (kind : Kind) (decode : Object → List α) : List α :=
  pass.writers.flatMap fun writer => values decode (pass.reads kind writer)

/-- The durable catalog includes earlier receipts (also completed drains).
New receipts extend the existing KeyPhases type; custody is not inferred from
the presence of a sealed box. Rotation may extend this view before selection. -/
def caughtUp {world : World} (pass : CompletedPass world) (retained : KeyPhases.CaughtUp)
    (entries : Object → List Nat) (copies : Object → KeySelection.Copies) : KeyPhases.CaughtUp :=
  ⟨retained.received ++ observed pass .entry entries,
    ⟨retained.observation.custody, retained.observation.copies ++ observed pass .copy copies⟩⟩

theorem observed_hit {α : Type} {world : World} (pass : CompletedPass world)
    (kind : Kind) (decode : Object → List α) (writer : Nat) (known : writer ∈ pass.writers)
    (time number : Nat) (object : Object) (value : α)
    (read : ⟨time, .log kind writer number, some object⟩ ∈ pass.reads kind writer)
    (decoded : value ∈ decode object) : value ∈ observed pass kind decode := by
  apply List.mem_flatMap.mpr
  refine ⟨writer, known, List.mem_flatMap.mpr ?_⟩
  exact ⟨⟨time, .log kind writer number, some object⟩, read, decoded⟩

/-- The state at the end of an arbitrary trace ending in a send contains the
very catch-up checked by that request. It cannot be replaced by another fresh
catch-up chosen independently of the trace. -/
theorem last_send_checked {before after : Sender} {events : List IOEvent}
    {actor : Actor} {time : Nat} {request : Request}
    (run : IOTrace before (events ++ [.request actor time request]) after)
    (sending : sends request = true ∨ actor = .upload) (c : Catchup)
    (completed : after.completed = some c) : SendAllowed c time := by
  induction events generalizing before with
  | nil =>
      cases run with
      | cons step rest =>
          cases rest
          obtain ⟨checked, present, allowed⟩ := send_requires_check _ _ _ _ _ sending step
          have same : before = after := by
            simp only [gateStep] at step
            split at step <;> try contradiction
            split at step <;> simp_all
          have eq : checked = c := Option.some.inj (present.symm.trans (same ▸ completed))
          exact eq ▸ allowed
  | cons event events ih =>
      cases run with
      | cons _ rest => exact ih rest

/-- Bridge from the actual IOTrace completion and completed numbered reads
to KeyPhases.begin. The time bounds connect these reads to this catch-up.
Successful decoding and durable retention are the CompletedPass boundary. -/
theorem completed_catchup {before after : Sender} {events : List IOEvent}
    {actor : Actor} {time : Nat} {request : Request}
    (run : IOTrace before (events ++ [.request actor time request]) after)
    (sending : sends request = true ∨ actor = .upload) (c : Catchup)
    (completed : after.completed = some c) {world : World} (pass : CompletedPass world)
    (started : c.started ≤ pass.listedAt)
    (finished : ∀ kind writer, writer ∈ pass.writers → pass.finishTime kind writer ≤ c.finished)
    (retained : KeyPhases.CaughtUp) (entries : Object → List Nat)
    (copies : Object → KeySelection.Copies) :
    SendAllowed c time ∧
    KeyPhases.begin retained.observation.custody
      (.ok (retained.received ++ observed pass .entry entries))
      (.ok (retained.observation.copies ++ observed pass .copy copies)) =
        .ok (caughtUp pass retained entries copies) ∧
    ∀ kind writer, writer ∈ pass.writers → ∀ read ∈ pass.reads kind writer,
      c.started ≤ read.time ∧ read.time ≤ c.finished := by
  refine ⟨last_send_checked run sending c completed, rfl, ?_⟩
  intro kind writer known read member
  have bounds := scan_event_bounds (pass.scans kind writer known) read member
  exact ⟨Nat.le_trans started (Nat.le_trans (pass.afterListing kind writer known) bounds.1),
    Nat.le_trans bounds.2 (finished kind writer known)⟩

/-- A retained prefix plus a terminal scan contains each permanent receipt
in the available target prefix. Provider evolution supplies exact bytes. -/
theorem permanent_observed {α : Type} (states : Nat → Provider)
    (pass : CompletedPass (fun t => (states t).objects)) (kind : Kind)
    (decode : Object → List α) (cached : List α) (writer number target : Nat)
    (known : writer ∈ pass.writers) (object : Object) (value : α)
    (fixed : permanent (.log kind writer number) = true)
    (original : (states (pass.startTime kind writer)).objects (.log kind writer number) = some object)
    (evolves : ∀ t, pass.startTime kind writer ≤ t →
      ProviderRun (states (pass.startTime kind writer)) (states t))
    (available : Available (fun t => (states t).objects) kind writer
      (pass.startTime kind writer) (pass.finishTime kind writer)
      (pass.startPosition kind writer) target)
    (needed : number ≤ target)
    (retained : number ≤ pass.startPosition kind writer → value ∈ cached)
    (decoded : value ∈ decode object) : value ∈ cached ++ observed pass kind decode := by
  by_cases old : number ≤ pass.startPosition kind writer
  · exact List.mem_append_left _ (retained old)
  · obtain ⟨time, read⟩ := permanent_receipt_exact states kind writer _ _ _ _ target number _
      (pass.scans kind writer known) available (by omega) needed object original fixed evolves
    exact List.mem_append_right _ (observed_hit pass kind decode writer known time number object value read decoded)

/-- §9's final old prefix is supplied by actual entry scans and retained
receipts. The provider sample precedes these scans; the listing margin and
a provider tick are subtracted. Authentication and immutable paths are explicit. -/
theorem caughtUp_complete_old (H : CurrentReplay.History) (bound unit : Nat)
    (clock future : Provider) (storageTime : ProviderRun clock future) (positiveUnit : 0 < unit)
    (states : Nat → Provider) (pass : CompletedPass (fun t => (states t).objects))
    (retained : KeyPhases.CaughtUp) (entries : Object → List Nat)
    (copies : Object → KeySelection.Copies) (writer number target : Nat → Nat)
    (represented : ∀ e, e < bound → ∃ o,
      future.objects (.log .entry (writer e) (number e)) = some o ∧ o.value.storedAt = H.stored e)
    (known : ∀ e, e < bound → ∀ o,
      clock.objects (.log .entry (writer e) (number e)) = some o → writer e ∈ pass.writers)
    (sampleBefore : ∀ w, w ∈ pass.writers → ProviderRun clock (states (pass.startTime .entry w)))
    (evolves : ∀ w, w ∈ pass.writers → ∀ t, pass.startTime .entry w ≤ t →
      ProviderRun (states (pass.startTime .entry w)) (states t))
    (available : ∀ w, w ∈ pass.writers → Available (fun t => (states t).objects) .entry w
      (pass.startTime .entry w) (pass.finishTime .entry w) (pass.startPosition .entry w) (target w))
    (coverage : ∀ e, e < bound → ∀ o,
      clock.objects (.log .entry (writer e) (number e)) = some o → number e ≤ target (writer e))
    (cached : ∀ e, e < bound → number e ≤ pass.startPosition .entry (writer e) → e ∈ retained.received)
    (decoded : ∀ e, e < bound → ∀ o,
      clock.objects (.log .entry (writer e) (number e)) = some o → e ∈ entries o) :
    StorageFinality.CompleteOld H month bound (observationTime clock.time unit)
      (entrySet (caughtUp pass retained entries copies).received) := by
  apply Refinement.finality_precondition H.membership month bound unit _ clock future
    storageTime positiveUnit writer number represented
  intro e inHistory object present
  have discovered := known e inHistory object present
  have received := permanent_observed states pass .entry entries retained.received
    (writer e) (number e) (target (writer e)) discovered object e rfl
    (permanent_retained (sampleBefore _ discovered) _ _ rfl present)
    (evolves _ discovered) (available _ discovered) (coverage e inHistory object present)
    (cached e inHistory) (decoded e inHistory object present)
  simpa [entrySet, caughtUp] using received

/-- Joined §3 guarantee. `complete` can be discharged by caughtUp_complete_old.
Finality stabilizes the removal's verdict, not a later re-addition; no return
is therefore named separately. `noLaterCopy` is exactly §3/§11's window.
The one-day request bound is used for retirement drains, not silently treated
as absence of all later copies. Retries are not new sealing attempts. -/
theorem gated_first_send_revocation (H : CurrentReplay.History)
    (bound finalTime finalAt removal who removed : Nat) (audience : Audience)
    {before after : Sender} {events : List IOEvent} {actor : Actor} {time : Nat}
    (request : Request) (run : IOTrace before (events ++ [.request actor time request]) after)
    (sending : sends request = true ∨ actor = .upload) (c : Catchup)
    (completed : after.completed = some c) {world : World} (pass : CompletedPass world)
    (started : c.started ≤ pass.listedAt)
    (finished : ∀ kind writer, writer ∈ pass.writers → pass.finishTime kind writer ≤ c.finished)
    (retained : KeyPhases.CaughtUp) (entries : Object → List Nat)
    (copies : Object → KeySelection.Copies) (ready : KeyPhases.CaughtUp)
    (entriesRetained : ∀ e ∈ (caughtUp pass retained entries copies).received, e ∈ ready.received)
    (copiesRetained : ∀ copy ∈ (caughtUp pass retained entries copies).observation.copies,
      copy ∈ ready.observation.copies)
    (atFinality : EntrySet)
    (causal : CovenStorelog.Valid H.membership.log bound)
    (quiet : StorageFinality.quiet H month bound finalTime = true)
    (finalComplete : StorageFinality.CompleteOld H month bound finalTime atFinality)
    (complete : StorageFinality.CompleteOld H month bound finalTime
      (entrySet (caughtUp pass retained entries copies).received))
    (finalBeforeStart : finalAt < c.started)
    (inHistory : removal < bound) (old : StorageFinality.old H month finalTime removal = true)
    (removes : (H.log removal).action = .removeMember removed)
    (kept : removal ∈ (CurrentReplay.resolve H month bound atFinality).kept)
    (noReturn : ∀ e, e < bound → removal < e →
      e ∈ (CurrentReplay.resolve H month bound (entrySet ready.received)).kept →
      ∀ role access, (H.log e).action ≠ .addMember removed role access)
    (bytes : List Nat) (attempt : KeySelection.Attempt) (later : KeySelection.Copies)
    (path : Path) (sealedRequest : request = .create path attempt.bytes)
    (firstSend : KeyPhases.firstSend H month bound who audience ready bytes = some attempt)
    (noLaterCopy : (attempt.key, removed) ∉ later)
    (copyCoverage : ∀ writer number object, world time (.log .copy writer number) = some object →
      ∀ copy ∈ copies object,
        copy ∈ (caughtUp pass retained entries copies).observation.copies ++ later) :
    SendAllowed c time ∧ finalAt < time ∧ request = .create path attempt.bytes ∧
    (H.log removal).action = .removeMember removed ∧
    removal ∈ (CurrentReplay.resolve H month bound (entrySet ready.received)).kept ∧
    who ≠ removed ∧
    (attempt.key, removed) ∉ ready.observation.copies ++ later ∧
    ∀ writer number object, world time (.log .copy writer number) = some object →
      (attempt.key, removed) ∉ copies object := by
  obtain ⟨gate, _, _⟩ := completed_catchup run sending c completed pass started finished retained entries copies
  have readyComplete : StorageFinality.CompleteOld H month bound finalTime (entrySet ready.received) := by
    intro e inHistory old
    have received := complete e inHistory old
    simpa [entrySet] using entriesRetained e (by simpa [entrySet] using received)
  have stable := (StorageFinality.stability H month bound finalTime _ _ causal
    finalComplete readyComplete quiet inHistory old).mp kept
  have excluded := CovenStorelogData.Revocation.kept_removal_excludes H month bound removal removed
    (entrySet ready.received) causal removes stable noReturn audience
  have secrecy := KeyPhases.revocation H month bound who removed audience
    ready bytes attempt later firstSend excluded noLaterCopy
  refine ⟨gate, by have := gate.2.2.1; have := gate.2.2.2.1; omega,
    sealedRequest, removes, stable, secrecy.1, secrecy.2, ?_⟩
  intro writer number object present copied
  rcases List.mem_append.mp (copyCoverage writer number object present _ copied) with observed | laterCopy
  · exact secrecy.2 (List.mem_append_left _ (copiesRetained _ observed))
  · exact noLaterCopy laterCopy

/-- A retired writer's copy request in the same IOTrace obeys the existing
drain deadline. `landed` and `finalAt` use the common duration scale of
Retired.Publication; §4's clock-rate allowance alone is not that identification.
The request bound includes remote publication after a lost reply. -/
theorem retired_copy_settles {before after : Sender} {events : List IOEvent}
    (actor : Actor) (time writer number finalAt landed : Nat) (bytes : Bytes)
    (run : IOTrace before (events ++ [.request actor time (.create (.log .copy writer number) bytes)]) after)
    (c : Catchup) (completed : after.completed = some c)
    (storageDuration : time ≤ landed) (requestBound : landed ≤ time + day)
    (finalityStops : finalAt ≤ c.started → c.retired = true) :
    landed < finalAt + freshFor + day :=
  Retired.publication_before_deadline finalAt
    ⟨c, time, landed, last_send_checked run (Or.inl rfl) c completed,
      storageDuration, requestBound, finalityStops⟩

end CovenIO.Revocation
