import CovenIO.Discovery

namespace CovenIO

inductive Stop where
  | miss | snapshots | exhausted | refused
  | failed (reason : Failure)
  deriving DecidableEq, Repr

structure Execution where
  time : Nat
  position : Nat
  trace : List ReadEvent
  requests : List Request
  stop : Stop
  deriving DecidableEq, Repr

/-- Execute next-number reads against the storage state at each request.
Fuel bounds this finite execution, not log completeness. Exhaustion never
produces a successful terminal observation. Validation and transport failures
are separate, including a successful download permanently refused by its reader. -/
def readLog (world : World) (failure : Nat → Option Failure) (refused : Object → Bool)
    (kind : Kind) (writer : Nat) : Nat → Nat → Nat → Execution
  | 0, time, position => ⟨time, position, [], [], .exhausted⟩
  | fuel + 1, time, position =>
      match failure time with
      | some e => ⟨time, position, [], [.get (.log kind writer (position + 1))], .failed e⟩
      | none =>
          let path := Path.log kind writer (position + 1)
          match world time path with
          | none => ⟨time, position, [⟨time, path, none⟩], [.get path], .miss⟩
          | some object =>
              let event : ReadEvent := ⟨time, path, some object⟩
              if refused object then ⟨time, position, [event], [.get path], .refused⟩
              else
                let rest := readLog world failure refused kind writer fuel (time + 1) (position + 1)
                { rest with trace := event :: rest.trace, requests := .get path :: rest.requests }

theorem failed_request_counted (world : World) (failure : Nat → Option Failure) (refused : Object → Bool)
    (kind : Kind) (writer fuel time position : Nat) (reason : Failure) (failed : failure time = some reason) :
    (readLog world failure refused kind writer (fuel + 1) time position).requests =
      [.get (.log kind writer (position + 1))] := by simp [readLog, failed]

theorem executed_scan (world : World) (failure : Nat → Option Failure) (refused : Object → Bool)
    (kind : Kind) (writer fuel time position : Nat)
    (complete : (readLog world failure refused kind writer fuel time position).stop = .miss) :
    let result := readLog world failure refused kind writer fuel time position
    Scan world kind writer time position result.time result.position result.trace := by
  induction fuel generalizing time position with
  | zero => cases complete
  | succ fuel ih =>
      cases hf : failure time with
      | some e => simp [readLog, hf] at complete
      | none =>
          cases ho : world time (.log kind writer (position + 1)) with
          | none => simpa [readLog, hf, ho] using Scan.miss time position ho
          | some object =>
              cases hr : refused object with
              | true => simp [readLog, hf, ho, hr] at complete
              | false =>
                  have done : (readLog world failure refused kind writer fuel (time + 1) (position + 1)).stop = .miss := by
                    simpa [readLog, hf, ho, hr] using complete
                  simpa [readLog, hf, ho, hr] using Scan.hit time position object ho (ih _ _ done)

/-- A terminal write miss is interpreted at its own monotonic time. The
caller continues snapshot discovery when the return window has expired. -/
def readRecentLog (world : World) (failure : Nat → Option Failure) (refused : Object → Bool)
    (writer fuel time position : Nat) (saved : Option Nat) (observed : Nat)
    (elapsed : Nat → Nat) : Execution :=
  let result := readLog world failure refused .write writer fuel time position
  if result.stop = .miss ∧ needsSnapshots saved observed (elapsed result.time) then
    { result with stop := .snapshots }
  else result

theorem accepted_miss_is_recent (world : World) (failure : Nat → Option Failure)
    (refused : Object → Bool) (writer fuel time position : Nat) (saved : Option Nat)
    (observed : Nat) (elapsed : Nat → Nat)
    (accepted : (readRecentLog world failure refused writer fuel time position saved observed elapsed).stop = .miss) :
    let result := readLog world failure refused .write writer fuel time position
    result.stop = .miss ∧ needsSnapshots saved observed (elapsed result.time) = false := by
  dsimp only [readRecentLog] at accepted
  dsimp only
  split at accepted
  · cases accepted
  · rename_i no
    exact ⟨accepted, by cases h : needsSnapshots saved observed _ <;> simp_all⟩

/-- An observation covers each discovered writer in each of the three logs.
Starting positions denote durable bytes or applied snapshot coverage, never
posted peer positions. All start times follow the completed folder listing. -/
structure CompletedPass (world : World) where
  listedAt : Nat
  writers : List Nat
  folders : FolderComplete world listedAt writers
  startTime : Kind → Nat → Nat
  startPosition : Kind → Nat → Nat
  finishTime : Kind → Nat → Nat
  finishPosition : Kind → Nat → Nat
  reads : Kind → Nat → List ReadEvent
  afterListing : ∀ kind writer, writer ∈ writers → listedAt ≤ startTime kind writer
  scans : ∀ kind writer, writer ∈ writers →
    Scan world kind writer (startTime kind writer) (startPosition kind writer)
      (finishTime kind writer) (finishPosition kind writer) (reads kind writer)

theorem complete_pass (world : World) (pass : CompletedPass world) (targets : Kind → Nat → Nat)
    (availability : ∀ k w, w ∈ pass.writers →
      Available world k w (pass.startTime k w) (pass.finishTime k w)
        (pass.startPosition k w) (targets k w)) :
    ∀ k w, w ∈ pass.writers → targets k w ≤ pass.finishPosition k w := by
  intro k w member
  exact discovery_complete (pass.scans k w member) (targets k w) (availability k w member)

end CovenIO
