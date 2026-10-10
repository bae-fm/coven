import CovenIO.Discovery
import CovenStorelog.Horizon
import CovenStorelogData.Delivery

namespace CovenIO.Refinement

/-- Scan results form durable inputs. Misses do not overwrite retained bytes. -/
def retained : List ReadEvent → Path → Option Object
  | [], _ => none
  | event :: rest, path =>
      if event.path = path then match event.result with
        | some o => some o
        | none => retained rest path
      else retained rest path

theorem retained_hit (events : List ReadEvent) (event : ReadEvent) (o : Object)
    (member : event ∈ events) (hit : event.result = some o) :
    ∃ value, retained events event.path = some value := by
  induction events with
  | nil => cases member
  | cons e es ih =>
      rcases List.mem_cons.mp member with rfl | member
      · exact ⟨o, by simp [retained, hit]⟩
      · obtain ⟨value, hv⟩ := ih member
        by_cases eq : e.path = event.path
        · cases he : e.result with
          | none => exact ⟨value, by simp [retained, eq, he, hv]⟩
          | some other => exact ⟨other, by simp [retained, eq, he]⟩
        · exact ⟨value, by simp [retained, eq, hv]⟩

theorem pass_supplies_input {world : World} {kind : Kind} {writer start first finish last : Nat}
    {trace : List ReadEvent} (scan : Scan world kind writer start first finish last trace)
    (target : Nat) (available : Available world kind writer start finish first target)
    (n : Nat) (new : first < n) (needed : n ≤ target) :
    ∃ o, retained trace (.log kind writer n) = some o := by
  obtain ⟨time, o, member, _⟩ := delivery_from_pass scan target available n new needed
  exact retained_hit trace ⟨time, .log kind writer n, some o⟩ o member rfl

/-- Invoke the actual merge delivery step only after the supplied decoder
has authenticated and checked the complete object. Readiness is still the
caller's causal, key, schema and authority check. -/
def mergeDelivery {W Row Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (state : CovenMerge.St W Row Col)
    (trace : List ReadEvent) (path : Path) (decode : Object → Option W) :
    Option (CovenMerge.St W Row Col) := do
  let object ← retained trace path
  let write ← decode object
  pure (CovenMerge.step writes state write)

theorem merge_step_realized {W Row Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (state : CovenMerge.St W Row Col)
    {world : World} {writer start first finish last : Nat} {trace : List ReadEvent}
    (scan : Scan world .write writer start first finish last trace)
    (target : Nat) (available : Available world .write writer start finish first target)
    (n : Nat) (new : first < n) (needed : n ≤ target) (write : W) (decode : Object → Option W)
    (checked : ∀ o, retained trace (.log .write writer n) = some o → decode o = some write) :
    mergeDelivery writes state trace (.log .write writer n) decode =
      some (CovenMerge.step writes state write) := by
  obtain ⟨o, present⟩ := pass_supplies_input scan target available n new needed
  simp [mergeDelivery, present, checked o present]

def entryDelivery (log : CovenStorelog.Log) (bound : Nat) (device : CovenStorelog.Device)
    (trace : List ReadEvent) (path : Path) (decode : Object → Option Nat) : Option CovenStorelog.Device := do
  let object ← retained trace path
  let entry ← decode object
  pure (CovenStorelog.step log bound device entry)

theorem storelog_step_realized (log : CovenStorelog.Log) (bound : Nat) (device : CovenStorelog.Device)
    (trace : List ReadEvent) (path : Path) (decode : Object → Option Nat) (object : Object) (entry : Nat)
    (present : retained trace path = some object) (checked : decode object = some entry) :
    entryDelivery log bound device trace path decode = some (CovenStorelog.step log bound device entry) := by
  simp [entryDelivery, present, checked]

/-- The coupled model's additional guards are explicit. Byte availability
alone cannot establish keys, running membership, causality or reload readiness. -/
theorem coupled_write_consumed {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W CovenStorelogData.Row Col) (log : CovenStorelog.Log)
    (member device : Nat) (copies : CovenStorelogData.Copies)
    (header : CovenStorelogData.Key) (keys : List CovenStorelogData.Key)
    (reader : CovenStorelogData.Reader W Col) (write : W)
    (fresh : write ∉ reader.consumed) (reload : reader.pendingReload = false)
    (running : CovenStorelogData.running reader.state.log.result.state member device = true)
    (headerReady : (header, member) ∈ copies)
    (partsReady : keys.any (fun k => decide
      (CovenStorelogData.part log reader.state.log.result copies k member = .wait)) = false) :
    write ∈ (CovenStorelogData.receiveWrite writes log member device copies header keys reader write).consumed := by
  simp [CovenStorelogData.receiveWrite, fresh, reload, running, headerReady, partsReady]

/-- Exact named connection to C10: the `hs` argument of
`CovenStorelog.Horizon.current_stability` is `Horizon.CompleteOld`.
The global quiet-window test (`hq`), valid causality (`hv`) and timestamp
fence required to compute that test remain separate obligations. -/
theorem finality_precondition (H : CovenStorelog.Finality.History) (window bound T : Nat)
    (received : CovenStorelog.EntrySet)
    (complete : ∀ e, e < bound → H.stored e ≤ T → received e = true) :
    CovenStorelog.Horizon.CompleteOld H window bound T received := by
  intro e he ho
  have older : H.stored e + window < T := of_decide_eq_true ho
  exact complete e he (by omega)

end CovenIO.Refinement
