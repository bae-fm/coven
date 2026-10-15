import CovenIO.DroppedRegistrations
import CovenStorelog.Horizon

/-! §10: an installation registers its device id with one entry.

`DroppedRegistrations` retires a device that no kept entry registers by each
dropped entry that registered it, and drains it once that entry is final. Two
gaps remained. The drain is exact only if the device sends nothing after the
drop is final, and the late verdicts it causes are final only if no kept entry
ever registers that id again. Neither follows from replay: a later entry can
register the same id (a retried registration after a drop or a late landing),
and once readers have drained the id they never read that device's folder
again, so a re-registration written there would be seen by some readers and
not others.

The rule decided here: an installation registers its id with exactly one
entry, its first. If that entry drops or lands too late, the id is never
registered again; the installation starts over as a new device with a fresh
id. Under that rule (`RegistersOnce`), a registration that is final and not
kept stays unkept in every later complete replay, so its device is retired in
every one of them (`final_unkept_registration`, `final_registration_retires`),
every catch-up started after that finality finds the device unbound and
refuses to send (`catchup_after_final_unbound`), and the drain reads the
device's logs to their end (`registration_drain_complete`). A registration
that landed too late is one that replay never keeps, so the same theorems
retire its device: it is drained like a dropped one.

The replay here is §9's timed replay (`ReplayPolicy.resolve`), whose dropped
entries include the permanently late ones. -/

namespace CovenIO.Writers

open CovenStorelog (Log EntrySet Result)
open CovenStorelog.Finality (History)

/-- §10's rule for installations: at most one entry of the history registers
each device id. An installation registers itself once, with its first entry,
and never again under that id, whatever replay decides about that entry. -/
def RegistersOnce (M : Log) (n : Nat) : Prop :=
  ∀ a b d, a < n → b < n → registered (M a) = some d → registered (M b) = some d → a = b

section Policy

variable (H : History) (W n : Nat) (S : EntrySet)

/-- The settled replay behind `ReplayPolicy.resolve` and its bookkeeping:
every received candidate is kept or dropped, and kept ones are candidates. -/
private theorem policy_accounting :
    CovenStorelog.Accounting ((List.range n).filter (CovenStorelog.ReplayPolicy.admitted H W n S)) []
      (CovenStorelog.settle H.log (CovenStorelog.ReplayPolicy.authorViews H W n n)
        ((List.range n).filter (CovenStorelog.ReplayPolicy.admitted H W n S))
        (CovenStorelog.ReplayPolicy.conflict H.log (CovenStorelog.ReplayPolicy.authorViews H W n n))
        (CovenStorelog.ReplayPolicy.prefer H.log (CovenStorelog.ReplayPolicy.authorViews H W n n))
        (CovenStorelog.ReplayPolicy.realize (CovenStorelog.ReplayPolicy.authorViews H W n n))) :=
  CovenStorelog.settleN_accounting H.log _ _ ((List.nodup_range (n := n)).filter _) (by simp)
    (CovenStorelog.settle_eq_some H.log _ _ _ _ _)

/-- Only entries of the history are kept. -/
theorem policy_kept_lt {w : Nat} (kept : w ∈ (CovenStorelog.ReplayPolicy.resolve H W n S).kept) :
    w < n := by
  have settled := (policy_accounting H W n S).covered w
  have member := settled.mpr (Or.inr (Or.inl (by
    simpa [CovenStorelog.ReplayPolicy.resolve, CovenStorelog.ReplayPolicy.materialize,
      CovenStorelog.ReplayPolicy.finish, CovenStorelog.ReplayPolicy.replay] using kept)))
  simpa using (List.mem_filter.mp member).1

/-- Every received entry of the history is kept or dropped; a late one is
dropped. -/
theorem policy_disposed {w : Nat} (hw : w < n) (received : S w = true) :
    w ∈ (CovenStorelog.ReplayPolicy.resolve H W n S).kept ∨
      w ∈ (CovenStorelog.ReplayPolicy.resolve H W n S).dropped := by
  cases live : CovenStorelog.ReplayPolicy.admitted H W n S none w with
  | false =>
      exact Or.inr (CovenStorelog.Horizon.excluded_disposition H W n w S hw received live).2
  | true =>
      have candidate : w ∈ (List.range n).filter (CovenStorelog.ReplayPolicy.admitted H W n S) := by
        simp [hw, live]
      rcases ((policy_accounting H W n S).covered w).mp candidate with pending | kept | dropped
      · simp at pending
      · left
        simpa [CovenStorelog.ReplayPolicy.resolve, CovenStorelog.ReplayPolicy.materialize,
          CovenStorelog.ReplayPolicy.finish, CovenStorelog.ReplayPolicy.replay] using kept
      · right
        simp [CovenStorelog.ReplayPolicy.resolve, CovenStorelog.ReplayPolicy.materialize,
          CovenStorelog.ReplayPolicy.finish, CovenStorelog.ReplayPolicy.replay, dropped]

end Policy

/-- No kept entry ever registers the id again. Once the registration is final
and replay does not keep it (dropped, or landed too late), every later
complete catch-up keeps no registration of its device: under `RegistersOnce`
that entry is the only one that could, and finality fixes its verdict (C10). -/
theorem final_unkept_registration (H : History) (W n T : Nat) (S U : EntrySet)
    (valid : CovenStorelog.Valid H.log n) (quiet : CovenStorelog.Horizon.quiet H W n T = true)
    (atFinality : CovenStorelog.Horizon.CompleteOld H W n T S)
    (later : CovenStorelog.Horizon.CompleteOld H W n T U)
    (once : RegistersOnce H.log n) {R d : Nat} (hR : R < n)
    (final : CovenStorelog.Horizon.old H W T R = true)
    (registers : registered (H.log R) = some d)
    (unkept : R ∉ (CovenStorelog.ReplayPolicy.resolve H W n S).kept) :
    keptRegisters H.log (CovenStorelog.ReplayPolicy.resolve H W n U) d = false := by
  apply Bool.eq_false_iff.mpr
  intro some_kept
  obtain ⟨w, kept, names⟩ := List.any_eq_true.mp some_kept
  have same : w = R :=
    once w R d (policy_kept_lt H W n U kept) hR (by simpa using names) registers
  subst same
  exact unkept ((CovenStorelog.Horizon.current_stability H W n T S U valid quiet atFinality later
    hR final).1.mpr kept)

/-- The device stays retired: in every later complete catch-up the final
registration is dropped and retires its device at its own storage time, so
§10's 30-day window and §6's drain keep the same entry. -/
theorem final_registration_retires (H : History) (W n T : Nat) (S U : EntrySet)
    (valid : CovenStorelog.Valid H.log n) (quiet : CovenStorelog.Horizon.quiet H W n T = true)
    (atFinality : CovenStorelog.Horizon.CompleteOld H W n T S)
    (later : CovenStorelog.Horizon.CompleteOld H W n T U)
    (once : RegistersOnce H.log n) {R d : Nat} (hR : R < n)
    (final : CovenStorelog.Horizon.old H W T R = true)
    (registers : registered (H.log R) = some d)
    (unkept : R ∉ (CovenStorelog.ReplayPolicy.resolve H W n S).kept) :
    ⟨R, H.stored R, [d]⟩ ∈ retirements H.log (CovenStorelog.ReplayPolicy.resolve H W n U) H.stored := by
  have keptLater : R ∉ (CovenStorelog.ReplayPolicy.resolve H W n U).kept := fun kept =>
    unkept ((CovenStorelog.Horizon.current_stability H W n T S U valid quiet atFinality later
      hR final).1.mpr kept)
  have dropped : R ∈ (CovenStorelog.ReplayPolicy.resolve H W n U).dropped :=
    (policy_disposed H W n U hR (later R hR final)).resolve_left keptLater
  exact dropped_registration_retires H.log _ H.stored dropped registers
    (final_unkept_registration H W n T S U valid quiet atFinality later once hR final registers unkept)

/-- §10's binding a catch-up of device `d` reads from its replay: ordinary
sends need a kept entry registering `d`. A catch-up without one is retired for
sending (`Catchup.retired`); the only object such an installation publishes is
its own registration, at its fixed path, before any verdict on it. -/
def unbound (M : Log) (r : Result) (d : Nat) : Bool := !keptRegisters M r d

/-- The installation sends nothing after the drop is final: a catch-up whose
received set holds the certified old prefix, as every catch-up started after
that finality does, finds its device unbound. -/
theorem catchup_after_final_unbound (H : History) (W n T : Nat) (S U : EntrySet)
    (valid : CovenStorelog.Valid H.log n) (quiet : CovenStorelog.Horizon.quiet H W n T = true)
    (atFinality : CovenStorelog.Horizon.CompleteOld H W n T S)
    (later : CovenStorelog.Horizon.CompleteOld H W n T U)
    (once : RegistersOnce H.log n) {R d : Nat} (hR : R < n)
    (final : CovenStorelog.Horizon.old H W T R = true)
    (registers : registered (H.log R) = some d)
    (unkept : R ∉ (CovenStorelog.ReplayPolicy.resolve H W n S).kept) :
    unbound H.log (CovenStorelog.ReplayPolicy.resolve H W n U) d = true := by
  simp [unbound, final_unkept_registration H W n T S U valid quiet atFinality later once hR final
    registers unkept]

/-- The `seesFinal` witness of `Retired.Publication`: a catch-up of the device
that computes `retired` from its own complete replay, started at or after the
finality sample, is retired. -/
theorem registration_sees_final (H : History) (W n T : Nat) (S : EntrySet)
    (valid : CovenStorelog.Valid H.log n) (quiet : CovenStorelog.Horizon.quiet H W n T = true)
    (atFinality : CovenStorelog.Horizon.CompleteOld H W n T S)
    (once : RegistersOnce H.log n) {R d : Nat} (hR : R < n)
    (final : CovenStorelog.Horizon.old H W T R = true)
    (registers : registered (H.log R) = some d)
    (unkept : R ∉ (CovenStorelog.ReplayPolicy.resolve H W n S).kept)
    (finalAt : Nat) (catchup : Catchup)
    (honest : finalAt ≤ catchup.started → ∃ U, CovenStorelog.Horizon.CompleteOld H W n T U ∧
      catchup.retired = unbound H.log (CovenStorelog.ReplayPolicy.resolve H W n U) d) :
    finalAt ≤ catchup.started → catchup.retired = true := by
  intro after
  obtain ⟨U, later, computed⟩ := honest after
  rw [computed]
  exact catchup_after_final_unbound H W n T S U valid quiet atFinality later once hR final
    registers unkept

/-- How every object in the device's logs got there: its one registration,
published before the finality sample, or an ordinary send through a gated
catch-up (`Retired.Publication`). -/
def Published (finalAt : Nat) (o : Object) : Prop :=
  o.value.storedAt < finalAt ∨ ∃ p : Retired.Publication finalAt, p.landed = o.value.storedAt

/-- Nothing new appears in the device's logs after the gate's deadline: the
registration landed before the finality sample and every gated send lands
before `finalAt + freshFor + day` (`Retired.publication_before_deadline`). -/
theorem registration_closed (states : Nat → Provider) (writer cutoff finalAt : Nat)
    (waited : finalAt + freshFor + day < (states cutoff).time)
    (evolves : ∀ t, cutoff ≤ t → ProviderRun (states cutoff) (states t))
    (sent : ∀ t kind m o, (states t).objects (.log kind writer m) = some o → Published finalAt o) :
    Retired.ClosedAfter (fun t => (states t).objects) writer cutoff := by
  intro t ht kind m o found
  have bound : o.value.storedAt < finalAt + freshFor + day := by
    rcases sent t kind m o found with before | ⟨p, landed⟩
    · omega
    · have := Retired.publication_before_deadline finalAt p
      omega
  exact old_immutable_present (evolves t ht) _ o rfl found (by omega)

/-- §6's drain for a device retired by its final registration: reads forward
to a miss after the wait find every object that will ever be in that log. -/
theorem registration_drain_complete (states : Nat → Provider)
    (writer cutoff finalAt start first finish last target : Nat)
    (kind : Kind) (trace : List ReadEvent)
    (scan : Scan (fun t => (states t).objects) kind writer start first finish last trace)
    (afterWait : cutoff ≤ start)
    (available : Available (fun t => (states t).objects) kind writer start finish first target)
    (waited : finalAt + freshFor + day < (states cutoff).time)
    (evolves : ∀ t, cutoff ≤ t → ProviderRun (states cutoff) (states t))
    (sent : ∀ t kind m o, (states t).objects (.log kind writer m) = some o → Published finalAt o)
    (endAtCutoff : ∀ m o, (states cutoff).objects (.log kind writer m) = some o → m ≤ target)
    (time : Nat) (afterDrain : finish ≤ time) (m : Nat) (o : Object)
    (found : (states time).objects (.log kind writer m) = some o) : m ≤ last := by
  have order := (scan_terminal scan).1
  have closed := registration_closed states writer cutoff finalAt waited evolves sent
  exact Nat.le_trans (endAtCutoff m o (closed time (by omega) kind m o found))
    (discovery_complete scan target available)

end CovenIO.Writers
