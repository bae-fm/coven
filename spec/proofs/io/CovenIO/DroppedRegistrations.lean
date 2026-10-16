import CovenIO.Writers

/-! §6 and §10: what retires a device that no kept entry registers.

`Writers.known` probes every device a received registration registers, kept
or dropped, until it drains. A device is drained only after a kept entry retiring
it is final, and the kept retirements (removal of the device, removal of its
member joined through the devices kept registrations name) never name a device
whose every registration dropped: Carol's device once her own addition drops,
or Ben's phone registered while Ana removes Ben. Such a device was polled
forever, and its writes, which count by their own recorded view (§7.1), had no
landing deadline.

The rule decided here: a device that no kept entry registers is retired by
each dropped entry that registered it, at that entry's storage time. Its writes
then count only within 30 days of that time, and the device drains once that
entry's drop is final, exactly like a kept retirement (`Retired.drain_complete`
takes either). -/

namespace CovenIO.Writers

open CovenStorelog (Log Result)

/-- The device an entry registers: creation and a device addition register
their writing device (§10). -/
def registered (e : CovenStorelog.Entry) : Option Nat :=
  match e.action with
  | .create _ | .addDevice _ => some e.device
  | _ => none

/-- Some kept entry registers `d`. Removal and replacement keep the device on
record: they retire it, they do not unregister it. -/
def keptRegisters (M : Log) (r : Result) (d : Nat) : Bool :=
  r.kept.any fun w => registered (M w) == some d

/-- The devices kept registrations give member `m`. -/
def keptDevices (M : Log) (r : Result) (m : Nat) : List Nat :=
  r.kept.filterMap fun w => match (M w).action with
    | .create _ => if (M w).author = m then some (M w).device else none
    | .addDevice owner => if owner = m then some (M w).device else none
    | _ => none

/-- Kept retirements: a device removal, and a member removal joined through the
devices kept registrations give that member. -/
def keptRetirements (M : Log) (r : Result) (stored : Nat → Nat) : List Retired.Retirement :=
  r.kept.filterMap fun w => match (M w).action with
    | .removeDevice _ d => some ⟨w, stored w, [d]⟩
    | .removeMember m _ => some ⟨w, stored w, keptDevices M r m⟩
    | _ => none

/-- Each dropped registration of a device no kept entry registers. -/
def droppedRetirements (M : Log) (r : Result) (stored : Nat → Nat) : List Retired.Retirement :=
  r.dropped.filterMap fun w => match registered (M w) with
    | some d => if keptRegisters M r d then none else some ⟨w, stored w, [d]⟩
    | none => none

/-- The retirements §10's landing deadline and §6's drain use. -/
def retirements (M : Log) (r : Result) (stored : Nat → Nat) : List Retired.Retirement :=
  keptRetirements M r stored ++ droppedRetirements M r stored

theorem dropped_registration_retires (M : Log) (r : Result) (stored : Nat → Nat)
    {w d : Nat} (dropped : w ∈ r.dropped) (registers : registered (M w) = some d)
    (unregistered : keptRegisters M r d = false) :
    ⟨w, stored w, [d]⟩ ∈ retirements M r stored := by
  refine List.mem_append_right _ (List.mem_filterMap.mpr ⟨w, dropped, ?_⟩)
  simp [registers, unregistered]

/-- A dropped registration retires nothing while a kept entry registers the
same device: it stays a registered device, polled and judged as one. -/
theorem kept_registration_not_retired (M : Log) (r : Result) (stored : Nat → Nat) {d : Nat}
    (kept : keptRegisters M r d = true) (ret : Retired.Retirement)
    (member : ret ∈ droppedRetirements M r stored) : d ∉ ret.devices := by
  obtain ⟨w, _, hw⟩ := List.mem_filterMap.mp member
  cases hr : registered (M w) with
  | none => simp [hr] at hw
  | some e =>
      by_cases ke : keptRegisters M r e = true
      · simp [hr, ke] at hw
      · simp [hr, ke] at hw; subst hw
        simp only [List.mem_singleton]
        intro same; subst same; exact ke kept

/-- §10 for such a device: a write published more than 30 days after the
dropped registration's storage time is late on every reader. -/
theorem late_after_dropped_registration (M : Log) (r : Result) (stored : Nat → Nat)
    {w : Nat} (dropped : w ∈ r.dropped) (write : Write) (registers : registered (M w) = some write.writer)
    (unregistered : keptRegisters M r write.writer = false) (view : Bool)
    (late : stored w + month < write.storedAt) :
    Retired.writeAllowed (retirements M r stored) view write = false :=
  Retired.kept_retirement_rejects _ view write _
    (dropped_registration_retires M r stored dropped registers unregistered)
    (by simp) late

/-- The verdict is a function of the replay result and storage times: readers
holding the same received entries agree, in any arrival order (the replay
result depends on the received set only). -/
theorem dropped_verdict_agrees (M : Log) (r₁ r₂ : Result) (stored : Nat → Nat)
    (same : r₁ = r₂) (view : Bool) (write : Write) :
    Retired.writeAllowed (retirements M r₁ stored) view write =
      Retired.writeAllowed (retirements M r₂ stored) view write := by
  subst same; rfl

/-- The drain is exact for writes: once the dropped registration is final at a
qualifying sample `F` more than 30 days after it (§9's horizon), every write of
that device published at or after `F` is late, so none can land after the drain
reads. -/
theorem drained_device_writes_late (M : Log) (r : Result) (stored : Nat → Nat)
    {w : Nat} (dropped : w ∈ r.dropped) (write : Write) (registers : registered (M w) = some write.writer)
    (unregistered : keptRegisters M r write.writer = false) (view : Bool) {finalAt : Nat}
    (final : stored w + month < finalAt) (after : finalAt ≤ write.storedAt) :
    Retired.writeAllowed (retirements M r stored) view write = false :=
  late_after_dropped_registration M r stored dropped write registers unregistered view (by omega)

/-- No device known from a registration is left without a way to drain: either
a kept entry registers it (it is active, or a kept retirement drains it), or a
dropped registration retires it. -/
theorem registered_or_retired (M : Log) (r : Result) (stored : Nat → Nat)
    (received : List Nat) (partition : ∀ w ∈ received, w ∈ r.kept ∨ w ∈ r.dropped)
    {w d : Nat} (hw : w ∈ received) (registers : registered (M w) = some d) :
    keptRegisters M r d = true ∨ ∃ ret ∈ retirements M r stored, d ∈ ret.devices := by
  by_cases kept : keptRegisters M r d = true
  · exact Or.inl kept
  · refine Or.inr ⟨⟨w, stored w, [d]⟩, ?_, by simp⟩
    rcases partition w hw with inKept | inDropped
    · exact absurd (List.any_eq_true.mpr ⟨w, inKept, by simp [registers]⟩) kept
    · exact dropped_registration_retires M r stored inDropped registers (by simpa using kept)

/-- The request bound with the rule: two listings and three GETs per undrained
known writer, and once the dropped registration's drain completes, no idle
request names that device. -/
theorem dropped_drain_stops_polling (M : Log) (received listed : List Nat)
    (life : Nat → Retired.Life) (next : Kind → Nat → Nat) (d : Nat) :
    (idle next (Retired.polling life (known M received listed))).length =
      2 + 3 * (Retired.polling life (known M received listed)).length ∧
    ∀ request ∈ idle next (Retired.polling (Retired.retire life d) (known M received listed)),
      request.names d = false :=
  known_requests M received listed life next d

end CovenIO.Writers
