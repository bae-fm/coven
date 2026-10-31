namespace CovenStorage.Seen

/-! §17.10: stored bytes outlive the change that left them unnamed until every
other active device has seen it. Another device may have made a new file over
the bytes, with `existing_file` or by moving a row, without reading the
change, in a write it hasn't sent: nothing the uploader reads names the bytes,
yet that write will. So the uploader counts a candidate from the slot where it
lost its last name, and deletes it only once every other device has seen that
slot: it published a write that read through it, its entry added it after
that slot, or a removal cut it off.

A device makes its writes in order, each reading at least as far as the one
before, since its applied slot never goes back, and each reading through the
slot that added it, except slot one's offline writes, which read nothing
(§7.1). It sends them in that order, a prefix at a time (§6.3).

Proved here: once a device has seen the slot, every write it can still send
that counts read through it (`seen_unpublished`), so every write of every
other active device that missed the change is published, where the uploader's
reference evidence finds it (`missed_published`). `unseen_copy_unpublished`
shows why the rule exists: Ben's copy that missed Ana's delete names the
bytes, unpublished, while nothing published does. `next_write_publishes_copy`
shows Ben's next write publishing it, and `removed_holds_nothing` that a
removed device holds nothing back. -/

/-- One write of another device: the slot its device had read through when
it was made, and whether it names the stored bytes. -/
structure Write where
  readThrough : Nat
  names : Bool
deriving DecidableEq, Repr

/-- Another device: its writes in the order it made them, how many are
published, the slot whose entry added it, and whether a removal cut it off,
after which nothing it sends counts (§14.1). -/
structure Device where
  writes : List Write
  published : Nat
  added : Nat
  removed : Bool
deriving Repr

/-- §7.1: each write reads at least as far as the one before it, and at
least through the slot before the one that added its device. -/
def Device.Valid (d : Device) : Prop :=
  d.writes.Pairwise (fun a b => a.readThrough ≤ b.readThrough) ∧
    ∀ w ∈ d.writes, d.added ≤ w.readThrough + 1

/-- The writes still to be sent. -/
def Device.unpublished (d : Device) : List Write := d.writes.drop d.published

/-- §17.10: the device has seen slot `since`: a removal cut it off, its entry
added it after `since`, or it published a write that read through `since`. -/
def Device.seen (d : Device) (since : Nat) : Bool :=
  d.removed || decide (since < d.added) ||
    (d.writes.take d.published).any (fun w => decide (since ≤ w.readThrough))

/-- Once an active device has seen `since`, every write it can still send
read through `since`. -/
theorem seen_unpublished (d : Device) (since : Nat) (v : d.Valid)
    (h : d.seen since = true) (active : d.removed = false) :
    ∀ w ∈ d.unpublished, since ≤ w.readThrough := by
  intro w hw
  have hmem : w ∈ d.writes := List.mem_of_mem_drop hw
  simp only [Device.seen, active, Bool.false_or, Bool.or_eq_true, decide_eq_true_eq,
    List.any_eq_true] at h
  rcases h with added | ⟨p, hp, hle⟩
  · have := v.2 w hmem
    omega
  · have pair := v.1
    rw [← List.take_append_drop d.published d.writes, List.pairwise_append] at pair
    exact Nat.le_trans hle (pair.2.2 p hp w hw)

/-- §17.10: when every other device has seen `since`, each write of an
active one that didn't read through `since` is published. -/
theorem missed_published (others : List Device) (since : Nat)
    (valid : ∀ d ∈ others, d.Valid) (seen : ∀ d ∈ others, d.seen since = true) :
    ∀ d ∈ others, d.removed = false →
      ∀ w ∈ d.writes, w.readThrough < since → w ∈ d.writes.take d.published := by
  intro d hd active w hw missed
  rw [← List.take_append_drop d.published d.writes, List.mem_append] at hw
  rcases hw with hw | hw
  · exact hw
  · have := seen_unpublished d since (valid d hd) (seen d hd) active w hw
    omega

/-- Ana's delete is published in slot 40. Ben's phone, at slot 38, put the
photo in a Gifts row in a write it hasn't sent. He hasn't seen slot 40;
nothing he published names the bytes, and his unsent copy does. -/
theorem unseen_copy_unpublished :
    let ben : Device := ⟨[⟨38, true⟩], 0, 3, false⟩
    ben.seen 40 = false ∧ (ben.writes.take ben.published).all (!·.names) = true ∧
      ben.unpublished.any (·.names) = true := by decide

/-- Ben's next write after reading slot 40 goes out with his copy before it:
he has seen slot 40, and his copy is published. -/
theorem next_write_publishes_copy :
    let ben : Device := ⟨[⟨38, true⟩, ⟨41, false⟩], 2, 3, false⟩
    ben.seen 40 = true ∧ ben.unpublished = [] := by decide

/-- Once Ben is removed, nothing he sends counts, so he has seen every
slot. -/
theorem removed_holds_nothing :
    let ben : Device := ⟨[⟨38, true⟩], 0, 3, true⟩
    ben.seen 40 = true := by decide

end CovenStorage.Seen
