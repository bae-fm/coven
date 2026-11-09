/-!
# Forgetting applied-write entries no check reads

A device keeps an entry for each applied write: its device, number, slot,
timestamp and `read_through` (Appendix B.1). Once a write's kept records are
forgotten (`ForgetEdits`), the entry is read only by checks a later write
meets, and most entries no check needs. A device forgets an entry once
nothing reads its slot or timestamp, keeping three kinds (§16.7): every entry
above `T`, the slot every write still to be judged has read past; each
device's last landed write; and, at or below `T`, the latest stamped one.

This file proves that every check a write still to be judged meets gives the
same answer over the kept entries as over all of them:

* `landed_by_number`: whether a write landed is whether its device has a kept
  entry numbered at or above it, since a device's writes land in number order
  with no omitted predecessor. A repeated write, a named source's landing and
  a snapshot's coverage of a write named only as an identity read this.
* `stamped_after_kept`: a write that read past `T` is stamped after every
  write it read exactly when it is stamped after every kept one it read.
* `read_kept_iff`: a forgotten entry's write was read by every write that read
  past `T`, so counting it as read gives that write's past exactly.
* `reads_checked_kept`: a landing write's `read_through` is at or above every
  earlier landed write's of its device exactly when it is at or above its
  device's last kept write's.
-/

namespace CovenMerge.ForgetEntries

/-- An applied write's entry: its device, number, containing slot, timestamp
and `read_through`. Timestamps are distinct and totally ordered (§7.2). -/
structure Entry (D : Type) where
  dev : D
  num : Nat
  slot : Nat
  ts : Nat
  rt : Nat

variable {D : Type}

/-- A device's writes land in the order it numbered them, with no omitted
predecessor: every positive number below a landed write's landed too
(format D9). -/
def NumberOrder (log : List (Entry D)) : Prop :=
  ∀ e ∈ log, ∀ n, 1 ≤ n → n ≤ e.num → ∃ e' ∈ log, e'.dev = e.dev ∧ e'.num = n

/-- `e` is its device's last landed write. -/
def Last (log : List (Entry D)) (e : Entry D) : Prop :=
  ∀ e' ∈ log, e'.dev = e.dev → e'.num ≤ e.num

/-- What a device keeps of the applied writes `log` once it has forgotten
entries no check reads, with every write still to be judged having read past
slot `T`: every entry above `T`; each device's last landed write; and for each
entry at or below `T`, a kept one at or below `T` stamped no earlier, which the
entry with the latest timestamp there is. -/
structure Retains (log : List (Entry D)) (T : Nat) (kept : Entry D → Prop) : Prop where
  above : ∀ e ∈ log, T < e.slot → kept e
  last : ∀ e ∈ log, Last log e → kept e
  newest : ∀ e ∈ log, e.slot ≤ T → ∃ k ∈ log, kept k ∧ k.slot ≤ T ∧ e.ts ≤ k.ts

/-- Every device with a landed write has a last one. -/
theorem exists_last (log : List (Entry D)) (d : D) (some : ∃ x ∈ log, x.dev = d) :
    ∃ l ∈ log, l.dev = d ∧ ∀ x ∈ log, x.dev = d → x.num ≤ l.num := by
  induction log with
  | nil =>
    obtain ⟨x, mem, _⟩ := some
    cases mem
  | cons a rest ih =>
    rcases Classical.em (∃ x ∈ rest, x.dev = d) with inRest | notInRest
    · obtain ⟨l, mem, dev, most⟩ := ih inRest
      rcases Classical.em (a.dev = d ∧ l.num < a.num) with bigger | notBigger
      · refine ⟨a, List.mem_cons_self, bigger.1, ?_⟩
        intro x hx same
        rcases List.mem_cons.mp hx with isA | hx
        · rw [isA]
          exact Nat.le_refl _
        · have := most x hx same
          have := bigger.2
          omega
      · refine ⟨l, List.mem_cons_of_mem a mem, dev, ?_⟩
        intro x hx same
        rcases List.mem_cons.mp hx with isA | hx
        · rw [isA] at same ⊢
          have : ¬ l.num < a.num := fun lt => notBigger ⟨same, lt⟩
          omega
        · exact most x hx same
    · obtain ⟨x, hx, dev⟩ := some
      have isA : a.dev = d := by
        rcases List.mem_cons.mp hx with isA | hx
        · rw [← isA]
          exact dev
        · exact absurd ⟨x, hx, dev⟩ notInRest
      refine ⟨a, List.mem_cons_self, isA, ?_⟩
      intro y hy same
      rcases List.mem_cons.mp hy with isY | hy
      · rw [isY]
        exact Nat.le_refl _
      · exact absurd ⟨y, hy, same⟩ notInRest

/-- **Whether a write landed is decided by number.** A device's write `n`
landed exactly when the device has a kept entry numbered at or above `n`. -/
theorem landed_by_number {log : List (Entry D)} {T : Nat} {kept : Entry D → Prop}
    (order : NumberOrder log) (retains : Retains log T kept) (d : D) {n : Nat}
    (positive : 1 ≤ n) :
    (∃ e ∈ log, e.dev = d ∧ e.num = n) ↔ (∃ k ∈ log, kept k ∧ k.dev = d ∧ n ≤ k.num) := by
  constructor
  · rintro ⟨e, mem, dev, num⟩
    obtain ⟨l, lmem, ldev, most⟩ := exists_last log d ⟨e, mem, dev⟩
    have last : Last log l := fun x hx same => most x hx (same.trans ldev)
    exact ⟨l, lmem, retains.last l lmem last, ldev, num ▸ most e mem dev⟩
  · rintro ⟨k, mem, _, dev, le⟩
    obtain ⟨e, emem, edev, enum⟩ := order k mem n positive le
    exact ⟨e, emem, edev.trans dev, enum⟩

/-- The writes a write of device `d` numbered `n` had read, with
`read_through` `rt`: those in slots through `rt`, and its device's earlier
writes (§7.1). -/
def Read (rt : Nat) (d : D) (n : Nat) (e : Entry D) : Prop :=
  e.slot ≤ rt ∨ (e.dev = d ∧ e.num < n)

/-- **A forgotten entry's write was read** by every write that read past
`T`: only entries at or below `T` are forgotten. -/
theorem forgotten_read {log : List (Entry D)} {T : Nat} {kept : Entry D → Prop}
    (retains : Retains log T kept) {rt : Nat} (past : T ≤ rt) (d : D) (n : Nat)
    {e : Entry D} (mem : e ∈ log) (gone : ¬ kept e) : Read rt d n e := by
  refine Or.inl ?_
  rcases Nat.lt_or_ge T e.slot with above | below
  · exact absurd (retains.above e mem above) gone
  · exact Nat.le_trans below past

/-- **Counting a forgotten write as read gives a write's past exactly.** A
device that forgot entries answers whether a write that read past `T` had
read a landed write from its entry when kept, and as read when forgotten. -/
theorem read_kept_iff {log : List (Entry D)} {T : Nat} {kept : Entry D → Prop}
    (retains : Retains log T kept) {rt : Nat} (past : T ≤ rt) (d : D) (n : Nat)
    {e : Entry D} (mem : e ∈ log) :
    (¬ kept e ∨ Read rt d n e) ↔ Read rt d n e := by
  constructor
  · rintro (gone | read)
    · exact forgotten_read retains past d n mem gone
    · exact read
  · exact Or.inr

/-- **The newest write read is among the kept ones.** A write of device `d`
numbered `n` that read past `T` is stamped after every write it read exactly
when it is stamped after every kept write it read (§7.2). -/
theorem stamped_after_kept {log : List (Entry D)} {T : Nat} {kept : Entry D → Prop}
    (retains : Retains log T kept) {rt : Nat} (past : T ≤ rt) (d : D) (n t : Nat) :
    (∀ e ∈ log, Read rt d n e → e.ts < t) ↔
      (∀ e ∈ log, kept e → Read rt d n e → e.ts < t) := by
  constructor
  · intro all e mem _ read
    exact all e mem read
  · intro keptAll e mem read
    rcases Nat.lt_or_ge T e.slot with above | below
    · exact keptAll e mem (retains.above e mem above) read
    · obtain ⟨k, kmem, kkept, kslot, later⟩ := retains.newest e mem below
      exact Nat.lt_of_le_of_lt later
        (keptAll k kmem kkept (Or.inl (Nat.le_trans kslot past)))

/-- A device's landed writes read no less as their numbers grow: the order
every device's receipt check keeps (`ForgetEdits.ReadsChecked`). -/
def ReadsGrow (log : List (Entry D)) : Prop :=
  ∀ e ∈ log, ∀ e' ∈ log, e.dev = e'.dev → e.num ≤ e'.num → e.rt ≤ e'.rt

/-- **Reads never going back is checked against the last kept write.** A
write of device `d` read at least as much as every landed write of its
device exactly when it read at least as much as its device's last one, which
is kept. -/
theorem reads_checked_kept {log : List (Entry D)} {T : Nat} {kept : Entry D → Prop}
    (retains : Retains log T kept) (grow : ReadsGrow log) (d : D) (rt : Nat) :
    (∀ e ∈ log, e.dev = d → e.rt ≤ rt) ↔
      (∀ l ∈ log, kept l → Last log l → l.dev = d → l.rt ≤ rt) := by
  constructor
  · intro all l mem _ _ dev
    exact all l mem dev
  · intro lastRead e mem dev
    obtain ⟨l, lmem, ldev, most⟩ := exists_last log d ⟨e, mem, dev⟩
    have last : Last log l := fun x hx same => most x hx (same.trans ldev)
    exact Nat.le_trans (grow e mem l lmem (dev.trans ldev.symm) (most e mem dev))
      (lastRead l lmem (retains.last l lmem last) last ldev)

end CovenMerge.ForgetEntries
