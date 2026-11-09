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

Those take `Retains` as given. A device doesn't recompute it from every
entry: each applied slot moves `T` on from where the last pass reached and
forgets only what it looks at. The rest of the file proves that pass keeps
what `Retains` asks:

* `passed_tracks`: keeping the entry it kept as the latest stamped one at or
  below the old `T`, unless the latest stamped entry between the old `T` and
  the new one is stamped later, tracks the latest stamped entry at or below
  the new `T`. `tracks_reset` starts it over when a loaded snapshot or a
  migration makes the device look at every entry again.
* `not_last_iff`: an entry is not its device's last landed write exactly
  when its device keeps an entry numbered above it, which is how the pass
  tells.
* `pass_retains`: forgetting only entries at or below the new `T` that are
  neither their device's last landed write nor the tracked latest stamped
  one keeps what `Retains` asks, given what the last pass kept
  (`retains_later`) and the writes landed since (`land_retains`).
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

/-! ## The pass that keeps them -/

/-- What a device tracks as the latest stamped entry at or below slot `s`:
`some k` for that entry, `none` when no entry is at or below `s`. -/
def Tracks (log : List (Entry D)) (s : Nat) : Option (Entry D) → Prop
  | some k => k ∈ log ∧ k.slot ≤ s ∧ ∀ e ∈ log, e.slot ≤ s → e.ts ≤ k.ts
  | none => ∀ e ∈ log, s < e.slot

/-- The latest stamped entry in the slots after `s` through `t`, `none` when
no entry is there: what the pass reads from the applied writes. -/
def LatestIn (log : List (Entry D)) (s t : Nat) : Option (Entry D) → Prop
  | some l => l ∈ log ∧ s < l.slot ∧ l.slot ≤ t ∧
      ∀ e ∈ log, s < e.slot → e.slot ≤ t → e.ts ≤ l.ts
  | none => ∀ e ∈ log, s < e.slot → t < e.slot

/-- The pass's choice of the latest stamped entry at or below the new `T`:
the latest stamped one between the old `T` and the new one replaces the one
kept when it is stamped later. -/
def advance : Option (Entry D) → Option (Entry D) → Option (Entry D)
  | none, latest => latest
  | some kept, none => some kept
  | some kept, some latest => if kept.ts < latest.ts then some latest else some kept

/-- Looking at every entry again starts from slot 0, where no entry is, as
every slot is at least 1. -/
theorem tracks_reset {log : List (Entry D)} (positive : ∀ e ∈ log, 1 ≤ e.slot) :
    Tracks log 0 none :=
  fun e mem => positive e mem

/-- **The pass tracks the latest stamped entry.** From the one tracked at or
below `s` and the latest stamped one in the slots after `s` through `t`,
`advance` gives the latest stamped one at or below `t`. -/
theorem passed_tracks {log : List (Entry D)} {s t : Nat} {kept latest : Option (Entry D)}
    (le : s ≤ t) (tracked : Tracks log s kept) (between : LatestIn log s t latest) :
    Tracks log t (advance kept latest) := by
  cases kept with
  | none =>
    cases latest with
    | none =>
      intro e mem
      rcases Nat.lt_or_ge s e.slot with above | below
      · exact between e mem above
      · exact absurd (tracked e mem) (Nat.not_lt.mpr below)
    | some l =>
      obtain ⟨lmem, _, upTo, most⟩ := between
      exact ⟨lmem, upTo, fun e emem eslot => most e emem (tracked e emem) eslot⟩
  | some k =>
    obtain ⟨kmem, kslot, kmost⟩ := tracked
    cases latest with
    | none =>
      refine ⟨kmem, Nat.le_trans kslot le, fun e emem eslot => ?_⟩
      rcases Nat.lt_or_ge s e.slot with above | below
      · exact absurd eslot (Nat.not_le.mpr (between e emem above))
      · exact kmost e emem below
    | some l =>
      obtain ⟨lmem, _, upTo, lmost⟩ := between
      by_cases later : k.ts < l.ts
      · have chosen : advance (some k) (some l) = some l := by simp [advance, later]
        rw [chosen]
        refine ⟨lmem, upTo, fun e emem eslot => ?_⟩
        rcases Nat.lt_or_ge s e.slot with above | below
        · exact lmost e emem above eslot
        · exact Nat.le_trans (kmost e emem below) (Nat.le_of_lt later)
      · have chosen : advance (some k) (some l) = some k := by simp [advance, later]
        rw [chosen]
        refine ⟨kmem, Nat.le_trans kslot le, fun e emem eslot => ?_⟩
        rcases Nat.lt_or_ge s e.slot with above | below
        · exact Nat.le_trans (lmost e emem above eslot) (Nat.le_of_not_lt later)
        · exact kmost e emem below

/-- **The pass tells a last landed write by number.** With every device's
last landed write kept, an entry is not its device's last exactly when its
device keeps an entry numbered above it, as the pass asks of the kept
entries. -/
theorem not_last_iff {log : List (Entry D)} {kept : Entry D → Prop}
    (last : ∀ e ∈ log, Last log e → kept e) {e : Entry D} :
    ¬ Last log e ↔ ∃ k ∈ log, kept k ∧ k.dev = e.dev ∧ e.num + 1 ≤ k.num := by
  constructor
  · intro notLast
    have later : ∃ x ∈ log, x.dev = e.dev ∧ e.num < x.num :=
      Classical.byContradiction fun none =>
        notLast fun x hx dev =>
          Nat.le_of_not_lt fun lt => none ⟨x, hx, dev, lt⟩
    obtain ⟨x, xmem, xdev, lt⟩ := later
    obtain ⟨l, lmem, ldev, most⟩ := exists_last log e.dev ⟨x, xmem, xdev⟩
    have isLast : Last log l := fun y hy same => most y hy (same.trans ldev)
    exact ⟨l, lmem, last l lmem isLast, ldev, Nat.le_trans lt (most x xmem xdev)⟩
  · rintro ⟨k, kmem, _, dev, above⟩ isLast
    have := isLast k kmem dev
    omega

/-- What a pass keeps: what the device kept, less the entries it forgets. -/
def keeps (kept forgets : Entry D → Prop) (e : Entry D) : Prop := kept e ∧ ¬ forgets e

/-- **A pass keeps what `Retains` asks.** Before it, the device keeps every
entry above the new `T`, each device's last landed write, and the latest
stamped entry at or below `T`, which it tracks. It forgets only entries at
or below `T` that are neither their device's last landed write nor the
tracked one. -/
theorem pass_retains {log : List (Entry D)} {T : Nat} {kept forgets : Entry D → Prop}
    {newest : Option (Entry D)}
    (above : ∀ e ∈ log, T < e.slot → kept e)
    (last : ∀ e ∈ log, Last log e → kept e)
    (tracked : Tracks log T newest)
    (newestKept : ∀ k, newest = some k → kept k)
    (only : ∀ e ∈ log, forgets e → e.slot ≤ T ∧ ¬ Last log e ∧ newest ≠ some e) :
    Retains log T (keeps kept forgets) where
  above e mem gt :=
    ⟨above e mem gt, fun gone => absurd gt (Nat.not_lt.mpr (only e mem gone).1)⟩
  last e mem isLast := ⟨last e mem isLast, fun gone => (only e mem gone).2.1 isLast⟩
  newest e mem le := by
    cases newest with
    | none => exact absurd le (Nat.not_le.mpr (tracked e mem))
    | some k =>
      obtain ⟨kmem, kslot, kmost⟩ := tracked
      exact ⟨k, kmem, ⟨newestKept k rfl, fun gone => (only k kmem gone).2.2 rfl⟩, kslot,
        kmost e mem le⟩

/-- Entries landing above `s` leave the latest stamped entry at or below `s`
as it was. -/
theorem tracks_land {log new : List (Entry D)} {s : Nat} {newest : Option (Entry D)}
    (tracked : Tracks log s newest) (landed : ∀ e ∈ new, s < e.slot) :
    Tracks (log ++ new) s newest := by
  cases newest with
  | none =>
    intro e mem
    rcases List.mem_append.mp mem with old | fresh
    · exact tracked e old
    · exact landed e fresh
  | some k =>
    obtain ⟨kmem, kslot, kmost⟩ := tracked
    refine ⟨List.mem_append_left new kmem, kslot, fun e mem le => ?_⟩
    rcases List.mem_append.mp mem with old | fresh
    · exact kmost e old le
    · exact absurd le (Nat.not_le.mpr (landed e fresh))

/-- What one pass kept at `T` it keeps above any later `T'`: the next pass's
first premise. -/
theorem retains_later {log : List (Entry D)} {T T' : Nat} {kept : Entry D → Prop}
    (retains : Retains log T kept) (le : T ≤ T') :
    ∀ e ∈ log, T' < e.slot → kept e :=
  fun e mem gt => retains.above e mem (Nat.lt_of_le_of_lt le gt)

/-- **Writes landing past `T` keep it.** Entries landing in slots above `T`
are kept, and what was kept still meets `Retains`: an entry last among the
longer log was last among the shorter one. -/
theorem land_retains {log new : List (Entry D)} {T : Nat} {kept : Entry D → Prop}
    (retains : Retains log T kept) (landed : ∀ e ∈ new, T < e.slot ∧ kept e) :
    Retains (log ++ new) T kept where
  above e mem gt := by
    rcases List.mem_append.mp mem with old | fresh
    · exact retains.above e old gt
    · exact (landed e fresh).2
  last e mem isLast := by
    rcases List.mem_append.mp mem with old | fresh
    · exact retains.last e old fun x hx same => isLast x (List.mem_append_left new hx) same
    · exact (landed e fresh).2
  newest e mem le := by
    rcases List.mem_append.mp mem with old | fresh
    · obtain ⟨k, kmem, kkept, kslot, later⟩ := retains.newest e old le
      exact ⟨k, List.mem_append_left new kmem, kkept, kslot, later⟩
    · exact absurd le (Nat.not_le.mpr (landed e fresh).1)

end CovenMerge.ForgetEntries
