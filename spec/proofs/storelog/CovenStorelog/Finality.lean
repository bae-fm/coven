import CovenStorelog.FinalityReplay

namespace CovenStorelog.Finality

/-- Online authors can only have read entries that had already landed. -/
theorem read_landed_before (H : History) (n : Nat) (hv : Valid H n)
    {w a : Nat} (hw : w < n) (ha : hadRead H.log w a = true) :
    a < n ∧ H.stored a < H.stored w := by
  have ha' := hv.causal.past_lt w hw a ha
  have ht := (hv.online w a hw (by omega)).mp ha
  have hland := hv.attempt_le w hw
  exact ⟨by omega, by omega⟩

/-- An entry is late if some earlier landing was not in its first-attempt read. -/
def late (H : History) (n : Nat) (S : EntrySet) (w : Nat) : Bool :=
  (List.range n).any fun a => S a && decide (H.stored a < H.stored w) && !hadRead H.log w a

/-- The open lower boundary and closed upper boundary in rule 2. -/
def quiet (H : History) (W n : Nat) (S : EntrySet) (T : Nat) : Bool :=
  (List.range n).all fun w =>
    !(S w && decide (T < H.stored w + W) && decide (H.stored w ≤ T)) || !late H n S w

def old (H : History) (W T : Nat) : EntrySet := fun w => H.stored w + W ≤ T

def finalSet (H : History) (W n : Nat) (S : EntrySet) (T : Nat) : EntrySet :=
  fun w => decide (w < n) && S w && old H W T w && quiet H W n S T

/-- A device has received the prefix it is about to regard as final. Entries
inside the recent window need not be delivered in storage order. -/
def CompleteOld (H : History) (W n T : Nat) (S : EntrySet) : Prop :=
  ∀ w, w < n → old H W T w = true → S w = true

/-- The cutoff contains all historical evidence used by its entries. A future
landing cannot change an old entry's recorded author view. -/
theorem old_closed (H : History) (W n T : Nat) (hv : Valid H n) :
    Closed H.log (fun w => decide (w < n) && old H W T w) := by
  intro w hw a ha
  have hh : w < n ∧ H.stored w + W ≤ T := by
    simpa only [Bool.and_eq_true, decide_eq_true_eq, old] using hw
  have hb := read_landed_before H n hv hh.1 ha
  simp only [old, Bool.and_eq_true, decide_eq_true_eq]
  exact ⟨hb.1, by omega⟩

theorem quiet_reads (H : History) (W n T : Nat)
    (hq : quiet H W n (fun _ => true) T = true)
    {b a : Nat} (hb : b < n) (ha : a < n)
    (hwindow : T < H.stored b + W) (hland : H.stored b ≤ T)
    (hearlier : H.stored a < H.stored b) : hadRead H.log b a = true := by
  have h := List.all_eq_true.mp hq b (List.mem_range.mpr hb)
  simp only [hwindow, hland, decide_true, Bool.and_self,
    Bool.not_true, Bool.false_or, Bool.not_eq_true'] at h
  have ha' := (List.any_eq_false.mp h) a (List.mem_range.mpr ha)
  simpa [late, hearlier] using ha'

/-- Any survivor outside the old prefix read the whole prefix: recent entries
by quietness; future entries by rule 1. This includes backdated author clocks. -/
theorem survivor_reads_old (H : History) (W n T : Nat) (S : EntrySet)
    (hc : CompleteOld H W n T S) (hq : quiet H W n (fun _ => true) T = true)
    {b a : Nat} (hb : b < n) (ha : a < n)
    (hnew : old H W T b = false) (hold : old H W T a = true)
    (hs : tooLate H W n S b = false) : hadRead H.log b a = true := by
  have hnew' : T < H.stored b + W := by simpa [old] using hnew
  have hold' : H.stored a + W ≤ T := by simpa [old] using hold
  by_cases ht : H.stored b ≤ T
  · exact quiet_reads H W n T hq hb ha hnew' ht (by omega)
  · cases hp : hadRead H.log b a
    · have hd := (tooLate_iff H W n S b).mpr
        ⟨a, ha, hc a ha hold, by omega, hp⟩
      simp [hs] at hd
    · rfl

/-- Every witness against an old entry is itself old, so receiving the old
prefix suffices to decide its time rejection permanently. -/
theorem old_drop_fixed (H : History) (W n T : Nat) (S : EntrySet)
    (hc : CompleteOld H W n T S) {w : Nat} (hw : old H W T w = true) :
    tooLate H W n S w = tooLate H W n (fun _ => true) w := by
  apply Bool.eq_iff_iff.mpr
  rw [tooLate_iff, tooLate_iff]
  constructor
  · rintro ⟨a, ha, _, ht, hp⟩; exact ⟨a, ha, rfl, ht, hp⟩
  · rintro ⟨a, ha, _, ht, hp⟩
    have ho : old H W T a = true := by
      simp only [old, decide_eq_true_eq] at hw ⊢
      omega
    exact ⟨a, ha, hc a ha ho, ht, hp⟩

/-- Ordered candidates split at any predicate whose selected entries all
precede its unselected entries; rejected candidates can lie anywhere. -/
theorem range_split (n : Nat) (live cut : EntrySet)
    (ho : ∀ a b, a < n → b < n → live a = true → live b = true →
      cut a = true → cut b = false → a < b) :
    (List.range n).filter live =
      (List.range n).filter (fun w => live w && cut w) ++
      (List.range n).filter (fun w => live w && !cut w) := by
  induction n with
  | zero => rfl
  | succ n ih =>
      have hi := ih (fun a b ha hb => ho a b (by omega) (by omega))
      rw [List.range_succ, List.filter_append, List.filter_append, List.filter_append]
      cases hl : live n <;> cases hc : cut n <;> simp only [List.filter_cons,
        List.filter_nil, hl, hc, Bool.and_false, Bool.and_true, Bool.not_true,
        Bool.not_false, Bool.false_eq_true,
        ↓reduceIte, List.append_nil]
      · exact hi
      · exact hi
      · simpa only [List.append_assoc] using congrArg (· ++ [n]) hi
      · have hn : (List.range n).filter (fun w => live w && !cut w) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro b hb hf
          have hh : live b = true ∧ cut b = false := by
            simpa only [Bool.and_eq_true, Bool.not_eq_true'] using hf
          have hlt := ho n b (by omega) (by have := List.mem_range.mp hb; omega)
            hl hh.1 hc hh.2
          have := List.mem_range.mp hb
          omega
        simpa only [hn, List.nil_append, List.append_nil] using congrArg (· ++ [n]) hi

/-- The admitted old candidates form a settled timestamp prefix. -/
theorem resolve_old_prefix (H : History) (W n T : Nat) (S : EntrySet)
    (hv : Valid H n) (hc : CompleteOld H W n T S)
    (hq : quiet H W n (fun _ => true) T = true) :
    let live := admitted H W n S
    let P := (List.range n).filter (fun w => live w && old H W T w)
    let r := settle H.log (authorViews H W n) P (conflict H.log) (prefer H.log)
    ReplayPrefix.Agree P (resolve H W n S).kept r.kept ∧
    ReplayPrefix.Agree P (resolve H W n S).dropped r.dropped := by
  dsimp only
  let live := admitted H W n S
  let P := (List.range n).filter (fun w => live w && old H W T w)
  let suffix := (List.range n).filter (fun w => live w && !old H W T w)
  have read (b a : Nat) (hb : b < n) (ha : a < n)
      (hl : live b = true) (hn : old H W T b = false) (ho : old H W T a = true) :
      hadRead H.log b a = true := by
    exact survivor_reads_old H W n T S hc hq hb ha hn ho
      (by simpa [live, admitted] using (Bool.and_eq_true_iff.mp hl).2)
  have splitList : (List.range n).filter live = P ++ suffix := by
    apply range_split
    intro a b ha hb _ hl ho hn
    exact hv.causal.past_lt b hb a (read b a hb ha hl hn ho)
  have sep : ∀ b ∈ suffix, b ∉ P ∧ ∀ a ∈ P, conflict H.log b a = false := by
    intro b hb
    obtain ⟨hb, hl, hn⟩ := by
      simpa only [suffix, List.mem_filter, List.mem_range, Bool.and_eq_true,
        Bool.not_eq_true'] using hb
    constructor
    · simp [P, hn]
    · intro a ha
      obtain ⟨ha, _, ho⟩ := by
        simpa only [P, List.mem_filter, List.mem_range, Bool.and_eq_true] using ha
      exact read_no_conflict H.log b a (read b a hb ha hl hn ho)
  have hp := ReplayPrefix.settle_prefix H.log (authorViews H W n)
    (conflict H.log) (prefer H.log) P suffix ((List.nodup_range (n := n)).filter _) sep
  rw [← splitList] at hp
  refine ⟨hp.1, ?_⟩
  intro a ha
  have halive : tooLate H W n S a = false := by
    have := (List.mem_filter.mp ha).2
    simpa [live, admitted] using (Bool.and_eq_true_iff.mp (Bool.and_eq_true_iff.mp this).1).2
  simpa only [resolve, materialize, List.mem_append, List.mem_filter, List.mem_range,
    Bool.and_eq_true, halive, Bool.false_eq_true, and_false, or_false] using hp.2 a ha

/-- Storage-history stability, and device stability once the old prefix has
arrived. S and U can contain arbitrary later entries in arbitrary receipt order.
The finite bound can contain any continuation and any author timestamp order. -/
theorem stability (H : History) (W n T : Nat) (S U : EntrySet)
    (hv : Valid H n) (hs : CompleteOld H W n T S) (hu : CompleteOld H W n T U)
    (hq : quiet H W n (fun _ => true) T = true)
    {w : Nat} (hw : w < n) (ho : old H W T w = true) :
    (w ∈ (resolve H W n S).kept ↔ w ∈ (resolve H W n U).kept) ∧
    (w ∈ (resolve H W n S).dropped ↔ w ∈ (resolve H W n U).dropped) := by
  have hds := old_drop_fixed H W n T S hs ho
  have hdu := old_drop_fixed H W n T U hu ho
  cases hd : tooLate H W n (fun _ => true) w with
  | true =>
      have ds : w ∈ (resolve H W n S).dropped := by
        simp [resolve, materialize, hw, hs w hw ho, hds, hd]
      have du : w ∈ (resolve H W n U).dropped := by
        simp [resolve, materialize, hw, hu w hw ho, hdu, hd]
      have ks : w ∉ (resolve H W n S).kept := fun h => (partition H W n S w).2 h ds
      have ku : w ∉ (resolve H W n U).kept := fun h => (partition H W n U w).2 h du
      exact ⟨by simp [ks, ku], by simp [ds, du]⟩
  | false =>
      have eqP : (List.range n).filter (fun a => admitted H W n S a && old H W T a) =
          (List.range n).filter (fun a => admitted H W n U a && old H W T a) := by
        apply List.filter_congr
        intro a ha
        have ha' := List.mem_range.mp ha
        cases he : old H W T a
        · simp
        · simp [admitted, hs a ha' he, hu a ha' he,
            old_drop_fixed H W n T S hs he, old_drop_fixed H W n T U hu he]
      have ps := resolve_old_prefix H W n T S hv hs hq
      have pu := resolve_old_prefix H W n T U hv hu hq
      dsimp only at ps pu
      have hws : w ∈ (List.range n).filter (fun a => admitted H W n S a && old H W T a) := by
        simp [admitted, hw, hs w hw ho, ho, hds, hd]
      have hwu := eqP ▸ hws
      rw [eqP] at ps
      exact ⟨(ps.1 w hwu).trans (pu.1 w hwu).symm,
        (ps.2 w hwu).trans (pu.2 w hwu).symm⟩

/-- The complete storage prefix at an observation time. -/
def atTime (H : History) (T : Nat) : EntrySet :=
  fun w => decide (H.stored w ≤ T)

theorem time_complete (H : History) (W n T : Nat) :
    CompleteOld H W n T (atTime H T) := by
  intro w hw ho
  simp only [old, decide_eq_true_eq] at ho
  simp [atTime, show H.stored w ≤ T by omega]

/-- An earlier-storage witness cannot first land in a later continuation. -/
theorem late_atTime (H : History) (n T w : Nat) (hw : H.stored w ≤ T) :
    late H n (atTime H T) w = late H n (fun _ => true) w := by
  apply List.any_congr rfl
  intro a
  by_cases ht : H.stored a < H.stored w
  · simp [atTime, ht, show H.stored a ≤ T by omega]
  · simp [ht]

theorem quiet_atTime (H : History) (W n T : Nat) :
    quiet H W n (atTime H T) T = quiet H W n (fun _ => true) T := by
  apply List.all_congr rfl
  intro w
  by_cases ht : H.stored w ≤ T
  · simp [atTime, ht, late_atTime H n T w ht]
  · simp [atTime, ht]

/-- Rule 2 is sound for the storage history itself. Every further finite
continuation is covered by choosing its universe H,n and any later time U. -/
theorem storage_stability (H : History) (W n T U : Nat) (hv : Valid H n)
    (hq : quiet H W n (atTime H T) T = true) (htu : T ≤ U)
    {w : Nat} (hw : finalSet H W n (atTime H T) T w = true) :
    (w ∈ (resolve H W n (atTime H T)).kept ↔ w ∈ (resolve H W n (atTime H U)).kept) ∧
    (w ∈ (resolve H W n (atTime H T)).dropped ↔ w ∈ (resolve H W n (atTime H U)).dropped) := by
  have hh : w < n ∧ old H W T w = true := by
    simp only [finalSet, Bool.and_eq_true, decide_eq_true_eq] at hw
    exact ⟨hw.1.1.1, hw.1.2⟩
  apply stability H W n T _ _ hv (time_complete H W n T)
    (fun a ha ho => ?_) (by simpa only [quiet_atTime] using hq) hh.1 hh.2
  have hs : H.stored a + W ≤ T := by simpa [old] using ho
  simp [atTime, show H.stored a ≤ U by omega]

/-- No late landing for a whole window makes every entry before its boundary
final, including the exact W boundary. W can be any duration (30 days here). -/
theorem progress (H : History) (W n last T : Nat) (helapsed : last + W ≤ T)
    (hquiet : ∀ w, w < n → last < H.stored w → H.stored w ≤ T →
      late H n (fun _ => true) w = false) :
    ∀ w, w < n → H.stored w + W ≤ T → finalSet H W n (atTime H T) T w = true := by
  have hq : quiet H W n (fun _ => true) T = true := by
    apply List.all_eq_true.mpr
    intro w hw
    by_cases ht : T < H.stored w + W ∧ H.stored w ≤ T
    · have hl := hquiet w (List.mem_range.mp hw) (by omega) ht.2
      simp [hl]
    · have hn : H.stored w + W ≤ T ∨ T < H.stored w := by omega
      simpa only [Bool.true_and, Bool.or_eq_true, Bool.not_eq_true',
        Bool.and_eq_false_iff, decide_eq_false_iff_not, Nat.not_lt, Nat.not_le] using
        Or.inl hn
  intro w hw ho
  simp [finalSet, atTime, old, hw, ho, show H.stored w ≤ T by omega,
    quiet_atTime, hq]

structure Device where
  received : EntrySet
  result : Result
  final : EntrySet

def initial (H : History) (W n T : Nat) : Device :=
  ⟨fun _ => false, resolve H W n (fun _ => false), finalSet H W n (fun _ => false) T⟩

/-- Every delivery starts a fresh abstract replay. As in §9's static Log
model, author views refer to recorded past; a runtime must receive that evidence
before publishing. The delivery buffer itself is outside this model. -/
def receive (H : History) (W n T : Nat) (d : Device) (w : Nat) : Device :=
  let S := insert d.received w
  ⟨S, resolve H W n S, finalSet H W n S T⟩

theorem received_spec (H : History) (W n T : Nat) (L : List Nat) :
    L.foldl (receive H W n T) (initial H W n T) =
      ⟨entrySet L, resolve H W n (entrySet L), finalSet H W n (entrySet L) T⟩ := by
  have run : ∀ (tail seen : List Nat),
      tail.foldl (receive H W n T)
        ⟨entrySet seen, resolve H W n (entrySet seen), finalSet H W n (entrySet seen) T⟩ =
      ⟨entrySet (seen ++ tail), resolve H W n (entrySet (seen ++ tail)),
        finalSet H W n (entrySet (seen ++ tail)) T⟩ := by
    intro tail
    induction tail with
    | nil => intro seen; simp
    | cons w ws ih =>
        intro seen
        simpa only [List.foldl_cons, receive, insert_entrySet, List.append_assoc,
          List.singleton_append] using ih (seen ++ [w])
  have hempty : entrySet [] = (fun _ => false) := by funext w; simp [entrySet]
  have hr := run L []
  rw [hempty] at hr
  simpa only [List.nil_append, initial] using hr

/-- Equal entry sets at the same storage observation time give equal final
sets and equal full replay results, independent of arrival order. -/
theorem agreement (H : History) (W n T : Nat) (A B : List Nat)
    (h : ∀ w, w ∈ A ↔ w ∈ B) :
    A.foldl (receive H W n T) (initial H W n T) =
      B.foldl (receive H W n T) (initial H W n T) := by
  have hs : entrySet A = entrySet B := by funext w; simp [entrySet, h w]
  rw [received_spec, received_spec, hs]

/-- Rule 1 compares identical immutable metadata, never either device's clock.
Receipt order, repetition and a device's observation time do not occur in it. -/
theorem drop_agreement (H : History) (W n : Nat) (A B : List Nat)
    (h : ∀ w, w ∈ A ↔ w ∈ B) (w : Nat) :
    tooLate H W n (entrySet A) w = tooLate H W n (entrySet B) w := by
  have hs : entrySet A = entrySet B := by funext a; simp [entrySet, h a]
  rw [hs]

/-- Once the prefix through an entry's own landing has arrived, no later
receipt can change its rule-1 judgment. It need not wait for finality. -/
theorem drop_decided (H : History) (W n w : Nat) (S : EntrySet)
    (hc : ∀ a, a < n → H.stored a ≤ H.stored w → S a = true) :
    tooLate H W n S w = tooLate H W n (fun _ => true) w := by
  apply Bool.eq_iff_iff.mpr
  rw [tooLate_iff, tooLate_iff]
  constructor
  · rintro ⟨a, ha, _, ht, hp⟩; exact ⟨a, ha, rfl, ht, hp⟩
  · rintro ⟨a, ha, _, ht, hp⟩; exact ⟨a, ha, hc a ha (by omega), ht, hp⟩

/-- A caught-up device retains final dispositions after any further receipt
sequence. The new entries can have any permitted timestamps and arrival order. -/
theorem device_stability (H : History) (W n T : Nat) (A tail : List Nat)
    (hv : Valid H n) (hc : CompleteOld H W n T (entrySet A))
    (hq : quiet H W n (fun _ => true) T = true)
    {w : Nat} (hw : w < n) (ho : old H W T w = true) :
    let before := A.foldl (receive H W n T) (initial H W n T)
    let after := (A ++ tail).foldl (receive H W n T) (initial H W n T)
    (w ∈ before.result.kept ↔ w ∈ after.result.kept) ∧
    (w ∈ before.result.dropped ↔ w ∈ after.result.dropped) := by
  simp only [received_spec]
  apply stability H W n T _ _ hv hc ?_ hq hw ho
  intro a ha ho
  have := hc a ha ho
  simpa only [entrySet, List.mem_append, decide_eq_true_eq] using
    Or.inl (show a ∈ A by simpa [entrySet] using this)

/-- A normal, non-late entry cannot be rejected by rule 1, even for W = 0. -/
theorem normal_not_dropped (H : History) (W n w : Nat) (S : EntrySet)
    (hn : late H n S w = false) : tooLate H W n S w = false := by
  cases hd : tooLate H W n S w
  · rfl
  · obtain ⟨a, ha, hs, ht, hp⟩ := (tooLate_iff H W n S w).mp hd
    have hl : late H n S w = true := by
      apply List.any_eq_true.mpr
      exact ⟨a, List.mem_range.mpr ha, by simp [hs, hp, show H.stored a < H.stored w by omega]⟩
    simp [hn] at hl

end CovenStorelog.Finality
