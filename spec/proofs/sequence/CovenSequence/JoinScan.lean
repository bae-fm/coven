import CovenSequence.Retention

/-!
# Finding a joining member's key through retention

The scan projects copies of one usable store key for the request's exact
credential. Signature, box opening and membership are checked by the ordinary
reader after finding that key. A floor captures the copies for its sealing key
through `at`, which covers every selected snapshot. Key rotation supplies its
own copies before introducing the replacement key; this projection fixes a key.
Removal or replacement of the joining credential is outside an admission promise.

A scan round reads the current floor prefix, then the next kept slot. Retention
may change between rounds without any grace. A missing slot after a stale floor
read requires another floor read, not a decline; `miss_requires_refresh` proves
this from the full storage model. Successful reads and continued polling are
needed for liveness, as for the ordinary sequence reader.
-/
namespace CovenSequence.JoinScan

/-- Original copies of the floor's sealing key, captured through its writer's
applied position. The predicate includes the exact recipient credential. -/
def capture (copy : Nat → Bool) (capturedAt : Nat) : List Nat :=
  ((List.range capturedAt).map (· + 1)).filter copy

theorem captured_iff (copy : Nat → Bool) (capturedAt slot : Nat) :
    slot ∈ capture copy capturedAt ↔ 0 < slot ∧ slot ≤ capturedAt ∧ copy slot = true := by
  simp only [capture, List.mem_filter, List.mem_map, List.mem_range]
  constructor
  · rintro ⟨⟨i, hi, rfl⟩, hc⟩
    exact ⟨by omega, by omega, hc⟩
  · rintro ⟨hp, ha, hc⟩
    exact ⟨⟨slot - 1, by omega, by omega⟩, hc⟩

/-- Capture follows from the writer's applied position and snapshot coverage;
it is not an assumption that a deleted approval remains available. -/
theorem floor_captures_deleted_copy {S : Sig} (R : Rules S)
    {st : Store S} {seq : Nat → Slot S} (good : Good R st seq)
    (audience : S.Aud) (copy : Nat → Bool) (capturedAt slot : Nat)
    (applied : st.floor.covers audience ≤ capturedAt) (positive : 0 < slot)
    (deleted : slot < st.floor.start) (approval : copy slot = true) :
    slot ∈ capture copy capturedAt := by
  have := good.start_le audience
  exact (captured_iff copy capturedAt slot).mpr ⟨positive, by omega, approval⟩

/-- The floor writer has applied each selected announcement. Its coverage
precedes that announcement, so the capture includes every deleted slot. -/
theorem capture_covers_floor {S : Sig} (R : Rules S)
    {st : Store S} {seq : Nat → Slot S} (good : Good R st seq)
    (audience : S.Aud) (capturedAt : Nat)
    (announcements : ∀ a author, 1 ≤ a → a ≤ st.head →
      seq a = .snapshot author audience (st.floor.covers audience) (st.floor.path audience) →
      AppliedTo R seq audience a → a ≤ capturedAt) :
    st.floor.covers audience ≤ capturedAt := by
  rcases good.floor_announced audience with zero | ⟨a, author, positive, claimed, slot, applied⟩
  · omega
  · have bound := announcements a author positive claimed slot applied
    have valid := applied.2
    rw [slot] at valid
    simp only [audValid, Bool.and_eq_true, decide_eq_true_eq] at valid
    omega

theorem kept_copy_readable {S : Sig} (R : Rules S)
    {st : Store S} {seq : Nat → Slot S} (good : Good R st seq)
    (slot : Nat) (positive : 0 < slot) (claimed : slot ≤ st.head)
    (kept : st.floor.start ≤ slot) : st.slot slot = some (seq slot) := by
  rw [good.kept_genuine slot kept, good.first_genuine slot positive claimed]

/-- Clear metadata used by the scan. The projection permits deletion of the
whole prefix immediately. -/
structure View where
  first : Nat
  head : Nat
  copies : List Nat

structure Scan where
  /-- The next slot to inspect, not the last applied slot. -/
  scanned : Nat
  key : Bool
  deriving DecidableEq

inductive Result where
  | advance (scan : Scan)
  | key
  | endOfSequence
  deriving DecidableEq

/-- One round; an acquired key ends the keyless scan. -/
def read (copy : Nat → Bool) (view : View) (scan : Scan) : Result :=
  if scan.key || !view.copies.isEmpty then .key
  else
    let next := max scan.scanned view.first
    if next ≤ view.head then
      if copy next then .key else .advance ⟨next + 1, false⟩
    else .endOfSequence

/-- A retained sequence and a floor captured through its applied position. -/
def view (copy : Nat → Bool) (first head capturedAt : Nat) : View :=
  ⟨first, head, capture copy capturedAt⟩

/-- Every round finds a key or strictly advances toward the claimed copy.
Neither clocks nor a grace interval occur in the premises. -/
theorem approved_progress (copy : Nat → Bool) (first head capturedAt approval : Nat)
    (positive : 0 < approval) (claimed : approval ≤ head)
    (covered : first ≤ capturedAt + 1) (yes : copy approval = true)
    (scan : Scan) (unread : scan.scanned ≤ approval) :
    read copy (view copy first head capturedAt) scan = .key ∨
      ∃ next, read copy (view copy first head capturedAt) scan = .advance next ∧
        scan.scanned < next.scanned ∧ next.scanned ≤ approval := by
  by_cases captured : (capture copy capturedAt).isEmpty = false
  · exact Or.inl (by simp [read, view, captured])
  have empty : capture copy capturedAt = [] := by
    cases h : capture copy capturedAt with
    | nil => rfl
    | cons x xs => simp [h] at captured
  have kept : first ≤ approval := by
    by_cases h : first ≤ approval
    · exact h
    have hc := (captured_iff copy capturedAt approval).mpr ⟨positive, by omega, yes⟩
    simp [empty] at hc
  by_cases held : scan.key = true
  · exact Or.inl (by simp [read, held])
  have noKey : scan.key = false := by cases h : scan.key <;> simp_all
  have bound : max scan.scanned first ≤ approval := by omega
  by_cases hit : copy (max scan.scanned first) = true
  · exact Or.inl (by simp [read, view, empty, noKey, hit, show max scan.scanned first ≤ head by omega])
  · have before : max scan.scanned first < approval := by
      by_cases h : max scan.scanned first < approval
      · exact h
      have eq : max scan.scanned first = approval := by omega
      exact False.elim (hit (eq ▸ yes))
    refine Or.inr ⟨⟨max scan.scanned first + 1, false⟩, ?_, by simp; omega, by simp; omega⟩
    simp [read, view, empty, noKey, hit, show max scan.scanned first ≤ head by omega]

/-- Retention may choose a different floor and capture at every round. -/
def run (copy : Nat → Bool) (views : Nat → View) (round : Nat) (scan : Scan) : Nat → Result
  | 0 => .advance scan
  | fuel + 1 => match read copy (views round) scan with
    | .advance next => run copy views (round + 1) next fuel
    | result => result

theorem key_terminal (copy : Nat → Bool) (views : Nat → View) :
    ∀ fuel extra round scan, run copy views round scan fuel = .key →
      run copy views round scan (fuel + extra) = .key := by
  intro fuel
  induction fuel with
  | zero => intro extra round scan h; cases h
  | succ fuel ih =>
    intro extra round scan h
    rw [Nat.succ_add]
    simp only [run] at h ⊢
    cases hr : read copy (views round) scan with
    | key => simp
    | endOfSequence => simp [hr] at h
    | advance next =>
      simp only [hr] at h ⊢
      exact ih extra (round + 1) next h

/-- Any schedule of retention, including immediate deletion, yields the key
within the remaining distance to the approval plus one successful round. -/
theorem approved_eventually_key (copy : Nat → Bool) (first head capturedAt : Nat → Nat)
    (approval : Nat) (positive : 0 < approval) (yes : copy approval = true)
    (claimed : ∀ round, approval ≤ head round)
    (covered : ∀ round, first round ≤ capturedAt round + 1)
    (round : Nat) (scan : Scan) (unread : scan.scanned ≤ approval) :
    run copy (fun r => view copy (first r) (head r) (capturedAt r)) round scan
      (approval + 1 - scan.scanned) = .key := by
  generalize hn : approval + 1 - scan.scanned = fuel
  induction fuel using Nat.strongRecOn generalizing round scan with
  | ind fuel ih =>
    obtain ⟨n, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (by omega : fuel ≠ 0)
    simp only [run]
    rcases approved_progress copy (first round) (head round) (capturedAt round) approval
      positive (claimed round) (covered round) yes scan unread with found | ⟨next, step, advances, bound⟩
    · rw [found]
    · rw [step]
      have target := ih (approval + 1 - next.scanned) (by omega) (round + 1) next bound rfl
      have enough : approval + 1 - next.scanned ≤ n := by omega
      have extra := key_terminal copy (fun r => view copy (first r) (head r) (capturedAt r))
        (approval + 1 - next.scanned) (n - (approval + 1 - next.scanned)) (round + 1) next target
      simpa [show approval + 1 - next.scanned + (n - (approval + 1 - next.scanned)) = n by omega]
        using extra

/-- A slot miss below a claimed approval cannot be an end observation. Reading
the current floor either obtains its copy or strictly advances the scan. -/
theorem miss_requires_refresh {S : Sig} (R : Rules S)
    {st : Store S} {seq : Nat → Slot S} (good : Good R st seq)
    {next approval : Nat} (positive : 0 < next) (unread : next ≤ approval)
    (claimed : approval ≤ st.head) (missing : st.slot next = none) :
    next < st.floor.start := by
  by_cases h : next < st.floor.start
  · exact h
  rw [good.kept_genuine next (by omega), good.first_genuine next positive (by omega)] at missing
  cases missing

/-- Even if retention runs again between the slot miss and the floor GET,
the fresh prefix supplies the copy or a strictly later scan position. -/
theorem raced_miss_recovers {S : Sig} (R : Rules S)
    {st st' : Store S} {seq : Nat → Slot S} (good : Good R st seq)
    (changes : StoreSteps R seq st st') (audience : S.Aud)
    (copy : Nat → Bool) (capturedAt next approval : Nat)
    (applied : st'.floor.covers audience ≤ capturedAt)
    (positive : 0 < next) (unread : next ≤ approval) (claimed : approval ≤ st.head)
    (yes : copy approval = true) (missing : st.slot next = none) :
    approval ∈ capture copy capturedAt ∨ next < st'.floor.start ∧ st'.floor.start ≤ approval := by
  have past := miss_requires_refresh R good positive unread claimed missing
  have mono : st.floor.start ≤ st'.floor.start := by
    have monotone : ∀ {a b : Store S}, StoreSteps R seq a b → a.floor.start ≤ b.floor.start := by
      intro a b steps
      induction steps with
      | refl => exact Nat.le_refl _
      | step step _ ih => exact Nat.le_trans (store_mono R step).1 ih
    exact monotone changes
  by_cases deleted : approval < st'.floor.start
  · exact Or.inl (floor_captures_deleted_copy R (stores_good R changes good)
      audience copy capturedAt approval applied (by omega) deleted yes)
  · exact Or.inr ⟨by omega, by omega⟩

/-- A scan with no matching copies reaches the real end; a deleted prefix
does not keep a genuinely declined request waiting forever. -/
theorem no_copy_reaches_end (first head : Nat) :
    ∀ fuel round scan, scan.key = false → 0 < fuel → head + 1 < scan.scanned + fuel →
      run (fun _ => false) (fun _ => ⟨first, head, []⟩) round scan fuel = .endOfSequence := by
  intro fuel
  induction fuel with
  | zero => intro round scan key positive enough; omega
  | succ fuel ih =>
    intro round scan key positive enough
    simp only [run, read, key, List.isEmpty_nil, Bool.not_true, Bool.or_self, Bool.false_eq_true,
      ↓reduceIte]
    by_cases kept : max scan.scanned first ≤ head
    · simp only [kept, ↓reduceIte]
      exact ih (round + 1) ⟨max scan.scanned first + 1, false⟩ rfl (by omega) (by simp; omega)
    · simp [kept]

inductive Outcome where
  | waiting
  | admitted
  | declined
  deriving DecidableEq

/-- Finding a key grants no authority. `binding` is the ordinary reader's
verified entry from the invite's approving device for this exact pair. -/
def outcome (key binding pending : Bool) : Outcome :=
  if key && binding then .admitted else if pending then .waiting else .declined

theorem approved_never_declined (pending : Bool) : outcome true true pending = .admitted := by
  simp [outcome]

theorem real_decline (key : Bool) : outcome key false false = .declined := by
  simp [outcome]

/-- An applied approval admits its member; retention cannot undo it. Only a
later explicit removal can change this membership fact. -/
theorem approval_keeps_membership {S : Sig} (R : Rules S) (seq : Nat → Slot S)
    (member : S.Mem) (author : S.Dev) (role : Role) (approval head : Nat)
    (positive : 0 < approval) (claimed : approval ≤ head)
    (entry : seq approval = .admin author (.addMember member role))
    (applied : Applied R seq approval)
    (notRemoved : ∀ k, approval < k → k ≤ head → ∀ d,
      seq k ≠ .admin d (.removeMember member)) :
    (ledgerAt R seq head).member member = true := by
  induction head with
  | zero => omega
  | succ head ih =>
    by_cases last : approval = head + 1
    · subst approval
      simp only [Applied, Nat.add_sub_cancel] at applied
      rw [ledgerAt_succ]
      simp only [ledgerStep, applied, ↓reduceIte, noteSlot_member]
      simp [entry, ledgerEffect, applyEntry]
    · have previous := ih (by omega) (fun k lower upper => notRemoved k lower (by omega))
      rw [ledgerAt_succ]
      unfold ledgerStep
      simp only [noteSlot_member]
      split
      · cases hs : seq (head + 1) with
        | admin d e =>
          cases e with
          | removeMember target =>
            have different : member ≠ target := by
              intro same
              subst target
              exact notRemoved (head + 1) (by omega) (by omega) d hs
            simpa [ledgerEffect, applyEntry, different] using previous
          | addMember target role =>
            simp [ledgerEffect, applyEntry, previous]
          | _ => simpa [ledgerEffect, applyEntry] using previous
        | _ => simpa [ledgerEffect] using previous
      · exact previous

/-- Compose the scan with the existing authenticated floor/suffix reader.
The admission remains current at the head; later removal is not an invitation
rejection. Reader cryptography and the exact approver check are its boundary. -/
theorem approved_eventually_admitted {S : Sig} (R : Rules S)
    {st : Store S} {seq : Nat → Slot S} (good : Good R st seq) (audience : S.Aud)
    (member : S.Mem) (author : S.Dev) (role : Role) (binding : Nat)
    (bindingPositive : 0 < binding) (bindingClaimed : binding ≤ st.head)
    (entry : seq binding = .admin author (.addMember member role))
    (applied : Applied R seq binding)
    (notRemoved : ∀ k, binding < k → k ≤ st.head → ∀ d,
      seq k ≠ .admin d (.removeMember member))
    (copy : Nat → Bool) (first head capturedAt : Nat → Nat) (approval : Nat)
    (positive : 0 < approval) (yes : copy approval = true)
    (claimed : ∀ round, approval ≤ head round)
    (covered : ∀ round, first round ≤ capturedAt round + 1)
    (scan : Scan) (unread : scan.scanned ≤ approval) :
    run copy (fun r => view copy (first r) (head r) (capturedAt r)) 0 scan
        (approval + 1 - scan.scanned) = .key ∧
    ∃ reader, ReadSteps R audience true st
      ⟨st.floor.covers audience, ledgerAt R seq (st.floor.covers audience),
        audAt R seq audience (st.floor.covers audience), rowsAt R seq audience (st.floor.covers audience),
        fun _ => none⟩ reader ∧ reader.pos = st.head ∧ reader.ledger.member member = true := by
  refine ⟨approved_eventually_key copy first head capturedAt approval positive yes claimed covered 0 scan unread, ?_⟩
  obtain ⟨reader, steps, position, state⟩ := joiner_reaches_head R good audience
  have ledger : reader.ledger = ledgerAt R seq st.head := congrArg Prod.fst state
  exact ⟨reader, steps, position, by
    rw [ledger]
    exact approval_keeps_membership R seq member author role binding st.head
      bindingPositive bindingClaimed entry applied notRemoved⟩

end CovenSequence.JoinScan
