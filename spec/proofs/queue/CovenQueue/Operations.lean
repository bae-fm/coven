import CovenQueue.Upload

/-! Crash recovery for one §18 operation. The journal retains its data before
any remote step. A completed journal denotes the deleted row; its data in
this mathematical state is a history witness, not another database table. -/
namespace CovenQueue.Operations

inductive Path where
  | copy (number : Nat)
  | entry (number : Nat)
  | snapshot (number : Nat)
  deriving DecidableEq, Repr

structure Object where
  path : Path
  bytes : Bytes
  deriving DecidableEq, Repr

structure Copy where
  number : Nat
  recipient : Nat
  sealed : Bytes
  deriving DecidableEq, Repr

structure Introduction where
  audience : Nat
  key : Nat
  keyHash : Nat
  deriving DecidableEq, Repr

structure Rotation extends Introduction where
  keyBytes : Bytes
  copies : List Copy
  entryNumber : Nat
  /-- Fixed plaintext and sealing key; the encoder is deterministic (§6). -/
  entryPlaintext : Bytes
  sealingKey : Nat
  deriving DecidableEq, Repr

structure Snapshot where
  number : Nat
  key : Nat
  sealed : Bytes
  /-- The captured state, including positions and schema, never the live DB. -/
  captured : Bytes
  deriving DecidableEq, Repr

inductive Data where
  | rotation (data : Rotation)
  | snapshot (data : Snapshot)
  deriving DecidableEq, Repr

/-- The format/crypto boundary: the rotation encoder includes the audience,
key id and hash as well as the fixed entry plaintext and sealing key.
Snapshot opening includes parsing and both signature checks (§15). -/
structure Codec where
  entry : Rotation → Bytes
  openEntry : Bytes → Option Introduction
  entry_roundtrip : ∀ r, openEntry (entry r) = some r.toIntroduction
  openSnapshot : Nat → Bytes → Option Bytes

/-- Storage projected onto this attempt's reserved paths. Unrelated objects
are outside this state; the supplied reservations must not collide with them. -/
abbrev Store := Path → Option Bytes

def Rotation.objects (codec : Codec) (r : Rotation) : List Object :=
  r.copies.map (fun c => ⟨.copy c.number, c.sealed⟩) ++
    [⟨.entry r.entryNumber, codec.entry r⟩]

def Data.objects (codec : Codec) : Data → List Object
  | .rotation r => r.objects codec
  | .snapshot s => [⟨.snapshot s.number, s.sealed⟩]

def Data.localBytes : Data → Bytes
  | .rotation r => r.keyBytes
  | .snapshot s => s.sealed

def Data.localChecked (codec : Codec) : Data → Prop
  | .rotation _ => True
  | .snapshot s => codec.openSnapshot s.key s.sealed = some s.captured

/-- Copy checkpoints refine §18.1's copy step so each confirmed copy survives
restart. Zero precedes the custody/file transaction; n+1 follows n uploads. -/
structure Row where
  id : Nat
  data : Data
  last_step : Nat
  deriving DecidableEq, Repr

inductive Journal where
  | pending (row : Row)
  | finished (id : Nat) (data : Data)
  deriving DecidableEq, Repr

def Journal.row : Journal → Option Row
  | .pending r => some r
  | .finished _ _ => none

def Journal.data : Journal → Data
  | .pending r => r.data
  | .finished _ d => d

def Journal.id : Journal → Nat
  | .pending r => r.id
  | .finished id _ => id

def Journal.position (codec : Codec) : Journal → Nat
  | .pending r => r.last_step
  | .finished _ d => (d.objects codec).length + 2

structure Memory where
  localDraft : Option Bytes
  reply : Option Object
  deriving DecidableEq, Repr

def Memory.empty : Memory := ⟨none, none⟩

structure State where
  journal : Journal
  /-- This attempt's custody value or retained sealed temporary file. -/
  retained : Option Bytes
  stored : Store
  /-- Provider-owned requests; process death cannot recall them. -/
  sent : List (Nat × Object)
  memory : Memory

def initial (id : Nat) (d : Data) : State :=
  ⟨.pending ⟨id, d, 0⟩, none, fun _ => none, [], .empty⟩

def State.crash (s : State) : State := { s with memory := .empty }

def create (store : Store) (o : Object) : Store :=
  if (store o.path).isNone then fun p => if p = o.path then some o.bytes else store p
  else store

theorem create_retains (store : Store) (o : Object) (p : Path) (b : Bytes)
    (h : store p = some b) : create store o p = some b := by
  unfold create
  split
  · by_cases hp : p = o.path
    · subst p; simp_all
    · simp [hp, h]
  · exact h

/-- Replay decisions are supplied independently of storage success. -/
abbrev Kept := Nat → Prop

inductive Step (codec : Codec) (kept : Kept) : State → State → Prop where
  | stageLocal (s : State) (r : Row) : s.journal = .pending r → r.last_step = 0 →
      Step codec kept s { s with memory := { s.memory with localDraft := some r.data.localBytes } }
  | persistLocal (s : State) (r : Row) : s.journal = .pending r → r.last_step = 0 →
      s.memory.localDraft = some r.data.localBytes →
      Step codec kept s { s with retained := some r.data.localBytes }
  | commitLocal (s : State) (r : Row) : s.journal = .pending r → r.last_step = 0 →
      s.retained = some r.data.localBytes → r.data.localChecked codec →
      Step codec kept s { s with
        journal := .pending { r with last_step := 1 }
        memory := .empty }
  | send (s : State) (r : Row) (n : Nat) (o : Object) :
      s.journal = .pending r → r.last_step = n + 1 →
      (r.data.objects codec)[n]? = some o →
      Step codec kept s { s with sent := (n, o) :: s.sent }
  | land (s : State) (n : Nat) (o : Object) : (n, o) ∈ s.sent →
      Step codec kept s { s with stored := create s.stored o }
  | receive (s : State) (r : Row) (n : Nat) (o : Object) :
      s.journal = .pending r → r.last_step = n + 1 →
      (r.data.objects codec)[n]? = some o → s.stored o.path = some o.bytes →
      Step codec kept s { s with memory := { s.memory with reply := some o } }
  | checkpoint (s : State) (r : Row) (n : Nat) (o : Object) :
      s.journal = .pending r → r.last_step = n + 1 →
      (r.data.objects codec)[n]? = some o → s.memory.reply = some o →
      s.stored o.path = some o.bytes →
      Step codec kept s { s with
        journal := .pending { r with last_step := n + 2 }
        memory := .empty }
  | finishSnapshot (s : State) (r : Row) (snapshot : Snapshot) :
      s.journal = .pending r → r.data = .snapshot snapshot → r.last_step = 2 →
      Step codec kept s { s with journal := .finished r.id r.data, memory := .empty }
  | finishRotation (s : State) (r : Row) (rotation : Rotation) :
      s.journal = .pending r → r.data = .rotation rotation →
      r.last_step = (r.data.objects codec).length + 1 → kept rotation.entryNumber →
      Step codec kept s { s with journal := .finished r.id r.data, memory := .empty }
  | crash (s : State) : Step codec kept s s.crash
  | wait (s : State) : Step codec kept s s

inductive Reachable (codec : Codec) (kept : Kept) (id : Nat) (d : Data) : State → Prop
  | initial : Reachable codec kept id d (initial id d)
  | step {s t} : Reachable codec kept id d s → Step codec kept s t →
      Reachable codec kept id d t

theorem step_fixed {codec : Codec} {kept : Kept} {s t : State} (h : Step codec kept s t) :
    t.journal.data = s.journal.data ∧ t.journal.id = s.journal.id := by
  cases h <;> simp_all [State.crash, Journal.data, Journal.id]

theorem fixed_data {codec : Codec} {kept : Kept} {id : Nat} {d : Data} {s : State}
    (h : Reachable codec kept id d s) : s.journal.data = d ∧ s.journal.id = id := by
  induction h with
  | initial => exact ⟨rfl, rfl⟩
  | step _ step ih =>
    have fixed := step_fixed step
    exact ⟨fixed.1.trans ih.1, fixed.2.trans ih.2⟩

def StoredThrough (codec : Codec) (d : Data) (stored : Store) (n : Nat) : Prop :=
  ∀ i, i < n → ∀ o, (d.objects codec)[i]? = some o → stored o.path = some o.bytes

theorem through_retained {codec : Codec} {d : Data} {a b : Store} {n : Nat}
    (retain : ∀ p bytes, a p = some bytes → b p = some bytes)
    (h : StoredThrough codec d a n) : StoredThrough codec d b n := by
  intro i hi o ho
  exact retain _ _ (h i hi o ho)

structure Valid (codec : Codec) (s : State) : Prop where
  bound : s.journal.position codec ≤ (s.journal.data.objects codec).length + 2
  pendingBound : ∀ r, s.journal = .pending r → r.last_step ≤ (r.data.objects codec).length + 1
  localReady : 0 < s.journal.position codec →
    s.retained = some s.journal.data.localBytes ∧ s.journal.data.localChecked codec
  confirmed : StoredThrough codec s.journal.data s.stored
    (s.journal.position codec - 1)
  requests : ∀ n o, (n, o) ∈ s.sent →
    (s.journal.data.objects codec)[n]? = some o ∧
    StoredThrough codec s.journal.data s.stored n
  objects : ∀ p bytes, s.stored p = some bytes →
    ∃ n o, (s.journal.data.objects codec)[n]? = some o ∧
      p = o.path ∧ bytes = o.bytes ∧ StoredThrough codec s.journal.data s.stored n

private theorem get_bound {α : Type} {xs : List α} {n : Nat} {x : α}
    (h : xs[n]? = some x) : n < xs.length := by
  exact (List.getElem?_eq_some_iff.mp h).1

private theorem extend_through {codec : Codec} {d : Data} {store : Store} {n : Nat} {o : Object}
    (h : StoredThrough codec d store n) (found : (d.objects codec)[n]? = some o)
    (stored : store o.path = some o.bytes) : StoredThrough codec d store (n + 1) := by
  intro i hi x hx
  by_cases eq : i = n
  · subst i
    have : x = o := Option.some.inj (hx.symm.trans found)
    subst x; exact stored
  · exact h i (by omega) x hx

private theorem finish_through {codec : Codec} {d : Data} {store : Store}
    (h : StoredThrough codec d store (d.objects codec).length) :
    StoredThrough codec d store ((d.objects codec).length + 1) := by
  intro i _ o ho
  exact h i (get_bound ho) o ho

theorem step_valid {codec : Codec} {kept : Kept} {s t : State}
    (step : Step codec kept s t) (v : Valid codec s) : Valid codec t := by
  cases step with
  | stageLocal | receive | crash | wait =>
    exact ⟨v.bound, v.pendingBound, v.localReady, v.confirmed, v.requests, v.objects⟩
  | send r n o hj hn ho =>
    refine { v with requests := ?_ }
    intro i x hx
    rcases List.mem_cons.mp hx with eq | old
    · cases eq
      refine ⟨by simpa [hj, Journal.data] using ho, ?_⟩
      simpa [hj, Journal.position, hn] using v.confirmed
    · exact v.requests i x old
  | persistLocal r hj hz draft =>
    refine { v with localReady := ?_ }
    intro positive
    simp [hj, Journal.position, hz] at positive
  | commitLocal r hj hz hd checked =>
    have data : s.journal.data = r.data := by simp [hj, Journal.data]
    refine ⟨by simp [Journal.position, Journal.data], ?_, ?_, ?_, ?_, ?_⟩
    · intro row eq; cases eq; simp
    · intro _; exact ⟨hd, checked⟩
    · intro i hi; simp [Journal.position] at hi
    · simpa [Journal.data, ← data] using v.requests
    · simpa [Journal.data, ← data] using v.objects
  | land n o sent =>
    have req := v.requests n o sent
    have retain := create_retains s.stored o
    refine { v with
      confirmed := through_retained retain v.confirmed
      requests := ?_
      objects := ?_ }
    · intro i x hx
      exact ⟨(v.requests i x hx).1, through_retained retain (v.requests i x hx).2⟩
    · intro p bytes hp
      change create s.stored o p = some bytes at hp
      unfold create at hp
      split at hp
      · dsimp only at hp
        split at hp
        · rename_i path
          have bytesEq : bytes = o.bytes := (Option.some.inj hp).symm
          exact ⟨n, o, req.1, path, bytesEq, through_retained retain req.2⟩
        · obtain ⟨i, x, hx, path, eq, before⟩ := v.objects p bytes hp
          exact ⟨i, x, hx, path, eq, through_retained retain before⟩
      · obtain ⟨i, x, hx, path, eq, before⟩ := v.objects p bytes hp
        exact ⟨i, x, hx, path, eq, through_retained retain before⟩
  | checkpoint r n o hj hn ho reply stored =>
    have data : s.journal.data = r.data := by simp [hj, Journal.data]
    have pos : s.journal.position codec = n + 1 := by simp [hj, Journal.position, hn]
    have bound := get_bound ho
    refine ⟨by simp [Journal.position, Journal.data]; omega, ?_, ?_, ?_, ?_, ?_⟩
    · intro row eq; cases eq; dsimp; omega
    · intro _; simpa [hj, Journal.data] using v.localReady (by omega)
    · have before : StoredThrough codec r.data s.stored n := by simpa [data, pos] using v.confirmed
      simpa [Journal.position, Journal.data] using extend_through before ho stored
    · simpa [Journal.data, ← data] using v.requests
    · simpa [Journal.data, ← data] using v.objects
  | finishSnapshot r snapshot hj hd hn =>
    have data : s.journal.data = r.data := by simp [hj, Journal.data]
    have pos : s.journal.position codec = 2 := by simp [hj, Journal.position, hn]
    refine ⟨by simp [Journal.position, Journal.data], ?_, ?_, ?_, ?_, ?_⟩
    · intro row eq; cases eq
    · intro _; simpa [hj, Journal.data] using v.localReady (by omega)
    · have before : StoredThrough codec r.data s.stored (r.data.objects codec).length := by
        simpa [data, pos, hd, Data.objects] using v.confirmed
      simpa [Journal.position, Journal.data] using finish_through before
    · simpa [Journal.data, ← data] using v.requests
    · simpa [Journal.data, ← data] using v.objects
  | finishRotation r rotation hj hd hn kept =>
    have data : s.journal.data = r.data := by simp [hj, Journal.data]
    have pos : s.journal.position codec = (r.data.objects codec).length + 1 := by
      simp [hj, Journal.position, hn]
    refine ⟨by simp [Journal.position, Journal.data], ?_, ?_, ?_, ?_, ?_⟩
    · intro row eq; cases eq
    · intro _; simpa [hj, Journal.data] using v.localReady (by omega)
    · have before : StoredThrough codec r.data s.stored (r.data.objects codec).length := by
        simpa [data, pos] using v.confirmed
      simpa [Journal.position, Journal.data] using finish_through before
    · simpa [Journal.data, ← data] using v.requests
    · simpa [Journal.data, ← data] using v.objects

theorem reachable_valid {codec : Codec} {kept : Kept} {id : Nat} {d : Data} {s : State}
    (h : Reachable codec kept id d s) : Valid codec s := by
  induction h with
  | initial =>
    refine ⟨by simp [initial, Journal.position, Journal.data], ?_, ?_, ?_, ?_, ?_⟩
    · intro row eq; cases eq; simp
    · simp [initial, Journal.position]
    · intro i hi; simp [initial, Journal.position] at hi
    · intro n o h; simp [initial] at h
    · intro p b h; simp [initial] at h
  | step _ step ih => exact step_valid step ih

/-- The stored entry itself witnesses that every prerequisite copy is already
stored, even if the publisher has not received its entry's reply. -/
theorem rotation_entry_after_copies {codec : Codec} {kept : Kept} {id : Nat}
    {r : Rotation} {s : State} (h : Reachable codec kept id (.rotation r) s)
    {bytes : Bytes} (entry : s.stored (.entry r.entryNumber) = some bytes) :
    bytes = codec.entry r ∧ ∀ c ∈ r.copies, s.stored (.copy c.number) = some c.sealed := by
  have fixed := (fixed_data h).1
  obtain ⟨n, o, ho, path, hb, before⟩ := (reachable_valid h).objects _ _ entry
  rw [fixed] at ho before
  have bound := get_bound ho
  simp only [Data.objects, Rotation.objects, List.length_append, List.length_map,
    List.length_singleton] at bound
  have hn : n = r.copies.length := by
    by_cases eq : n = r.copies.length
    · exact eq
    have lt : n < r.copies.length := by omega
    simp only [Data.objects, Rotation.objects] at ho
    rw [List.getElem?_append_left (by simpa using lt), List.getElem?_map] at ho
    obtain ⟨c, _, hc⟩ := Option.map_eq_some_iff.mp ho
    subst o
    cases path
  have obj : o = ⟨.entry r.entryNumber, codec.entry r⟩ := by
    simpa [Data.objects, Rotation.objects, hn, List.getElem?_append_right] using ho.symm
  subst o
  refine ⟨hb, ?_⟩
  intro c hc
  obtain ⟨i, hi, eq⟩ := List.mem_iff_getElem.mp hc
  apply before i (by omega) ⟨.copy c.number, c.sealed⟩
  simp only [Data.objects, Rotation.objects]
  rw [List.getElem?_append_left (by simpa using hi), List.getElem?_map,
    List.getElem?_eq_getElem hi, eq]
  rfl

theorem finished_publication {codec : Codec} {kept : Kept} {id : Nat} {d : Data} {s : State}
    (h : Reachable codec kept id d s) (done : s.journal.row = none) :
    s.journal = .finished id d ∧ s.retained = some d.localBytes ∧ d.localChecked codec ∧
    ∀ o ∈ d.objects codec, s.stored o.path = some o.bytes := by
  have fixed := fixed_data h
  have v := reachable_valid h
  cases hj : s.journal with
  | pending r => simp [hj, Journal.row] at done
  | finished i data =>
    have hi : i = id := by simpa [hj, Journal.id] using fixed.2
    have hd : data = d := by simpa [hj, Journal.data] using fixed.1
    subst i; subst data
    refine ⟨rfl, ?_, ?_, ?_⟩
    · exact (v.localReady (by simp [hj, Journal.position])).1.trans (congrArg some (congrArg Data.localBytes fixed.1))
    · simpa [fixed.1] using (v.localReady (by simp [hj, Journal.position])).2
    · intro o ho
      obtain ⟨n, hn, eq⟩ := List.mem_iff_getElem.mp ho
      apply v.confirmed n (by simp [hj, Journal.position]; omega) o
      simpa [hj, Journal.data, List.getElem?_eq_getElem hn] using congrArg some eq

theorem step_monotone {codec : Codec} {kept : Kept} {s t : State} (h : Step codec kept s t) :
    s.journal.position codec ≤ t.journal.position codec := by
  cases h <;> simp_all [State.crash, Journal.position, Data.objects]

theorem deleted_row_stays_deleted {codec : Codec} {kept : Kept} {s t : State}
    (h : Step codec kept s t) (done : s.journal.row = none) : t.journal.row = none := by
  cases h <;> simp_all [State.crash, Journal.row]

/-- Publication can only follow the successful local snapshot check. -/
theorem remote_locally_checked {codec : Codec} {kept : Kept} {id : Nat} {d : Data} {s : State}
    (h : Reachable codec kept id d s) :
    (∀ n o, (n, o) ∈ s.sent → d.localChecked codec) ∧
    (∀ p bytes, s.stored p = some bytes → d.localChecked codec) := by
  induction h with
  | initial =>
    constructor
    · intro n o h; simp [initial] at h
    · intro p bytes h; simp [initial] at h
  | @step s t reachable step ih =>
    cases step with
    | send r n o hj hn ho =>
      refine ⟨?_, ih.2⟩
      intro i x hx
      rcases List.mem_cons.mp hx with eq | old
      · have checked := (reachable_valid reachable).localReady (by simp [hj, Journal.position, hn])
        simpa [(fixed_data reachable).1] using checked.2
      · exact ih.1 i x old
    | land n o sent =>
      refine ⟨ih.1, ?_⟩
      intro p bytes _
      exact ih.1 n o sent
    | stageLocal | persistLocal | commitLocal | receive | checkpoint | finishSnapshot |
        finishRotation | crash | wait => exact ih

theorem snapshot_stored_checked {codec : Codec} {kept : Kept} {id : Nat}
    {snapshot : Snapshot} {s : State} (h : Reachable codec kept id (.snapshot snapshot) s)
    {bytes : Bytes} (stored : s.stored (.snapshot snapshot.number) = some bytes) :
    bytes = snapshot.sealed ∧ codec.openSnapshot snapshot.key bytes = some snapshot.captured := by
  obtain ⟨n, o, ho, _, equal, _⟩ := (reachable_valid h).objects _ _ stored
  rw [(fixed_data h).1] at ho
  have mem := List.mem_of_getElem? ho
  simp only [Data.objects, List.mem_singleton] at mem
  subst o
  have checked := (remote_locally_checked h).2 _ _ stored
  exact ⟨equal, by simpa [equal, Data.localChecked] using checked⟩

/-- A peer reads the introducing entry independently of the publisher's
journal. This is its checked payload; replay supplies authority separately.
Copies alone expose no introduction (§11, §18.2). -/
def readIntroduction (codec : Codec) (s : State) (number : Nat) : Option Introduction := do
  let bytes ← s.stored (.entry number)
  codec.openEntry bytes

theorem visible_rotation_has_copies {codec : Codec} {kept : Kept} {id : Nat}
    {r : Rotation} {s : State} (h : Reachable codec kept id (.rotation r) s)
    {introduction : Introduction}
    (visible : readIntroduction codec s r.entryNumber = some introduction) :
    introduction = r.toIntroduction ∧
      ∀ c ∈ r.copies, s.stored (.copy c.number) = some c.sealed := by
  cases stored : s.stored (.entry r.entryNumber) with
  | none => simp [readIntroduction, stored] at visible
  | some bytes =>
    have published := rotation_entry_after_copies h stored
    rw [published.1] at stored
    simp [readIntroduction, stored, codec.entry_roundtrip] at visible
    exact ⟨visible.symm, published.2⟩

/-- A fresh replay attempt is distinct from retrying the same request. §18
starts again after a dropped entry; §9 gives the replacement a new number.
All old physical objects remain, including the dropped entry. -/
inductive Restart (codec : Codec) (dropped : Nat → Prop) : State → State → Prop where
  | rotation (s : State) (row : Row) (old fresh : Rotation) :
      s.journal = .pending row → row.data = .rotation old →
      row.last_step = (row.data.objects codec).length + 1 →
      dropped old.entryNumber → old.entryNumber < fresh.entryNumber →
      old.key ≠ fresh.key → fresh.audience = old.audience →
      (∀ c ∈ old.copies, ∀ next ∈ fresh.copies, c.number < next.number) →
      Restart codec dropped s { s with
        journal := .pending ⟨row.id, .rotation fresh, 0⟩
        memory := .empty }

/-- Storage acceptance does not establish the replay-kept condition. -/
theorem finished_rotation_requires_kept {codec : Codec} {kept : Kept} {id : Nat}
    {rotation : Rotation} {s : State}
    (h : Reachable codec kept id (.rotation rotation) s) (done : s.journal.row = none) :
    kept rotation.entryNumber := by
  induction h with
  | initial => simp [initial, Journal.row] at done
  | @step s t reachable step ih =>
    cases step with
    | finishRotation row r hj hd _ accepted =>
      have data := (fixed_data reachable).1
      simp [hj, Journal.data, hd] at data
      subst r
      exact accepted
    | finishSnapshot row snapshot hj hd _ =>
      have data := (fixed_data reachable).1
      simp [hj, Journal.data, hd] at data
    | commitLocal | checkpoint => simp [Journal.row] at done
    | stageLocal | persistLocal | send | land | receive | crash | wait => exact ih done

theorem crash_preserves_operation (s : State) :
    s.crash.journal = s.journal ∧ s.crash.retained = s.retained ∧
    s.crash.stored = s.stored ∧ s.crash.sent = s.sent ∧ s.crash.memory = .empty :=
  ⟨rfl, rfl, rfl, rfl, rfl⟩

theorem publication_retries_identical {codec : Codec} {kept : Kept} {id : Nat} {d : Data}
    {s t : State} (hs : Reachable codec kept id d s) (ht : Reachable codec kept id d t)
    {n : Nat} {a b : Object} (ha : (n, a) ∈ s.sent) (hb : (n, b) ∈ t.sent) :
    a = b := by
  have sa := ((reachable_valid hs).requests n a ha).1
  have sb := ((reachable_valid ht).requests n b hb).1
  rw [(fixed_data hs).1] at sa
  rw [(fixed_data ht).1] at sb
  exact Option.some.inj (sa.symm.trans sb)

theorem failed_snapshot_check_blocks_publication {codec : Codec} {kept : Kept} {id : Nat}
    {snapshot : Snapshot} {s : State} (h : Reachable codec kept id (.snapshot snapshot) s)
    (failed : codec.openSnapshot snapshot.key snapshot.sealed ≠ some snapshot.captured) :
    s.stored (.snapshot snapshot.number) = none := by
  cases stored : s.stored (.snapshot snapshot.number) with
  | none => rfl
  | some bytes =>
    have checked := snapshot_stored_checked h stored
    exact False.elim (failed (checked.1 ▸ checked.2))

end CovenQueue.Operations
