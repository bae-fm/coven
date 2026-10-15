import CovenStorage.Objects

/-! §11, §13: replacing a key copy the provider damaged (T206).

T184 (`OwnCopyDamage`) proved that an undecodable object at
`<store>/keys/<writer>/<n>` is always provider damage, and that it leaves
every reading device's first phase incomplete for as long as it stays
listed: create-once forbids rewriting the path. The rule modeled here: the
device that published the copy, on reading its own copy as undecodable,
reseals the same key to the same recipient under its next number and
publishes a store-log entry in its own log naming the damaged number and the
replacement number. A reader skips an undecodable copy that such a kept
entry of the copy's writer marks replaced.

The model extends T184's: several copies of devices' databases (a backup,
the database a restore overwrites, a second live clone), the provider
corrupting any stored object, create-once publication with §10's
comparison of an occupied path, and readings that keep the bytes first read.
A copy now carries routing facts (audience, key id, recipient and exact
sealing key, D9), and each database remembers the facts of the copies it
confirmed, since only those let it reseal what it published. The entry names
numbers only: its writer is the device whose log holds it, so no device can
mark another writer's copy. -/

namespace CovenStorage.CopyReplacement

/-- What a key-copy path holds: a sealed copy with its sealing randomness and
its routing facts, or bytes whose prefix does not decode. -/
inductive Content where
  | sealed (randomness facts : Nat)
  | garbage
  deriving DecidableEq, Repr

def Content.decodes : Content → Bool
  | .sealed _ _ => true
  | .garbage => false

/-- A stored copy. `writer`, `facts` (the routing facts it was sealed with)
and `damaged` are model bookkeeping; damage keeps them. -/
structure Stored where
  content : Content
  writer : Nat
  facts : Nat
  damaged : Bool
  deriving DecidableEq, Repr

/-- `<store>/keys/<writer>/<n>` as (writer, n). -/
abbrev Key := Nat × Nat

/-- A published replacement entry: the device whose log holds it, the damaged
number and the replacement number. -/
structure Mark where
  writer : Nat
  damaged : Nat
  replacement : Nat
  deriving DecidableEq, Repr

/-- A replacement reserved together with its entry: the entry publishes only
once its copy is confirmed. -/
structure Pending where
  damaged : Nat
  replacement : Nat
  facts : Nat
  deriving DecidableEq, Repr

/-- One copy of a device's database: its device id, the greatest copy number
it reserved, the reserved copy it has not confirmed (number, randomness,
facts), the confirmed copies with their facts, its queued replacement entry,
and the copies it has read, by path, with the bytes first read. -/
structure Copy where
  device : Nat
  reserved : Nat
  outstanding : Option (Nat × Nat × Nat)
  published : List (Nat × Nat)
  pending : Option Pending
  retained : List (Key × Stored)
  deriving DecidableEq, Repr

/-- Database copies in slots below `slots`; stored copies and the paths a
listing returns; the published replacement entries; device ids that reset. -/
structure World where
  copies : Nat → Option Copy
  slots : Nat
  storage : Key → Option Stored
  listed : List Key
  marks : List Mark
  resets : List Nat
  next : Nat

def World.put (w : World) (i : Nat) (c : Option Copy) : World :=
  { w with copies := fun k => if k = i then c else w.copies k }

def World.store (w : World) (p : Key) (o : Stored) : World :=
  { w with storage := fun q => if q = p then some o else w.storage q }

def fresh (device : Nat) : Copy := ⟨device, 0, none, [], none, []⟩

/-- §10: the reset discards the database and installs a new device id. -/
def reset (w : World) (i old : Nat) : World :=
  { w with copies := fun k => if k = i then some (fresh w.next) else w.copies k
           resets := old :: w.resets
           next := w.next + 1 }

/-- §11: reserve the next number with fixed sealed bytes for some routing
facts, after the previous one is settled. -/
def reserve (w : World) (i facts : Nat) : World :=
  match w.copies i with
  | some c =>
    match c.outstanding with
    | none =>
      { w.put i (some { c with reserved := c.reserved + 1,
                               outstanding := some (c.reserved + 1, w.next, facts) }) with
        next := w.next + 1 }
    | some _ => w
  | none => w

/-- §4, §10: create-once at the copy's own path. Equal bytes on an occupied
path confirm; different bytes reset this copy. A confirmed copy's facts are
remembered. -/
def publish (w : World) (i : Nat) : World :=
  match w.copies i with
  | some c =>
    match c.outstanding with
    | some (n, r, f) =>
      let confirmed := w.put i (some { c with outstanding := none,
                                              published := (n, f) :: c.published })
      match w.storage (c.device, n) with
      | none =>
        { confirmed.store (c.device, n) ⟨.sealed r f, c.device, f, false⟩ with
          listed := w.listed ++ [(c.device, n)] }
      | some o => if o.content = .sealed r f then confirmed else reset w i c.device
    | none => w
  | none => w

/-- The provider changes a stored object's bytes; §2 assumes it does not. -/
def damage (w : World) (p : Key) : World :=
  match w.storage p with
  | some o => w.store p { o with content := .garbage, damaged := true }
  | none => w

def recall : List (Key × Stored) → Key → Option Stored
  | [], _ => none
  | (q, o) :: rest, p => if q = p then some o else recall rest p

/-- A reading of every writer's copy log: a path already read keeps its
retained bytes, any other listed path is read from storage. -/
def view (w : World) (c : Copy) : List (Key × Stored) :=
  w.listed.filterMap fun p =>
    match recall c.retained p with
    | some o => some (p, o)
    | none => (w.storage p).map (p, ·)

/-- Some published entry of `p`'s writer marks `p` replaced. -/
def marked (marks : List Mark) (p : Key) : Bool :=
  marks.any fun m => m.writer == p.1 && m.damaged == p.2

def factsOf (published : List (Nat × Nat)) (n : Nat) : Option Nat :=
  (published.find? fun x => x.1 == n).map (·.2)

/-- This database confirmed copy `p` itself and recorded its facts. -/
def recorded (c : Copy) (p : Key) : Bool :=
  p.1 == c.device && (factsOf c.published p.2).isSome

/-- SP step 2 with replacement: the copy observation is complete when every
copy read decodes, its writer's entry marks it replaced, or it is this
database's own confirmed copy, whose facts it recorded and stands in for. -/
def complete (marks : List Mark) (c : Copy) : Bool :=
  c.retained.all fun x => x.2.content.decodes || marked marks x.1 || recorded c x.1

def scan (w : World) (i : Nat) : World :=
  match w.copies i with
  | some c => w.put i (some { c with retained := view w c })
  | none => w

/-- This copy read its own number `n` as undecodable. -/
def unreadableAt (c : Copy) (n : Nat) : Bool :=
  c.retained.any fun x => x.1 == (c.device, n) && !x.2.content.decodes

/-- The publisher, having read its own confirmed copy `n` as undecodable and
seeing no entry marking it, reserves its next number with the same facts
and fresh randomness, and queues the entry naming both numbers. Like every
send it needs sending authority: its own last reading is complete (§11). -/
def replace (w : World) (i n : Nat) : World :=
  match w.copies i with
  | some c =>
    match c.outstanding, c.pending, factsOf c.published n with
    | none, none, some f =>
      if unreadableAt c n && !marked w.marks (c.device, n) && complete w.marks c then
        { w.put i (some { c with reserved := c.reserved + 1,
                                 outstanding := some (c.reserved + 1, w.next, f),
                                 pending := some ⟨n, c.reserved + 1, f⟩ }) with
          next := w.next + 1 }
      else w
    | _, _, _ => w
  | none => w

/-- The queued entry publishes once its replacement copy is confirmed, with
sending authority. -/
def announce (w : World) (i : Nat) : World :=
  match w.copies i with
  | some c =>
    match c.pending with
    | some p =>
      if (p.replacement, p.facts) ∈ c.published ∧ complete w.marks c then
        { w.put i (some { c with pending := none }) with
          marks := w.marks ++ [⟨c.device, p.damaged, p.replacement⟩] }
      else w
    | none => w
  | none => w

/-- A backup or a second live copy of one database. -/
def clone (w : World) (i : Nat) : World :=
  match w.copies i with
  | some c => { w with copies := fun k => if k = w.slots then some c else w.copies k
                       slots := w.slots + 1 }
  | none => w

/-- The database a restore overwrites. -/
def drop (w : World) (i : Nat) : World := w.put i none

inductive Step where
  | reserve (i facts : Nat)
  | publish (i : Nat)
  | scan (i : Nat)
  | damage (p : Key)
  | replace (i n : Nat)
  | announce (i : Nat)
  | clone (i : Nat)
  | drop (i : Nat)
  deriving DecidableEq, Repr

def step (w : World) : Step → World
  | .reserve i f => reserve w i f
  | .publish i => publish w i
  | .scan i => scan w i
  | .damage p => damage w p
  | .replace i n => replace w i n
  | .announce i => announce w i
  | .clone i => clone w i
  | .drop i => drop w i

def run (w : World) (steps : List Step) : World := steps.foldl step w

/-- Installations with nothing reserved, read or stored. -/
def start (devices : List Nat) (next : Nat) : World :=
  ⟨fun k => devices[k]?.map fresh, devices.length, fun _ => none, [], [], [], next⟩

/-! ## Invariants -/

/-- Sealed bytes carry the facts recorded with them; undecodable bytes are damage. -/
def Consistent (o : Stored) : Prop :=
  (∀ r f, o.content = .sealed r f → o.facts = f) ∧ (o.content = .garbage → o.damaged = true)

/-- What a stored object can become at its path: damage changes only its bytes. -/
def Later (o o' : Stored) : Prop :=
  o'.writer = o.writer ∧ o'.facts = o.facts ∧ (o.content = .garbage → o'.content = .garbage)

theorem Later.refl (o : Stored) : Later o o := ⟨rfl, rfl, id⟩

structure Grows (w w' : World) : Prop where
  storage : ∀ p o, w.storage p = some o → ∃ o', w'.storage p = some o' ∧ Later o o'
  marks : ∀ m ∈ w.marks, m ∈ w'.marks

/-- Retained bytes were read from storage and keep its facts; confirmed
copies are stored with their facts; a queued replacement names a confirmed
copy whose stored bytes no longer decode. -/
def CopyInv (w : World) (c : Copy) : Prop :=
  (∀ x ∈ c.retained, Consistent x.2 ∧ ∃ o, w.storage x.1 = some o ∧ o.facts = x.2.facts ∧
      (x.2.content = .garbage → o.content = .garbage)) ∧
  (∀ n f, (n, f) ∈ c.published → ∃ o, w.storage (c.device, n) = some o ∧ o.facts = f) ∧
  (∀ p, c.pending = some p → (p.damaged, p.facts) ∈ c.published ∧
      ∃ o, w.storage (c.device, p.damaged) = some o ∧ o.content = .garbage)

/-- A marked copy's bytes no longer decode, and its replacement, in the
same writer's log, carries the same facts. -/
def MarkInv (w : World) (m : Mark) : Prop :=
  ∃ o o', w.storage (m.writer, m.damaged) = some o ∧ o.content = .garbage ∧
    w.storage (m.writer, m.replacement) = some o' ∧ o'.writer = m.writer ∧ o'.facts = o.facts

structure Inv (w : World) : Prop where
  stored : ∀ p o, w.storage p = some o → o.writer = p.1 ∧ Consistent o
  listed : ∀ p o, w.storage p = some o → p ∈ w.listed
  copies : ∀ k c, w.copies k = some c → CopyInv w c
  marks : ∀ m ∈ w.marks, MarkInv w m

theorem copyInv_grows {w w' : World} (g : Grows w w') {c : Copy} (h : CopyInv w c) :
    CopyInv w' c := by
  obtain ⟨hr, hp, hq⟩ := h
  refine ⟨?_, ?_, ?_⟩
  · intro x hx
    obtain ⟨hc, o, ho, hf, hg⟩ := hr x hx
    obtain ⟨o', ho', hw, hf', hg'⟩ := g.storage _ _ ho
    exact ⟨hc, o', ho', hf'.trans hf, fun e => hg' (hg e)⟩
  · intro n f hn
    obtain ⟨o, ho, hf⟩ := hp n f hn
    obtain ⟨o', ho', -, hf', -⟩ := g.storage _ _ ho
    exact ⟨o', ho', hf'.trans hf⟩
  · intro p hpp
    obtain ⟨hm, o, ho, hg⟩ := hq p hpp
    obtain ⟨o', ho', -, -, hg'⟩ := g.storage _ _ ho
    exact ⟨hm, o', ho', hg' hg⟩

theorem markInv_grows {w w' : World} (g : Grows w w') {m : Mark} (h : MarkInv w m) :
    MarkInv w' m := by
  obtain ⟨o, o', ho, hg, ho', hw, hf⟩ := h
  obtain ⟨p, hp, -, hpf, hpg⟩ := g.storage _ _ ho
  obtain ⟨p', hp', hpw', hpf', -⟩ := g.storage _ _ ho'
  exact ⟨p, p', hp, hpg hg, hp', hpw'.trans hw, hpf'.trans (hf.trans hpf.symm)⟩

theorem grows_same {w w' : World} (hs : w'.storage = w.storage)
    (hm : ∀ m ∈ w.marks, m ∈ w'.marks) : Grows w w' :=
  ⟨fun _ o ho => ⟨o, hs ▸ ho, Later.refl o⟩, hm⟩

theorem grows_store {w w' : World} {p : Key} {o : Stored}
    (hs : w'.storage = fun q => if q = p then some o else w.storage q)
    (hp : w.storage p = none) (hm : ∀ m ∈ w.marks, m ∈ w'.marks) : Grows w w' := by
  refine ⟨?_, hm⟩
  intro q x hq
  refine ⟨x, ?_, Later.refl x⟩
  rw [hs]
  have : q ≠ p := by intro he; subst he; rw [hp] at hq; cases hq
  simp only [this, ↓reduceIte]; exact hq

/-- The common shape of a step: storage grows, and every copy, stored object
and mark is checked in the new world. -/
theorem inv_of_grows {w w' : World} (h : Inv w) (g : Grows w w')
    (stored : ∀ p o, w'.storage p = some o → o.writer = p.1 ∧ Consistent o)
    (listed : ∀ p o, w'.storage p = some o → p ∈ w'.listed)
    (copies : Grows w w' → ∀ k c, w'.copies k = some c → w.copies k = some c ∨ CopyInv w' c)
    (marks : ∀ m ∈ w'.marks, m ∈ w.marks ∨ MarkInv w' m) : Inv w' := by
  refine ⟨stored, listed, ?_, ?_⟩
  · intro k c hk
    rcases copies g k c hk with old | new
    · exact copyInv_grows g (h.copies k c old)
    · exact new
  · intro m hm
    rcases marks m hm with old | new
    · exact markInv_grows g (h.marks m old)
    · exact new

theorem put_copies {w : World} {i : Nat} {x : Option Copy} {k : Nat} {c : Copy}
    (hk : (w.put i x).copies k = some c) : w.copies k = some c ∨ x = some c := by
  change (if k = i then x else w.copies k) = some c at hk
  split at hk
  · exact .inr hk
  · exact .inl hk

theorem recall_mem : ∀ (l : List (Key × Stored)) (p : Key) (o : Stored),
    recall l p = some o → (p, o) ∈ l
  | [], _, _, h => by simp [recall] at h
  | (q, x) :: rest, p, o, h => by
    simp only [recall] at h
    split at h
    · rename_i he; subst he; cases h; exact List.mem_cons_self
    · exact List.mem_cons_of_mem _ (recall_mem rest p o h)

/-- A reading holds retained bytes or bytes read from storage at a listed path. -/
theorem view_mem {w : World} {c : Copy} {x : Key × Stored} (hx : x ∈ view w c) :
    x.1 ∈ w.listed ∧ (x ∈ c.retained ∨ w.storage x.1 = some x.2) := by
  simp only [view, List.mem_filterMap] at hx
  obtain ⟨p, hp, hx⟩ := hx
  split at hx
  · rename_i o ho; cases hx; exact ⟨hp, .inl (recall_mem _ _ _ ho)⟩
  · cases hs : w.storage p with
    | none => simp [hs] at hx
    | some o => simp [hs] at hx; subst hx; exact ⟨hp, .inr hs⟩

theorem unreadableAt_some {c : Copy} {n : Nat} (h : unreadableAt c n = true) :
    ∃ x ∈ c.retained, x.1 = (c.device, n) ∧ x.2.content = .garbage := by
  simp only [unreadableAt, List.any_eq_true, Bool.and_eq_true, beq_iff_eq,
    Bool.not_eq_true'] at h
  obtain ⟨x, hx, he, hd⟩ := h
  refine ⟨x, hx, he, ?_⟩
  cases hc : x.2.content with
  | sealed r f => rw [hc] at hd; cases hd
  | garbage => rfl

theorem factsOf_mem {l : List (Nat × Nat)} {n f : Nat} (h : factsOf l n = some f) :
    (n, f) ∈ l := by
  simp only [factsOf, Option.map_eq_some_iff] at h
  obtain ⟨x, hx, rfl⟩ := h
  have hm := List.mem_of_find?_eq_some hx
  have he := List.find?_some hx
  simp only [beq_iff_eq] at he
  rw [← he]
  exact hm

theorem step_inv {w : World} (h : Inv w) (s : Step) : Inv (step w s) := by
  cases s with
  | reserve i f =>
    simp only [step, reserve]
    split
    · rename_i c hc
      split
      · have old := h.copies i c hc
        refine inv_of_grows h (grows_same rfl (fun _ hm => hm)) h.stored h.listed ?_
          (fun m hm => .inl hm)
        intro _ k x hk
        rcases put_copies hk with o | e
        · exact .inl o
        · cases e; exact .inr ⟨old.1, old.2.1, old.2.2⟩
      · exact h
    · exact h
  | publish i =>
    simp only [step, publish]
    split
    · rename_i c hc
      have old := h.copies i c hc
      split
      · rename_i n r f hout
        split
        · rename_i hempty
          -- Stored at an empty path.
          refine inv_of_grows h (grows_store rfl hempty (fun _ hm => hm)) ?_ ?_ ?_
            (fun m hm => .inl hm)
          · intro p o ho
            change (if p = (c.device, n) then _ else w.storage p) = some o at ho
            split at ho
            · rename_i he; subst he; cases ho
              exact ⟨rfl, ⟨fun r' f' e => by cases e; rfl, fun e => by cases e⟩⟩
            · exact h.stored p o ho
          · intro p o ho
            change (if p = (c.device, n) then _ else w.storage p) = some o at ho
            change p ∈ w.listed ++ [(c.device, n)]
            split at ho
            · rename_i he; subst he; simp
            · exact List.mem_append_left _ (h.listed p o ho)
          · intro g k x hk
            rcases put_copies hk with o | e
            · exact .inl o
            · cases e
              have new := copyInv_grows g old
              refine .inr ⟨new.1, ?_, fun q hq =>
                ⟨List.mem_cons_of_mem _ (new.2.2 q hq).1, (new.2.2 q hq).2⟩⟩
              intro n' f' hn'
              rcases List.mem_cons.mp hn' with e | e
              · cases e
                refine ⟨⟨.sealed r f, c.device, f, false⟩, ?_, rfl⟩
                change (if ((c.device, n) : Key) = (c.device, n) then _ else _) = _
                simp
              · exact new.2.1 n' f' e
        · rename_i o ho
          split
          · rename_i heq
            refine inv_of_grows h (grows_same rfl (fun _ hm => hm)) h.stored h.listed ?_
              (fun m hm => .inl hm)
            intro _ k x hk
            rcases put_copies hk with o' | e
            · exact .inl o'
            · cases e
              refine .inr ⟨old.1, ?_, fun q hq =>
                ⟨List.mem_cons_of_mem _ (old.2.2 q hq).1, (old.2.2 q hq).2⟩⟩
              intro n' f' hn'
              rcases List.mem_cons.mp hn' with e | e
              · cases e
                exact ⟨o, ho, (h.stored _ o ho).2.1 r f heq⟩
              · exact old.2.1 n' f' e
          · refine inv_of_grows h (grows_same rfl (fun _ hm => hm)) h.stored h.listed ?_
              (fun m hm => .inl hm)
            intro _ k x hk
            change (if k = i then some (fresh w.next) else w.copies k) = some x at hk
            split at hk
            · cases hk
              exact .inr ⟨by simp [fresh], by simp [fresh], by simp [fresh]⟩
            · exact .inl hk
      · exact h
    · exact h
  | scan i =>
    simp only [step, scan]
    split
    · rename_i c hc
      have old := h.copies i c hc
      refine inv_of_grows h (grows_same rfl (fun _ hm => hm)) h.stored h.listed ?_
        (fun m hm => .inl hm)
      intro _ k x hk
      rcases put_copies hk with o | e
      · exact .inl o
      · cases e
        refine .inr ⟨?_, old.2.1, old.2.2⟩
        intro x hx
        rcases (view_mem hx).2 with r | s
        · exact old.1 x r
        · exact ⟨(h.stored _ _ s).2, x.2, s, rfl, id⟩
    · exact h
  | damage p =>
    simp only [step, damage]
    split
    · rename_i o ho
      have g : Grows w (w.store p { o with content := .garbage, damaged := true }) := by
        refine ⟨?_, fun _ hm => hm⟩
        intro q x hq
        change ∃ o', (if q = p then _ else w.storage q) = some o' ∧ _
        by_cases he : q = p
        · subst he; rw [ho] at hq; cases hq
          exact ⟨{ o with content := .garbage, damaged := true }, by simp, rfl, rfl, fun _ => rfl⟩
        · simp only [he, ↓reduceIte]; exact ⟨x, hq, Later.refl x⟩
      refine inv_of_grows h g ?_ ?_ (fun _ k c hk => .inl hk) (fun m hm => .inl hm)
      · intro q x hq
        change (if q = p then _ else w.storage q) = some x at hq
        split at hq
        · rename_i he; subst he; cases hq
          refine ⟨(h.stored _ o ho).1, ?_⟩
          unfold Consistent
          exact ⟨(fun _ _ e => nomatch e), fun _ => rfl⟩
        · exact h.stored q x hq
      · intro q x hq
        change (if q = p then _ else w.storage q) = some x at hq
        split at hq
        · rename_i he; subst he; exact h.listed _ o ho
        · exact h.listed q x hq
    · exact h
  | replace i n =>
    simp only [step, replace]
    split
    · rename_i c hc
      have old := h.copies i c hc
      split
      · rename_i f hf
        split
        · rename_i hg
          simp only [Bool.and_eq_true, Bool.not_eq_true'] at hg
          obtain ⟨x, hx, hxp, hxg⟩ := unreadableAt_some hg.1.1
          obtain ⟨-, o, ho, -, hog⟩ := old.1 x hx
          refine inv_of_grows h (grows_same rfl (fun _ hm => hm)) h.stored h.listed ?_
            (fun m hm => .inl hm)
          intro _ k y hk
          rcases put_copies hk with o' | e
          · exact .inl o'
          · cases e
            refine .inr ⟨old.1, old.2.1, ?_⟩
            intro q hq
            simp only [Option.some.injEq] at hq; subst hq
            exact ⟨factsOf_mem hf, o, hxp ▸ ho, hog hxg⟩
        · exact h
      · exact h
    · exact h
  | announce i =>
    simp only [step, announce]
    split
    · rename_i c hc
      have old := h.copies i c hc
      split
      · rename_i q hq
        split
        · rename_i hm
          refine inv_of_grows h (grows_same rfl (fun _ h => List.mem_append_left _ h))
            h.stored h.listed ?_ ?_
          · intro _ k y hk
            rcases put_copies hk with o' | e
            · exact .inl o'
            · cases e; exact .inr ⟨old.1, old.2.1, by simp⟩
          · intro m hmm
            rcases List.mem_append.mp hmm with e | e
            · exact .inl e
            · simp only [List.mem_singleton] at e; subst e
              refine .inr ?_
              obtain ⟨hd, o, ho, hog⟩ := old.2.2 q hq
              obtain ⟨o1, ho1, hf1⟩ := old.2.1 _ _ hd
              rw [ho] at ho1; cases ho1
              obtain ⟨o', ho', hf'⟩ := old.2.1 _ _ hm.1
              exact ⟨o, o', ho, hog, ho', (h.stored _ _ ho').1, hf'.trans hf1.symm⟩
        · exact h
      · exact h
    · exact h
  | clone i =>
    simp only [step, clone]
    split
    · rename_i c hc
      refine inv_of_grows h (grows_same rfl (fun _ hm => hm)) h.stored h.listed ?_
        (fun m hm => .inl hm)
      intro g k x hk
      change (if k = w.slots then some c else w.copies k) = some x at hk
      split at hk
      · cases hk; exact .inr (copyInv_grows g (h.copies i c hc))
      · exact .inl hk
    · exact h
  | drop i =>
    simp only [step, drop]
    refine inv_of_grows h (grows_same rfl (fun _ hm => hm)) h.stored h.listed ?_
      (fun m hm => .inl hm)
    intro _ k x hk
    rcases put_copies hk with o | e
    · exact .inl o
    · cases e

theorem run_inv {w : World} (h : Inv w) (steps : List Step) : Inv (run w steps) := by
  induction steps generalizing w with
  | nil => exact h
  | cons s ss ih => exact ih (step_inv h s)

theorem start_inv (devices : List Nat) (next : Nat) : Inv (start devices next) := by
  refine ⟨?_, ?_, ?_, ?_⟩
  · intro p o ho; simp [start] at ho
  · intro p o ho; simp [start] at ho
  · intro k c hk
    simp only [start, Option.map_eq_some_iff] at hk
    obtain ⟨_, -, rfl⟩ := hk
    exact ⟨by simp [fresh], by simp [fresh], by simp [fresh]⟩
  · intro m hm; simp [start] at hm

/-- Reachable from installations that have stored nothing. -/
def Reachable (w : World) : Prop := ∃ devices next steps, w = run (start devices next) steps

theorem reachable_inv {w : World} (h : Reachable w) : Inv w := by
  obtain ⟨devices, next, steps, rfl⟩ := h
  exact run_inv (start_inv devices next) steps

/-! ## Readers skip only damaged copies, and lose no routing facts -/

/-- In every history, a copy some published entry marks replaced holds
undecodable, damaged bytes, and the replacement its writer named holds a
copy by that writer with the same routing facts. So a reader that skips
the marked copy skips only damage, never a copy it needs: what the copy
told readers (its audience, key and recipient, which §11's exposure rule
reads) is at the replacement. -/
theorem marked_is_damage (w : World) (h : Reachable w) (m : Mark) (hm : m ∈ w.marks) :
    ∃ o o', w.storage (m.writer, m.damaged) = some o ∧ o.content.decodes = false ∧
      o.damaged = true ∧ w.storage (m.writer, m.replacement) = some o' ∧
      o'.writer = m.writer ∧ o'.facts = o.facts := by
  have hi := reachable_inv h
  obtain ⟨o, o', ho, hg, ho', hw, hf⟩ := hi.marks m hm
  exact ⟨o, o', ho, by rw [hg]; rfl, (hi.stored _ _ ho).2.2 hg, ho', hw, hf⟩

/-- A reader skips only bytes it read as undecodable, and those are damage
whose stored facts equal what it read; the replacement carries them. A
reader holding decodable bytes for a marked copy read them before the
damage and keeps them. -/
theorem skipped_is_damage (w : World) (h : Reachable w) (k : Nat) (c : Copy)
    (hc : w.copies k = some c) (x : Key × Stored) (hx : x ∈ c.retained)
    (hu : x.2.content.decodes = false) (hm : marked w.marks x.1 = true) :
    x.2.damaged = true ∧ ∃ m ∈ w.marks, m.writer = x.1.1 ∧ m.damaged = x.1.2 ∧
      ∃ o', w.storage (m.writer, m.replacement) = some o' ∧ o'.writer = x.1.1 ∧
        o'.facts = x.2.facts := by
  have hi := reachable_inv h
  obtain ⟨hcons, o, ho, hof, -⟩ := (hi.copies k c hc).1 x hx
  have garbage : x.2.content = .garbage := by
    cases e : x.2.content with
    | sealed r f => rw [e] at hu; cases hu
    | garbage => rfl
  refine ⟨hcons.2 garbage, ?_⟩
  simp only [marked, List.any_eq_true, Bool.and_eq_true, beq_iff_eq] at hm
  obtain ⟨m, hmm, hw, hd⟩ := hm
  obtain ⟨o1, o', ho1, -, ho', hw', hf'⟩ := hi.marks m hmm
  have : (m.writer, m.damaged) = x.1 := by rw [hw, hd]
  rw [this, ho] at ho1; cases ho1
  exact ⟨m, hmm, hw, hd, o', ho', hw'.trans hw, hf'.trans hof⟩

theorem factsOf_cons_isSome {l : List (Nat × Nat)} {x : Nat × Nat} {n : Nat}
    (h : (factsOf l n).isSome = true) : (factsOf (x :: l) n).isSome = true := by
  simp only [factsOf, List.find?_cons] at h ⊢
  split <;> simp_all

/-- Confirming more copies keeps a complete reading complete. -/
theorem complete_of_published {marks : List Mark} {c c' : Copy} (hd : c'.device = c.device)
    (hr : c'.retained = c.retained)
    (hp : ∀ n, (factsOf c.published n).isSome = true → (factsOf c'.published n).isSome = true)
    (h : complete marks c = true) : complete marks c' = true := by
  simp only [complete, List.all_eq_true, Bool.or_eq_true, hr] at h ⊢
  intro x hx
  rcases h x hx with a | r
  · exact .inl a
  · right
    simp only [recorded, Bool.and_eq_true, beq_iff_eq, hd] at r ⊢
    exact ⟨r.1, hp _ r.2⟩

/-! ## The publisher's own record stands in for its damaged copy -/

/-- What a database recorded for its own confirmed copy is what the copy
held: a reading of that path, decodable or not, has the recorded facts. So
the publisher's observation, with its record standing in for undecodable
bytes, is exactly the observation the undamaged copy would give. -/
theorem own_record_matches (w : World) (h : Reachable w) (k : Nat) (c : Copy)
    (hc : w.copies k = some c) (x : Key × Stored) (hx : x ∈ c.retained)
    (hd : x.1.1 = c.device) (f : Nat) (hf : factsOf c.published x.1.2 = some f) :
    x.2.facts = f := by
  have hi := reachable_inv h
  obtain ⟨-, o, ho, hof, -⟩ := (hi.copies k c hc).1 x hx
  obtain ⟨o', ho', hf'⟩ := (hi.copies k c hc).2.1 _ _ (factsOf_mem hf)
  have : x.1 = (c.device, x.1.2) := by rw [← hd]
  rw [this, ho'] at ho; cases ho
  exact hof.symm.trans hf'

/-! ## Readers complete once every damaged copy is marked or their own -/

/-- Once every listed undecodable object is marked by its writer's entry, or
is this database's own recorded copy, its next scan completes its first
phase, whatever bytes it retained: retained undecodable bytes stay
undecodable in storage. Every other reader needs only the marks; the
publisher completes before its replacement is published, so it can send it. -/
theorem complete_once_marked (w : World) (h : Reachable w) (i : Nat) (c : Copy)
    (hc : w.copies i = some c)
    (hall : ∀ p o, p ∈ w.listed → w.storage p = some o → o.content.decodes = false →
      marked w.marks p = true ∨ recorded c p = true) :
    ∃ c', (scan w i).copies i = some c' ∧ complete w.marks c' = true := by
  have hi := reachable_inv h
  refine ⟨{ c with retained := view w c }, by simp [scan, hc, World.put], ?_⟩
  simp only [complete, List.all_eq_true, Bool.or_eq_true]
  intro x hx
  have recorded_same : recorded { c with retained := view w c } x.1 = recorded c x.1 := rfl
  rw [recorded_same]
  cases hd : x.2.content.decodes
  · obtain ⟨hl, src⟩ := view_mem hx
    have either : marked w.marks x.1 = true ∨ recorded c x.1 = true := by
      rcases src with r | s
      · obtain ⟨-, o, ho, -, hg⟩ := (hi.copies i c hc).1 x r
        have garbage : x.2.content = .garbage := by
          cases e : x.2.content with
          | sealed r f => rw [e] at hd; cases hd
          | garbage => rfl
        exact hall x.1 o hl ho (by rw [hg garbage]; rfl)
      · exact hall x.1 x.2 hl s hd
    rcases either with m | r
    · exact .inl (.inr m)
    · exact .inr r
  · exact .inl (.inl rfl)

/-! ## The publisher marks the damage it reads -/

/-- A publisher that read its own confirmed copy `n` as undecodable, with
nothing reserved or queued and its next number free, marks it in three
steps: reserve the replacement with the entry, publish the copy, publish
the entry. -/
theorem publisher_marks (w : World) (i n f : Nat) (c : Copy) (hc : w.copies i = some c)
    (hout : c.outstanding = none) (hpend : c.pending = none)
    (hf : factsOf c.published n = some f) (hu : unreadableAt c n = true)
    (hnot : marked w.marks (c.device, n) = false) (hdone : complete w.marks c = true)
    (hfree : w.storage (c.device, c.reserved + 1) = none) :
    ⟨c.device, n, c.reserved + 1⟩ ∈
      (run w [.replace i n, .publish i, .announce i]).marks := by
  let after : Copy :=
    { c with reserved := c.reserved + 1, outstanding := none,
             published := (c.reserved + 1, f) :: c.published,
             pending := some ⟨n, c.reserved + 1, f⟩ }
  have more : complete w.marks after = true :=
    complete_of_published (c := c) (c' := after) rfl rfl (fun _ hm => factsOf_cons_isSome hm) hdone
  dsimp only [after] at more
  simp [run, step, replace, hc, hout, hpend, hf, hu, hnot, hdone, publish, World.put, hfree,
    announce, World.store, more]

end CovenStorage.CopyReplacement
