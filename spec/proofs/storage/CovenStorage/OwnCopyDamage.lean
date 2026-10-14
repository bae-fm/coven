import CovenStorage.Objects

/-! §10, §11: a key-copy number this device reserved, `<store>/keys/<device>/<n>`,
holds an object whose clear prefix does not decode. T182's scan resets the
device with `SlotMismatch` on it, reasoning that this installation seals only
decodable copies, so the object must be another live copy's. Who can have
put such an object there, and does the reset help?

The model runs several devices' database copies (a backup, the database a
restore overwrites, a second live clone) over one store's key-copy logs, and
lets the provider corrupt any stored object, which §2 assumes it does not.
Each copy reserves its next number with fixed sealed bytes, publishes it
create-once and compares an occupied path (§10, §11), and reads every
writer's copy log, keeping the bytes it first read (`discovery::scan`'s
retained input). Its first phase is complete only when every copy it holds
decodes (`read_copies`, SP step 2). One counter supplies sealing randomness
and fresh device ids. -/

namespace CovenStorage.OwnCopyDamage

/-- What a key-copy path holds: a sealed copy whose clear prefix decodes,
with its sealing randomness (D9 draws a fresh ephemeral key), or bytes whose
prefix does not decode and that ask for no update (D1). -/
inductive Content where
  | sealed (randomness : Nat)
  | garbage
  deriving DecidableEq, Repr

def Content.decodes : Content → Bool
  | .sealed _ => true
  | .garbage => false

/-- A stored copy. `writer` is the device id of the copy that published it and
`damaged` says the provider changed its bytes: model bookkeeping only. -/
structure Stored where
  content : Content
  writer : Nat
  damaged : Bool
  deriving DecidableEq, Repr

/-- `<store>/keys/<writer>/<n>` as (writer, n). -/
abbrev Key := Nat × Nat

/-- One copy of a device's database: its device id (custody), the greatest
copy number it reserved (`key_upload::last_reserved`), the reserved copy it
has not confirmed yet with its fixed bytes, its unsent app edits, and the
copies it has read, by path, with the bytes first read. -/
structure Copy where
  device : Nat
  reserved : Nat
  outstanding : Option (Nat × Content)
  edits : Nat
  retained : List (Key × Stored)
  deriving DecidableEq, Repr

/-- Database copies in slots below `slots`; the stored copies and the paths
a listing returns, in publication order; the device ids that took §10's
reset. -/
structure World where
  copies : Nat → Option Copy
  slots : Nat
  storage : Key → Option Stored
  listed : List Key
  resets : List Nat
  next : Nat

/-- What a scan does on an own reserved number holding undecodable bytes:
reset as another live copy (T182), or report damage and wait like any other
unreadable copy. -/
inductive Rule where
  | resetOnUnreadable
  | waitOnUnreadable
  deriving DecidableEq, Repr

def World.put (w : World) (i : Nat) (c : Option Copy) : World :=
  { w with copies := fun k => if k = i then c else w.copies k }

def World.store (w : World) (p : Key) (o : Stored) : World :=
  { w with storage := fun q => if q = p then some o else w.storage q }

/-- §10: the reset discards the database, unsent edits included, and
installs a new device under a fresh id that has read nothing yet (§12.1). -/
def reset (w : World) (i old : Nat) : World :=
  { w with copies := fun k => if k = i then some ⟨w.next, 0, none, 0, []⟩ else w.copies k
           resets := old :: w.resets
           next := w.next + 1 }

/-- An app write: one more unsent edit. -/
def edit (w : World) (i : Nat) : World :=
  match w.copies i with
  | some c => w.put i (some { c with edits := c.edits + 1 })
  | none => w

/-- §11: reserve the next number with fixed sealed bytes, after the previous
one is settled. -/
def reserve (w : World) (i : Nat) : World :=
  match w.copies i with
  | some c =>
    match c.outstanding with
    | none =>
      { w.put i (some { c with reserved := c.reserved + 1,
                               outstanding := some (c.reserved + 1, .sealed w.next) }) with
        next := w.next + 1 }
    | some _ => w
  | none => w

/-- §4, §10: create-once at the copy's own path. On an occupied path equal
bytes confirm and different bytes reset this copy. -/
def publish (w : World) (i : Nat) : World :=
  match w.copies i with
  | some c =>
    match c.outstanding with
    | some (n, b) =>
      match w.storage (c.device, n) with
      | none =>
        { (w.put i (some { c with outstanding := none })).store (c.device, n) ⟨b, c.device, false⟩ with
          listed := w.listed ++ [(c.device, n)] }
      | some o =>
        if o.content = b then w.put i (some { c with outstanding := none })
        else reset w i c.device
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

/-- SP step 2: the copy observation is complete only when every copy's
clear prefix decodes. -/
def complete (c : Copy) : Bool := c.retained.all fun x => x.2.content.decodes

/-- T182's trigger: an own number within the reservations whose prefix does
not decode. -/
def ownUnreadable (c : Copy) : Bool :=
  c.retained.any fun x => x.1.1 == c.device && decide (x.1.2 ≤ c.reserved) && !x.2.content.decodes

def scan (rule : Rule) (w : World) (i : Nat) : World :=
  match w.copies i with
  | some c =>
    let read := { c with retained := view w c }
    match rule with
    | .resetOnUnreadable => if ownUnreadable read then reset w i c.device else w.put i (some read)
    | .waitOnUnreadable => w.put i (some read)
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
  | edit (i : Nat)
  | reserve (i : Nat)
  | publish (i : Nat)
  | scan (i : Nat)
  | damage (p : Key)
  | clone (i : Nat)
  | drop (i : Nat)
  deriving DecidableEq, Repr

/-- Steps that copy, restore or delete a database rather than run it. -/
def Step.copiesDatabase : Step → Bool
  | .clone _ | .drop _ => true
  | _ => false

def step (rule : Rule) (w : World) : Step → World
  | .edit i => edit w i
  | .reserve i => reserve w i
  | .publish i => publish w i
  | .scan i => scan rule w i
  | .damage p => damage w p
  | .clone i => clone w i
  | .drop i => drop w i

def run (rule : Rule) (w : World) (steps : List Step) : World :=
  steps.foldl (step rule) w

/-- Installations with nothing reserved, read or stored. -/
def start (devices : List Nat) (next : Nat) : World :=
  ⟨fun k => devices[k]?.map (⟨·, 0, none, 0, []⟩), devices.length, fun _ => none, [], [], next⟩

/-! ## Whatever occupies `keys/<d>/<n>` a copy holding `d` published; undecodable bytes are damage -/

/-- An object at `p` was published by a copy holding `p`'s writer id, and
either decodes or was damaged. -/
def Good (p : Key) (o : Stored) : Prop :=
  o.writer = p.1 ∧ (o.content.decodes = true ∨ o.damaged = true)

def CopyGood (c : Copy) : Prop :=
  (∀ x ∈ c.retained, Good x.1 x.2) ∧ ∀ n b, c.outstanding = some (n, b) → b.decodes = true

structure Inv (w : World) : Prop where
  stored : ∀ p o, w.storage p = some o → Good p o
  copies : ∀ k c, w.copies k = some c → CopyGood c

theorem inv_of {w w' : World} (h : Inv w) (hc : w'.copies = w.copies)
    (hs : w'.storage = w.storage) : Inv w' :=
  ⟨fun p o ho => h.stored p o (hs ▸ ho), fun k c hk => h.copies k c (hc ▸ hk)⟩

theorem inv_put {w : World} (h : Inv w) (i : Nat) (x : Option Copy)
    (hx : ∀ c, x = some c → CopyGood c) : Inv (w.put i x) := by
  refine ⟨h.stored, ?_⟩
  intro k c hk
  change (if k = i then x else w.copies k) = some c at hk
  split at hk
  · exact hx c hk
  · exact h.copies k c hk

theorem inv_store {w : World} (h : Inv w) (p : Key) (o : Stored) (ho : Good p o) :
    Inv (w.store p o) := by
  refine ⟨?_, h.copies⟩
  intro q x hq
  change (if q = p then some o else w.storage q) = some x at hq
  split at hq
  · rename_i he; subst he; cases hq; exact ho
  · exact h.stored q x hq

theorem inv_reset {w : World} (h : Inv w) (i old : Nat) : Inv (reset w i old) := by
  refine ⟨h.stored, ?_⟩
  intro k c hk
  change (if k = i then _ else w.copies k) = some c at hk
  split at hk
  · cases hk; exact ⟨by simp, by simp⟩
  · exact h.copies k c hk

theorem recall_mem : ∀ (l : List (Key × Stored)) (p : Key) (o : Stored),
    recall l p = some o → (p, o) ∈ l
  | [], _, _, h => by simp [recall] at h
  | (q, x) :: rest, p, o, h => by
    simp only [recall] at h
    split at h
    · rename_i he; subst he; cases h; exact List.mem_cons_self
    · exact List.mem_cons_of_mem _ (recall_mem rest p o h)

/-- A reading holds retained bytes or bytes read from storage. -/
theorem view_mem {w : World} {c : Copy} {x : Key × Stored} (hx : x ∈ view w c) :
    x ∈ c.retained ∨ w.storage x.1 = some x.2 := by
  simp only [view, List.mem_filterMap] at hx
  obtain ⟨p, -, hp⟩ := hx
  split at hp
  · rename_i o ho; cases hp; exact .inl (recall_mem _ _ _ ho)
  · cases hs : w.storage p with
    | none => simp [hs] at hp
    | some o => simp [hs] at hp; subst hp; exact .inr hs

theorem view_good {w : World} (h : Inv w) {c : Copy} (hc : CopyGood c) :
    ∀ x ∈ view w c, Good x.1 x.2 := by
  intro x hx
  rcases view_mem hx with r | s
  · exact hc.1 x r
  · exact h.stored x.1 x.2 s

theorem step_inv {w : World} (h : Inv w) (rule : Rule) (s : Step) : Inv (step rule w s) := by
  cases s with
  | edit i =>
    simp only [step, edit]; split
    · rename_i c hc
      exact inv_put h i _ (by intro y hy; cases hy; exact h.copies i c hc)
    · exact h
  | reserve i =>
    simp only [step, reserve]; split
    · rename_i c hc
      split
      · have good := h.copies i c hc
        exact inv_of (inv_put h i _ (by
          intro y hy; cases hy
          refine ⟨good.1, ?_⟩
          intro n b hb; simp at hb; obtain ⟨-, rfl⟩ := hb; rfl)) rfl rfl
      · exact h
    · exact h
  | publish i =>
    simp only [step, publish]; split
    · rename_i c hc
      have good := h.copies i c hc
      split
      · rename_i n b hb
        have put := inv_put h i (some { c with outstanding := none })
          (by intro y hy; cases hy; exact ⟨good.1, by simp⟩)
        split
        · exact inv_of (inv_store put (c.device, n) ⟨b, c.device, false⟩
            ⟨rfl, .inl (good.2 n b hb)⟩) rfl rfl
        · split
          · exact put
          · exact inv_reset h i c.device
      · exact h
    · exact h
  | scan i =>
    simp only [step, scan]; split
    · rename_i c hc
      have read : CopyGood { c with retained := view w c } :=
        ⟨view_good h (h.copies i c hc), (h.copies i c hc).2⟩
      cases rule with
      | resetOnUnreadable =>
        simp only
        split
        · exact inv_reset h i c.device
        · exact inv_put h i _ (by intro y hy; cases hy; exact read)
      | waitOnUnreadable => exact inv_put h i _ (by intro y hy; cases hy; exact read)
    · exact h
  | damage p =>
    simp only [step, damage]; split
    · rename_i o ho
      exact inv_store h p _ ⟨(h.stored p o ho).1, .inr rfl⟩
    · exact h
  | clone i =>
    simp only [step, clone]; split
    · rename_i c hc
      refine ⟨h.stored, ?_⟩
      intro k x hk
      change (if k = w.slots then some c else w.copies k) = some x at hk
      split at hk
      · cases hk; exact h.copies i c hc
      · exact h.copies k x hk
    · exact h
  | drop i =>
    exact inv_put h i none (by intro y hy; cases hy)

theorem run_inv {w : World} (h : Inv w) (rule : Rule) (steps : List Step) :
    Inv (run rule w steps) := by
  induction steps generalizing w with
  | nil => exact h
  | cons s ss ih => exact ih (step_inv h rule s)

theorem start_inv (devices : List Nat) (next : Nat) : Inv (start devices next) := by
  refine ⟨?_, ?_⟩
  · intro p o ho; simp [start] at ho
  · intro k c hk
    simp only [start, Option.map_eq_some_iff] at hk
    obtain ⟨_, -, rfl⟩ := hk
    exact ⟨by simp, by simp⟩

/-- In every history, under either rule and whatever the provider damages,
an object at `<store>/keys/<d>/<n>` was published by a copy holding device
id `d`: this database, a backup or a clone of it, never another device. It
decodes unless the provider damaged it. -/
theorem occupant_own_or_damaged (devices : List Nat) (next : Nat) (rule : Rule)
    (steps : List Step) (p : Key) (o : Stored)
    (ho : (run rule (start devices next) steps).storage p = some o) :
    o.writer = p.1 ∧ (o.content.decodes = true ∨ o.damaged = true) :=
  (run_inv (start_inv devices next) rule steps).stored p o ho

/-- Every copy of every database seals only decodable bytes, so an
undecodable object at any copy path, stored or as some copy read it, is
storage damage, never another copy's object. -/
theorem unreadable_is_damage (devices : List Nat) (next : Nat) (rule : Rule)
    (steps : List Step) :
    let w := run rule (start devices next) steps
    (∀ p o, w.storage p = some o → o.content.decodes = false → o.damaged = true) ∧
    (∀ k c, w.copies k = some c → ∀ x ∈ c.retained,
      x.2.content.decodes = false → x.2.damaged = true) := by
  intro w
  have h := run_inv (start_inv devices next) rule steps
  refine ⟨?_, ?_⟩
  · intro p o ho hd
    have g := (h.stored p o ho).2
    simp_all
  · intro k c hk x hx hd
    have g := ((h.copies k c hk).1 x hx).2
    simp_all

/-- So T182's reset fires only on damage: whenever its trigger holds for a
copy's reading, that copy read a damaged object at its own path. -/
theorem trigger_is_damage (devices : List Nat) (next : Nat) (rule : Rule)
    (steps : List Step) (i : Nat) (c : Copy)
    (hc : (run rule (start devices next) steps).copies i = some c)
    (trigger : ownUnreadable { c with retained := view (run rule (start devices next) steps) c } = true) :
    ∃ x ∈ view (run rule (start devices next) steps) c,
      x.1.1 = c.device ∧ x.1.2 ≤ c.reserved ∧ x.2.content.decodes = false ∧ x.2.damaged = true := by
  have h := run_inv (start_inv devices next) rule steps
  simp only [ownUnreadable, List.any_eq_true, Bool.and_eq_true, beq_iff_eq,
    decide_eq_true_eq, Bool.not_eq_true'] at trigger
  obtain ⟨x, hx, ⟨hd, hn⟩, hu⟩ := trigger
  have g := (view_good h (h.copies i c hc) x hx).2
  refine ⟨x, hx, hd, hn, hu, ?_⟩
  simp_all

/-! ## Resetting does not complete the first phase -/

/-- A copy that has read nothing finds every listed object in storage: one
undecodable object leaves its first phase incomplete. -/
theorem fresh_reading_incomplete (w : World) (c : Copy) (hc : c.retained = [])
    (p : Key) (o : Stored) (hp : p ∈ w.listed) (ho : w.storage p = some o)
    (hd : o.content.decodes = false) :
    complete { c with retained := view w c } = false := by
  have mem : (p, o) ∈ view w c := by
    simp only [view, List.mem_filterMap]
    exact ⟨p, hp, by simp [hc, recall, ho]⟩
  simp only [complete, List.all_eq_false]
  exact ⟨(p, o), mem, by simp [hd]⟩

/-- The reset changes no stored object, and the new device has read nothing.
Its first scan, under either rule, either resets it again or leaves its
first phase incomplete: with undecodable bytes in storage, resetting never
yields a complete phase. -/
theorem reset_never_completes (rule : Rule) (w : World) (i old : Nat)
    (p : Key) (o : Stored) (hp : p ∈ w.listed) (ho : w.storage p = some o)
    (hd : o.content.decodes = false) :
    scan rule (reset w i old) i = reset (reset w i old) i w.next ∨
      ∃ c, (scan rule (reset w i old) i).copies i = some c ∧ complete c = false := by
  have fresh : (reset w i old).copies i = some ⟨w.next, 0, none, 0, []⟩ := by simp [reset]
  have incomplete := fresh_reading_incomplete (reset w i old) ⟨w.next, 0, none, 0, []⟩ rfl p o hp ho hd
  cases rule with
  | resetOnUnreadable =>
    simp only [scan, fresh]
    split
    · exact .inl rfl
    · exact .inr ⟨_, by simp [World.put], incomplete⟩
  | waitOnUnreadable =>
    simp only [scan, fresh]
    exact .inr ⟨_, by simp [World.put], incomplete⟩

/-! ## Waiting never resets a database that is never copied -/

structure Settled (w : World) : Prop where
  distinct : ∀ k k' c c', w.copies k = some c → w.copies k' = some c' →
    c.device = c'.device → k = k'
  ahead : ∀ k c m, w.copies k = some c → c.reserved < m → w.storage (c.device, m) = none
  pending : ∀ k c n b, w.copies k = some c → c.outstanding = some (n, b) →
    n = c.reserved ∧ w.storage (c.device, n) = none

/-- Replacing copy `i` by one with the same device, given its new reservation
facts, and storage changed only where the new facts allow. -/
theorem settled_update {w w' : World} (h : Settled w) {i : Nat} {c c' : Copy}
    (hc : w.copies i = some c) (hd : c'.device = c.device)
    (copies : w'.copies = (w.put i (some c')).copies)
    (others : ∀ q, q.1 ≠ c.device → w'.storage q = w.storage q)
    (own_ahead : ∀ m, c'.reserved < m → w'.storage (c.device, m) = none)
    (own_pending : ∀ n b, c'.outstanding = some (n, b) →
      n = c'.reserved ∧ w'.storage (c.device, n) = none) :
    Settled w' := by
  have view : ∀ k x, w'.copies k = some x →
      (k = i ∧ x = c') ∨ (k ≠ i ∧ w.copies k = some x) := by
    intro k x hk
    rw [copies] at hk
    change (if k = i then some c' else w.copies k) = some x at hk
    split at hk
    · cases hk; exact .inl ⟨by assumption, rfl⟩
    · exact .inr ⟨by assumption, hk⟩
  have other_device : ∀ k x, k ≠ i → w.copies k = some x → x.device ≠ c.device := by
    intro k x hk hx he
    exact hk (h.distinct k i x c hx hc he)
  refine ⟨?_, ?_, ?_⟩
  · intro k k' x x' hk hk' he
    rcases view k x hk with ⟨rfl, rfl⟩ | ⟨ki, hx⟩ <;>
      rcases view k' x' hk' with ⟨rfl, rfl⟩ | ⟨ki', hx'⟩
    · rfl
    · exact absurd (hd ▸ he).symm (other_device k' x' ki' hx')
    · exact absurd (hd ▸ he) (other_device k x ki hx)
    · exact h.distinct k k' x x' hx hx' he
  · intro k x m hk hm
    rcases view k x hk with ⟨rfl, rfl⟩ | ⟨ki, hx⟩
    · rw [hd]; exact own_ahead m hm
    · rw [others _ (other_device k x ki hx)]; exact h.ahead k x m hx hm
  · intro k x n b hk hn
    rcases view k x hk with ⟨rfl, rfl⟩ | ⟨ki, hx⟩
    · rw [hd]; exact own_pending n b hn
    · rw [others _ (other_device k x ki hx)]; exact h.pending k x n b hx hn

theorem settled_step {w : World} (h : Settled w) (s : Step) (hs : s.copiesDatabase = false) :
    Settled (step .waitOnUnreadable w s) ∧ (step .waitOnUnreadable w s).resets = w.resets := by
  cases s with
  | clone i => simp [Step.copiesDatabase] at hs
  | drop i => simp [Step.copiesDatabase] at hs
  | edit i =>
    simp only [step, edit]; split
    · rename_i c hc
      exact ⟨settled_update (c' := { c with edits := c.edits + 1 }) h hc rfl rfl (fun _ _ => rfl)
        (fun m hm => h.ahead i c m hc hm) (fun n b hn => h.pending i c n b hc hn), rfl⟩
    · exact ⟨h, rfl⟩
  | scan i =>
    simp only [step, scan]; split
    · rename_i c hc
      exact ⟨settled_update (c' := { c with retained := view w c }) h hc rfl rfl (fun _ _ => rfl)
        (fun m hm => h.ahead i c m hc hm) (fun n b hn => h.pending i c n b hc hn), rfl⟩
    · exact ⟨h, rfl⟩
  | reserve i =>
    simp only [step, reserve]; split
    · rename_i c hc
      split
      · refine ⟨settled_update (c' := ⟨c.device, c.reserved + 1,
            some (c.reserved + 1, .sealed w.next), c.edits, c.retained⟩) h hc rfl rfl
            (fun _ _ => rfl) ?_ ?_, rfl⟩
        · intro m hm; exact h.ahead i c m hc (by simp at hm; omega)
        · intro n b hn
          simp at hn; obtain ⟨rfl, -⟩ := hn
          exact ⟨rfl, h.ahead i c _ hc (by omega)⟩
      · exact ⟨h, rfl⟩
    · exact ⟨h, rfl⟩
  | publish i =>
    simp only [step, publish]; split
    · rename_i c hc
      split
      · rename_i n b hb
        obtain ⟨hn, empty⟩ := h.pending i c n b hc hb
        split
        · refine ⟨settled_update (c' := { c with outstanding := none }) h hc rfl rfl ?_ ?_ ?_, rfl⟩
          · intro q hq
            change (if q = (c.device, n) then _ else w.storage q) = w.storage q
            have : q ≠ (c.device, n) := fun he => hq (by rw [he])
            simp only [this, ↓reduceIte]
          · intro m hm
            change c.reserved < m at hm
            change (if (c.device, m) = (c.device, n) then _ else w.storage (c.device, m)) = none
            have : (c.device, m) ≠ (c.device, n) := by simp; omega
            simp only [this, ↓reduceIte]; exact h.ahead i c m hc hm
          · intro n' b' hn'; simp at hn'
        · rename_i o ho; rw [empty] at ho; cases ho
      · exact ⟨h, rfl⟩
    · exact ⟨h, rfl⟩
  | damage p =>
    simp only [step, damage]; split
    · rename_i o ho
      have keep : ∀ q, w.storage q = none →
          (w.store p { o with content := .garbage, damaged := true }).storage q = none := by
        intro q hq
        change (if q = p then _ else w.storage q) = none
        have : q ≠ p := by intro he; subst he; rw [ho] at hq; cases hq
        simp only [this, ↓reduceIte]; exact hq
      refine ⟨⟨h.distinct, ?_, ?_⟩, rfl⟩
      · intro k c m hk hm; exact keep _ (h.ahead k c m hk hm)
      · intro k c n b hk hn
        obtain ⟨e, s⟩ := h.pending k c n b hk hn
        exact ⟨e, keep _ s⟩
    · exact ⟨h, rfl⟩

/-- Under the waiting rule, a database that is never backed up, restored or
cloned never resets, whatever the provider damages. -/
theorem uncopied_waiting_never_resets {w : World} (h : Settled w) (steps : List Step)
    (hs : ∀ s ∈ steps, s.copiesDatabase = false) :
    Settled (run .waitOnUnreadable w steps) ∧ (run .waitOnUnreadable w steps).resets = w.resets := by
  induction steps generalizing w with
  | nil => exact ⟨h, rfl⟩
  | cons s ss ih =>
    obtain ⟨h', hr⟩ := settled_step h s (hs s List.mem_cons_self)
    obtain ⟨h'', hr'⟩ := ih h' (fun x hx => hs x (List.mem_cons_of_mem _ hx))
    exact ⟨h'', hr'.trans hr⟩

theorem start_settled (devices : List Nat) (next : Nat) (nodup : devices.Nodup) :
    Settled (start devices next) := by
  refine ⟨?_, ?_, ?_⟩
  · intro k k' c c' hk hk' he
    simp only [start, Option.map_eq_some_iff] at hk hk'
    obtain ⟨d, hd, rfl⟩ := hk
    obtain ⟨d', hd', rfl⟩ := hk'
    simp only at he; subst he
    exact (nodup.getElem?_inj (List.getElem?_eq_some_iff.mp hd).1).mp (hd.trans hd'.symm)
  · intro k c m _ _; rfl
  · intro k c n b hk hn
    simp only [start, Option.map_eq_some_iff] at hk
    obtain ⟨_, -, rfl⟩ := hk; cases hn

end CovenStorage.OwnCopyDamage
