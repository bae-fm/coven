import CovenStorage.Objects

/-! §13's S3 key replacement as seen from the admin desktop's database:
journal an attempt id, make and check a key (§4), seal it to the member,
publish it at `<store>/access-keys/<member>/<attempt>` (D10), then record it in
a set-access entry, or discard it first. The question is §10's: is a delivery
path this device's own path, so that different stored bytes there mean another
copy of this database, and is resetting that copy safe? -/

namespace CovenStorage.KeyDelivery

/-- One replacement in a database's operation journal (§18). `key` is the
made key, whose secret the journal holds (§4); `sealed` is the delivery's
fixed bytes once recorded; `delivered` is step 2. -/
structure Journal where
  attempt : Nat
  key : Option Nat
  sealed : Option Bytes
  delivered : Bool
  deriving DecidableEq, Repr

/-- One copy of a device's database. A backup, the database it is restored
over, and a second live clone are all copies; `device` is the custody id. -/
structure Copy where
  device : Nat
  journal : Option Journal
  deriving DecidableEq, Repr

/-- A delivery as stored. `writer` is the device whose copy sent it: model
bookkeeping only, not a field of the object. -/
structure Stored where
  bytes : Bytes
  writer : Nat
  deriving DecidableEq, Repr

/-- A live key at the provider and the minting device its provider-side name
carries (T237). -/
structure ProviderKey where
  id : Nat
  device : Nat
  deriving DecidableEq, Repr

/-- `copies` holds database copies in slots below `slots`; `storage` maps an
attempt id to its delivery path's occupant; `recorded` lists keys in kept
set-access entries; `resets` lists devices that took §10's reset. One counter
supplies fresh attempt ids, key ids, sealing randomness and new device ids.
`pending` lists, per slot, the keys that database holds as pending
`DeleteAccessKey` (§13). -/
structure World where
  copies : Nat → Option Copy
  slots : Nat
  storage : Nat → Option Stored
  provider : List ProviderKey
  recorded : List Nat
  resets : List Nat
  next : Nat
  pending : Nat → List Nat

/-- The D9 delivery box draws a fresh ephemeral key and nonce (`random`, what
coven-format seals today). `bound` derives them from the sealed inputs, as D11
does for nonces elsewhere; it is here only to compare. -/
inductive Sealer where
  | random
  | bound
  deriving DecidableEq, Repr

def Sealer.seal : Sealer → Nat → Nat → Bytes
  | .random, key, randomness => [key, randomness]
  | .bound, key, _ => [key]

def World.put (w : World) (i : Nat) (c : Option Copy) : World :=
  { w with copies := fun k => if k = i then c else w.copies k }

/-- `replace_member_access_key` journals a fresh attempt first. -/
def begin (w : World) (i : Nat) : World :=
  match w.copies i with
  | some ⟨d, none⟩ =>
    { w with copies := fun k => if k = i then some ⟨d, some ⟨w.next, none, none, false⟩⟩ else w.copies k
             next := w.next + 1 }
  | _ => w

/-- §4: one mint call names its principal afresh, deletes nothing (T259), makes
the key and checks it from the member's side; only a checked key's secret
joins the journal (T269). A call that fails rolls back what its own names made
and journals nothing, so it is no step here. The key's provider-side name
carries the minting device (T237). -/
def mint (w : World) (i : Nat) : World :=
  match w.copies i with
  | some ⟨d, some ⟨a, none, s, v⟩⟩ =>
    { w with copies := fun k => if k = i then some ⟨d, some ⟨a, some w.next, s, v⟩⟩ else w.copies k
             provider := w.provider ++ [⟨w.next, d⟩]
             next := w.next + 1 }
  | _ => w

/-- Seal once; retries resend the journaled bytes. -/
def sealKey (sealer : Sealer) (w : World) (i : Nat) : World :=
  match w.copies i with
  | some ⟨d, some ⟨a, some k, none, v⟩⟩ =>
    { w with copies := fun x => if x = i then some ⟨d, some ⟨a, some k, some (sealer.seal k w.next), v⟩⟩
                               else w.copies x
             next := w.next + 1 }
  | _ => w

/-- §10 on an occupied path: equal complete bytes count as delivered;
different bytes reset this copy as a new device, which keeps the deletions
the old database owed but not the key it made (T267). A failed comparison
read changes nothing and is not a step here. -/
def deliver (w : World) (i : Nat) : World :=
  match w.copies i with
  | some ⟨d, some ⟨a, k, some b, false⟩⟩ =>
    match w.storage a with
    | none =>
      { w with copies := fun x => if x = i then some ⟨d, some ⟨a, k, some b, true⟩⟩ else w.copies x
               storage := fun x => if x = a then some ⟨b, d⟩ else w.storage x }
    | some o =>
      if o.bytes = b then w.put i (some ⟨d, some ⟨a, k, some b, true⟩⟩)
      else { w with copies := fun x => if x = i then some ⟨w.next, none⟩ else w.copies x
                    resets := d :: w.resets, next := w.next + 1 }
  | _ => w

/-- The set-access entry is kept; deleting the old key is outside this model. -/
def record (w : World) (i : Nat) : World :=
  match w.copies i with
  | some ⟨d, some ⟨_, some k, _, true⟩⟩ =>
    { w with copies := fun x => if x = i then some ⟨d, none⟩ else w.copies x
             recorded := k :: w.recorded }
  | _ => w

/-- A backup or a second live copy of one database, with its pending deletions. -/
def clone (w : World) (i : Nat) : World :=
  match w.copies i with
  | some c => { w with copies := fun k => if k = w.slots then some c else w.copies k
                       slots := w.slots + 1
                       pending := fun k => if k = w.slots then w.pending i else w.pending k }
  | none => w

/-- The database a restore overwrites, with its pending deletions. -/
def drop (w : World) (i : Nat) : World :=
  { w.put i none with pending := fun k => if k = i then [] else w.pending k }

/-- §13: discarding the replacement before its entry drops the journal and
makes nothing pending, since another live copy may still record the made key
(T269). §13's reclaim takes it once the device is retired. -/
def discard (w : World) (i : Nat) : World :=
  match w.copies i with
  | some ⟨d, some _⟩ => w.put i (some ⟨d, none⟩)
  | _ => w

inductive Step where
  | begin (i : Nat)
  | mint (i : Nat)
  | sealKey (i : Nat)
  | deliver (i : Nat)
  | record (i : Nat)
  | clone (i : Nat)
  | drop (i : Nat)
  | discard (i : Nat)
  deriving DecidableEq, Repr

/-- Steps after which a made key can be live with no surviving journal holding
it: copying, restoring or deleting a database, or discarding its operation. -/
def Step.strands : Step → Bool
  | .clone _ | .drop _ | .discard _ => true
  | _ => false

def step (sealer : Sealer) (w : World) : Step → World
  | .begin i => begin w i
  | .mint i => mint w i
  | .sealKey i => sealKey sealer w i
  | .deliver i => deliver w i
  | .record i => record w i
  | .clone i => clone w i
  | .drop i => drop w i
  | .discard i => discard w i

def run (sealer : Sealer) (w : World) (steps : List Step) : World :=
  steps.foldl (step sealer) w

/-- Installations with no operation in flight and an empty store. -/
def start (devices : List Nat) (next : Nat) : World :=
  ⟨fun k => devices[k]?.map (⟨·, none⟩), devices.length, fun _ => none, [], [], [], next,
    fun _ => []⟩

def att (c : Copy) : Option Nat := c.journal.map (·.attempt)

def heldKey (c : Option Copy) : Option Nat := (c.bind (·.journal)).bind (·.key)

/-- §13's "leaves no key unrecorded": every key live at the provider is in a
kept entry, in the journal of a surviving database, which can still record
it or make it pending `DeleteAccessKey`, or already pending deletion there. -/
def Accounted (w : World) : Prop :=
  ∀ p ∈ w.provider, p.id ∈ w.recorded ∨
    (∃ k, k < w.slots ∧ heldKey (w.copies k) = some p.id) ∨
    ∃ k, k < w.slots ∧ p.id ∈ w.pending k

instance (w : World) : Decidable (Accounted w) :=
  inferInstanceAs (Decidable (∀ p ∈ w.provider,
    p.id ∈ w.recorded ∨ (∃ k, k < w.slots ∧ heldKey (w.copies k) = some p.id) ∨
      ∃ k, k < w.slots ∧ p.id ∈ w.pending k))

/-! ### Every occupant of a delivery path came from a copy of its device -/

structure Inv (w : World) : Prop where
  bounded : ∀ k, w.slots ≤ k → w.copies k = none
  fresh : ∀ k c a, w.copies k = some c → att c = some a → a < w.next
  storedFresh : ∀ a o, w.storage a = some o → a < w.next
  own : ∀ k c a o, w.copies k = some c → att c = some a → w.storage a = some o →
    o.writer = c.device
  sameDevice : ∀ k k' c c' a, w.copies k = some c → w.copies k' = some c' →
    att c = some a → att c' = some a → c.device = c'.device

theorem put_copies (w : World) (i : Nat) (c : Option Copy) (k : Nat) :
    (w.put i c).copies k = if k = i then c else w.copies k := rfl

theorem inv_congr {w w' : World} (h : Inv w) (hc : w'.copies = w.copies)
    (hs : w'.slots = w.slots) (hst : w'.storage = w.storage) (hn : w.next ≤ w'.next) :
    Inv w' := by
  obtain ⟨b, f, sf, o, sd⟩ := h
  refine ⟨?_, ?_, ?_, ?_, ?_⟩
  · intro k hk; rw [hc]; exact b k (hs ▸ hk)
  · intro k c a hk ha; rw [hc] at hk; exact Nat.lt_of_lt_of_le (f k c a hk ha) hn
  · intro a x hx; rw [hst] at hx; exact Nat.lt_of_lt_of_le (sf a x hx) hn
  · intro k c a x hk ha hx; rw [hc] at hk; rw [hst] at hx; exact o k c a x hk ha hx
  · intro k k' c c' a hk hk'; rw [hc] at hk hk'; exact sd k k' c c' a hk hk'

theorem slot_bound {w : World} (h : Inv w) {i : Nat} {c : Copy} (hc : w.copies i = some c) :
    i < w.slots := by
  apply Nat.lt_of_not_le; intro hle; rw [h.bounded i hle] at hc; cases hc

/-- Replacing a copy by one with the same device and attempt. -/
theorem put_same {w : World} (h : Inv w) {i : Nat} {c c' : Copy} (hc : w.copies i = some c)
    (hd : c'.device = c.device) (ha : att c' = att c) : Inv (w.put i (some c')) := by
  have view : ∀ k x, (w.put i (some c')).copies k = some x →
      ∃ y, w.copies k = some y ∧ x.device = y.device ∧ att x = att y := by
    intro k x hk
    rw [put_copies] at hk
    split at hk
    · subst k; cases hk; exact ⟨c, hc, hd, ha⟩
    · exact ⟨x, hk, rfl, rfl⟩
  refine ⟨?_, ?_, h.storedFresh, ?_, ?_⟩
  · intro k hk
    have hk : w.slots ≤ k := hk
    have hi := slot_bound h hc
    rw [put_copies]; simp only [show ¬ k = i by omega, ↓reduceIte]
    exact h.bounded k hk
  · intro k x a hk hx
    obtain ⟨y, hy, -, hya⟩ := view k x hk
    exact h.fresh k y a hy (hya ▸ hx)
  · intro k x a o hk hx ho
    obtain ⟨y, hy, hyd, hya⟩ := view k x hk
    rw [hyd]; exact h.own k y a o hy (hya ▸ hx) ho
  · intro k k' x x' a hk hk' hx hx'
    obtain ⟨y, hy, hyd, hya⟩ := view k x hk
    obtain ⟨y', hy', hyd', hya'⟩ := view k' x' hk'
    rw [hyd, hyd']; exact h.sameDevice k k' y y' a hy hy' (hya ▸ hx) (hya' ▸ hx')

/-- Replacing a copy by one with no operation in flight, of any device. -/
theorem put_idle {w : World} (h : Inv w) {i : Nat} {c : Copy} (hc : w.copies i = some c)
    (x : Option Copy) (hx : ∀ y, x = some y → y.journal = none) : Inv (w.put i x) := by
  have view : ∀ k y, (w.put i x).copies k = some y → att y ≠ none →
      w.copies k = some y := by
    intro k y hk hy
    rw [put_copies] at hk
    split at hk
    · subst hk; simp [att, hx y rfl] at hy
    · exact hk
  refine ⟨?_, ?_, h.storedFresh, ?_, ?_⟩
  · intro k hk
    have hk : w.slots ≤ k := hk
    have hi := slot_bound h hc
    rw [put_copies]; simp only [show ¬ k = i by omega, ↓reduceIte]
    exact h.bounded k hk
  · intro k y a hk ha
    exact h.fresh k y a (view k y hk (by simp [ha])) ha
  · intro k y a o hk ha ho
    exact h.own k y a o (view k y hk (by simp [ha])) ha ho
  · intro k k' y y' a hk hk' ha ha'
    exact h.sameDevice k k' y y' a (view k y hk (by simp [ha])) (view k' y' hk' (by simp [ha']))
      ha ha'

theorem begin_inv {w : World} (h : Inv w) (i : Nat) : Inv (begin w i) := by
  unfold begin
  split
  · rename_i d hc
    have hi := slot_bound h hc
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro k hk
      show (if k = i then _ else _) = none
      have hk : w.slots ≤ k := hk
      simp only [show ¬ k = i by omega, ↓reduceIte]; exact h.bounded k hk
    · intro k c a hk ha
      change (if k = i then _ else _) = some c at hk
      split at hk
      · cases hk; simp [att] at ha; show a < w.next + 1; omega
      · have := h.fresh k c a hk ha; show a < w.next + 1; omega
    · intro a o ho
      have := h.storedFresh a o ho; show a < w.next + 1; omega
    · intro k c a o hk ha ho
      change (if k = i then _ else _) = some c at hk
      split at hk
      · cases hk; simp [att] at ha; subst ha
        have := h.storedFresh _ o ho; omega
      · exact h.own k c a o hk ha ho
    · intro k k' c c' a hk hk' ha ha'
      change (if k = i then _ else _) = some c at hk
      change (if k' = i then _ else _) = some c' at hk'
      split at hk <;> split at hk'
      · cases hk; cases hk'; rfl
      · cases hk; simp [att] at ha; subst ha
        have := h.fresh k' c' _ hk' ha'; omega
      · cases hk'; simp [att] at ha'; subst ha'
        have := h.fresh k c _ hk ha; omega
      · exact h.sameDevice k k' c c' a hk hk' ha ha'
  · exact h

theorem mint_inv {w : World} (h : Inv w) (i : Nat) : Inv (mint w i) := by
  unfold mint
  split
  · rename_i d a s v hc
    exact inv_congr (put_same (c' := ⟨d, some ⟨a, some w.next, s, v⟩⟩) h hc rfl rfl) rfl rfl rfl
      (Nat.le_succ _)
  · exact h

theorem sealKey_inv {w : World} (h : Inv w) (sealer : Sealer) (i : Nat) : Inv (sealKey sealer w i) := by
  unfold sealKey
  split
  · rename_i d a k v hc
    exact inv_congr
      (put_same (c' := ⟨d, some ⟨a, some k, some (sealer.seal k w.next), v⟩⟩) h hc rfl rfl)
      rfl rfl rfl (Nat.le_succ _)
  · exact h

theorem record_inv {w : World} (h : Inv w) (i : Nat) : Inv (record w i) := by
  unfold record
  split
  · rename_i d a k s hc
    exact inv_congr (put_idle h hc (some ⟨d, none⟩) (by intro y hy; cases hy; rfl)) rfl rfl rfl
      (Nat.le_refl _)
  · exact h

theorem drop_inv {w : World} (h : Inv w) (i : Nat) : Inv (drop w i) := by
  have put : Inv (w.put i none) := by
    cases hc : w.copies i with
    | none =>
      have : w.put i none = w := by
        cases w; simp only [World.put, World.mk.injEq, and_true]
        funext k; split <;> simp_all
      rw [this]; exact h
    | some c => exact put_idle h hc none (by intro y hy; cases hy)
  exact inv_congr put rfl rfl rfl (Nat.le_refl _)

theorem clone_inv {w : World} (h : Inv w) (i : Nat) : Inv (clone w i) := by
  unfold clone
  split
  · rename_i c hc
    have view : ∀ k x, (if k = w.slots then some c else w.copies k) = some x →
        ∃ k', w.copies k' = some x := by
      intro k x hk
      split at hk
      · cases hk; exact ⟨i, hc⟩
      · exact ⟨k, hk⟩
    refine ⟨?_, ?_, h.storedFresh, ?_, ?_⟩
    · intro k hk
      show (if k = w.slots then _ else _) = none
      have hk : w.slots + 1 ≤ k := hk
      simp only [show ¬ k = w.slots by omega, ↓reduceIte]; exact h.bounded k (by omega)
    · intro k x a hk ha
      obtain ⟨k', hk'⟩ := view k x hk; exact h.fresh k' x a hk' ha
    · intro k x a o hk ha ho
      obtain ⟨k', hk'⟩ := view k x hk; exact h.own k' x a o hk' ha ho
    · intro k k' x x' a hk hk' ha ha'
      obtain ⟨j, hj⟩ := view k x hk
      obtain ⟨j', hj'⟩ := view k' x' hk'
      exact h.sameDevice j j' x x' a hj hj' ha ha'
  · exact h

theorem deliver_inv {w : World} (h : Inv w) (i : Nat) : Inv (deliver w i) := by
  unfold deliver
  split
  · rename_i d a k b hc
    split
    · rename_i hs
      have base := put_same (c' := ⟨d, some ⟨a, k, some b, true⟩⟩) h hc rfl rfl
      have ha : a < w.next := h.fresh i _ a hc rfl
      refine ⟨base.bounded, base.fresh, ?_, ?_, base.sameDevice⟩
      · intro x o ho
        change (if x = a then _ else _) = some o at ho
        split at ho
        · subst x; exact ha
        · exact h.storedFresh x o ho
      · intro k' c' x o hk hx ho
        change (if x = a then _ else _) = some o at ho
        split at ho
        · subst x; cases ho
          exact base.sameDevice i k' ⟨d, some ⟨a, k, some b, true⟩⟩ c' a (by simp [World.put]) hk rfl hx
        · exact base.own k' c' x o hk hx ho
    · split
      · exact put_same h hc rfl rfl
      · exact inv_congr (put_idle h hc (some ⟨w.next, none⟩) (by intro y hy; cases hy; rfl))
          rfl rfl rfl (Nat.le_succ _)
  · exact h

theorem discard_inv {w : World} (h : Inv w) (i : Nat) : Inv (discard w i) := by
  unfold discard
  split
  · rename_i d _ hc
    exact put_idle h hc (some ⟨d, none⟩) (by intro y hy; cases hy; rfl)
  · exact h

theorem step_inv {w : World} (h : Inv w) (sealer : Sealer) (s : Step) : Inv (step sealer w s) := by
  cases s with
  | begin i => exact begin_inv h i
  | mint i => exact mint_inv h i
  | sealKey i => exact sealKey_inv h sealer i
  | deliver i => exact deliver_inv h i
  | record i => exact record_inv h i
  | clone i => exact clone_inv h i
  | drop i => exact drop_inv h i
  | discard i => exact discard_inv h i

theorem run_inv {w : World} (h : Inv w) (sealer : Sealer) (steps : List Step) :
    Inv (run sealer w steps) := by
  induction steps generalizing w with
  | nil => exact h
  | cons s ss ih => exact ih (step_inv h sealer s)

/-- No step of a database deletes a key at the provider: a mint call names its
principal afresh and deletes nothing (§4, T259), so a copy's mint cannot take
a key another copy of its database made and recorded. -/
theorem step_keeps_provider (sealer : Sealer) (w : World) (s : Step) :
    ∀ p ∈ w.provider, p ∈ (step sealer w s).provider := by
  intro p hp
  cases s with
  | mint i =>
    simp only [step]; unfold mint; split
    · exact List.mem_append_left _ hp
    · exact hp
  | begin i => simp only [step]; unfold begin; split <;> exact hp
  | sealKey i => simp only [step]; unfold sealKey; split <;> exact hp
  | deliver i =>
    simp only [step]; unfold deliver; split
    · split
      · exact hp
      · split <;> exact hp
    · exact hp
  | record i => simp only [step]; unfold record; split <;> exact hp
  | clone i => simp only [step]; unfold clone; split <;> exact hp
  | drop i => exact hp
  | discard i => simp only [step]; unfold discard; split <;> exact hp

theorem run_keeps_provider (sealer : Sealer) (w : World) (steps : List Step) :
    ∀ p ∈ w.provider, p ∈ (run sealer w steps).provider := by
  induction steps generalizing w with
  | nil => intro p hp; exact hp
  | cons s ss ih => intro p hp; exact ih _ p (step_keeps_provider sealer w s p hp)

theorem start_inv (devices : List Nat) (next : Nat) : Inv (start devices next) := by
  refine ⟨?_, ?_, ?_, ?_, ?_⟩
  · intro k hk; simp [start] at hk ⊢; omega
  · intro k c a hk ha
    simp only [start, Option.map_eq_some_iff] at hk
    obtain ⟨_, -, rfl⟩ := hk; simp [att] at ha
  · intro a o ho; simp [start] at ho
  · intro k c a o _ _ ho; simp [start] at ho
  · intro k k' c c' a hk _ ha
    simp only [start, Option.map_eq_some_iff] at hk
    obtain ⟨_, -, rfl⟩ := hk; simp [att] at ha

/-- The attempt id is fresh and journaled only by copies of the device that
drew it, and a copy publishes only at its own attempt. So in every history,
whatever occupies a delivery path was sent by the device whose database
journals that attempt: from a backup, a restored copy, or a clone of that
database, never by another device. This holds for both sealers. -/
theorem occupant_is_own_copy (devices : List Nat) (next : Nat) (sealer : Sealer)
    (steps : List Step) (k : Nat) (c : Copy) (j : Journal) (o : Stored)
    (hc : (run sealer (start devices next) steps).copies k = some c) (hj : c.journal = some j)
    (ho : (run sealer (start devices next) steps).storage j.attempt = some o) :
    o.writer = c.device :=
  (run_inv (start_inv devices next) sealer steps).own k c j.attempt o hc (by simp [att, hj]) ho

/-- So a delivery that resets its copy found bytes from another copy of the
same device: §10's stale-copy condition, as for writes and key copies. -/
theorem reset_is_own_copy (devices : List Nat) (next : Nat) (sealer : Sealer)
    (steps : List Step) (i : Nat) :
    let w := run sealer (start devices next) steps
    ∀ d a k b o, w.copies i = some ⟨d, some ⟨a, k, some b, false⟩⟩ →
      w.storage a = some o → o.bytes ≠ b →
      (deliver w i).resets = d :: w.resets ∧ o.writer = d := by
  intro w d a k b o hc ho hb
  refine ⟨?_, ?_⟩
  · simp [deliver, hc, ho, hb]
  · exact occupant_is_own_copy devices next sealer steps i _ ⟨a, k, some b, false⟩ o hc rfl ho

/-! ### A database that is never copied never resets and loses no key -/

structure Settled (w : World) : Prop extends Inv w where
  unique : ∀ k k' c c' a, w.copies k = some c → w.copies k' = some c' →
    att c = some a → att c' = some a → k = k'
  stored : ∀ k c j o, w.copies k = some c → c.journal = some j →
    w.storage j.attempt = some o → j.sealed = some o.bytes
  accounted : Accounted w

theorem held_put (w : World) (i k : Nat) (x : Option Copy) :
    heldKey ((w.put i x).copies k) = if k = i then heldKey x else heldKey (w.copies k) := by
  rw [put_copies]; split <;> rfl

/-- Steps that leave copy `i`'s attempt and key alone keep keys accounted. -/
theorem accounted_put_same {w : World} (ha : Accounted w) {i : Nat} {c c' : Copy}
    (hc : w.copies i = some c) (hk : heldKey (some c') = heldKey (some c)) :
    Accounted (w.put i (some c')) := by
  intro p hp
  rcases ha p hp with r | ⟨k, hk', held⟩ | pend
  · exact .inl r
  · refine .inr (.inl ⟨k, hk', ?_⟩)
    rw [held_put]; split
    · subst k; rw [hk, ← hc]; exact held
    · exact held
  · exact .inr (.inr pend)

/-- Replacing a copy by one with the same attempt leaves every other attempt alone. -/
theorem view_put {w : World} {i : Nat} {c c' : Copy} (hc : w.copies i = some c)
    (ha : att c' = att c) : ∀ k y, (w.put i (some c')).copies k = some y →
      ∃ z, w.copies k = some z ∧ att y = att z := by
  intro k y hk; rw [put_copies] at hk; split at hk
  · subst k; cases hk; exact ⟨_, hc, ha⟩
  · exact ⟨y, hk, rfl⟩

theorem unique_put {w : World} (h : Settled w) {i : Nat} {c c' : Copy}
    (hc : w.copies i = some c) (ha : att c' = att c) :
    ∀ k₁ k₂ x x' a, (w.put i (some c')).copies k₁ = some x →
      (w.put i (some c')).copies k₂ = some x' → att x = some a → att x' = some a → k₁ = k₂ := by
  intro k₁ k₂ x x' a hk hk' hx hx'
  obtain ⟨z, hz, hza⟩ := view_put hc ha k₁ x hk
  obtain ⟨z', hz', hza'⟩ := view_put hc ha k₂ x' hk'
  exact h.unique k₁ k₂ z z' a hz hz' (hza ▸ hx) (hza' ▸ hx')

theorem settled_step {w : World} (h : Settled w) (sealer : Sealer) (s : Step)
    (hs : s.strands = false) :
    Settled (step sealer w s) ∧ (step sealer w s).resets = w.resets := by
  have inv := step_inv h.toInv sealer s
  cases s with
  | clone i => simp [Step.strands] at hs
  | drop i => simp [Step.strands] at hs
  | discard i => simp [Step.strands] at hs
  | begin i =>
    simp only [step] at inv ⊢
    revert inv; unfold begin; split
    · rename_i d hc
      intro inv
      refine ⟨⟨inv, ?_, ?_, ?_⟩, rfl⟩
      · intro k k' c c' a hk hk' ha ha'
        change (if k = i then _ else _) = some c at hk
        change (if k' = i then _ else _) = some c' at hk'
        split at hk <;> split at hk'
        · omega
        · cases hk; simp [att] at ha; subst ha
          have := h.fresh k' c' _ hk' ha'; omega
        · cases hk'; simp [att] at ha'; subst ha'
          have := h.fresh k c _ hk ha; omega
        · exact h.unique k k' c c' a hk hk' ha ha'
      · intro k c j o hk hj ho
        change (if k = i then _ else _) = some c at hk
        split at hk
        · cases hk; cases hj; have := h.storedFresh _ o ho; simp at this
        · exact h.stored k c j o hk hj ho
      · intro p hp
        rcases h.accounted p hp with r | ⟨k, hk', held⟩ | pend
        · exact .inl r
        · refine .inr (.inl ⟨k, hk', ?_⟩)
          change heldKey (if k = i then _ else _) = _
          split
          · subst k; simp [heldKey, hc] at held
          · exact held
        · exact .inr (.inr pend)
    · intro _; exact ⟨h, rfl⟩
  | mint i =>
    simp only [step] at inv ⊢
    revert inv; unfold mint; split
    · rename_i d a s v hc
      intro inv
      refine ⟨⟨inv, unique_put h hc rfl, ?_, ?_⟩, rfl⟩
      · intro k c j o hk hj ho
        change (if k = i then _ else _) = some c at hk
        split at hk
        · subst k; cases hk; cases hj
          exact h.stored i _ ⟨a, none, s, v⟩ o hc rfl ho
        · exact h.stored k c j o hk hj ho
      · intro p hp
        have hi := slot_bound h.toInv hc
        simp only [List.mem_append, List.mem_singleton] at hp
        rcases hp with hp | rfl
        · rcases h.accounted p hp with r | ⟨k, hk', held⟩ | pend
          · exact .inl r
          · refine .inr (.inl ⟨k, hk', ?_⟩)
            change heldKey (if k = i then _ else _) = _
            split
            · subst k; simp [heldKey, hc] at held
            · exact held
          · exact .inr (.inr pend)
        · exact .inr (.inl ⟨i, hi, by simp [heldKey]⟩)
    · intro _; exact ⟨h, rfl⟩
  | sealKey i =>
    simp only [step] at inv ⊢
    revert inv; unfold sealKey; split
    · rename_i d a k v hc
      intro inv
      have none_stored : w.storage a = none := by
        cases ho : w.storage a with
        | none => rfl
        | some o => have := h.stored i _ ⟨a, some k, none, v⟩ o hc rfl ho; simp at this
      refine ⟨⟨inv, unique_put h hc rfl, ?_, ?_⟩, rfl⟩
      · intro k₁ c j o hk hj ho
        change (if k₁ = i then _ else _) = some c at hk
        split at hk
        · subst k₁; cases hk; cases hj; simp [none_stored] at ho
        · exact h.stored k₁ c j o hk hj ho
      · exact accounted_put_same h.accounted hc (by simp [heldKey])
    · intro _; exact ⟨h, rfl⟩
  | record i =>
    simp only [step] at inv ⊢
    revert inv; unfold record; split
    · rename_i d a k s hc
      intro inv
      refine ⟨⟨inv, ?_, ?_, ?_⟩, rfl⟩
      · intro k₁ k₂ c c' x hk hk' hx hx'
        change (if k₁ = i then _ else _) = some c at hk
        change (if k₂ = i then _ else _) = some c' at hk'
        split at hk
        · cases hk; simp [att] at hx
        · split at hk'
          · cases hk'; simp [att] at hx'
          · exact h.unique k₁ k₂ c c' x hk hk' hx hx'
      · intro k₁ c j o hk hj ho
        change (if k₁ = i then _ else _) = some c at hk
        split at hk
        · cases hk; cases hj
        · exact h.stored k₁ c j o hk hj ho
      · intro p hp
        rcases h.accounted p hp with r | ⟨k₁, hk', held⟩ | pend
        · exact .inl (List.mem_cons_of_mem _ r)
        · by_cases hki : k₁ = i
          · subst k₁; simp [heldKey, hc] at held; subst held; exact .inl List.mem_cons_self
          · refine .inr (.inl ⟨k₁, hk', ?_⟩)
            change heldKey (if k₁ = i then _ else _) = _
            simp only [hki, ↓reduceIte]; exact held
        · exact .inr (.inr pend)
    · intro _; exact ⟨h, rfl⟩
  | deliver i =>
    simp only [step] at inv ⊢
    revert inv; unfold deliver; split
    · rename_i d a k b hc
      have unique' := unique_put (c' := ⟨d, some ⟨a, k, some b, true⟩⟩) h hc rfl
      split
      · rename_i hs
        intro inv
        refine ⟨⟨inv, unique', ?_, ?_⟩, rfl⟩
        · intro k₁ c j o hk hj ho
          change (if k₁ = i then _ else _) = some c at hk
          change (if j.attempt = a then _ else _) = some o at ho
          split at hk
          · subst k₁; cases hk; cases hj; simp at ho; subst ho; rfl
          · rename_i hne
            split at ho
            · rename_i hja
              exact absurd (unique' k₁ i c ⟨d, some ⟨a, k, some b, true⟩⟩ a (by simp [World.put, hk, hne])
                (by simp [World.put]) (by simp [att, hj, hja]) rfl) hne
            · exact h.stored k₁ c j o hk hj ho
        · exact accounted_put_same h.accounted hc rfl
      · rename_i o hs
        have equal : o.bytes = b := by
          have := h.stored i _ ⟨a, k, some b, false⟩ o hc rfl hs; simp at this; exact this.symm
        simp only [equal, ↓reduceIte]
        intro inv
        refine ⟨⟨inv, unique', ?_, ?_⟩, rfl⟩
        · intro k₁ c j o' hk hj ho
          change (if k₁ = i then _ else _) = some c at hk
          split at hk
          · subst k₁; cases hk; cases hj; change w.storage a = some o' at ho; rw [hs] at ho; cases ho; simp [equal]
          · exact h.stored k₁ c j o' hk hj ho
        · exact accounted_put_same h.accounted hc rfl
    · intro _; exact ⟨h, rfl⟩

/-- An installation whose database is never backed up, restored or cloned
never takes a delivery reset, and while it also discards nothing, every key it
makes stays recorded or journaled. The reset fires
only when another copy of the database exists. -/
theorem uncopied_never_resets {w : World} (h : Settled w) (sealer : Sealer)
    (steps : List Step) (hs : ∀ s ∈ steps, s.strands = false) :
    Settled (run sealer w steps) ∧ (run sealer w steps).resets = w.resets := by
  induction steps generalizing w with
  | nil => exact ⟨h, rfl⟩
  | cons s ss ih =>
    obtain ⟨h', hr⟩ := settled_step h sealer s (hs s List.mem_cons_self)
    obtain ⟨h'', hr'⟩ := ih h' (fun x hx => hs x (List.mem_cons_of_mem _ hx))
    exact ⟨h'', hr'.trans hr⟩

theorem start_settled (devices : List Nat) (next : Nat) : Settled (start devices next) := by
  refine ⟨start_inv devices next, ?_, ?_, ?_⟩
  · intro k k' c c' a hk _ ha
    simp only [start, Option.map_eq_some_iff] at hk
    obtain ⟨_, -, rfl⟩ := hk; simp [att] at ha
  · intro k c j o _ _ ho; simp [start] at ho
  · intro p hp; simp [start] at hp

end CovenStorage.KeyDelivery
