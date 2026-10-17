import CovenSequence.Model

/-! Authored reads are immutable. Slot order determines when a receiver has
consumed a cause; it does not add causes to an already authored write. -/
namespace CovenSequence.Causality

structure Original (Dev Stamp Bytes : Type) where
  device : Dev
  number : Nat
  timestamp : Stamp
  schema : Nat
  readThrough : Nat
  bytes : Bytes
  deriving DecidableEq

variable {Dev Stamp Bytes : Type}
abbrev Write := Original Dev Stamp Bytes

structure Local (Dev Stamp Bytes : Type) where
  applied : Nat
  nextNumber : Nat
  queue : List (Original Dev Stamp Bytes)

def author (s : Local Dev Stamp Bytes) (d : Dev) (t : Stamp) (v : Nat) (b : Bytes) :
    Original Dev Stamp Bytes := ⟨d, s.nextNumber, t, v, s.applied, b⟩

def commit (s : Local Dev Stamp Bytes) (d : Dev) (t : Stamp) (v : Nat) (b : Bytes) :
    Local Dev Stamp Bytes :=
  { s with nextNumber := s.nextNumber + 1, queue := s.queue ++ [author s d t v b] }

def catchUp (s : Local Dev Stamp Bytes) (k : Nat) : Local Dev Stamp Bytes :=
  { s with applied := k }

theorem authored_readThrough (s : Local Dev Stamp Bytes) (d : Dev) (t : Stamp) (v : Nat) (b : Bytes) :
    (author s d t v b).readThrough = s.applied := rfl

theorem catchup_preserves_originals (s : Local Dev Stamp Bytes) (slots : List Nat) :
    (slots.foldl catchUp s).queue = s.queue := by
  induction slots generalizing s with
  | nil => rfl
  | cons k ks ih => exact ih (catchUp s k)

/-- The merge's `past write cause`: the consumed prefix at authorship plus
all own predecessors, including predecessors published after that prefix. -/
def Past (slot : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Nat)
    (w a : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes)) : Prop :=
  (0 < slot a ∧ slot a ≤ w.readThrough) ∨ (a.device = w.device ∧ a.number < w.number)

instance [DecidableEq Dev]
    (slot : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Nat) (w a : Write) :
    Decidable (Past slot w a) := inferInstanceAs (Decidable (_ ∨ _))

/-- The publication obligations: catch up before claiming, publish own writes
in number order, and never move the local applied position backwards. -/
structure Ordered (slot : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Nat)
    (published : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Prop) : Prop where
  after_read : ∀ w, published w → w.readThrough < slot w
  own_order : ∀ a w, published a → published w → a.device = w.device →
    a.number < w.number → slot a ≤ slot w
  reads_mono : ∀ a w, published a → published w → a.device = w.device →
    a.number < w.number → a.readThrough ≤ w.readThrough

def Before (slot : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Nat)
    (a w : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes)) : Prop :=
  slot a < slot w ∨ (slot a = slot w ∧ a.number < w.number)

/-- Strict slot application, with write-number order inside a slot, consumes
every recorded cause before the write, whether its effects survived or not. -/
theorem ordered_application_causal {slot : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Nat}
    {published : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Prop}
    (h : Ordered slot published) {w a : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes)}
    (hw : published w) (ha : published a) (past : Past slot w a) : Before slot a w := by
  rcases past with ⟨_, le⟩ | ⟨same, earlier⟩
  · exact Or.inl (Nat.lt_of_le_of_lt le (h.after_read w hw))
  · have le := h.own_order a w ha hw same earlier
    by_cases lt : slot a < slot w
    · exact Or.inl lt
    · exact Or.inr ⟨by omega, earlier⟩

/-- Authored pasts are closed under causes, the shape required by the merge. -/
theorem past_transitive {slot : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Nat}
    {published : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Prop}
    (h : Ordered slot published) {w a b : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes)}
    (hw : published w) (ha : published a) (hb : published b)
    (wa : Past slot w a) (ab : Past slot a b) : Past slot w b := by
  have positive := h.after_read b hb
  rcases wa with ⟨_, ar⟩ | ⟨ad, an⟩
  · have before := ordered_application_causal h ha hb ab
    exact Or.inl ⟨by omega, by rcases before with lt | ⟨eq, _⟩ <;> omega⟩
  · rcases ab with ⟨bp, br⟩ | ⟨bd, bn⟩
    · exact Or.inl ⟨bp, Nat.le_trans br (h.reads_mono a w ha hw ad an)⟩
    · exact Or.inr ⟨bd.trans ad, by omega⟩

/-- Receivers can have read different suffixes. Agreement on the cause's
containing slot and the write's original header gives exactly the same past. -/
theorem receivers_agree {slot₁ slot₂ : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Nat}
    (w a : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes)) (same : slot₁ a = slot₂ a) :
    Past slot₁ w a ↔ Past slot₂ w a := by simp only [Past, same]

theorem own_predecessor_is_read (slot : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Nat)
    (w a : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes))
    (same : a.device = w.device) (earlier : a.number < w.number) : Past slot w a :=
  Or.inr ⟨same, earlier⟩

/-- Publishing later does not turn an intervening remote write into a read. -/
theorem intervening_write_not_read (slot : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) → Nat)
    (w a : Write (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes))
    (later : w.readThrough < slot a) (other : a.device ≠ w.device) : ¬ Past slot w a := by
  rintro (⟨_, le⟩ | ⟨same, _⟩)
  · omega
  · exact other same

end CovenSequence.Causality
