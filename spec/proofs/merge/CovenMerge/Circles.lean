import CovenMerge.Audience

/-! Entry-named circle removal (§8, §14.7, API E4). The Boolean rule engine
is shared with the compatibility runner; current inputs carry the kept entry. -/

namespace CovenMerge

abbrev EntryInputs (Row K : Type) := RemovalInputs Row K (Option Nat)

def EntryInputs.erase {Row K : Type} (I : EntryInputs Row K) : Inputs Row K :=
  { I with inDeletedCircle := fun r => (I.inDeletedCircle r).isSome }

section
variable {W Row Col K : Type} [DecidableEq W] [DecidableEq Row] [DecidableEq K]

/-- Replace the compatibility rule's tag with the responsible entry. -/
def namedRule (entry : Option Nat) : Rule → Option Rule
  | .deletedCircle => entry.map Rule.deletedCircleEntry
  | rule => some rule

def entryView (I : EntryInputs Row K) : View Row :=
  let v := view I.erase
  { v with rules := fun r => (v.rules r).filterMap (namedRule (I.inDeletedCircle r)) }

theorem deleted_rule_names_entry (I : EntryInputs Row K) (r : Row) (e : Nat)
    (hp : I.present r = true) (he : I.inDeletedCircle r = some e) (hr : r ∈ I.rows) :
    (entryView I).removed r = true ∧
      Rule.deletedCircleEntry e ∈ (entryView I).rules r := by
  have hf : fires I.erase (removal I.erase) r = true := by
    simp [fires, EntryInputs.erase, he]
  have hd := normal_closed (close_normal I.erase.rows (fires I.erase) _) r hr hf
  have hd' : removal I.erase r = true := hd
  constructor
  · change (I.present r && removal I.erase r) = true
    simp [hp, hd']
  · simp only [entryView, view, EntryInputs.erase] at *
    simp [hp, hd', rulesOf, he, namedRule]

theorem named_rules_nonempty (I : EntryInputs Row K) {r : Row}
    (h : (entryView I).removed r = true) : (entryView I).rules r ≠ [] := by
  have hn := removed_has_rule I.erase h
  have survives : ∀ rule ∈ (view I.erase).rules r,
      ∃ named, namedRule (I.inDeletedCircle r) rule = some named := by
    intro rule hm
    cases rule <;> try exact ⟨_, rfl⟩
    cases he : I.inDeletedCircle r with
    | some e => exact ⟨.deletedCircleEntry e, by simp [namedRule]⟩
    | none =>
      have no : Rule.deletedCircle ∉ (view I.erase).rules r := by
        have hc : I.erase.inDeletedCircle r = false := by simp [EntryInputs.erase, he]
        simp only [view]
        split
        · simp [rulesOf, hc]
        · simp
      exact (no hm).elim
  obtain ⟨rule, hm⟩ := List.exists_mem_of_ne_nil _ hn
  obtain ⟨named, he⟩ := survives rule hm
  intro hempty
  have : named ∈ (entryView I).rules r := List.mem_filterMap.mpr ⟨rule, hm, he⟩
  rw [hempty] at this
  exact List.not_mem_nil this

/-- Applying or dropping a circle entry only recomputes the view. -/
def observeEntries (st : St W Row Col) (I : EntryInputs Row K) : St W Row Col × View Row :=
  (st, entryView I)

theorem circle_entries_preserve_merge (st : St W Row Col) (I : EntryInputs Row K) :
    (observeEntries st I).1 = st := rfl

def entryDevice [DecidableEq Col] (M : Writes W Row Col)
    (inputs : St W Row Col → EntryInputs Row K) (L : List W) : Device W Row Col :=
  let st := L.foldl (step M) St.init
  let v := entryView (inputs st)
  ⟨st, v, lossRecord st v⟩

theorem entry_device_converges [DecidableEq Col] {M : Writes W Row Col} (valid : Valid M)
    (inputs : St W Row Col → EntryInputs Row K) {a b : List W}
    (ha : CausalOrder M a) (hb : CausalOrder M b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    entryDevice M inputs a = entryDevice M inputs b := by
  unfold entryDevice
  rw [merge_converges valid ha hb same]

theorem entry_device_rule_order [DecidableEq Col] {M : Writes W Row Col} (valid : Valid M)
    (inputs : St W Row Col → EntryInputs Row K) {a b : List W}
    (ha : CausalOrder M a) (hb : CausalOrder M b) (same : ∀ w, w ∈ a ↔ w ∈ b)
    {D E : Row → Bool}
    (hd : let I := (inputs (a.foldl (step M) St.init)).erase
      Stratified I.rows (FiresP (fires I)) (rivalBefore I) (start I) D)
    (he : let I := (inputs (b.foldl (step M) St.init)).erase
      Stratified I.rows (FiresP (fires I)) (rivalBefore I) (start I) E) : D = E :=
  rule_order_converges valid (fun st => (inputs st).erase) ha hb same hd he

theorem entry_rule_order (I : EntryInputs Row K) {D : Row → Bool}
    (h : Stratified I.rows (FiresP (fires I.erase)) (rivalBefore I.erase)
      (start I.erase) D) : D = removal I.erase := any_order_removal I.erase h

/-- If replay removes the last applicable cause, the row returns. -/
theorem row_returns (I : EntryInputs Row K) {r : Row}
    (hp : I.present r = true) (h : (entryView I).rules r = []) :
    (entryView I).shown r = true := by
  cases hd : removal I.erase r
  · change (I.present r && !removal I.erase r) = true
    simp [hp, hd]
  · have hr : (entryView I).removed r = true := by
      change (I.present r && removal I.erase r) = true
      simp [hp, hd]
    exact (named_rules_nonempty I hr h).elim

/-- Any two surviving rows cannot have an ordered rival claim. In particular,
two audiences with the same key show at most one row (§8, §14.2). -/
theorem shown_not_rivals (I : Inputs Row K) {x y : Row} (hy : y ∈ I.rows)
    (hxShown : (view I).shown x = true) (hyShown : (view I).shown y = true) :
    rivalBefore I y x = false := by
  have hx : removal I x = false := by
    simp only [view, Bool.and_eq_true, Bool.not_eq_true'] at hxShown
    exact hxShown.2
  have hy' : removal I y = false := by
    simp only [view, Bool.and_eq_true, Bool.not_eq_true'] at hyShown
    exact hyShown.2
  have hpass : pass1 I y = false := by
    cases hp : pass1 I y
    · rfl
    · have hg := star_grow (close_star I.rows (fires I)
        (uniqueLosers I.rows (rivalBefore I) (pass1 I))) y
        (by simp [uniqueLosers, hp])
      change removal I y = true at hg
      rw [hy'] at hg
      contradiction
  cases hr : rivalBefore I y x
  · rfl
  · have hu : uniqueLosers I.rows (rivalBefore I) (pass1 I) x = true := by
      simp only [uniqueLosers, Bool.or_eq_true]
      exact Or.inr (List.any_eq_true.mpr ⟨y, hy, by simp [hr, hpass]⟩)
    have hg := star_grow (close_star I.rows (fires I) _) x hu
    change removal I x = true at hg
    rw [hx] at hg
    contradiction

theorem same_key_shows_once (I : Inputs Row K) {x y : Row}
    (hx : x ∈ I.rows) (hy : y ∈ I.rows)
    (ordered : rivalOf I true x y = true ∨ rivalOf I true y x = true) :
    ¬ ((view I).shown x = true ∧ (view I).shown y = true) := by
  rintro ⟨sx, sy⟩
  rcases ordered with h | h
  · have := shown_not_rivals I hx sy sx
    simp [rivalBefore, h] at this
  · have := shown_not_rivals I hy sx sy
    simp [rivalBefore, h] at this

end
end CovenMerge
