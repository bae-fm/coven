import CovenMerge.Circles

/-!
# Installing a snapshot (§8.4, §16.5)

A reload installs a snapshot's merge records as they are and runs the removal
rules only over a region of rows: the rows with a reason of their own, and
the rows the rules reach from them. Every present row outside the region is
shown, with no rule recorded.

* `Region`: what the region holds: every present row with a failing CHECK, a
  deleted circle, or a reference that is stale or names an absent parent;
  both rows of every rival claim; and every row that references a row of the
  region.
* `install_removal`: the rules run over the region's rows remove exactly what
  the rules run over every row remove.
* `install_view`: the install's view, the region's rules inside it and every
  present row shown outside it, is `view`.
* `install_replays`, `install_entry_replays`: loading a snapshot of a closed
  set of writes, applying the writes after it and installing gives the device
  that applying every write from the start gives: the same merged state, the
  same rows shown and removed, and the same loss records.
* `Releases`: a track whose release is gone and its file. A region without
  the file shows it, and a region without the first claimant shows the row
  that loses to it; the full region removes both.
-/

namespace CovenMerge

section
variable {Row K : Type} [DecidableEq Row] [DecidableEq K]

/-- What the install's region holds, for the rules' inputs `I`. -/
structure Region (I : Inputs Row K) (R : Row → Bool) : Prop where
  /-- A present row with a reason of its own: a failing CHECK, a deleted
  circle, or a reference that is stale or names an absent parent. -/
  reasons : ∀ x ∈ I.rows, I.present x = true →
    (I.checkFails x = true ∨ I.inDeletedCircle x = true ∨
      ∃ r ∈ I.refs x, r.stale = true ∨ I.present r.parent = false) → R x = true
  /-- Both rows of a rival claim. -/
  rivals : ∀ x y, y ∈ I.rows → rivalBefore I y x = true → R x = true ∧ R y = true
  /-- A row referencing a row of the region. -/
  children : ∀ x y, R x = true → y ∈ I.rows → ∀ r ∈ I.refs y, r.parent = x → R y = true

/-- The rules' inputs over the region's rows alone. The rules read every other
row as its generation has it: present, and never taken out. -/
def within (I : Inputs Row K) (R : Row → Bool) : Inputs Row K :=
  { I with rows := I.rows.filter R }

theorem killStep_mono {rows rows' : List Row} {F : (Row → Bool) → Row → Prop}
    (h : ∀ x, x ∈ rows → x ∈ rows') {D D' : Row → Bool} (s : Star (KillStep rows F) D D') :
    Star (KillStep rows' F) D D' := by
  induction s with
  | refl => exact Star.refl _
  | head hab _ ih =>
    obtain ⟨x, hx, hDx, hf, rfl⟩ := hab
    exact Star.head ⟨x, h x hx, hDx, hf, rfl⟩ ih

/-- A run of the rules changes no row outside the rows it runs over. -/
theorem star_outside {rows : List Row} {F : (Row → Bool) → Row → Prop} {D D' : Row → Bool}
    (s : Star (KillStep rows F) D D') {x : Row} (hx : x ∉ rows) : D' x = D x := by
  induction s with
  | refl => rfl
  | head hab _ ih =>
    obtain ⟨y, hy, _, _, rfl⟩ := hab
    rw [ih]
    have hxy : x ≠ y := fun e => hx (e ▸ hy)
    simp [kill, hxy]

variable {I : Inputs Row K} {R : Row → Bool}

theorem not_mem_within {x : Row} (hx : R x = false) : x ∉ (within I R).rows := by
  simp [within, hx]

/-- No rival claim comes before a row outside the region. -/
theorem no_rival_outside (hR : Region I R) {x : Row} (hx : R x = false) (D : Row → Bool) :
    (I.rows.any fun y => rivalBefore I y x && !D y) = false := by
  cases h : I.rows.any fun y => rivalBefore I y x && !D y
  · rfl
  · simp only [List.any_eq_true, Bool.and_eq_true] at h
    obtain ⟨y, hy, hr, _⟩ := h
    rw [(hR.rivals x y hy hr).1] at hx
    cases hx

/-- Over the region's rows, a row's rival claims are the same as over every
row. -/
theorem rivals_within (hR : Region I R) (other : Bool) (x : Row) (D : Row → Bool) :
    ((within I R).rows.any fun y => rivalOf I other y x && !D y) =
      (I.rows.any fun y => rivalOf I other y x && !D y) := by
  simp only [within, List.any_filter]
  apply any_congr_mem
  intro y hy
  cases hr : rivalOf I other y x
  · simp
  · have hb : rivalBefore I y x = true := by
      cases other <;> simp [rivalBefore, hr]
    simp [(hR.rivals x y hy hb).2]

theorem uniqueLosers_within (hR : Region I R) (D : Row → Bool) :
    uniqueLosers (within I R).rows (rivalBefore I) D = uniqueLosers I.rows (rivalBefore I) D := by
  funext x
  simp only [uniqueLosers]
  congr 1
  simp only [within, List.any_filter]
  apply any_congr_mem
  intro y hy
  cases hr : rivalBefore I y x
  · simp
  · simp [(hR.rivals x y hy hr).2]

/-- A set of removals that is the starting one outside the region, and in
which no rule fires for a row of the region still in, has no rule firing for
any row still in. -/
theorem normal_widen (hR : Region I R) {D : Row → Bool}
    (outside : ∀ z, R z = false → D z = start I z)
    (n : Normal (KillStep (within I R).rows (FiresP (fires I))) D) :
    Normal (KillStep I.rows (FiresP (fires I))) D := by
  rintro ⟨D', x, hx, hDx, hf, _⟩
  cases hRx : R x
  · have hp : I.present x = true := by
      have := outside x hRx
      rw [hDx] at this
      simp only [start] at this
      cases h : I.present x
      · rw [h] at this; cases this
      · rfl
    have reason := hR.reasons x hx hp
    simp only [FiresP, fires, fkFires, Bool.or_eq_true, List.any_eq_true] at hf
    rcases hf with (hc | ⟨r, hr, hs | hd⟩) | hc
    · rw [reason (Or.inl hc)] at hRx; cases hRx
    · rw [reason (Or.inr (Or.inr ⟨r, hr, Or.inl hs⟩))] at hRx; cases hRx
    · cases hRp : R r.parent
      · have hgone : I.present r.parent = false := by
          have := outside r.parent hRp
          rw [hd] at this
          simp only [start] at this
          cases h : I.present r.parent
          · rfl
          · rw [h] at this; cases this
        rw [reason (Or.inr (Or.inr ⟨r, hr, Or.inr hgone⟩))] at hRx; cases hRx
      · rw [hR.children r.parent x hRp hx r hr rfl] at hRx; cases hRx
    · rw [reason (Or.inr (Or.inl hc))] at hRx; cases hRx
  · exact n ⟨D', x, by simp [within, hx, hRx], hDx, hf, ‹_›⟩

theorem fires_within : fires (within I R) = fires I := rfl
theorem rivalBefore_within : rivalBefore (within I R) = rivalBefore I := rfl
theorem start_within : start (within I R) = start I := rfl

/-- Outside the region, the region's step 1 is the starting state. -/
theorem pass1_outside {x : Row} (hx : R x = false) : pass1 (within I R) x = start I x :=
  star_outside (close_star _ _ _) (not_mem_within hx)

/-- Outside the region, the region's rules remove only absent rows. -/
theorem removal_within_outside (hR : Region I R) {x : Row} (hx : R x = false) :
    removal (within I R) x = start I x := by
  have := star_outside (close_star (within I R).rows (fires (within I R))
    (uniqueLosers (within I R).rows (rivalBefore (within I R)) (pass1 (within I R))))
    (not_mem_within hx)
  change removal (within I R) x = _ at this
  rw [this, rivalBefore_within, uniqueLosers_within hR]
  show (pass1 (within I R) x || I.rows.any fun y => rivalBefore I y x && !pass1 (within I R) y) =
    start I x
  rw [pass1_outside hx, no_rival_outside hR hx, Bool.or_false]

/-- The region's step 1 is step 1 over every row. -/
theorem pass1_within (hR : Region I R) : pass1 (within I R) = pass1 I := by
  apply kill_unique_normal (fires_monotone I)
    (killStep_mono (fun x hx => (List.mem_filter.1 hx).1) (close_star _ _ _))
    (close_star I.rows (fires I) (start I))
    (normal_widen hR (fun z hz => pass1_outside hz) (close_normal _ _ _))
    (close_normal _ _ _)

/-- **The rules run over the region remove what they remove over every
row.** -/
theorem install_removal (hR : Region I R) : removal (within I R) = removal I := by
  apply any_order_removal I
  refine ⟨pass1 (within I R),
    killStep_mono (fun x hx => (List.mem_filter.1 hx).1) (close_star _ _ _),
    normal_widen hR (fun z hz => pass1_outside hz) (close_normal _ _ _), ?_,
    normal_widen hR (fun z hz => removal_within_outside hR hz) (close_normal _ _ _)⟩
  rw [← uniqueLosers_within hR]
  exact killStep_mono (fun x hx => (List.mem_filter.1 hx).1) (close_star _ _ _)

/-- Outside the region, the rules over every row remove only absent rows. -/
theorem outside_removal (hR : Region I R) {x : Row} (hx : R x = false) :
    removal I x = start I x := by
  rw [← install_removal hR, removal_within_outside hR hx]

theorem rulesOf_within (hR : Region I R) : rulesOf (within I R) = rulesOf I := by
  funext x
  have hl : ∀ other, lostIn2 (within I R) other x = lostIn2 I other x := by
    intro other
    simp only [lostIn2, pass1_within hR]
    rw [show rivalOf (within I R) other = rivalOf I other from rfl, rivals_within hR]
  simp only [rulesOf, install_removal hR, hl]
  rfl

/-- What an install shows: the region's rules over the region's rows, and
every present row outside it shown, with no rule recorded. -/
def installView (I : Inputs Row K) (R : Row → Bool) : View Row :=
  let v := view (within I R)
  { shown := fun x => if R x then v.shown x else I.present x
    removed := fun x => if R x then v.removed x else false
    rules := fun x => if R x then v.rules x else [] }

/-- **An install shows what the rules over every row show.** -/
theorem install_view (hR : Region I R) : installView I R = view I := by
  have hv : view (within I R) = view I := by
    simp only [view, install_removal hR, rulesOf_within hR]
    rfl
  simp only [installView, hv]
  have out : ∀ x, R x = false → removal I x = !I.present x := fun x hx => outside_removal hR hx
  congr 1
  · funext x
    cases hx : R x
    · simp [out x hx]
    · rfl
  · funext x
    cases hx : R x
    · simp [out x hx]
    · rfl
  · funext x
    cases hx : R x
    · simp [out x hx]
    · rfl

/-- A present row outside the region is shown. -/
theorem outside_shown (hR : Region I R) {x : Row} (hx : R x = false)
    (hp : I.present x = true) : (view I).shown x = true := by
  rw [← install_view hR]
  simp [installView, hx, hp]

end

/-! ## Installing reaches the state replaying reaches -/

section
variable {W Row Col K : Type} [DecidableEq W] [DecidableEq Row] [DecidableEq Col] [DecidableEq K]

/-- The device an install leaves: `snapshot` loaded, the writes `L` after it
applied, and the rules run over `region`. -/
def installDevice (M : Writes W Row Col) (inputs : St W Row Col → Inputs Row K)
    (region : St W Row Col → Row → Bool) (snapshot : St W Row Col) (L : List W) :
    Device W Row Col :=
  let st := L.foldl (step M) snapshot
  let v := installView (inputs st) (region st)
  { merged := st, view := v, losses := lossRecord st v }

/-- **Installing a snapshot leaves what replaying its writes leaves.** A
snapshot of the closed set `C`, loaded, with the writes `L₁` after it applied
and the rules run over a region, gives the device that applying `L₂` from the
start gives, when `C` and `L₁` together are `L₂`'s writes. -/
theorem install_replays {M : Writes W Row Col} (hV : Valid M)
    (inputs : St W Row Col → Inputs Row K) (region : St W Row Col → Row → Bool)
    {C : W → Prop} (hC : Closed M C) {snapshot : St W Row Col} (hs : IsSpec M C snapshot)
    {L₁ L₂ : List W} (h₁ : CausalFrom M C L₁) (h₂ : CausalOrder M L₂)
    (hset : ∀ x, (C x ∨ x ∈ L₁) ↔ x ∈ L₂)
    (hR : Region (inputs (L₁.foldl (step M) snapshot)) (region (L₁.foldl (step M) snapshot))) :
    installDevice M inputs region snapshot L₁ = device M inputs L₂ := by
  have he : L₁.foldl (step M) snapshot = L₂.foldl (step M) St.init :=
    isSpec_unique (foldl_isSpec hV hC hs h₁).1
      (isSpec_congr (fun x => (hset x).symm) (run_isSpec hV h₂))
  unfold installDevice device
  simp only
  rw [install_view hR, he]

/-- An install's view with each deleted-circle rule naming its entry. -/
def installEntryView (I : EntryInputs Row K) (R : Row → Bool) : View Row :=
  let v := installView I.erase R
  { v with rules := fun r => (v.rules r).filterMap (namedRule (I.inDeletedCircle r)) }

theorem install_entry_view {I : EntryInputs Row K} {R : Row → Bool} (hR : Region I.erase R) :
    installEntryView I R = entryView I := by
  simp only [installEntryView, entryView, install_view hR]

def installEntryDevice (M : Writes W Row Col) (inputs : St W Row Col → EntryInputs Row K)
    (region : St W Row Col → Row → Bool) (snapshot : St W Row Col) (L : List W) :
    Device W Row Col :=
  let st := L.foldl (step M) snapshot
  let v := installEntryView (inputs st) (region st)
  { merged := st, view := v, losses := lossRecord st v }

/-- The same, with deleted-circle rules naming their entries. -/
theorem install_entry_replays {M : Writes W Row Col} (hV : Valid M)
    (inputs : St W Row Col → EntryInputs Row K) (region : St W Row Col → Row → Bool)
    {C : W → Prop} (hC : Closed M C) {snapshot : St W Row Col} (hs : IsSpec M C snapshot)
    {L₁ L₂ : List W} (h₁ : CausalFrom M C L₁) (h₂ : CausalOrder M L₂)
    (hset : ∀ x, (C x ∨ x ∈ L₁) ↔ x ∈ L₂)
    (hR : Region (inputs (L₁.foldl (step M) snapshot)).erase
      (region (L₁.foldl (step M) snapshot))) :
    installEntryDevice M inputs region snapshot L₁ = entryDevice M inputs L₂ := by
  have he : L₁.foldl (step M) snapshot = L₂.foldl (step M) St.init :=
    isSpec_unique (foldl_isSpec hV hC hs h₁).1
      (isSpec_congr (fun x => (hset x).symm) (run_isSpec hV h₂))
  unfold installEntryDevice entryDevice
  simp only
  rw [install_entry_view hR, he]

end

/-! ## Why the region holds children and both rivals

Release 0 is gone. Track 1 references it; file 2 references track 1. Rows 3
and 4 claim one value, row 3's claim first. Track 1 has a reason of its own;
file 2 has none, but goes with its track; row 4 loses to row 3. -/

namespace Releases

def I : Inputs Nat Nat where
  rows := [0, 1, 2, 3, 4]
  present x := x != 0
  refs x := if x = 1 then [⟨0, false⟩] else if x = 2 then [⟨1, false⟩] else []
  checkFails _ := false
  inDeletedCircle _ := false
  claims x := if x = 3 then [⟨⟨["path"], none⟩, 7, 10, false⟩]
    else if x = 4 then [⟨⟨["path"], none⟩, 7, 11, false⟩] else []
  rank x := x

/-- The region an install finds: track 1, its file, and both claimants. -/
def full (x : Nat) : Bool := x = 1 || x = 2 || x = 3 || x = 4

theorem full_region : Region I full := by
  refine ⟨?_, ?_, ?_⟩
  · intro x hx hp _
    simp only [I, List.mem_cons, List.mem_nil_iff, or_false] at hx
    rcases hx with rfl | rfl | rfl | rfl | rfl
    · simp [I] at hp
    all_goals decide
  · intro x y _ hr
    have hc : ∀ z, z ≠ 3 → z ≠ 4 → I.claims z = [] := by
      intro z h3 h4
      simp [I, h3, h4]
    have inside : ∀ z, (z ≠ 3 → z ≠ 4 → False) → full z = true := by
      intro z h
      by_cases h3 : z = 3
      · subst h3; decide
      · by_cases h4 : z = 4
        · subst h4; decide
        · exact (h h3 h4).elim
    refine ⟨inside x fun h3 h4 => ?_, inside y fun h3 h4 => ?_⟩
    · simp [rivalBefore, rivalOf, hc x h3 h4] at hr
    · simp [rivalBefore, rivalOf, hc y h3 h4] at hr
  · intro x y hx hy r hr hp
    simp only [I, List.mem_cons, List.mem_nil_iff, or_false] at hy
    rcases hy with rfl | rfl | rfl | rfl | rfl <;> simp [I] at hr <;> subst hr <;> subst hp <;>
      decide

theorem installed : installView I full = view I := install_view full_region

/-- File 2 is taken out with its track; row 4 loses to row 3. -/
theorem removed : (view I).removed 1 = true ∧ (view I).removed 2 = true ∧
    (view I).shown 3 = true ∧ (view I).removed 4 = true := by decide

/-- A region of the rows with a reason of their own alone, without the file
that references track 1, shows the file. -/
def seeds (x : Nat) : Bool := x = 1 || x = 3 || x = 4

theorem without_children : installView I seeds ≠ view I := by
  intro h
  have := congrArg (fun v => v.shown 2) h
  revert this
  decide

/-- A region without the first claimant shows the row that loses to it. -/
def loser (x : Nat) : Bool := x = 1 || x = 2 || x = 4

theorem without_rival : installView I loser ≠ view I := by
  intro h
  have := congrArg (fun v => v.shown 4) h
  revert this
  decide

end Releases

end CovenMerge
