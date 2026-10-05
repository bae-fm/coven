import CovenMerge.Removal

/-!
# Audiences (§14)

A row is one table, key and audience, with generations of its own (§14.2).
A write's row changes are split into parts by audience, each change in the
part of its row's audience; a device applies the parts it can read and counts
the rest as applied (§14.4).

* `project`: the writes as a device that reads some audiences sees them.
* `audience_converges`: devices that read the same audiences and applied the
  same writes hold the same merged state.
* `fold_atRow`, `audiences_agree`: a row's merged state depends only on the
  changes to that row, so devices that read different audiences agree on
  every row both read.
* `restrict`, `removal_local`: removal agrees on a set of rows both devices
  have, closed under parents and rivals.
* `rivals_closed`: when every unique claim includes the audience, unique
  rivals share an audience (§14.1).
* `fingerprint_local`: without the rule for a key in two audiences, removal
  agrees on every audience both devices read (§19.1).
* `Moved.agree`, `Moved.store_wins`: Ana moves note 1 into her circle while
  Ben re-adds it in the store; devices in and out of the circle agree on the
  store's note 1, which wins over the circle's.
-/

namespace CovenMerge

section
variable {W Row Col A : Type} [DecidableEq W]

/-- The writes as a device that reads the audiences `reads` sees them: the
change to row `r` sits in the part of `r`'s audience `aud r`. -/
def project (M : Writes W Row Col) (aud : Row → A) (reads : A → Bool) : Writes W Row Col where
  ts := M.ts
  past := M.past
  chg w r := if reads (aud r) then M.chg w r else none

theorem causalOrder_project {M : Writes W Row Col} {aud : Row → A} {reads : A → Bool}
    {L : List W} (h : CausalOrder M L) : CausalOrder (project M aud reads) L := by
  induction h with
  | nil => exact CausalOrder.nil
  | snoc _ hwL hpw ih => exact CausalOrder.snoc ih hwL hpw

/-- The writes as a device sees them still meet the assumptions of the merge:
a change's generation was reached by a change to the same row, in the same
audience. -/
theorem valid_project {M : Writes W Row Col} (hV : Valid M) (aud : Row → A) (reads : A → Bool) :
    Valid (project M aud reads) where
  ts_inj := hV.ts_inj
  past_ts := hV.past_ts
  gen_seen w r ch h := by
    simp only [project] at h ⊢
    by_cases hr : reads (aud r) = true
    · rw [ite_pos'' hr] at h
      rcases hV.gen_seen w r ch h with h0 | ⟨x, ch', hp, hx, h1, h2⟩
      · exact Or.inl h0
      · exact Or.inr ⟨x, ch', hp, by rw [ite_pos'' hr]; exact hx, h1, h2⟩
    · rw [ite_neg'' hr] at h; cases h
  parity w r ch h := by
    simp only [project] at h
    by_cases hr : reads (aud r) = true
    · rw [ite_pos'' hr] at h; exact hV.parity w r ch h
    · rw [ite_neg'' hr] at h; cases h

/-- **Same audiences, same merged state.** -/
theorem audience_converges {M : Writes W Row Col} (hV : Valid M) (aud : Row → A)
    (reads : A → Bool) {L₁ L₂ : List W} (h₁ : CausalOrder M L₁) (h₂ : CausalOrder M L₂)
    (hset : ∀ x, x ∈ L₁ ↔ x ∈ L₂) :
    L₁.foldl (step (project M aud reads)) St.init =
      L₂.foldl (step (project M aud reads)) (St.init : St W Row Col) :=
  merge_converges (valid_project hV aud reads) (causalOrder_project h₁) (causalOrder_project h₂) hset

/-- A state's part for row `r`. -/
def atRow (st : St W Row Col) (r : Row) :=
  (st.gen r, st.genWrite r, st.cell r, st.lost r)

/-- The step's per-row functions read a write set only through `ts` and
`past`. -/
theorem steps_congr {M₁ M₂ : Writes W Row Col} (hts : M₁.ts = M₂.ts) (hp : M₁.past = M₂.past) :
    genWriteStep M₁ = genWriteStep M₂ ∧ cellStep M₁ = cellStep M₂ ∧ lostStep M₁ = lostStep M₂ := by
  cases M₁
  cases M₂
  simp only at hts hp
  subst hts hp
  exact ⟨rfl, rfl, rfl⟩

theorem step_atRow {M₁ M₂ : Writes W Row Col} (hts : M₁.ts = M₂.ts) (hp : M₁.past = M₂.past)
    {r : Row} (hc : ∀ w, M₁.chg w r = M₂.chg w r) {s₁ s₂ : St W Row Col}
    (h : atRow s₁ r = atRow s₂ r) (w : W) : atRow (step M₁ s₁ w) r = atRow (step M₂ s₂ w) r := by
  simp only [atRow, Prod.mk.injEq] at h ⊢
  obtain ⟨hg, hgw, hcell, hl⟩ := h
  obtain ⟨e1, e2, e3⟩ := steps_congr hts hp
  simp only [step, hc w, hg, hgw, hcell, hl, e1, e2, e3]
  exact ⟨trivial, trivial, trivial, trivial⟩

/-- **A row's merged state depends only on the changes to that row.** -/
theorem fold_atRow {M₁ M₂ : Writes W Row Col} (hts : M₁.ts = M₂.ts) (hp : M₁.past = M₂.past)
    {r : Row} (hc : ∀ w, M₁.chg w r = M₂.chg w r) :
    ∀ (L : List W) (s₁ s₂ : St W Row Col), atRow s₁ r = atRow s₂ r →
      atRow (L.foldl (step M₁) s₁) r = atRow (L.foldl (step M₂) s₂) r := by
  intro L
  induction L with
  | nil => intro s₁ s₂ h; exact h
  | cons w L ih =>
    intro s₁ s₂ h
    exact ih _ _ (step_atRow hts hp hc h w)

/-- **Devices that read different audiences agree on every row both read.** -/
theorem audiences_agree (M : Writes W Row Col) (aud : Row → A) (readsA readsB : A → Bool)
    (L : List W) {r : Row} (hA : readsA (aud r) = true) (hB : readsB (aud r) = true) :
    atRow (L.foldl (step (project M aud readsA)) St.init) r =
      atRow (L.foldl (step (project M aud readsB)) St.init) r :=
  fold_atRow (M₁ := project M aud readsA) (M₂ := project M aud readsB)
    rfl rfl (fun w => by simp only [project, hA, hB, ite_true]) L St.init St.init rfl

end

/-! ## Removal on devices that have different rows -/

section
variable {Row K : Type} [DecidableEq Row] [DecidableEq K]

/-- The rules' inputs on a device that has only the rows `vis`. -/
def restrict (G : Inputs Row K) (vis : Row → Bool) : Inputs Row K :=
  { G with rows := G.rows.filter vis, present := fun x => vis x && G.present x }

/-- A set of rows `R` both devices have, closed under what the rules read
for them. -/
structure Closure (G : Inputs Row K) (visA visB R : Row → Bool) : Prop where
  visible : ∀ x, R x = true → visA x = true ∧ visB x = true
  parents : ∀ x, R x = true → ∀ r ∈ G.refs x, R r.parent = true
  rivals : ∀ x, R x = true → ∀ y ∈ G.rows, rivalBefore G y x = true → R y = true

theorem present_restrict {G : Inputs Row K} {vis : Row → Bool} {x : Row} (h : vis x = true) :
    (restrict G vis).present x = G.present x := by simp [restrict, h]

theorem fires_restrict_eq {G : Inputs Row K} {visA visB R : Row → Bool}
    (hc : Closure G visA visB R) {D D' : Row → Bool} (hD : ∀ y, R y = true → D y = D' y)
    {x : Row} (hx : R x = true) :
    fires (restrict G visA) D x = fires (restrict G visB) D' x := by
  have hfk : fkFires (restrict G visA) D x = fkFires (restrict G visB) D' x := by
    unfold fkFires
    apply any_congr_mem
    intro r hr
    rw [hD _ (hc.parents x hx r hr)]
  unfold fires
  rw [hfk]
  rfl

theorem rivals_restrict_eq {G : Inputs Row K} {visA visB R : Row → Bool}
    (hc : Closure G visA visB R) {D D' : Row → Bool} (hD : ∀ y, R y = true → D y = D' y)
    {x : Row} (hx : R x = true) :
    ((restrict G visA).rows.any fun y => rivalBefore (restrict G visA) y x && !D y) =
      ((restrict G visB).rows.any fun y => rivalBefore (restrict G visB) y x && !D' y) := by
  rw [show rivalBefore (restrict G visA) = rivalBefore G from rfl,
    show rivalBefore (restrict G visB) = rivalBefore G from rfl]
  simp only [restrict, List.any_filter]
  apply any_congr_mem
  intro y hy
  cases hr : rivalBefore G y x
  · simp
  · have hyR := hc.rivals x hx y hy hr
    have hv := hc.visible y hyR
    simp [hv.1, hv.2, hD y hyR]

/-- **Removal is local.** Two devices that have different rows of the same
merged state end with the same removals on any set of rows both have that is
closed under parents and rivals. -/
theorem removal_local {G : Inputs Row K} {visA visB R : Row → Bool}
    (hc : Closure G visA visB R) :
    ∀ x, R x = true → removal (restrict G visA) x = removal (restrict G visB) x := by
  have hA : AgreeOn R (FiresP (fires (restrict G visA))) (FiresP (fires (restrict G visB))) := by
    intro D D' x hx hD
    simp only [FiresP]
    rw [fires_restrict_eq hc hD hx]
  have hrows : ∀ x, R x = true →
      (x ∈ (restrict G visA).rows ↔ x ∈ (restrict G visB).rows) := by
    intro x hx
    have hv := hc.visible x hx
    simp [restrict, List.mem_filter, hv.1, hv.2]
  have h0 : ∀ x, R x = true → start (restrict G visA) x = start (restrict G visB) x := by
    intro x hx
    have hv := hc.visible x hx
    simp [start, present_restrict hv.1, present_restrict hv.2]
  exact stratified_local (fires_monotone _) (fires_monotone _) hA hrows
    (fun D D' x hx hD => rivals_restrict_eq hc hD hx) h0
    (removal_stratified _) (removal_stratified _)

/-- When every unique claim includes the claiming row's audience, as §14.1
requires, a row's unique rivals share its audience. -/
theorem rivals_closed {A V : Type} [DecidableEq A] [DecidableEq V] (G : Inputs Row (A × V))
    (aud : Row → A) (hkey : ∀ x, ∀ c ∈ G.claims x, c.other = false → c.key.1 = aud x) {x y : Row}
    (h : rivalOf G false y x = true) : aud y = aud x := by
  unfold rivalOf at h
  simp only [Bool.and_eq_true, List.any_eq_true, decide_eq_true_eq] at h
  obtain ⟨_, c, hc, hco, c', hc', ⟨⟨⟨hco', _⟩, hk⟩, _⟩⟩ := h
  rw [← hkey y c' hc' hco', ← hkey x c hc hco, hk]

/-- The inputs a fingerprint uses: the claims of keys present in two
audiences left out, since which row shows for such a key depends on which
circles a device reads (§14.2, §19.1). -/
def forFingerprint (G : Inputs Row K) : Inputs Row K :=
  { G with claims := fun x => (G.claims x).filter (fun c => !c.other) }

/-- Without the claims of keys in two audiences, a rival is a unique rival. -/
theorem rivalBefore_forFingerprint {G : Inputs Row K} {x y : Row} :
    rivalBefore (forFingerprint G) y x = rivalOf G false y x := by
  have h : rivalOf (forFingerprint G) true y x = false := by
    unfold rivalOf
    simp only [forFingerprint, Bool.and_eq_false_iff, List.any_eq_false, List.mem_filter]
    right
    intro c ⟨_, hc⟩ h
    simp only [Bool.not_eq_true'] at hc
    simp [hc] at h
  have h2 : rivalOf (forFingerprint G) false y x = rivalOf G false y x := by
    have nb : ∀ (b t : Bool), (!b && (decide (b = false) && t)) = (decide (b = false) && t) := by
      intro b t; cases b <;> rfl
    unfold rivalOf
    simp only [forFingerprint, List.any_filter, Bool.and_assoc, nb]
    rfl
  unfold rivalBefore
  rw [h, h2, Bool.or_false]

/-- **Fingerprints agree.** Two devices end with the same removals, computed
for the fingerprint, on any set of rows both have that is closed under
parents and unique rivals: every row of an audience both read, by §14.5 and
`rivals_closed`. -/
theorem fingerprint_local {G : Inputs Row K} {visA visB R : Row → Bool}
    (hvis : ∀ x, R x = true → visA x = true ∧ visB x = true)
    (hpar : ∀ x, R x = true → ∀ r ∈ G.refs x, R r.parent = true)
    (hriv : ∀ x, R x = true → ∀ y ∈ G.rows,
      rivalBefore (forFingerprint G) y x = true → R y = true) :
    ∀ x, R x = true → removal (restrict (forFingerprint G) visA) x =
      removal (restrict (forFingerprint G) visB) x :=
  removal_local ⟨hvis, hpar, hriv⟩

end

/-! ## Ana moves a note into her circle while Ben re-adds it in the store

Rows are one table, key and audience: row 1 is the store's note 1, row 2 the
circle's note 1. Cell 0 is the body.

* write 1, Ana, stamp 1: insert the store's note 1.
* write 2, Ana, stamp 20: move it into her circle: delete row 1, insert row 2.
* write 4, Ben, stamp 30, had read writes 1 and 2: re-add the store's note 1.
* write 5, Carol, in the circle, stamp 40, had read writes 1 and 2: edit the
  circle's note 1. -/

namespace Moved

def M : Writes Nat Nat Nat where
  ts w := if w = 1 then 1 else if w = 2 then 20 else if w = 4 then 30 else 40
  past w a := (w = 2 && a = 1) || (w = 4 && (a = 1 || a = 2)) || (w = 5 && (a = 1 || a = 2))
  chg w r :=
    if w = 1 ∧ r = 1 then some ⟨.ins, 0, fun c => c = 0⟩
    else if w = 2 ∧ r = 1 then some ⟨.del, 1, fun _ => false⟩
    else if w = 2 ∧ r = 2 then some ⟨.ins, 0, fun c => c = 0⟩
    else if w = 4 ∧ r = 1 then some ⟨.ins, 2, fun c => c = 0⟩
    else if w = 5 ∧ r = 2 then some ⟨.upd, 1, fun c => c = 0⟩
    else none

/-- Row 2 is in the circle. -/
def inCircle (r : Nat) : Bool := r = 2

def carol : St Nat Nat Nat := [1, 2, 4, 5].foldl (step (project M inCircle (fun _ => true))) St.init
def dan : St Nat Nat Nat :=
  [1, 2, 4, 5].foldl (step (project M inCircle (fun c => !c))) St.init

/-- Carol and Dan agree on the store's note 1: generation 3, Ben's body.
Carol also has the circle's note 1, with her own body. -/
theorem agree :
    atRow carol 1 = atRow dan 1 ∧ carol.gen 1 = 3 ∧ carol.cell 1 0 = some 4 ∧
    carol.gen 2 = 1 ∧ carol.cell 2 0 = some 5 ∧ dan.gen 2 = 0 :=
  ⟨audiences_agree M inCircle _ _ _ (by decide) (by decide), by decide, by decide, by decide,
    by decide, by decide⟩

/-- On Carol's device, note 1 is present in the store and in the circle. The
store's wins, and the circle's is removed like a unique loser (§14.2). -/
def carolInputs : Inputs Nat Nat where
  rows := [1, 2]
  present _ := true
  refs _ := []
  checkFails _ := false
  inDeletedCircle _ := false
  claims r := if r = 1 then [⟨0, 1, 0, true⟩] else [⟨0, 1, 20, true⟩]
  rank r := r

theorem store_wins : (view carolInputs).shown 1 = true ∧ (view carolInputs).removed 2 = true ∧
    (view carolInputs).rules 2 = [Rule.otherAudience] := by decide

end Moved

end CovenMerge
