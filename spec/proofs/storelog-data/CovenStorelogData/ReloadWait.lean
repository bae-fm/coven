import CovenStorelogData.RefusedRetention

/-! §15, §19.1: the reload waits on a newest snapshot it cannot load.

Each audience the reload replaces loads its current newest counting
snapshot: the newest listed snapshot whose signed prefix this version
verifies, whatever this device found in its body or key (`choose`). When
this device cannot load that snapshot, because it holds a refusal of its
body, does not accept the key sealing its rows, or its rows need a newer
app schema, the reload waits on it and never loads an older one:
- loading applies only that newest snapshot, after its body, key and schema
  checked (`choose_loads_newest`);
- the reload waits exactly while that newest snapshot is listed and this
  device cannot load it (`choose_waits_iff`, `waits_listed`);
- the wait ends when a newer snapshot this device can load lands
  (`wait_ends_when_newer_lands`) or the listing stops naming the snapshot
  (`wait_ends_when_unlisted`), and the reload's own record for a body or key
  wait retries automatically (`waits_retry`);
- with the retention of `RefusedRetention`, a write retention deleted is
  covered by whatever snapshot the reload loads or waits on
  (`choose_never_needs_deleted`), so no audience of a reload skips a part
  of it (`deleted_part_never_skipped`).

Passing over the unloadable newest for the next one, as §19.1 said, loses a
deleted write's part when the reload spans two audiences:
`two_audience_pass_over_counterexample`. -/
namespace CovenStorelogData.Pending
open CovenStorelog

/-! ## §15's order -/

theorem before_irrefl (a : Listed) : a.before a = false := by
  simp [Listed.before]

theorem before_trans {a b c : Listed} (ab : a.before b = true) (bc : b.before c = true) :
    a.before c = true := by
  simp only [Listed.before, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq,
    beq_iff_eq] at ab bc ⊢
  omega

theorem before_asymm {a b : Listed} (ab : a.before b = true) : b.before a = false := by
  cases h : b.before a
  · rfl
  · have := before_trans ab h
    rw [before_irrefl] at this
    cases this

/-- Two snapshots at different paths are ordered one way or the other. -/
theorem before_total {a b : Listed} (paths : a.path ≠ b.path) :
    a.before b = true ∨ b.before a = true := by
  have : a.path.1 ≠ b.path.1 ∨ a.path.2 ≠ b.path.2 := by
    by_cases h1 : a.path.1 = b.path.1
    · exact .inr fun h2 => paths (Prod.ext h1 h2)
    · exact .inl h1
  simp only [Listed.before, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq]
  omega

/-- One step of `newest`. -/
def pick (best : Option Listed) (s : Listed) : Option Listed :=
  match best with
  | none => some s
  | some b => if s.before b then some s else some b

theorem newest_fold (l : List Listed) : newest l = l.foldl pick none := rfl

theorem newest_append (l : List Listed) (n : Listed) :
    newest (l ++ [n]) = pick (newest l) n := by
  simp [newest_fold, List.foldl_append]

private theorem fold_pick (l : List Listed) (b : Listed) :
    ∃ m, l.foldl pick (some b) = some m ∧ (m = b ∨ m ∈ l) ∧ (m = b ∨ m.before b = true) ∧
      ∀ y ∈ l, y.before m = false := by
  induction l generalizing b with
  | nil => exact ⟨b, rfl, .inl rfl, .inl rfl, by simp⟩
  | cons x xs ih =>
      by_cases hx : x.before b = true
      · obtain ⟨m, hm, mem, chain, min⟩ := ih x
        have step : (x :: xs).foldl pick (some b) = xs.foldl pick (some x) := by
          simp [List.foldl, pick, hx]
        refine ⟨m, step ▸ hm, ?_, ?_, ?_⟩
        · rcases mem with rfl | h
          · exact .inr List.mem_cons_self
          · exact .inr (List.mem_cons_of_mem _ h)
        · right
          rcases chain with rfl | h
          · exact hx
          · exact before_trans h hx
        · intro y hy
          rcases List.mem_cons.mp hy with rfl | hy
          · rcases chain with rfl | h
            · exact before_irrefl _
            · exact before_asymm h
          · exact min y hy
      · have hx' : x.before b = false := by simpa using hx
        obtain ⟨m, hm, mem, chain, min⟩ := ih b
        have step : (x :: xs).foldl pick (some b) = xs.foldl pick (some b) := by
          simp [List.foldl, pick, hx']
        refine ⟨m, step ▸ hm, ?_, chain, ?_⟩
        · rcases mem with h | h
          · exact .inl h
          · exact .inr (List.mem_cons_of_mem _ h)
        · intro y hy
          rcases List.mem_cons.mp hy with rfl | hy
          · cases h : y.before m
            · rfl
            · rcases chain with rfl | hc
              · rw [hx'] at h; cases h
              · rw [before_trans h hc] at hx'; cases hx'
          · exact min y hy

/-- `newest` picks a listed snapshot that no other listed snapshot precedes. -/
theorem newest_spec {l : List Listed} {m : Listed} (h : newest l = some m) :
    m ∈ l ∧ ∀ y ∈ l, y.before m = false := by
  cases l with
  | nil => simp [newest] at h
  | cons x xs =>
      obtain ⟨m', hm, mem, chain, min⟩ := fold_pick xs x
      have : newest (x :: xs) = xs.foldl pick (some x) := by
        rw [newest_fold]; rfl
      rw [this, hm] at h
      cases h
      refine ⟨?_, fun y hy => ?_⟩
      · rcases mem with rfl | h
        · exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ h
      · rcases List.mem_cons.mp hy with rfl | hy
        · rcases chain with rfl | h
          · exact before_irrefl _
          · exact before_asymm h
        · exact min y hy

theorem newest_some {l : List Listed} {x : Listed} (mem : x ∈ l) : ∃ m, newest l = some m := by
  cases l with
  | nil => cases mem
  | cons y ys =>
      obtain ⟨m, hm, -⟩ := fold_pick ys y
      exact ⟨m, by rw [newest_fold]; exact hm⟩

theorem path_inj {l : List Listed} (paths : (l.map (·.path)).Nodup) {a b : Listed}
    (ha : a ∈ l) (hb : b ∈ l) (same : a.path = b.path) : a = b := by
  induction l with
  | nil => cases ha
  | cons x xs ih =>
      simp only [List.map_cons, List.nodup_cons] at paths
      rcases List.mem_cons.mp ha with rfl | ha' <;> rcases List.mem_cons.mp hb with rfl | hb'
      · rfl
      · exact absurd (List.mem_map.mpr ⟨b, hb', same.symm⟩ : a.path ∈ xs.map (·.path)) paths.1
      · exact absurd (List.mem_map.mpr ⟨a, ha', same⟩ : b.path ∈ xs.map (·.path)) paths.1
      · exact ih paths.2 ha' hb'

/-- With distinct paths, the snapshot no other precedes is the newest. -/
theorem newest_eq_of_min {l : List Listed} {m : Listed} (paths : (l.map (·.path)).Nodup)
    (mem : m ∈ l) (min : ∀ y ∈ l, y.before m = false) : newest l = some m := by
  obtain ⟨m', hm'⟩ := newest_some mem
  obtain ⟨mem', min'⟩ := newest_spec hm'
  rw [hm']
  by_cases hp : m'.path = m.path
  · rw [path_inj paths mem' mem hp]
  · rcases before_total hp with h | h
    · rw [min m' mem'] at h; cases h
    · rw [min' m mem] at h; cases h

/-! ## The choice -/

theorem loadWait_none {c : Checks} {s : Listed} :
    c.loadWait s = none ↔
      c.bodyRefusal s = none ∧ c.keyHeld s = true ∧ c.schemaWait s = none := by
  unfold Checks.loadWait
  cases c.bodyRefusal s <;> cases c.keyHeld s <;> cases c.schemaWait s <;> simp

theorem selected_verified {c : Checks} {listed : List Listed} {s : Listed}
    (h : selectChecked c listed = .ok (some s)) :
    s ∈ listed ∧ c.verifies s = true ∧ ∀ y ∈ listed, c.verifies y = true → y.before s = false := by
  obtain ⟨-, hn⟩ := selectChecked_ok h
  obtain ⟨mem, min⟩ := newest_spec hn
  obtain ⟨listed', verified⟩ := List.mem_filter.mp mem
  exact ⟨listed', verified, fun y hy vy => min y (List.mem_filter.mpr ⟨hy, vy⟩)⟩

/-- Loading never applies refused bytes and never an older snapshot: it
loads only the current newest counting snapshot, after its prefix verified,
with no refusal of its body held here, a key this device accepts and rows
at this app's schema. -/
theorem choose_loads_newest {c : Checks} {listed : List Listed} {s : Listed}
    (h : choose c listed = .load s) :
    selectChecked c listed = .ok (some s) ∧ s ∈ listed ∧ c.verifies s = true ∧
      c.bodyRefusal s = none ∧ c.keyHeld s = true ∧ c.schemaWait s = none := by
  unfold choose at h
  cases hf : listed.find? c.waits with
  | some _ => simp [hf] at h
  | none =>
      cases hn : newest (listed.filter c.verifies) with
      | none => simp [hf, hn] at h
      | some t =>
          cases hw : c.loadWait t with
          | some _ => simp [hf, hn, hw] at h
          | none =>
              simp only [hf, hn, hw, Choice.load.injEq] at h
              subst h
              have sel : selectChecked c listed = .ok (some t) := by
                simp [selectChecked, hf, hn]
              obtain ⟨mem, verified, -⟩ := selected_verified sel
              exact ⟨sel, mem, verified, loadWait_none.mp hw⟩

/-- The reload waits on a snapshot exactly while it is the current newest
counting snapshot and this device cannot load it. -/
theorem choose_waits_iff (c : Checks) (listed : List Listed) (s : Listed) (why : LoadWait) :
    choose c listed = .waits s why ↔
      selectChecked c listed = .ok (some s) ∧ c.loadWait s = some why := by
  constructor
  · intro h
    unfold choose at h
    cases hf : listed.find? c.waits with
    | some _ => simp [hf] at h
    | none =>
        cases hn : newest (listed.filter c.verifies) with
        | none => simp [hf, hn] at h
        | some t =>
            cases hw : c.loadWait t with
            | none => simp [hf, hn, hw] at h
            | some why' =>
                simp only [hf, hn, hw, Choice.waits.injEq] at h
                obtain ⟨rfl, rfl⟩ := h
                exact ⟨by simp [selectChecked, hf, hn], hw⟩
  · rintro ⟨sel, hw⟩
    rw [choose_selected sel, hw]

/-- The snapshot waited on is listed, and its prefix verified. -/
theorem waits_listed {c : Checks} {listed : List Listed} {s : Listed} {why : LoadWait}
    (h : choose c listed = .waits s why) : s ∈ listed ∧ c.verifies s = true :=
  let ⟨mem, verified, _⟩ := selected_verified ((choose_waits_iff c listed s why).mp h).1
  ⟨mem, verified⟩

/-- A newer snapshot this device can load lands: the reload loads it. -/
theorem wait_ends_when_newer_lands {c : Checks} {listed : List Listed} {s n : Listed}
    {why : LoadWait} (waiting : choose c listed = .waits s why) (newer : n.before s = true)
    (verified : c.verifies n = true) (loadable : c.loadWait n = none) :
    choose c (listed ++ [n]) = .load n := by
  obtain ⟨nowait, hs⟩ := selectChecked_ok ((choose_waits_iff c listed s why).mp waiting).1
  have readable : c.waits n = false := by
    simp only [Checks.verifies, Bool.and_eq_true] at verified
    simp [Checks.waits, verified.1]
  have nowait' : (listed ++ [n]).find? c.waits = none := by
    simp [List.find?_append, nowait, readable]
  have hn : newest ((listed ++ [n]).filter c.verifies) = some n := by
    rw [List.filter_append]
    simp only [List.filter_cons, List.filter_nil, verified, ite_true]
    rw [newest_append, hs]
    simp [pick, newer]
  simp only [choose, nowait', hn, loadable]

/-- The listing stops naming the snapshot: the reload no longer waits on
it. The next newest decides, and the listing clears this device's record of
the snapshot (`absent_subject_cleared`). -/
theorem wait_ends_when_unlisted (c : Checks) (listed : List Listed) (s : Listed) (why : LoadWait) :
    choose c (listed.filter (·.path != s.path)) ≠ .waits s why := by
  intro h
  have := (List.mem_filter.mp (waits_listed h).1).2
  simp at this

/-- The reload's own record: Rust `operation_condition` with what
`take_current` or `load_snapshot` returned. A refused body names the
snapshot, whose own record is the refusal; a key names its introduction. -/
def Choice.reason (audience : Audience) (object : Listed → String) (key : Listed → Nat) :
    Choice → Option Reason
  | .update format => some (.updateRequired format)
  | .waits s .body => some (.waits (.object (object s)))
  | .waits s .key => some (.keyUnavailable audience (key s))
  | .waits _ (.schema version) => some (.updateRequired version)
  | _ => none

/-- A wait on a body or a key retries on later passes, so a newer snapshot
or the listing dropping this one ends it; a wait for a newer app schema
retries after an update. -/
theorem waits_retry (audience : Audience) (object : Listed → String) (key : Listed → Nat)
    (s : Listed) (why : LoadWait) :
    ((Choice.waits s why).reason audience object key).map retry =
      some (match why with | .schema _ => .afterUpdate | _ => .automatic) := by
  cases why <;> rfl

/-! ## Never a write retention deleted -/

/-- After `RefusedRetention`'s retention deleted a write, any later version's
reload loads a snapshot covering it or waits on one that does. -/
theorem choose_never_needs_deleted (earlier later : Checks) (listed : List Listed) (w : Nat × Nat)
    (deletes : coversHolding earlier listed w = true) (updated : Checks.Later earlier later listed) :
    ∃ s, s.covers w = true ∧
      (choose later listed = .load s ∨ ∃ why, choose later listed = .waits s why) := by
  obtain ⟨s, sel, covers⟩ :=
    classified_safe _ earlier later listed w deletes updated fun _ _ _ _ never => by simp at never
  refine ⟨s, covers, ?_⟩
  rw [choose_selected sel]
  cases c : later.loadWait s with
  | none => exact .inl rfl
  | some why => exact .inr ⟨why, rfl⟩

/-- A reload spans several audiences, each with the verdicts of the version
that ran retention, the reloading version's, and its listing. When every
audience of a deleted write's parts deleted it, each such audience the
reload loads covers it: no part is skipped. -/
theorem deleted_part_never_skipped (parts : List (Checks × Checks × List Listed))
    (w : Nat × Nat) (deleted : ∀ p ∈ parts, coversHolding p.1 p.2.2 w = true)
    (updated : ∀ p ∈ parts, Checks.Later p.1 p.2.1 p.2.2) :
    ∀ p ∈ parts, ∀ s, choose p.2.1 p.2.2 = .load s → s.covers w = true := by
  intro p hp s loads
  obtain ⟨t, covers, h | ⟨why, h⟩⟩ :=
    choose_never_needs_deleted p.1 p.2.1 p.2.2 w (deleted p hp) (updated p hp)
  · rw [loads] at h; cases h; exact covers
  · rw [loads] at h; cases h

/-! ## Passing over it fails (checked counterexample) -/

/-- §19.1 as written, and Rust's reload before T154: each audience loads the
newest snapshot it can load, passing over a newer one it cannot. -/
def passOver (c : Checks) (listed : List Listed) : Choice :=
  match listed.find? c.waits with
  | some s => .update s.format
  | none => match newest (listed.filter fun s => c.verifies s && c.loadWait s == none) with
    | none => .logs
    | some s => .load s

def Choice.selected : Choice → Option Listed
  | .load s => some s
  | _ => none

/-- Rust's reload: a required write storage no longer holds counts as
deleted when a selected snapshot covers it, and each audience whose own
selection does not cover it skips its part, as having none (§15). -/
def skipsPart (selections : List Choice) (mine : Choice) (w : Nat × Nat) : Bool :=
  selections.any (fun choice => coveredBy choice.selected w) && !coveredBy mine.selected w

/-- Ana (device 1) writes 1 and 2, each with a store part and a Gifts part.
Her Gifts snapshots cover write 1, then writes 1 and 2; her store snapshot
covers both. Ben's version refuses the newer Gifts snapshot's body. -/
def giftsOld : Listed := ⟨(1, 1), [(1, 1)], 1⟩
def giftsNew : Listed := ⟨(1, 2), [(1, 2)], 1⟩
def gifts : List Listed := [giftsOld, giftsNew]
def storeBoth : List Listed := [⟨(1, 3), [(1, 2)], 1⟩]
def refusesGifts : Checks :=
  { passes with bodyRefusal := fun s => if s = giftsNew then some .decryption else none }

/-- Retention deletes write 2: in each part audience the current newest
covers it. Passing over the refused Gifts snapshot, Ben loads the older
Gifts snapshot; the store selection covers write 2, so its absence counts
as a deletion and Gifts skips its part, though Gifts' newest covers it: the
part is lost. In one audience the absence would not count. The waiting
reload waits on the refused snapshot instead. -/
theorem two_audience_pass_over_counterexample :
    coversHolding passes storeBoth (1, 2) = true ∧ coversHolding passes gifts (1, 2) = true ∧
      passOver refusesGifts storeBoth = .load ⟨(1, 3), [(1, 2)], 1⟩ ∧
      passOver refusesGifts gifts = .load giftsOld ∧
      skipsPart [passOver refusesGifts storeBoth, passOver refusesGifts gifts]
        (passOver refusesGifts gifts) (1, 2) = true ∧
      giftsNew.covers (1, 2) = true ∧
      skipsPart [passOver refusesGifts gifts] (passOver refusesGifts gifts) (1, 2) = false ∧
      choose refusesGifts gifts = .waits giftsNew .body := by
  refine ⟨by decide, by decide, rfl, ?_, ?_, by decide, ?_, ?_⟩ <;> decide

end CovenStorelogData.Pending
