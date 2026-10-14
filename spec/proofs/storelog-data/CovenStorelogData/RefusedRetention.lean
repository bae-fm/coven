import CovenStorelogData.PendingScans

/-! §15, §19.1: log retention and snapshots this device refused.

§19.1 lets a different installed version check a refused object's bytes
once more, whatever check failed. A snapshot this version refuses can
therefore be the one a later version, or a device whose check passes,
selects as the audience's newest. Retention that passes over it judges
deletion against an older snapshot and can delete a write that newest
snapshot leaves out:
- passing over a refused body (Rust's retention, which uses its loading
  candidates) fails: `refused_body_retention_counterexample`, and the same
  for a snapshot whose key this device has not authorized,
  `missing_key_retention_counterexample`;
- passing over a refused prefix (§15 as written: "a refused prefix
  establishes nothing and does not hold deletion") fails:
  `refused_prefix_retention_counterexample`.

The safe rule judges by the newest verified prefix whatever its body, and
holds while any listed prefix waits or is refused: `holding_safe`,
`holding_never_skips`. A body refusal never holds it
(`retention_ignores_bodies`); a refused prefix holds it until a version
verifies that prefix or the listing stops naming it
(`refused_prefix_holds`, `holding_without_refusals`). A hold that exempts
some refusal kinds is safe exactly when no later version verifies a prefix
refused with those kinds (`classified_safe`); the spec declares no such
kind (`every_refusal_after_update`, `every_kind_rechecked`). -/
namespace CovenStorelogData.Pending

/-! ## Refusal kinds and when they are checked again -/

/-- api.md `Refusal`: the failed check that refuses an object's bytes (§19.1). -/
inductive RefusalKind where
  | decryption | signature | parse | invalidWrite | notAuthorized
  | invalidCausality | wrongIdentity | contentHash
  deriving DecidableEq, Repr

/-- A refusal's pending reason: `Refused(kind)`. The model's `Reason.refused`
omits the variant, which no retry decision reads. -/
def RefusalKind.reason (_ : RefusalKind) : Reason := .refused

/-- api.md `PendingRecord::retry`: every refusal, of any kind, waits for an
update. None is final. -/
theorem every_refusal_after_update (k : RefusalKind) : retry k.reason = .afterUpdate := rfl

/-- A refusal this device recorded, with the coven version that made it. -/
structure LocalRefusal where
  kind : RefusalKind
  version : Nat
  deriving DecidableEq, Repr

/-- §19.1: "A different installed version permits one new attempt.
Reopening the same version does not retry the same immutable bytes." -/
def LocalRefusal.mayRecheck (r : LocalRefusal) (installed : Nat) : Bool :=
  installed != r.version

/-- Every kind of refusal is checked again by a newer version: the spec
makes none of them permanent across versions. -/
theorem every_kind_rechecked (k : RefusalKind) (version installed : Nat)
    (newer : version < installed) : (LocalRefusal.mk k version).mayRecheck installed = true := by
  simp [LocalRefusal.mayRecheck]
  omega

/-- The refusing version never checks the same bytes again. -/
theorem same_version_keeps (r : LocalRefusal) : r.mayRecheck r.version = false := by
  simp [LocalRefusal.mayRecheck]

/-! ## What one version finds -/

/-- What one installed version's checks find for each listed snapshot. The
signed positions belong to the object (`Listed.writes`); only the verdicts
depend on the version. `bodyRefusal` is a refusal this device recorded when
loading; `keyHeld` is whether a key introduction it accepts seals the rows. -/
structure Checks where
  /-- The newest prefix format this version reads. -/
  supported : Nat
  prefixRefusal : Listed → Option RefusalKind
  bodyRefusal : Listed → Option RefusalKind
  keyHeld : Listed → Bool

/-- The prefix waits for an update (§15). The registration wait is not modeled. -/
def Checks.waits (c : Checks) (s : Listed) : Bool := !s.readable c.supported

/-- The prefix is read and its check passes: its positions establish coverage. -/
def Checks.verifies (c : Checks) (s : Listed) : Bool :=
  s.readable c.supported && (c.prefixRefusal s).isNone

/-- A loading candidate: a verified prefix, no recorded body refusal and a
held key. -/
def Checks.loadable (c : Checks) (s : Listed) : Bool :=
  c.verifies s && (c.bodyRefusal s).isNone && c.keyHeld s

def coveredBy : Option Listed → Nat × Nat → Bool
  | some s, w => s.covers w
  | none, _ => false

/-! ## The reload at a version -/

/-- §15's reload, and its current newest counting snapshot: wait while a
listed prefix waits for an update; otherwise the newest verified prefix,
whatever this device found in its body. A refused prefix establishes
nothing (§19.1). -/
def selectChecked (c : Checks) (listed : List Listed) : Except Reason (Option Listed) :=
  match listed.find? c.waits with
  | some s => .error (.updateRequired s.format)
  | none => .ok (newest (listed.filter c.verifies))

/-- With no refused prefix this is `select`, the reload of
`reload_waits_for_newer`. -/
theorem selectChecked_eq_select (c : Checks) (listed : List Listed)
    (clean : ∀ s ∈ listed, c.prefixRefusal s = none) :
    selectChecked c listed = select c.supported listed := by
  unfold selectChecked select
  cases h : listed.find? c.waits with
  | some s =>
      have same : (listed.find? fun s => !s.readable c.supported) = some s := h
      rw [same]
  | none =>
      have all : listed.filter c.verifies = listed := by
        apply List.filter_eq_self.mpr
        intro s hs
        have := List.find?_eq_none.mp h s hs
        simp only [Checks.waits, Bool.not_eq_true', Bool.not_eq_false] at this
        simp [Checks.verifies, this, clean s hs]
      have same : (listed.find? fun s => !s.readable c.supported) = none := h
      rw [same, all]

/-- What §15 does with a required write that a successful GET finds missing
and a fresh catalog shows was covered: load the current newest snapshot if
it reaches the write, or skip the write as having no part in this audience.
`load` waits instead when that snapshot's body is refused here (§19.1). -/
inductive MissingWrite where
  | waits (reason : Reason)
  | load (s : Listed)
  | skip
  deriving DecidableEq, Repr

def missingDecision (c : Checks) (listed : List Listed) (w : Nat × Nat) : MissingWrite :=
  match selectChecked c listed with
  | .error reason => .waits reason
  | .ok selected => match selected with
    | some s => if s.covers w then .load s else .skip
    | none => .skip

/-! ## Retention rules -/

/-- Rust `retain_snapshot_objects`: hold while a listed prefix waits for an
update; otherwise judge by the newest loading candidate, passing over
refused prefixes, refused bodies and snapshots without a held key. The other
§15 gates (historical coverage a day old, posted positions or 30 storage
days, finality) are separate conjuncts, taken to pass. -/
def coversPassingOver (c : Checks) (listed : List Listed) (w : Nat × Nat) : Bool :=
  !listed.any c.waits && coveredBy (newest (listed.filter c.loadable)) w

/-- A listed snapshot holds retention when its prefix waits, or is refused
with a kind not classified `permanent`. -/
def Checks.holds (permanent : RefusalKind → Bool) (c : Checks) (s : Listed) : Bool :=
  c.waits s || match c.prefixRefusal s with
    | some k => !permanent k
    | none => false

/-- Retention that holds as `Checks.holds` says and otherwise judges by the
newest verified prefix, whatever its body or key. -/
def coversClassified (permanent : RefusalKind → Bool) (c : Checks) (listed : List Listed)
    (w : Nat × Nat) : Bool :=
  !listed.any (c.holds permanent) && coveredBy (newest (listed.filter c.verifies)) w

/-- §15 as written: the current newest counting snapshot covers the write; a
prefix waiting for an update holds deletion, and a refused prefix
"establishes nothing and does not hold deletion". -/
def coversAsWritten : Checks → List Listed → Nat × Nat → Bool := coversClassified fun _ => true

/-- The safe rule: every refused prefix holds, since a newer version may
verify it (§19.1). -/
def coversHolding : Checks → List Listed → Nat × Nat → Bool := coversClassified fun _ => false

/-- A later installed version reads every format the earlier one read and
verifies every prefix the earlier one verified. A later version that refuses
a prefix the earlier one verified means the earlier accepted bytes it should
have refused; that is outside the model. -/
structure Checks.Later (earlier later : Checks) (listed : List Listed) : Prop where
  formats : earlier.supported ≤ later.supported
  keeps : ∀ s ∈ listed, earlier.verifies s = true → later.verifies s = true

/-- A classification is sound when no later version verifies a listed prefix
the earlier version refused with a kind classified permanent. -/
def Sound (permanent : RefusalKind → Bool) (earlier later : Checks) (listed : List Listed) :
    Prop :=
  ∀ s ∈ listed, ∀ k, earlier.prefixRefusal s = some k → permanent k = true →
    later.verifies s = false

theorem Checks.holds_false {permanent : RefusalKind → Bool} {c : Checks} {s : Listed}
    (h : c.holds permanent s = false) :
    s.readable c.supported = true ∧ ∀ k, c.prefixRefusal s = some k → permanent k = true := by
  unfold Checks.holds Checks.waits at h
  rw [Bool.or_eq_false_iff] at h
  obtain ⟨hw, hk⟩ := h
  refine ⟨by simpa using hw, fun k hr => ?_⟩
  rw [hr] at hk
  simpa using hk

/-- The general rule's safety: when it deletes a write, every later version
whose verdicts the classification predicts selects a current newest snapshot
that covers the write, so its reload never needs it from the log. -/
theorem classified_safe (permanent : RefusalKind → Bool) (earlier later : Checks)
    (listed : List Listed) (w : Nat × Nat)
    (deletes : coversClassified permanent earlier listed w = true)
    (updated : Checks.Later earlier later listed) (sound : Sound permanent earlier later listed) :
    ∃ s, selectChecked later listed = .ok (some s) ∧ s.covers w = true := by
  simp only [coversClassified, Bool.and_eq_true, Bool.not_eq_true', List.any_eq_false] at deletes
  obtain ⟨free, covers⟩ := deletes
  have clear : ∀ s ∈ listed, earlier.holds permanent s = false := fun s hs => by
    simpa using free s hs
  have same : listed.filter later.verifies = listed.filter earlier.verifies := by
    apply List.filter_congr
    intro s hs
    obtain ⟨readable, perm⟩ := Checks.holds_false (clear s hs)
    cases hv : earlier.verifies s with
    | true => exact updated.keeps s hs hv
    | false =>
        cases hr : earlier.prefixRefusal s with
        | none => simp [Checks.verifies, readable, hr] at hv
        | some k => exact sound s hs k hr (perm k hr)
  have nowait : listed.find? later.waits = none := by
    apply List.find?_eq_none.mpr
    intro s hs
    have readable := (Checks.holds_false (clear s hs)).1
    simp only [Listed.readable, decide_eq_true_eq] at readable
    have := updated.formats
    simp only [Checks.waits, Listed.readable, Bool.not_eq_true', decide_eq_false_iff_not,
      Nat.not_le]
    omega
  cases hn : newest (listed.filter earlier.verifies) with
  | none => simp [coveredBy, hn] at covers
  | some s =>
      refine ⟨s, ?_, by simpa [coveredBy, hn] using covers⟩
      simp [selectChecked, nowait, same, hn]

/-- Its missing-write decision: a write it deleted is never skipped. The
later reload loads the current newest, which covers the write; if this
device refuses that snapshot's body, the reload waits (§15, §19.1). -/
theorem classified_never_skips (permanent : RefusalKind → Bool) (earlier later : Checks)
    (listed : List Listed) (w : Nat × Nat)
    (deletes : coversClassified permanent earlier listed w = true)
    (updated : Checks.Later earlier later listed) (sound : Sound permanent earlier later listed) :
    ∃ s, missingDecision later listed w = .load s ∧ s.covers w = true := by
  obtain ⟨s, sel, covers⟩ := classified_safe permanent earlier later listed w deletes updated sound
  exact ⟨s, by simp [missingDecision, sel, covers], covers⟩

/-- The safe rule needs no classification premise. -/
theorem holding_safe (earlier later : Checks) (listed : List Listed) (w : Nat × Nat)
    (deletes : coversHolding earlier listed w = true) (updated : Checks.Later earlier later listed)
    (target : Nat → Nat) :
    ∃ s, selectChecked later listed = .ok (some s) ∧ needs s target w = false := by
  obtain ⟨s, sel, covers⟩ :=
    classified_safe _ earlier later listed w deletes updated fun _ _ _ _ never => by simp at never
  exact ⟨s, sel, by simp [needs, covers]⟩

theorem holding_never_skips (earlier later : Checks) (listed : List Listed) (w : Nat × Nat)
    (deletes : coversHolding earlier listed w = true) (updated : Checks.Later earlier later listed) :
    ∃ s, missingDecision later listed w = .load s ∧ s.covers w = true :=
  classified_never_skips _ earlier later listed w deletes updated fun _ _ _ _ never => by
    simp at never

/-- §15 as written is safe while this version refuses no listed prefix: a
refused body alone does not change the current newest snapshot. -/
theorem as_written_safe_for_bodies (earlier later : Checks) (listed : List Listed)
    (w : Nat × Nat) (deletes : coversAsWritten earlier listed w = true)
    (updated : Checks.Later earlier later listed)
    (prefixes : ∀ s ∈ listed, earlier.prefixRefusal s = none) :
    ∃ s, missingDecision later listed w = .load s ∧ s.covers w = true :=
  classified_never_skips _ earlier later listed w deletes updated fun s hs k hr _ => by
    rw [prefixes s hs] at hr
    cases hr

/-! ## What the safe rule costs -/

/-- Retention never reads a body or key verdict: a refused body, or a
snapshot this device cannot load, holds nothing. -/
theorem retention_ignores_bodies (permanent : RefusalKind → Bool) (c : Checks)
    (body : Listed → Option RefusalKind) (key : Listed → Bool) (listed : List Listed)
    (w : Nat × Nat) :
    coversClassified permanent { c with bodyRefusal := body, keyHeld := key } listed w =
      coversClassified permanent c listed w := rfl

/-- A listed snapshot whose prefix this version refused holds every write of
its audience, whatever the other snapshots cover. -/
theorem refused_prefix_holds (c : Checks) (listed : List Listed) (s : Listed) (k : RefusalKind)
    (present : s ∈ listed) (refused : c.prefixRefusal s = some k) (w : Nat × Nat) :
    coversHolding c listed w = false := by
  have held : listed.any (c.holds fun _ => false) = true :=
    List.any_eq_true.mpr ⟨s, present, by simp [Checks.holds, refused]⟩
  simp [coversHolding, coversClassified, held]

/-- The hold ends when no listed prefix is refused, because a newer version
verified it or the listing stopped naming it: then the safe rule is
T149's `retentionCoversAll`. -/
theorem holding_without_refusals (c : Checks) (listed : List Listed) (w : Nat × Nat)
    (clean : ∀ s ∈ listed, c.prefixRefusal s = none) :
    coversHolding c listed w = retentionCoversAll c.supported listed w := by
  have holds : ∀ s ∈ listed, c.holds (fun _ => false) s = !s.readable c.supported := by
    intro s hs
    simp [Checks.holds, Checks.waits, clean s hs]
  have verifies : listed.filter c.verifies = listed.filter (·.readable c.supported) :=
    List.filter_congr fun s hs => by simp [Checks.verifies, clean s hs]
  have all : (!listed.any (c.holds fun _ => false)) = listed.all (·.readable c.supported) := by
    cases h : listed.all (·.readable c.supported) with
    | true =>
        have := List.all_eq_true.mp h
        simp only [Bool.not_eq_true', List.any_eq_false]
        intro s hs
        simp [holds s hs, this s hs]
    | false =>
        obtain ⟨s, hs, bad⟩ := List.all_eq_false.mp h
        have : listed.any (c.holds fun _ => false) = true :=
          List.any_eq_true.mpr ⟨s, hs, by simpa [holds s hs] using bad⟩
        simp [this]
  unfold coversHolding coversClassified retentionCoversAll retentionCovers
  rw [all, verifies]
  cases newest (listed.filter (·.readable c.supported)) <;> rfl

/-! ## Checked histories

Ana (device 1) and Ben (device 2) snapshot the store concurrently, both in
format 1. Ana's snapshot covers her writes through 10 and Ben's through 2;
Ben's covers Ana through 3 and himself through 20, so it covers more writes
and is the store's newest (§15). Ana has uploaded her writes through 10, so
her reload's fixed target includes them. -/

def anaOwn : Listed := ⟨(1, 1), [(1, 10), (2, 2)], 1⟩
def benNewest : Listed := ⟨(2, 1), [(1, 3), (2, 20)], 1⟩
def store : List Listed := [anaOwn, benNewest]
def anaTarget : Nat → Nat := fun d => if d = 1 then 10 else 20

/-- A version whose checks of both snapshots pass. -/
def passes : Checks := ⟨1, fun _ => none, fun _ => none, fun _ => true⟩

/-- Ana's version refuses Ben's body, say a parse bug the next version fixes. -/
def refusesBody : Checks := { passes with bodyRefusal := fun s => if s = benNewest then some .parse else none }

/-- Ana's version refuses Ben's prefix signature, say a key-resolution bug. -/
def refusesPrefix : Checks :=
  { passes with prefixRefusal := fun s => if s = benNewest then some .signature else none }

/-- Ana's device has not authorized the key introduction sealing Ben's rows. -/
def lacksKey : Checks := { passes with keyHeld := fun s => s != benNewest }

theorem store_later (earlier : Checks) (formats : earlier.supported ≤ 1) :
    Checks.Later earlier passes store :=
  ⟨formats, fun s hs _ => by simp only [store, List.mem_cons, List.not_mem_nil] at hs
                             rcases hs with rfl | rfl | h <;> first | decide | cases h⟩

/-- Rust's retention, passing over the body Ana refused, judges by her own
snapshot and deletes her write 4. A version whose check passes (Ana's after
the update, or any device on it) selects Ben's snapshot, needs write 4, and
§15's missing-write rule skips it as having no part: its row changes are
lost. §15 as written and the safe rule both keep it. -/
theorem refused_body_retention_counterexample :
    coversPassingOver refusesBody store (1, 4) = true ∧
      Checks.Later refusesBody passes store ∧
      selectChecked passes store = .ok (some benNewest) ∧
      needs benNewest anaTarget (1, 4) = true ∧
      missingDecision passes store (1, 4) = .skip ∧
      coversAsWritten refusesBody store (1, 4) = false ∧
      coversHolding refusesBody store (1, 4) = false :=
  ⟨by decide, store_later _ (Nat.le_refl _), rfl, by decide, rfl, by decide, by decide⟩

/-- The same failure when Rust's retention passes over a snapshot whose key
introduction Ana has not authorized. -/
theorem missing_key_retention_counterexample :
    coversPassingOver lacksKey store (1, 4) = true ∧
      Checks.Later lacksKey passes store ∧
      missingDecision passes store (1, 4) = .skip ∧
      coversAsWritten lacksKey store (1, 4) = false :=
  ⟨by decide, store_later _ (Nat.le_refl _), rfl, by decide⟩

/-- §15 as written: Ana's version refuses Ben's prefix, which "establishes
nothing and does not hold deletion", so retention judges by her own snapshot
and deletes write 4. §19.1 lets her updated version check that prefix again;
it verifies, the reload selects Ben's snapshot and skips write 4. Treating
the signature refusal as permanent is unsound here; the safe rule keeps the
write. -/
theorem refused_prefix_retention_counterexample :
    coversAsWritten refusesPrefix store (1, 4) = true ∧
      (LocalRefusal.mk .signature 1).mayRecheck 2 = true ∧
      Checks.Later refusesPrefix passes store ∧
      selectChecked passes store = .ok (some benNewest) ∧
      needs benNewest anaTarget (1, 4) = true ∧
      missingDecision passes store (1, 4) = .skip ∧
      ¬ Sound (fun _ => true) refusesPrefix passes store ∧
      coversHolding refusesPrefix store (1, 4) = false := by
  refine ⟨by decide, rfl, store_later _ (Nat.le_refl _), rfl, by decide, rfl, ?_, by decide⟩
  intro sound
  have := sound benNewest (by simp [store]) .signature (by decide) rfl
  exact absurd this (by decide)

/-- Once Ana's version verifies both prefixes, the safe rule deletes write 4
only if the newest, Ben's, covers it, and it does not; it deletes write 3,
which Ben's covers, whatever Ana found in Ben's body. -/
theorem holding_judges_by_newest :
    coversHolding refusesBody store (1, 4) = false ∧
      coversHolding refusesBody store (1, 3) = true ∧
      coversHolding refusesPrefix store (1, 3) = false :=
  ⟨by decide, by decide, by decide⟩

end CovenStorelogData.Pending
