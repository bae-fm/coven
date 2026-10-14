import CovenStorelogData.ReloadWait

/-! §15, §19.1: three decisions that compare against a snapshot other than
the current newest, checked against the waiting reload.

- Deleting this device's own superseded snapshot: Rust deletes one once the
  newest snapshot this device can load covers everything it covers. That
  snapshot's prefix verified and it precedes the deleted one, so the
  deletion changes neither any version's reload choice nor its retention:
  `superseded_deletion_invisible`.
- Snapshot growth: Rust measures growth since the newest snapshot this
  device can load. Growth only decides when a snapshot is written. A new
  snapshot that covers everything the current newest covers, as §15's
  preparation rule requires, keeps every write retention deleted covered,
  whatever triggered it: `landing_keeps_deletions`. Judged against the
  newest loadable snapshot instead, that preparation rule would lose a
  deleted write: `loadable_preparation_counterexample`.
- A reload that needs a write this device refused: Rust records the write's
  refusal as the reload's own reason, which retries only after an update, so
  the reload stays blocked after a snapshot covering the gap lands, though
  §15 lets that snapshot load without the write:
  `refused_write_reload_counterexample`. A wait on the refused write would
  retry and load it: `waiting_on_the_write_loads`. -/
namespace CovenStorelogData.Pending
open CovenStorelog

/-! ## Deleting an own superseded snapshot -/

private theorem find?_filter_same {p q : Listed → Bool} (l : List Listed)
    (kept : ∀ x ∈ l, q x = false → p x = false) : (l.filter q).find? p = l.find? p := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have rest := ih fun y hy => kept y (List.mem_cons_of_mem _ hy)
      cases hq : q x
      · have hp := kept x List.mem_cons_self hq
        simp [hq, hp, rest]
      · simp [hq, List.find?_cons, rest]

private theorem any_filter_same {p q : Listed → Bool} (l : List Listed)
    (kept : ∀ x ∈ l, q x = false → p x = false) : (l.filter q).any p = l.any p := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have rest := ih fun y hy => kept y (List.mem_cons_of_mem _ hy)
      cases hq : q x
      · have hp := kept x List.mem_cons_self hq
        simp [hq, hp, rest]
      · simp [hq, rest]

/-- Removing a snapshot that a kept one precedes leaves the newest. -/
private theorem newest_without {l : List Listed} (paths : (l.map (·.path)).Nodup)
    {o c : Listed} (hc : c ∈ l) (newer : c.before o = true) (keep : Listed → Bool)
    (only : ∀ x ∈ l, keep x = false → x = o) :
    newest (l.filter keep) = newest l := by
  obtain ⟨m, hm⟩ := newest_some hc
  obtain ⟨mem, min⟩ := newest_spec hm
  have kept : keep m = true := by
    cases h : keep m
    · have := only m mem h
      subst this
      rw [min c hc] at newer
      cases newer
    · rfl
  rw [hm]
  exact newest_eq_of_min (List.Nodup.sublist ((List.filter_sublist).map _) paths)
    (List.mem_filter.mpr ⟨mem, kept⟩) fun y hy => min y (List.mem_filter.mp hy).1

/-- Deleting a listed snapshot `o` that a listed snapshot `c`, verified at
this version, precedes in §15's order changes neither the reload's choice
nor log retention, at this version or any later one: `o` is never the
current newest while `c` is listed. Rust's own-snapshot deletion compares
against the newest snapshot this device can load, which is verified and
precedes every other one it can load that follows the boundary. -/
theorem superseded_deletion_invisible (earlier later : Checks) (listed : List Listed)
    (paths : (listed.map (·.path)).Nodup) (o c : Listed) (ho : o ∈ listed) (hc : c ∈ listed)
    (vo : earlier.verifies o = true) (vc : earlier.verifies c = true) (newer : c.before o = true)
    (updated : Checks.Later earlier later listed) :
    choose later (listed.filter (·.path != o.path)) = choose later listed ∧
      ∀ permanent w, coversClassified permanent later (listed.filter (·.path != o.path)) w =
        coversClassified permanent later listed w := by
  have vo' := updated.keeps o ho vo
  have vc' := updated.keeps c hc vc
  have only : ∀ x ∈ listed, (x.path != o.path) = false → x = o := fun x hx h =>
    path_inj paths hx ho (by simpa using h)
  have distinct : c.path ≠ o.path := fun h => by
    rw [path_inj paths hc ho h, before_irrefl] at newer; cases newer
  have readable : later.waits o = false := by
    simp only [Checks.verifies, Bool.and_eq_true] at vo'
    simp [Checks.waits, vo'.1]
  have waits : (listed.filter (·.path != o.path)).find? later.waits = listed.find? later.waits :=
    find?_filter_same listed fun x hx h => by rw [only x hx h]; exact readable
  have filtered : (listed.filter (·.path != o.path)).filter later.verifies =
      (listed.filter later.verifies).filter (·.path != o.path) := by
    simp only [List.filter_filter, Bool.and_comm]
  have same : newest ((listed.filter (·.path != o.path)).filter later.verifies) =
      newest (listed.filter later.verifies) := by
    rw [filtered]
    exact newest_without (List.Nodup.sublist ((List.filter_sublist).map _) paths)
      (List.mem_filter.mpr ⟨hc, vc'⟩) newer _
      (fun x hx h => only x (List.mem_filter.mp hx).1 h)
  refine ⟨by simp only [choose, waits, same], fun permanent w => ?_⟩
  have holds : (listed.filter (·.path != o.path)).any (later.holds permanent) =
      listed.any (later.holds permanent) :=
    any_filter_same listed fun x hx h => by
      rw [only x hx h]
      simp only [Checks.verifies, Bool.and_eq_true, Option.isNone_iff_eq_none] at vo'
      simp [Checks.holds, Checks.waits, vo'.1, vo'.2]
  simp only [coversClassified, holds, same]

/-! ## Snapshot growth -/

/-- §15's preparation rule (IO `canPrepare`): the new snapshot covers every
write the snapshot it compares against covers. -/
def Listed.dominates (n s : Listed) : Prop := ∀ w, s.covers w = true → n.covers w = true

/-- A write retention deleted stays covered once a new snapshot lands, if
the new one covers everything the current newest counting snapshot covers
for the reloading version. What triggered the write does not appear. -/
theorem landing_keeps_deletions (earlier later : Checks) (listed : List Listed) (w : Nat × Nat)
    (n : Listed) (deletes : coversHolding earlier listed w = true)
    (updated : Checks.Later earlier later listed) (readable : later.waits n = false)
    (prepared : ∀ s, selectChecked later listed = .ok (some s) → n.dominates s) :
    ∃ s, s.covers w = true ∧
      (choose later (listed ++ [n]) = .load s ∨ ∃ why, choose later (listed ++ [n]) = .waits s why) := by
  obtain ⟨s, sel, covers⟩ :=
    classified_safe _ earlier later listed w deletes updated fun _ _ _ _ never => by simp at never
  obtain ⟨nowait, hs⟩ := selectChecked_ok sel
  have nowait' : (listed ++ [n]).find? later.waits = none := by
    simp [List.find?_append, nowait, readable]
  have pick_covers : ∃ t, selectChecked later (listed ++ [n]) = .ok (some t) ∧ t.covers w = true := by
    cases hv : later.verifies n
    · exact ⟨s, by simp [selectChecked, nowait', hv, hs], covers⟩
    · cases hb : n.before s
      · exact ⟨s, by simp [selectChecked, nowait', hv, newest_append, hs, pick,
          hb], covers⟩
      · exact ⟨n, by simp [selectChecked, nowait', hv, newest_append, hs, pick,
          hb], prepared s sel w covers⟩
  obtain ⟨t, sel', covers'⟩ := pick_covers
  refine ⟨t, covers', ?_⟩
  rw [choose_selected sel']
  cases later.loadWait t with
  | none => exact .inl rfl
  | some why => exact .inr ⟨why, rfl⟩

/-- `RefusedRetention`'s store: Ana's version refuses the body of Ben's
newest snapshot (his writes through 20), so the newest she can load is her
own (Ben's through 2). Retention judges by Ben's prefix and deletes Ben's
write 5. A snapshot prepared to cover only Ana's own, her writes through 30
and Ben's through 2, covers more writes and becomes the newest; it does not
cover Ben's write 5, so the reload skips it. -/
def loadableOnly : Listed := ⟨(1, 2), [(1, 30), (2, 2)], 1⟩

theorem loadable_preparation_counterexample :
    coversHolding refusesBody store (2, 5) = true ∧
      choose refusesBody store = .waits benNewest .body ∧
      loadableOnly.dominates anaOwn ∧ ¬ loadableOnly.dominates benNewest ∧
      missingDecision passes (store ++ [loadableOnly]) (2, 5) = .skip := by
  refine ⟨by decide, by decide, ?_, ?_, by decide⟩
  · intro w h
    simp only [Listed.covers, anaOwn, loadableOnly, List.any_cons, List.any_nil, Bool.or_false,
      Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h ⊢
    omega
  · intro h
    have := h (2, 5) (by decide)
    simp [loadableOnly, Listed.covers] at this

/-! ## A reload that needs a refused write -/

/-- A refused write stops its log at that number (§19.1). -/
def blocked (refused : List (Nat × Nat)) (w : Nat × Nat) : Bool :=
  refused.any fun r => r.1 == w.1 && decide (r.2 ≤ w.2)

/-- One attempt of a reload of one audience: wait as `choose` says, or read
every write up to the fixed target that the selection does not cover. -/
inductive Attempt where
  | waits (choice : Choice)
  | blockedAt (write : Nat × Nat)
  | loads (s : Listed)
  deriving DecidableEq, Repr

def attempt (c : Checks) (listed : List Listed) (target : Nat → Nat)
    (refused : List (Nat × Nat)) (logs : List (Nat × Nat)) : Attempt :=
  match choose c listed with
  | .load s => match logs.find? fun w => needs s target w && blocked refused w with
    | some w => .blockedAt w
    | none => .loads s
  | other => .waits other

/-- Rust's record for a blocked reload: the write's own refusal
(`pending_logs::required`, `record_operation_error`). -/
def blockedRecord : Reason := .refused

/-- Rust `operation_can_retry` at the same installed version: work is driven
again only while its record retries automatically. -/
def drivenAgain (record : Reason) : Bool := retry record == .automatic

/-- Carol's store listing holds Ana's snapshot through Ana 3 and Ben 10, and
an older one through Ana 4. Carol's version refuses Ana's write 4. Her
reload's target reaches Ana 4 and Ben 10. -/
def throughThree : Listed := ⟨(1, 1), [(1, 3), (2, 10)], 1⟩
def throughFour : Listed := ⟨(2, 1), [(1, 4), (2, 1)], 1⟩
def coversGap : Listed := ⟨(2, 2), [(1, 4), (2, 10)], 1⟩
def carolTarget : Nat → Nat := fun d => if d = 1 then 4 else 10
def carolLogs : List (Nat × Nat) := [(1, 4)] ++ (List.range 10).map fun n => (2, n + 1)

/-- The reload selects Ana's snapshot through 3, needs write 4 and is
blocked; Rust records the write's refusal as its reason, which is not driven
again at this version. Once a snapshot covering the gap lands, the reload
would load it without the write (§15: "A snapshot covering it can load
without reading it"), but it is never attempted before an update. -/
theorem refused_write_reload_counterexample :
    attempt passes [throughThree, throughFour] carolTarget [(1, 4)] carolLogs = .blockedAt (1, 4) ∧
      drivenAgain blockedRecord = false ∧
      attempt passes [throughThree, throughFour, coversGap] carolTarget [(1, 4)] carolLogs =
        .loads coversGap := by
  decide

/-- Recording the reload as waiting on the refused write, as the reload
waits on a refused snapshot, retries automatically: the next attempt loads
the covering snapshot. The write's own refusal stays its record. -/
theorem waiting_on_the_write_loads (path : String) :
    drivenAgain (.waits (.object path)) = true ∧
      attempt passes [throughThree, throughFour, coversGap] carolTarget [(1, 4)] carolLogs =
        .loads coversGap :=
  ⟨rfl, by decide⟩

end CovenStorelogData.Pending
