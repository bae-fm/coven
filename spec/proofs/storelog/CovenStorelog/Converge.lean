import CovenStorelog.Resolve

namespace CovenStorelog

def entrySet (L : List Nat) : EntrySet := fun w => w ∈ L
def insert (S : EntrySet) (w : Nat) : EntrySet := fun a => decide (a = w) || S a

structure Device where
  received : EntrySet
  result : Result

def initial (M : Log) (n : Nat) : Device := ⟨fun _ => false, resolve M n (fun _ => false)⟩

/-- A receipt publishes the newly resolved state and entry dispositions together.
There is no interval containing a member list for a different received set.
`Ready` is the precondition at the caller, not a timestamp-based wait. -/
def step (M : Log) (n : Nat) (d : Device) (w : Nat) : Device :=
  let S := insert d.received w
  ⟨S, resolve M n S⟩

def Ready (M : Log) (n : Nat) (d : Device) (w : Nat) : Prop :=
  w < n ∧ d.received w = false ∧ ∀ a, hadRead M w a = true → d.received a = true

def IsSpec (M : Log) (n : Nat) (S : EntrySet) (d : Device) : Prop :=
  d.received = S ∧ d.result = resolve M n S

theorem step_spec (M : Log) (n : Nat) {S : EntrySet} {d : Device}
    (h : IsSpec M n S d) (w : Nat) : IsSpec M n (insert S w) (step M n d w) := by
  rcases h with ⟨h, _⟩
  simp [IsSpec, step, h]

theorem closed_insert (M : Log) {S : EntrySet} {w : Nat}
    (hs : Closed M S) (hp : ∀ a, hadRead M w a = true → S a = true) :
    Closed M (insert S w) := by
  intro x hx a ha
  simp only [insert, Bool.or_eq_true, decide_eq_true_eq] at hx ⊢
  rcases hx with rfl | hx
  · exact Or.inr (hp a ha)
  · exact Or.inr (hs x hx a ha)

theorem insert_entrySet (L : List Nat) (w : Nat) :
    insert (entrySet L) w = entrySet (L ++ [w]) := by
  funext a
  simp [insert, entrySet, Bool.or_comm]

/-- The induction is over receipt, and retains the same closed-set meaning as
Appendix B. `resolve` replays all received entries from the first, including
recomputing drops and each entry's historical authority. -/
theorem run_isSpec (M : Log) (n : Nat) {L : List Nat} (h : CausalOrder M L) :
    IsSpec M n (entrySet L) (L.foldl (step M n) (initial M n)) ∧ Closed M (entrySet L) := by
  induction h with
  | nil =>
      constructor
      · exact ⟨rfl, rfl⟩
      · intro w hw; simp [entrySet] at hw
  | @snoc L w _ _ hp ih =>
      have hp' : ∀ a, hadRead M w a = true → entrySet L a = true := by
        simpa [entrySet] using hp
      rw [List.foldl_append]
      simp only [List.foldl_cons, List.foldl_nil]
      rw [← insert_entrySet]
      exact ⟨step_spec M n ih.1 w, closed_insert M ih.2 hp'⟩

theorem causal_ready (M : Log) (n : Nat) {L : List Nat} {w : Nat}
    (hL : CausalOrder M L) (hw : w < n) (hn : w ∉ L)
    (hp : ∀ a, hadRead M w a = true → a ∈ L) :
    Ready M n (L.foldl (step M n) (initial M n)) w := by
  have hs := (run_isSpec M n hL).1.1
  unfold Ready
  rw [hs]
  simpa [entrySet] using And.intro hw (And.intro hn hp)

/-- Timestamp order is a causal materialization order for a closed set; it
does not decide receipt readiness. Both causality assumptions are used here. -/
theorem timestampOrder_causal (M : Log) (n : Nat) (S : EntrySet)
    (hp : ∀ w, w < n → ∀ a, hadRead M w a = true → a < w)
    (hc : Closed M S) : CausalOrder M ((List.range n).filter S) := by
  induction n with
  | zero => exact .nil
  | succ n ih =>
      have hi := ih (fun w hw => hp w (by omega))
      rw [List.range_succ, List.filter_append]
      cases hs : S n with
      | false => simpa [hs] using hi
      | true =>
          simp only [List.filter_cons, hs, List.filter_nil, ite_true]
          apply CausalOrder.snoc hi
          · simp
          · intro a ha
            exact List.mem_filter.mpr ⟨List.mem_range.mpr (hp n (by omega) a ha), hc n hs a ha⟩

theorem full_history_causal (M : Log) (n : Nat) (hv : Valid M n) :
    CausalOrder M (List.range n) := by
  have h := timestampOrder_causal M n (fun _ => true) hv.past_lt
    (fun _ _ _ _ => rfl)
  have he : (List.range n).filter (fun _ => true) = List.range n :=
    List.filter_eq_self.mpr (fun _ _ => rfl)
  rwa [he] at h

/-- The recursively computed author view is the result of any actual causal
receipt of exactly the author's recorded past, not a supplied permission bit. -/
theorem author_view_reached (M : Log) (w : Nat) (L : List Nat)
    (hc : CausalOrder M L) (hs : ∀ a, a ∈ L ↔ hadRead M w a = true) :
    (L.foldl (step M w) (initial M w)).result.state = authorView M w := by
  have he : entrySet L = hadRead M w := by
    funext a
    apply Bool.eq_iff_iff.mpr
    simp [entrySet, hs a]
  rw [(run_isSpec M w hc).1.2, he]
  rfl

theorem author_past_causal (M : Log) (n : Nat) (hv : Valid M n) {w : Nat} (hw : w < n) :
    CausalOrder M ((List.range w).filter (hadRead M w)) :=
  timestampOrder_causal M w (hadRead M w) (fun a ha => hv.past_lt a (by omega))
    (hv.past_closed w hw)

theorem isSpec_unique (M : Log) (n : Nat) {S : EntrySet} {a b : Device}
    (ha : IsSpec M n S a) (hb : IsSpec M n S b) : a = b := by
  cases a
  cases b
  simp only [IsSpec] at ha hb
  obtain ⟨rfl, rfl⟩ := ha
  obtain ⟨rfl, rfl⟩ := hb
  rfl

/-- Every two causal orders of the same set yield identical roles, devices,
circles, versions, resets, kept identities, and drops in this model. -/
theorem storelog_converges (M : Log) (n : Nat) {A B : List Nat}
    (ha : CausalOrder M A) (hb : CausalOrder M B)
    (hset : ∀ w, w ∈ A ↔ w ∈ B) :
    A.foldl (step M n) (initial M n) = B.foldl (step M n) (initial M n) := by
  have he : entrySet A = entrySet B := by funext w; simp [entrySet, hset w]
  exact isSpec_unique M n (he ▸ (run_isSpec M n ha).1) (run_isSpec M n hb).1

end CovenStorelog
