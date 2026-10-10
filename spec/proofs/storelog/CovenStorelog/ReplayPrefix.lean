import CovenStorelog.Resolve

/-! A settled timestamp prefix is unaffected by a suffix that cannot conflict
with it. The proof uses the actual restart replay, including permanent drops
within a replay; it does not replace replay with pairwise winner selection. -/
namespace CovenStorelog
namespace ReplayPrefix

variable {realize : State → Nat → Entry → Option State}
variable (M : Log) (views : Nat → State) (conflict prefer : Nat → Nat → Bool)

abbrev run := scan M views (conflict := conflict) (prefer := prefer) (realize := realize)

theorem scan_append (xs ys : List Nat) (r : Result) :
    run (realize := realize) M views conflict prefer (xs ++ ys) r =
      match run (realize := realize) M views conflict prefer xs r with
      | .complete q => run (realize := realize) M views conflict prefer ys q
      | .restart ds => .restart ds := by
  induction xs generalizing r with
  | nil => rfl
  | cons w ws ih =>
      simp only [List.cons_append, run, scan]
      split
      · exact ih r
      · split
        · exact ih _
        · split
          · exact ih _
          · cases realize r.state w (M w) with
            | none => exact ih _
            | some next =>
                simp only
                split
                · exact ih _
                · split
                  · exact ih _
                  · rfl

/-- A completed pass never removes a kept or dropped identity; only a restart
can remove a kept identity. Outside the todo list it adds neither. -/
theorem complete_frame {todo : List Nat} {r q : Result}
    (h : run (realize := realize) M views conflict prefer todo r = .complete q) (x : Nat) :
    (x ∈ r.kept → x ∈ q.kept) ∧ (x ∈ r.dropped → x ∈ q.dropped) ∧
    (x ∉ todo → (x ∈ q.kept ↔ x ∈ r.kept) ∧
      (x ∈ q.dropped ↔ x ∈ r.dropped)) := by
  induction todo generalizing r with
  | nil => cases h; exact ⟨id, id, fun _ => ⟨Iff.rfl, Iff.rfl⟩⟩
  | cons w ws ih =>
      simp only [run, scan] at h
      repeat' first | split at h | cases he : realize r.state w (M w)
          <;> simp only [he] at h
      all_goals first
        | cases h
        | have hh := ih h
          refine ⟨?_, ?_, ?_⟩
          · exact fun hx => hh.1 (by first | exact hx | exact List.mem_cons_of_mem _ hx)
          · exact fun hx => hh.2.1 (by first | exact hx | exact List.mem_cons_of_mem _ hx)
          · intro hx
            have hn : x ≠ w ∧ x ∉ ws := by simpa only [List.mem_cons, not_or] using hx
            simpa [hn.1] using hh.2.2 hn.2

/-- Repeating a completed pass with its final drops skips exactly the failed
entries. It reproduces the same state and kept list. -/
theorem reseed {todo : List Nat} {r q : Result} (hu : todo.Nodup)
    (h : run (realize := realize) M views conflict prefer todo r = .complete q) :
    run (realize := realize) M views conflict prefer todo { r with dropped := q.dropped } = .complete q := by
  induction todo generalizing r with
  | nil => cases h; rfl
  | cons w ws ih =>
      obtain ⟨hn, hu⟩ := List.nodup_cons.mp hu
      simp only [run, scan] at h ⊢
      split at h
      · rename_i hd
        have hout := (complete_frame M views conflict prefer h w).2.1 hd
        simp only [hout, ite_true]
        exact ih hu h
      · rename_i hd
        split at h
        · have hout := (complete_frame M views conflict prefer h w).2.1 List.mem_cons_self
          simp only [hout, ite_true]
          exact ih (r := { r with dropped := w :: r.dropped}) hu h
        · rename_i ha
          split at h
          · have hout : w ∉ q.dropped := by
              have hf := (complete_frame M views conflict prefer h w).2.2 hn
              exact fun hx => hd (hf.2.mp hx)
            rename_i hi
            simpa [hout, ha, hi] using
              ih (r := { r with kept := w :: r.kept}) hu h
          · rename_i hi
            cases he : realize r.state w (M w) with
            | none =>
                simp only [he] at h
                have hout := (complete_frame M views conflict prefer h w).2.1 List.mem_cons_self
                simp only [hout, ite_true]
                exact ih (r := { r with dropped := w :: r.dropped}) hu h
            | some next =>
                simp only [he] at h
                split at h
                · have hout := (complete_frame M views conflict prefer h w).2.1 List.mem_cons_self
                  simp only [hout, ite_true]
                  exact ih (r := { r with dropped := w :: r.dropped}) hu h
                · rename_i hp
                  split at h
                  · have hout : w ∉ q.dropped := by
                      have hf := (complete_frame M views conflict prefer h w).2.2 hn
                      exact fun hx => hd (hf.2.mp hx)
                    rename_i hempty
                    simpa only [hout, ha, hi, hp, hempty, Bool.false_eq_true, ↓reduceIte] using
                      ih (r := { r with state := next, kept := w :: r.kept}) hu h
                  · cases h

/-- Equality of membership, independent of list order and unrelated drops. -/
def Agree (P a b : List Nat) : Prop := ∀ x ∈ P, x ∈ a ↔ x ∈ b

def SamePass (P : List Nat) : Pass → Pass → Prop
  | .complete a, .complete b =>
      a.state = b.state ∧ a.kept = b.kept ∧ Agree P a.dropped b.dropped
  | .restart a, .restart b => Agree P a b
  | _, _ => False

theorem transport (P todo : List Nat) (s : State) (ks ds es : List Nat)
    (ht : ∀ x ∈ todo, x ∈ P) (hd : Agree P ds es) :
    SamePass P (run (realize := realize) M views conflict prefer todo ⟨s, ks, ds⟩)
      (run (realize := realize) M views conflict prefer todo ⟨s, ks, es⟩) := by
  induction todo generalizing s ks ds es with
  | nil => exact ⟨rfl, rfl, hd⟩
  | cons w ws ih =>
      have hw := ht w List.mem_cons_self
      have hws : ∀ x ∈ ws, x ∈ P := fun x hx => ht x (List.mem_cons_of_mem _ hx)
      have hd' : Agree P (w :: ds) (w :: es) := by
        intro x hx; simp only [List.mem_cons, hd x hx]
      simp only [run, scan, hd w hw]
      split
      · exact ih s ks ds es hws hd
      · split
        · exact ih s ks _ _ hws hd'
        · split
          · exact ih s _ ds es hws hd
          · cases realize s w (M w) with
            | none => exact ih s ks _ _ hws hd'
            | some next =>
                simp only
                split
                · exact ih s ks _ _ hws hd'
                · split
                  · exact ih next _ ds es hws hd
                  · intro x hx
                    simp only [List.mem_append, hd x hx]

def Framed (P : List Nat) (r : Result) : Pass → Prop
  | .complete q => Agree P q.kept r.kept ∧ Agree P q.dropped r.dropped
  | .restart ds => Agree P ds r.dropped

/-- A suffix cannot modify prefix drops, even on a restart, when none of its
entries conflicts with the prefix. A completed suffix preserves kept ids too. -/
theorem suffix_frame (P todo : List Nat) (r : Result)
    (ht : ∀ w ∈ todo, w ∉ P ∧ ∀ x ∈ P, conflict w x = false) :
    Framed P r (run (realize := realize) M views conflict prefer todo r) := by
  induction todo generalizing r with
  | nil => exact ⟨fun _ _ => Iff.rfl, fun _ _ => Iff.rfl⟩
  | cons w ws ih =>
      have hw := ht w List.mem_cons_self
      have hws : ∀ y ∈ ws, y ∉ P ∧ ∀ x ∈ P, conflict y x = false :=
        fun y hy => ht y (List.mem_cons_of_mem _ hy)
      have hn : ∀ x ∈ P, x ≠ w := fun x hx he => hw.1 (he ▸ hx)
      have drop (s : State) :
          Framed P r (run (realize := realize) M views conflict prefer ws ⟨s, r.kept, w :: r.dropped⟩) := by
        have hh := ih ⟨s, r.kept, w :: r.dropped⟩ hws
        cases he : run (realize := realize) M views conflict prefer ws ⟨s, r.kept, w :: r.dropped⟩ with
        | complete q =>
            simp only [he, Framed] at hh ⊢
            exact ⟨hh.1, fun x hx => by simpa [hn x hx] using hh.2 x hx⟩
        | restart ds =>
            simp only [he, Framed] at hh ⊢
            exact fun x hx => by simpa [hn x hx] using hh x hx
      have keep (s : State) :
          Framed P r (run (realize := realize) M views conflict prefer ws ⟨s, w :: r.kept, r.dropped⟩) := by
        have hh := ih ⟨s, w :: r.kept, r.dropped⟩ hws
        cases he : run (realize := realize) M views conflict prefer ws ⟨s, w :: r.kept, r.dropped⟩ with
        | complete q =>
            simp only [he, Framed] at hh ⊢
            exact ⟨fun x hx => by simpa [hn x hx] using hh.1 x hx, hh.2⟩
        | restart ds => simpa only [he, Framed] using hh
      simp only [run, scan]
      split
      · exact ih r hws
      · split
        · exact drop r.state
        · split
          · exact keep r.state
          · cases realize r.state w (M w) with
            | none => exact drop r.state
            | some next =>
                simp only
                split
                · exact drop r.state
                · split
                  · exact keep next
                  · intro x hx
                    simp [List.mem_append, hw.2 x hx]

/-- Once the prefix has completed, every later suffix restart reproduces its
statuses. The suffix may change arbitrarily and restart repeatedly. -/
theorem after_complete (P suffix : List Nat) (hu : P.Nodup)
    (ht : ∀ w ∈ suffix, w ∉ P ∧ ∀ x ∈ P, conflict w x = false)
    {fuel : Nat} {ds : List Nat} {q out : Result}
    (hp : run (realize := realize) M views conflict prefer P ⟨State.empty, [], ds⟩ = .complete q)
    (h : settleN M views (P ++ suffix) fuel ds conflict prefer realize = some out) :
    Agree P out.kept q.kept ∧ Agree P out.dropped q.dropped := by
  induction fuel generalizing ds q with
  | zero => cases h
  | succ fuel ih =>
      have hs := suffix_frame (realize := realize) M views conflict prefer P suffix q ht
      simp only [settleN, scan_append, hp] at h
      cases he : run (realize := realize) M views conflict prefer suffix q with
      | complete r =>
          simp only [he, Option.some.injEq] at h
          subst out
          simpa only [he, Framed] using hs
      | restart es =>
          simp only [he, Framed] at h hs
          have hr := reseed M views conflict prefer hu hp
          have hc := transport (realize := realize) M views conflict prefer P P State.empty [] q.dropped es
            (fun _ hh => hh) (fun x hx => (hs x hx).symm)
          simp only [hr] at hc
          cases hn : run (realize := realize) M views conflict prefer P ⟨State.empty, [], es⟩ with
          | restart more => simp [hn, SamePass] at hc
          | complete next =>
              simp only [hn, SamePass] at hc
              have hi := ih hn h
              refine ⟨?_, ?_⟩
              · intro x hx
                exact (hi.1 x hx).trans (by rw [hc.2.1])
              · intro x hx
                exact (hi.2 x hx).trans (hc.2.2 x hx).symm

/-- The prefix and full replay perform the same prefix restarts until the
prefix completes; all subsequent restarts preserve its result. -/
theorem settleN_prefix (P suffix : List Nat) (hu : P.Nodup)
    (ht : ∀ w ∈ suffix, w ∉ P ∧ ∀ x ∈ P, conflict w x = false)
    {fuel fullFuel : Nat} {ds : List Nat} {q out : Result}
    (hp : settleN M views P fuel ds conflict prefer realize = some q)
    (h : settleN M views (P ++ suffix) fullFuel ds conflict prefer realize = some out) :
    Agree P out.kept q.kept ∧ Agree P out.dropped q.dropped := by
  induction fuel generalizing ds fullFuel with
  | zero => cases hp
  | succ fuel ih =>
      cases he : run (realize := realize) M views conflict prefer P ⟨State.empty, [], ds⟩ with
      | complete r =>
          simp only [settleN, he, Option.some.injEq] at hp
          subst q
          exact after_complete M views conflict prefer P suffix hu ht he h
      | restart es =>
          simp only [settleN, he] at hp
          cases fullFuel with
          | zero => cases h
          | succ fullFuel =>
              simp only [settleN, scan_append, he] at h
              exact ih hp h

theorem settle_prefix (P suffix : List Nat) (hu : P.Nodup)
    (ht : ∀ w ∈ suffix, w ∉ P ∧ ∀ x ∈ P, conflict w x = false)
    (realize : State → Nat → Entry → Option State := checkedEffect) :
    Agree P (settle M views (P ++ suffix) conflict prefer realize).kept
      (settle M views P conflict prefer realize).kept ∧
    Agree P (settle M views (P ++ suffix) conflict prefer realize).dropped
      (settle M views P conflict prefer realize).dropped :=
  settleN_prefix M views conflict prefer P suffix hu ht
    (settle_eq_some M views P conflict prefer realize)
    (settle_eq_some M views (P ++ suffix) conflict prefer realize)

end ReplayPrefix
end CovenStorelog
