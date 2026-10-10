import CovenStorelogData.CurrentReplay

/-! §9 entry fate: the real restart replay realizes only its final kept
entries. Replaying those effects can fail; the theorem proves that it does not. -/
namespace CovenStorelogData.ReplayEffects
open CovenStorelog

def applyEntry (M : Log) (state : State) (e : Nat) : Option State :=
  if alreadyInPlace state (M e) then some state else checkedEffect state e (M e)

def applyKept (M : Log) : List Nat → State → Option State
  | [], state => some state
  | e :: es, state => (applyEntry M state e).bind (applyKept M es)

theorem scan_realizes (M : Log) (views : Nat → State) (conflict prefer : Nat → Nat → Bool)
    {todo : List Nat} {r q : Result} (unique : todo.Nodup)
    (fresh : ∀ e ∈ todo, e ∉ r.kept)
    (complete : scan M views todo r conflict prefer = .complete q) :
    applyKept M (todo.filter (· ∈ q.kept)) r.state = some q.state := by
  induction todo generalizing r with
  | nil => cases complete; rfl
  | cons w ws ih =>
      obtain ⟨notTail, tailUnique⟩ := List.nodup_cons.mp unique
      have notKept := fresh w List.mem_cons_self
      have tailFresh : ∀ e ∈ ws, e ∉ r.kept :=
        fun e he => fresh e (List.mem_cons_of_mem w he)
      have skip (drops : List Nat)
          (h : scan M views ws ⟨r.state, r.kept, drops⟩ conflict prefer = .complete q) :
          applyKept M ((w :: ws).filter (· ∈ q.kept)) r.state = some q.state := by
        have frame := (ReplayPrefix.complete_frame M views conflict prefer h w).2.2 notTail
        have absent : w ∉ q.kept := fun hw => notKept (frame.1.mp hw)
        simpa [absent] using ih (r := ⟨r.state, r.kept, drops⟩) tailUnique tailFresh h
      have keep (next : State) (effect : applyEntry M r.state w = some next)
          (h : scan M views ws ⟨next, w :: r.kept, r.dropped⟩ conflict prefer = .complete q) :
          applyKept M ((w :: ws).filter (· ∈ q.kept)) r.state = some q.state := by
        have present := (ReplayPrefix.complete_frame M views conflict prefer h w).1 List.mem_cons_self
        have remaining : ∀ e ∈ ws, e ∉ w :: r.kept := by
          intro e he
          simp only [List.mem_cons, not_or]
          exact ⟨fun eq => notTail (eq ▸ he), tailFresh e he⟩
        simpa [present, applyKept, effect] using ih tailUnique remaining h
      simp only [scan] at complete
      split at complete
      · exact skip _ complete
      · split at complete
        · exact skip _ complete
        · split at complete
          · rename_i same
            exact keep r.state (by simp [applyEntry, same]) complete
          · rename_i different
            cases effect : checkedEffect r.state w (M w) with
            | none => simp only [effect] at complete; exact skip _ complete
            | some next =>
                simp only [effect] at complete
                split at complete
                · exact skip _ complete
                · split at complete
                  · exact keep next (by simp [applyEntry, different, effect]) complete
                  · cases complete

theorem settleN_realizes (M : Log) (views : Nat → State)
    (conflict prefer : Nat → Nat → Bool) (entries : List Nat) (unique : entries.Nodup)
    {fuel : Nat} {drops : List Nat} {out : Result}
    (h : settleN M views entries fuel drops conflict prefer = some out) :
    applyKept M (entries.filter (· ∈ out.kept)) State.empty = some out.state := by
  induction fuel generalizing drops with
  | zero => cases h
  | succ fuel ih =>
      cases pass : scan M views entries ⟨State.empty, [], drops⟩ conflict prefer with
      | complete result =>
          simp only [settleN, pass, Option.some.injEq] at h
          subst out
          exact scan_realizes M views conflict prefer unique (by simp) pass
      | restart more =>
          simp only [settleN, pass] at h
          exact ih h

theorem entry_fate (H : Finality.History) (W n : Nat) (S : EntrySet) :
    let views := CurrentReplay.authorViews H W n n
    let candidates := (List.range n).filter (CurrentReplay.admitted H W n views S)
    let result := CurrentReplay.resolve H W n S
    applyKept H.log (candidates.filter (· ∈ result.kept)) State.empty = some result.state := by
  dsimp only [CurrentReplay.resolve, CurrentReplay.materialize]
  exact settleN_realizes H.log (CurrentReplay.authorViews H W n n)
    (CurrentReplay.conflict H.log _) (CurrentReplay.prefer H.log _) _
    ((List.nodup_range (n := n)).filter _) (settle_eq_some H.log _ _ _ _)

end CovenStorelogData.ReplayEffects
