/-! §20.9's provider access for one account, as the owner's device keeps it:
the access the applied sequence intends, the access the device has confirmed,
and at most one request at a time. A request fixes its target from the
intention before it is sent; a later entry that changes the intention while
it runs lets it finish; its confirmation either ends the account's work, when
the target is still intended, or queues the opposite request. The provider
applies a request's target while it runs, and its success reply says it did:
that is the provider property the live sharing tests check, assumed here. -/
namespace CovenStorage.ProviderAccess

/-- An unfinished request: queued, its target not yet fixed; sent with its
target fixed; or reported failed, its effect unknown. -/
inductive Request where
  | queued
  | fixed (target : Bool)
  | failed
  deriving DecidableEq, Repr

structure Account where
  /-- The access the applied sequence intends. -/
  intended : Bool
  /-- The access the device has confirmed; `none` while a request's effect is
  unknown. -/
  granted : Option Bool
  /-- The account's unfinished work, at most one request. -/
  request : Option Request
  /-- The provider's own state, which no device reads directly. -/
  actual : Bool
  deriving DecidableEq, Repr

/-- A success reply for `t`: confirmed, and the account's work ends if `t` is
still intended, or the opposite request is queued. -/
def confirm (a : Account) (t : Bool) : Account :=
  { a with granted := some t, request := if a.intended = t then none else some .queued }

/-- The owner's device and the provider, one step at a time. -/
inductive Step : Account → Account → Prop
  /-- An applied entry changes the intention. A request whose target is fixed,
  or a reported failure the change leaves intended, stays as it is; otherwise
  work is queued when the confirmed access differs, and ends when it doesn't. -/
  | adoptKeeps (a : Account) (v : Bool) (t : Bool) (h : a.request = some (.fixed t)) :
      Step a { a with intended := v }
  | adoptKeepsFailure (a : Account) (h : a.request = some .failed) :
      Step a a
  | adoptQueues (a : Account) (v : Bool) (h : ∀ t, a.request ≠ some (.fixed t))
      (needed : a.granted ≠ some v) :
      Step a { a with intended := v, request := some .queued }
  | adoptEnds (a : Account) (v : Bool) (h : ∀ t, a.request ≠ some (.fixed t))
      (unneeded : a.granted = some v) :
      Step a { a with intended := v, request := none }
  /-- Fix the target from the current intention before sending; what the
  provider holds is unknown until the reply. -/
  | start (a : Account) (h : a.request = some .queued ∨ a.request = some .failed) :
      Step a { a with request := some (.fixed a.intended), granted := none }
  /-- The provider applies the target of the request that runs. -/
  | apply (a : Account) (t : Bool) (h : a.request = some (.fixed t)) :
      Step a { a with actual := t }
  /-- No conclusive reply: the request is reported failed, to be fixed again. -/
  | fail (a : Account) (t : Bool) (h : a.request = some (.fixed t)) :
      Step a { a with request := some .failed }
  /-- A success reply confirms the target, which the provider then holds. The
  account's work ends if the target is still intended; otherwise the opposite
  request is queued in the same commit. -/
  | complete (a : Account) (t : Bool) (h : a.request = some (.fixed t)) (applied : a.actual = t) :
      Step a (confirm a t)

inductive Steps : Account → Account → Prop
  | refl (a : Account) : Steps a a
  | step {a b c : Account} : Step a b → Steps b c → Steps a c

/-- What every step keeps true: while a request runs, nothing is confirmed;
confirmed access is what the provider holds; and an account with no work
left is confirmed at its intention. -/
structure Good (a : Account) : Prop where
  running : ∀ t, a.request = some (.fixed t) → a.granted = none
  confirmed : ∀ b, a.granted = some b → a.actual = b
  settled : a.request = none → a.granted = some a.intended

/-- A new account starts as its first entry leaves it: confirmed at the
access the entry describes, with work queued when that isn't intended. -/
theorem Good.initial (v g : Bool) :
    Good (if g = v then ⟨v, some g, none, g⟩ else ⟨v, some g, some .queued, g⟩) := by
  by_cases h : g = v
  · subst h; exact ⟨by simp, by simp, by simp⟩
  · simp only [h, ite_false]; exact ⟨by simp, by simp, by simp⟩

theorem step_good {a b : Account} (s : Step a b) (g : Good a) : Good b := by
  cases s with
  | adoptKeeps v t h => exact ⟨fun t' ht => g.running t' (by simpa using ht), g.confirmed,
      fun hr => by simp [h] at hr⟩
  | adoptKeepsFailure => exact g
  | adoptQueues v h needed => exact ⟨fun _ ht => by simp at ht, g.confirmed, fun hr => by simp at hr⟩
  | adoptEnds v h unneeded => exact ⟨fun _ ht => by simp at ht, g.confirmed, fun _ => unneeded⟩
  | start h => exact ⟨fun _ _ => rfl, fun _ hb => by simp at hb, fun hr => by simp at hr⟩
  | apply t h =>
    have none_granted := g.running t h
    exact ⟨fun _ _ => none_granted,
      fun _ hb => by simp [none_granted] at hb, fun hr => by simp [h] at hr⟩
  | fail t h =>
    exact ⟨fun _ ht => by simp at ht, g.confirmed, fun hr => by simp at hr⟩
  | complete t h applied =>
    refine ⟨fun t' ht => ?_, fun b hb => ?_, fun hr => ?_⟩
    · by_cases hi : a.intended = t <;> simp [confirm, hi] at ht
    · simp [confirm] at hb; subst hb; exact applied
    · by_cases hi : a.intended = t
      · simp [confirm, hi]
      · simp [confirm, hi] at hr

theorem steps_good {a b : Account} (run : Steps a b) (g : Good a) : Good b := by
  induction run with
  | refl => exact g
  | step s _ ih => exact ih (step_good s g)

/-- **Work ends only at the intended access.** Whenever the owner's device
has no request left for an account, the provider holds exactly the access the
applied sequence intends, however the intention changed while requests ran. -/
theorem settled_matches {a b : Account} (g : Good a) (run : Steps a b)
    (done : b.request = none) : b.actual = b.intended :=
  let gb := steps_good run g
  gb.confirmed _ (gb.settled done)

/-- **A request an entry made obsolete is reversed.** Confirming a target the
intention has since left queues the opposite request instead of ending the
work, and the next request sent fixes its target at the intention. -/
theorem obsolete_reversed {a c : Account} {t : Bool} (obsolete : a.intended ≠ t)
    (s₂ : Step (confirm a t) c) (sent : ∃ t', c.request = some (.fixed t')) :
    (confirm a t).request = some .queued ∧ c.request = some (.fixed a.intended) := by
  refine ⟨by simp [confirm, obsolete], ?_⟩
  have q : (confirm a t).request = some .queued := by
    simp [confirm, obsolete]
  cases s₂ with
  | adoptKeeps v t' h' => simp [q] at h'
  | adoptKeepsFailure h' => simp [q] at h'
  | adoptQueues v _ _ => simp at sent
  | adoptEnds v _ _ => simp at sent
  | start _ => simp [confirm]
  | apply t' h' => simp [q] at h'
  | fail t' h' => simp at sent
  | complete t' h' _ => simp [q] at h'

/-- **Work settles once the intention holds still.** From any account with
work left, a run in which the provider answers every request reaches no work
left, at the same intention: at most a request already running and its
reversal. -/
theorem settles {a : Account} (work : a.request ≠ none) :
    ∃ b, Steps a b ∧ b.request = none ∧ b.intended = a.intended := by
  -- Send the intention and confirm it, from an account with no request running.
  have send : ∀ x : Account, (x.request = some .queued ∨ x.request = some .failed) →
      ∃ b, Steps x b ∧ b.request = none ∧ b.intended = x.intended := by
    intro x hx
    let x₁ : Account := { x with request := some (.fixed x.intended), granted := none }
    let x₂ : Account := { x₁ with actual := x.intended }
    refine ⟨confirm x₂ x.intended, ?_, by simp [confirm, x₂, x₁], by simp [confirm, x₂, x₁]⟩
    exact .step (.start x hx) (.step (.apply x₁ x.intended rfl)
      (.step (.complete x₂ x.intended rfl rfl) (.refl _)))
  cases hr : a.request with
  | none => exact absurd hr work
  | some r =>
    cases r with
    | queued => exact send a (.inl hr)
    | failed => exact send a (.inr hr)
    | fixed t =>
      let a₁ : Account := { a with actual := t }
      let a₂ : Account := confirm a₁ t
      by_cases hi : a.intended = t
      · exact ⟨a₂, .step (.apply a t hr) (.step (.complete a₁ t hr rfl) (.refl _)),
          by simp [a₂, a₁, confirm, hi], by simp [a₂, a₁, confirm]⟩
      · obtain ⟨b, run, hb, hbi⟩ := send a₂ (.inl (by simp [a₂, a₁, confirm, hi]))
        exact ⟨b, .step (.apply a t hr) (.step (.complete a₁ t hr rfl) run), hb,
          by simpa [a₂, a₁, confirm] using hbi⟩

/-- **Ending on any success reply would report stale access.** A device that
ended an account's work on every success reply, without comparing its target
with the intention, would report Dan's access settled while the provider still
grants it: his removal applied while the grant ran. -/
theorem no_recheck_reports_stale :
    let running : Account := ⟨true, none, some (.fixed true), false⟩
    let removed : Account := { running with intended := false }
    let applied : Account := { removed with actual := true }
    Steps running applied ∧
      ({ applied with granted := some true, request := none } : Account).actual ≠
        ({ applied with granted := some true, request := none } : Account).intended := by
  intro running removed applied
  refine ⟨.step (.adoptKeeps running false true rfl) (.step (.apply removed true rfl) (.refl _)), ?_⟩
  simp [applied, removed]

end CovenStorage.ProviderAccess
