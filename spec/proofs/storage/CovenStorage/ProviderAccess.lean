/-!
# Sharing one account from the owner's device

On Dropbox, OneDrive and iCloud, the devices of the member whose account
holds the store share it with other accounts and take sharing back, as they
apply history (§14.2, §20.9). For each account a device keeps one row
(`_coven_provider_access`): whether history intends the account shared, the
grant the provider last confirmed, and at most one request, with its step in
the operation journal. A pass fixes the request's target from the intention
before it calls the provider, sends it, and records the reply. Applying an
entry may change the intention at any moment, while a request runs too.

The provider holds coven's own grant to the account (`direct`), and may hold
grants coven leaves in place (`other`): through a parent folder, a public
link or a group, to an unidentified grantee, or the owner's own. A
revocation that finds them reports `AccessRemains`, and stays listed until
the owner removes them and the app retries it, or an entry intends the
account shared again.

The model follows one account on one of the owner's devices. Its steps:
an entry applies (`apply`); a pass fixes a queued request's target (`fix`),
sends it (`send`), and records the reply, the target confirmed (`confirm`)
or a revocation's grants left in place (`remain`); the provider carries out
the request (`land`); the call ends without a reply, a storage error or a
crash (`drop`); the app retries a revocation reported as `AccessRemains`
(`retry`); the owner removes the grants left in place (`unshare`). Each
step is one transaction.

Assumption: a request takes effect at the provider, if at all, before its
call ends; one whose call ended without a reply never takes effect later.

`Rules` names three rules; `Rules.correct` has all of them, and leaving any
one out is a checked counterexample:

- `exclusive`: applying an entry leaves a request whose target is fixed
  alone, and a target is fixed only while no request runs. So the device
  sends one request at a time, and lets a running one finish when the
  intention changes. Without it, an earlier request still running lands
  after a later one (`stray`).
- `forget`: fixing a target forgets the confirmed grant. A request that ends
  without a reply leaves the provider's state unknown.
- `opposite`: a reply for a target history no longer intends queues the
  opposite request, whether it confirmed the target or reported grants left
  in place.
-/
namespace CovenStorage.ProviderAccess

/-- The account's request, as its row and journal step hold it. -/
inductive Req where
  | none
  /-- Work is needed; no target is fixed. -/
  | queued
  /-- Its target is fixed from the intention, before storage is called. -/
  | fixed (target : Bool)
  /-- A revocation found grants left in place: listed as `AccessRemains`. -/
  | remains
  deriving DecidableEq, Repr

/-- The device's call to the provider. -/
inductive Call where
  | none
  /-- Sent; the provider hasn't carried it out. -/
  | sent (target : Bool)
  /-- Carried out; its reply not yet recorded. -/
  | landed (target : Bool)
  deriving DecidableEq, Repr

structure World where
  /-- Whether the applied history intends the account shared. -/
  intended : Bool
  /-- The grant the provider last confirmed; `none` while unknown. -/
  granted : Option Bool
  request : Req
  call : Call
  /-- An earlier request still running beside the current one. -/
  stray : Option Bool
  /-- Coven's own grant to the account at the provider. -/
  direct : Bool
  /-- Grants coven leaves in place reach the account. -/
  other : Bool
  deriving DecidableEq, Repr

/-- An account the device first learns of with nothing to do: its grant is
the one history describes. -/
def start (shared other : Bool) : World :=
  ⟨shared, some shared, .none, .none, none, shared, other⟩

structure Rules where
  exclusive : Bool
  forget : Bool
  opposite : Bool
  deriving DecidableEq, Repr

def Rules.correct : Rules := ⟨true, true, true⟩
def Rules.overlapping : Rules := ⟨false, true, true⟩
def Rules.remembering : Rules := ⟨true, false, true⟩
def Rules.finishing : Rules := ⟨true, true, false⟩

inductive Act where
  /-- An entry applies; history now intends the account shared or not. -/
  | apply (shared : Bool)
  | fix
  | retry
  | send
  | land
  /-- The earlier request still running lands. -/
  | landStray
  | confirm
  | remain
  | drop
  | unshare
  deriving DecidableEq, Repr

/-- The intention changes: the work ends when the confirmed grant already
matches it, and is queued otherwise. -/
def adopt (w : World) (shared : Bool) : World :=
  if w.granted = some shared then { w with intended := shared, request := .none }
  else { w with intended := shared, request := .queued }

def applyEntry (R : Rules) (w : World) (shared : Bool) : World :=
  match w.request with
  | .fixed _ => if R.exclusive then { w with intended := shared } else adopt w shared
  | .remains => if w.intended = shared then w else adopt w shared
  | _ => adopt w shared

/-- Fix the target from the intention. A call still running is left to
land as it may. -/
def fixTarget (R : Rules) (w : World) : World :=
  { w with
    granted := if R.forget then none else w.granted
    request := .fixed w.intended
    call := .none
    stray := match w.call with
      | .sent t => some t
      | _ => w.stray }

def isFixed : Req → Bool
  | .fixed _ => true
  | _ => false

def enabled (R : Rules) (w : World) : Act → Bool
  | .apply _ => true
  | .fix => w.request = .queued && w.stray.isNone && (!R.exclusive || w.call = .none)
  | .retry => w.request = .remains && w.call = .none
  | .send => isFixed w.request && w.call = .none
  | .land => match w.call with
    | .sent _ => true
    | _ => false
  | .landStray => w.stray.isSome
  | .confirm => match w.call with
    | .landed t => w.request = .fixed t && (t || !w.other)
    | _ => false
  | .remain => w.call = .landed false && w.request = .fixed false && w.other
  | .drop => w.call != .none
  | .unshare => w.other

def act (R : Rules) (w : World) : Act → World
  | .apply shared => applyEntry R w shared
  | .fix | .retry => fixTarget R w
  | .send => match w.request with
    | .fixed t => { w with call := .sent t }
    | _ => w
  | .land => match w.call with
    | .sent t => { w with call := .landed t, direct := t }
    | _ => w
  | .landStray => match w.stray with
    | some t => { w with stray := none, direct := t }
    | none => w
  | .confirm => match w.call with
    | .landed t =>
      { w with call := .none, granted := some t,
               request := if w.intended = t || !R.opposite then .none else .queued }
    | _ => w
  | .remain =>
    { w with call := .none, request := if !w.intended || !R.opposite then .remains else .queued }
  | .drop => { w with call := .none }
  | .unshare => { w with other := false }

inductive Steps (R : Rules) : World → World → Prop
  | refl (w : World) : Steps R w w
  | step {w w' : World} (a : Act) : enabled R w a = true → Steps R (act R w a) w' → Steps R w w'

def Reachable (R : Rules) (w : World) : Prop := ∃ shared other, Steps R (start shared other) w

/-- Take each action in turn while it is enabled. -/
def trace (R : Rules) : World → List Act → Option World
  | w, [] => some w
  | w, a :: rest => if enabled R w a then trace R (act R w a) rest else none

theorem Steps.trans {R : Rules} {a b d : World} (h : Steps R a b) (k : Steps R b d) :
    Steps R a d := by
  induction h with
  | refl => exact k
  | step x e _ ih => exact .step x e (ih k)

/-- Nothing listed and nothing running for the account. -/
def quiet (w : World) : Bool := w.request = .none && w.call = .none && w.stray.isNone

/-! ## What every reachable state keeps -/

structure Inv (w : World) : Prop where
  stray : w.stray = none
  sent : ∀ t, w.call = .sent t → w.request = .fixed t
  landed : ∀ t, w.call = .landed t → w.request = .fixed t ∧ w.direct = t
  /-- A confirmed grant is the provider's, and no request runs beside it. -/
  granted : ∀ g, w.granted = some g → w.direct = g ∧ w.call = .none
  fixed : ∀ t, w.request = .fixed t → w.granted = none
  /-- With nothing listed, the confirmed grant is the intended one. -/
  idle : w.request = .none → w.granted = some w.intended
  remains : w.request = .remains →
    w.intended = false ∧ w.direct = false ∧ w.granted = none ∧ w.call = .none

theorem inv_start (shared other : Bool) : Inv (start shared other) := by
  constructor <;> simp [start]

/-- A request that isn't fixed has no call running. -/
theorem idle_call {w : World} (inv : Inv w) (h : isFixed w.request = false) : w.call = .none := by
  rcases hc : w.call with _ | t | t
  · rfl
  · have := inv.sent t hc; simp [this, isFixed] at h
  · have := (inv.landed t hc).1; simp [this, isFixed] at h

set_option maxHeartbeats 1000000 in
theorem inv_step (w : World) (a : Act) (inv : Inv w) (e : enabled Rules.correct w a = true) :
    Inv (act Rules.correct w a) := by
  obtain ⟨i, g, r, c, s, d, o⟩ := w
  have ic := idle_call inv
  obtain ⟨hs, hsent, hlanded, hgranted, hfixed, hidle, hremains⟩ := inv
  simp only at hs hsent hlanded hgranted hfixed hidle hremains ic
  subst hs
  cases a
  case apply shared =>
    rcases r with _ | _ | t | _
    · obtain rfl := ic rfl
      rcases g with _ | _ | _ <;> cases i <;> cases shared <;> constructor <;>
        simp_all [act, applyEntry, adopt]
    · obtain rfl := ic rfl
      rcases g with _ | _ | _ <;> cases i <;> cases shared <;> constructor <;>
        simp_all [act, applyEntry, adopt]
    · constructor <;> simp_all [act, applyEntry, Rules.correct]
    · obtain ⟨rfl, rfl, rfl, rfl⟩ := hremains rfl
      cases shared <;> constructor <;> simp_all [act, applyEntry, adopt]
  case fix | retry =>
    have c0 : c = .none := by
      rcases r with _ | _ | _ | _ <;> simp_all [enabled, Rules.correct]
    subst c0
    constructor <;> simp [act, fixTarget, Rules.correct]
  case confirm | remain =>
    rcases r with _ | _ | (_ | _) | _ <;> rcases c with _ | (_ | _) | (_ | _) <;>
      simp only [enabled] at e <;> simp at e <;> cases i <;>
      constructor <;> simp_all [act, Rules.correct]
  all_goals
    rcases r with _ | _ | (_ | _) | _ <;> rcases c with _ | (_ | _) | (_ | _) <;>
      rcases g with _ | _ | _ <;>
      simp only [enabled, act] at e ⊢ <;>
      constructor <;> (try split) <;> simp_all

theorem reachable_inv {w : World} (h : Reachable Rules.correct w) : Inv w := by
  obtain ⟨shared, other, h⟩ := h
  suffices ∀ {a b}, Steps Rules.correct a b → Inv a → Inv b from this h (inv_start shared other)
  intro a b h
  induction h with
  | refl => exact id
  | step x e _ ih => exact fun inv => ih (inv_step _ x inv e)

/-- One request at a time: no earlier request runs, and the one running is
the target the account's row has fixed, which applying entries leaves
alone until its reply is recorded. -/
theorem one_request {w : World} (h : Reachable Rules.correct w) :
    w.stray = none ∧ ∀ t, (w.call = .sent t ∨ w.call = .landed t) → w.request = .fixed t := by
  have inv := reachable_inv h
  refine ⟨inv.stray, fun t c => ?_⟩
  rcases c with c | c
  · exact inv.sent t c
  · exact (inv.landed t c).1

/-- When nothing is listed for the account and nothing runs, coven's grant
at the provider is what history intends. -/
theorem quiet_exact {w : World} (h : Reachable Rules.correct w) (q : quiet w = true) :
    w.direct = w.intended := by
  have inv := reachable_inv h
  have r : w.request = .none := by simp [quiet] at q; exact q.1.1
  exact (inv.granted _ (inv.idle r)).1

/-- `AccessRemains` is listed only for an account history doesn't intend
shared, whose own grant coven has taken back. -/
theorem remains_unshared {w : World} (h : Reachable Rules.correct w)
    (r : w.request = .remains) : w.intended = false ∧ w.direct = false :=
  let ⟨i, d, _⟩ := (reachable_inv h).remains r
  ⟨i, d⟩

/-- A revocation listed as `AccessRemains` leaves the list only when the
app retries it, or an entry intends the account shared again. -/
theorem remains_listed {w : World} (h : Reachable Rules.correct w) (r : w.request = .remains)
    (a : Act) (e : enabled Rules.correct w a = true)
    (left : (act Rules.correct w a).request ≠ .remains) : a = .retry ∨ a = .apply true := by
  have inv := reachable_inv h
  have s := inv.stray
  obtain ⟨i, -, -, c⟩ := inv.remains r
  cases a with
  | apply shared =>
    cases shared
    · simp [act, applyEntry, r, i] at left
    · exact .inr rfl
  | retry => exact .inl rfl
  | _ => simp_all [enabled, act, isFixed]

/-- A reply for a target history no longer intends, whether it confirms the
target or lists grants left in place, queues the opposite request on the
same row instead of ending the work or reporting `AccessRemains`. -/
theorem obsolete_reversed {w : World} (a : Act) (reply : a = .confirm ∨ a = .remain)
    (e : enabled Rules.correct w a = true)
    (obsolete : ∀ t, w.request = .fixed t → w.intended ≠ t) :
    (act Rules.correct w a).request = .queued := by
  rcases reply with rfl | rfl
  · rcases hc : w.call with _ | t | t <;> simp [enabled, hc] at e
    have := obsolete t e.1
    simp [act, hc, Rules.correct, this]
  · simp only [enabled, Bool.and_eq_true, decide_eq_true_eq] at e
    have := obsolete false e.1.2
    simp [act, Rules.correct] at this ⊢
    exact this

/-! ## Every account's work ends -/

/-- Coven's own steps, the app's retry of what it is shown, and the owner
removing the grants it reported. -/
def own : Act → Bool
  | .fix | .retry | .send | .land | .landStray | .confirm | .remain | .unshare => true
  | _ => false

inductive Moves : World → World → Prop
  | refl (w : World) : Moves w w
  | step {w w' : World} (a : Act) (own : own a = true) (e : enabled Rules.correct w a = true)
      (rest : Moves (act Rules.correct w a) w') : Moves w w'

/-- How far the request is from ending. -/
def stage (w : World) : Nat :=
  match w.request, w.call with
  | .none, _ => 0
  | .remains, _ => if w.other then 1 else 10
  | .fixed _, .landed _ => 2
  | .fixed _, .sent _ => 3
  | .fixed _, .none => 4
  | .queued, _ => 5

def measure (w : World) : Nat :=
  (if w.other then 100 else 0) +
    (match w.request with
     | .fixed t => if t = w.intended then 0 else 20
     | _ => 0) +
    stage w

theorem progress (w : World) (inv : Inv w) (busy : quiet w = false) :
    ∃ a, own a = true ∧ enabled Rules.correct w a = true ∧
      measure (act Rules.correct w a) < measure w := by
  have ic := idle_call inv
  obtain ⟨i, g, r, c, s, d, o⟩ := w
  obtain ⟨hs, hsent, hlanded, -, -, -, hremains⟩ := inv
  simp only at hs hsent hlanded hremains ic
  subst hs
  rcases r with _ | _ | t | _
  · simp [quiet, ic rfl] at busy
  · refine ⟨.fix, rfl, ?_, ?_⟩ <;>
      simp [enabled, act, fixTarget, Rules.correct, ic rfl, measure, stage] <;> split <;> omega
  · rcases c with _ | ct | ct
    · refine ⟨.send, rfl, ?_, ?_⟩ <;> cases t <;> cases i <;> cases o <;>
        simp [enabled, act, isFixed, measure, stage]
    · obtain rfl : t = ct := by simpa using hsent ct rfl
      refine ⟨.land, rfl, ?_, ?_⟩ <;> cases t <;> cases i <;> cases o <;>
        simp [enabled, act, measure, stage]
    · obtain ⟨rfl, -⟩ : t = ct ∧ _ := by simpa using hlanded ct rfl
      by_cases rem : t = false ∧ o = true
      · obtain ⟨rfl, rfl⟩ := rem
        refine ⟨.remain, rfl, ?_, ?_⟩ <;> simp [enabled, act, Rules.correct, measure, stage]
        cases i <;> simp
      · refine ⟨.confirm, rfl, ?_, ?_⟩
        · cases t <;> cases o <;> simp_all [enabled]
        · cases t <;> cases i <;> cases o <;> simp_all [act, Rules.correct, measure, stage]
  · obtain ⟨rfl, -, -, rfl⟩ := hremains rfl
    cases o
    · refine ⟨.retry, rfl, ?_, ?_⟩ <;>
        simp [enabled, act, fixTarget, Rules.correct, measure, stage]
    · refine ⟨.unshare, rfl, ?_, ?_⟩ <;> simp [enabled, act, measure, stage]

/-- Every account's work ends, on every path: from any reachable state, with
history no longer changing the intention and the provider answering, coven's
own steps, the app retrying what it is shown and the owner removing the
grants coven reported reach a state with nothing listed and nothing
running, where coven's grant is the intended one (`quiet_exact`). -/
theorem settles {w : World} (h : Reachable Rules.correct w) :
    ∃ w', Moves w w' ∧ quiet w' = true := by
  generalize hm : measure w = m
  induction m using Nat.strongRecOn generalizing w with
  | ind m ih =>
    by_cases q : quiet w = true
    · exact ⟨w, .refl w, q⟩
    · obtain ⟨a, o, e, less⟩ := progress w (reachable_inv h) (by simpa using q)
      have h' : Reachable Rules.correct (act Rules.correct w a) := by
        obtain ⟨sh, ot, s⟩ := h
        exact ⟨sh, ot, s.trans (.step a e (.refl _))⟩
      obtain ⟨w'', more, quiet⟩ := ih _ (hm ▸ less) h' rfl
      exact ⟨w'', .step a o e more, quiet⟩

/-! ## Each rule is needed -/

/-- Ana's device grants Dan's account for an invite, and history takes the
intention back while the grant runs. Without `exclusive`, applying that
queues a revocation at once, which the device sends beside the grant; the
revocation lands and is confirmed, and nothing is listed. Then the grant,
still running, lands: Dan's account is shared against history, with nothing
listed to take it back. -/
theorem overlapping_requests_strand :
    ∃ w, trace Rules.overlapping (start false false)
        [.apply true, .fix, .send, .apply false, .fix, .send, .land, .confirm, .landStray] =
          some w ∧ quiet w = true ∧ w.direct = true ∧ w.intended = false := by
  exact ⟨_, rfl, rfl, rfl, rfl⟩

/-- Dan's account is shared, and also reaches the store through a parent
folder. His removal applies, and the revocation finds the parent folder's
grant: `AccessRemains`. A new invite to Dan then applies. Remembering the
grant confirmed before the revocation, the device sees the account as
shared and ends the work, though it took its own grant back. When Ana, as
she was told, removes the parent folder's grant, Dan's invite has no access
and nothing is listed. -/
theorem kept_confirmation_strands :
    ∃ w, trace Rules.remembering (start true true)
        [.apply false, .fix, .send, .land, .remain, .apply true, .unshare] = some w ∧
      quiet w = true ∧ w.direct = false ∧ w.other = false ∧ w.intended = true := by
  exact ⟨_, rfl, rfl, rfl, rfl, rfl⟩

/-- Without `opposite`, a grant that history took back while it ran is
confirmed and the work ends: Dan's account stays shared, with nothing
listed. -/
theorem obsolete_finish_strands :
    ∃ w, trace Rules.finishing (start false false)
        [.apply true, .fix, .send, .apply false, .land, .confirm] = some w ∧
      quiet w = true ∧ w.direct = true ∧ w.intended = false := by
  exact ⟨_, rfl, rfl, rfl, rfl⟩

/-- Without `opposite`, a revocation that finds grants left in place after
history intends the account shared again is listed as `AccessRemains`:
the owner is told to remove grants from an account the store intends
shared, while coven's own grant stays taken back. -/
theorem obsolete_revocation_reported :
    ∃ w, trace Rules.finishing (start true true)
        [.apply false, .fix, .send, .apply true, .land, .remain] = some w ∧
      w.request = .remains ∧ w.intended = true ∧ w.direct = false := by
  exact ⟨_, rfl, rfl, rfl, rfl⟩

end CovenStorage.ProviderAccess
