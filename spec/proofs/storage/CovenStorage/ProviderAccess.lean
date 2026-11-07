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

/-- The same from any state that keeps the invariant. -/
theorem settles_of_inv {w : World} (inv : Inv w) :
    ∃ w', Moves w w' ∧ quiet w' = true := by
  generalize hm : measure w = m
  induction m using Nat.strongRecOn generalizing w with
  | ind m ih =>
    by_cases q : quiet w = true
    · exact ⟨w, .refl w, q⟩
    · obtain ⟨a, o, e, less⟩ := progress w inv (by simpa using q)
      obtain ⟨w'', more, quiet⟩ :=
        ih _ (hm ▸ less) (inv_step w a inv e) rfl
      exact ⟨w'', .step a o e more, quiet⟩

/-- Every account's work ends, on every path: from any reachable state, with
history no longer changing the intention and the provider answering, coven's
own steps, the app retrying what it is shown and the owner removing the
grants coven reported reach a state with nothing listed and nothing
running, where coven's grant is the intended one (`quiet_exact`). -/
theorem settles {w : World} (h : Reachable Rules.correct w) :
    ∃ w', Moves w w' ∧ quiet w' = true :=
  settles_of_inv (reachable_inv h)

/-- Coven's own steps leave the intention as it is. -/
theorem moves_intended {w w' : World} (m : Moves w w') : w'.intended = w.intended := by
  induction m with
  | refl => rfl
  | step a h _ _ ih =>
    rw [ih]
    cases a <;> simp [own] at h <;> simp only [act, fixTarget] <;> (try split) <;> rfl

/-- Where coven's own steps end with nothing listed or running, coven's grant
is the intention the state had (`quiet_exact`, from a state keeping the
invariant). -/
theorem moves_quiet_exact {w w' : World} (inv : Inv w) (m : Moves w w')
    (q : quiet w' = true) : w'.direct = w.intended := by
  have inv' : Inv w' := by
    suffices ∀ {a b}, Moves a b → Inv a → Inv b from this m inv
    intro a b m
    induction m with
    | refl => exact id
    | step x _ e _ ih => exact fun inv => ih (inv_step _ x inv e)
  have r : w'.request = .none := by simp [quiet] at q; exact q.1.1
  rw [← moves_intended m]
  exact (inv'.granted _ (inv'.idle r)).1

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


/-! ## Two owner devices

Each of the owner's devices keeps its own row and sends its own requests, at
its own place in history; both reach the one provider. A device reads the
sequence's entries in order (`read`), and may stop for good at any point: its
removal, a reset in its place, or its member's keys replaced (§14.1). A
stopped device sends nothing more and records no reply, but a request it had
sent may still land at the provider.

Neither device can tell when that request has landed, or that it never will.
The person can: signing the lost device out at the provider (§14.2, "One lost
device") ends its access, so nothing it sent can land after. The person tells
a surviving device so with `cutOff`, which drops the lost device's request
that had not landed and bars it from landing any. The survivor then takes
back the account (`takeBack`): it forgets the grant it last confirmed, which
the lost device's request may have changed, and queues a revocation, when the
sequence no longer intends the account shared. That revocation runs and lands
after the cut-off, so it is the last request to reach the account. -/

/-- One owner device: its row and call, how many entries it has read, whether
it stopped for good, and whether the person has confirmed that nothing it
sent can land any more. -/
structure Device where
  intended : Bool
  granted : Option Bool
  request : Req
  call : Call
  read : Nat
  stopped : Bool
  cutOff : Bool
  deriving DecidableEq, Repr

/-- The owner's two devices, the account's intentions in the order the
sequence's entries set them, and the provider. -/
structure Pair where
  history : List Bool
  first : Device
  second : Device
  direct : Bool
  other : Bool
  deriving DecidableEq, Repr

inductive Who where
  | first
  | second
  deriving DecidableEq, Repr

def Who.other : Who → Who
  | .first => .second
  | .second => .first

def Pair.device (p : Pair) : Who → Device
  | .first => p.first
  | .second => p.second

def Pair.set (p : Pair) : Who → Device → Pair
  | .first, d => { p with first := d }
  | .second, d => { p with second := d }

/-- A device's view: its own row and call, and the provider. -/
def view (p : Pair) (d : Device) : World :=
  ⟨d.intended, d.granted, d.request, d.call, none, p.direct, p.other⟩

inductive PairAct where
  /-- The device applies the next entry it hasn't read. -/
  | read (who : Who)
  /-- One of the single device's steps, other than applying an entry. -/
  | on (who : Who) (a : Act)
  | stop (who : Who)
  /-- The person confirms at the provider that the stopped device can land
  nothing more. -/
  | cutOff (who : Who)
  /-- The other device, confirmed cut off, took the account's sharing back. -/
  | takeBack (who : Who)
  deriving DecidableEq, Repr

def entrySteps : Act → Bool
  | .apply _ => false
  | _ => true

/-- A stopped device sends nothing, but the provider may still carry out the
request it had sent, until the person confirms it cut off. -/
def pairEnabled (p : Pair) : PairAct → Bool
  | .read who => !(p.device who).stopped && (p.device who).read < p.history.length
  | .on who a =>
    (!(p.device who).stopped || (a == .land && !(p.device who).cutOff)) && entrySteps a &&
      enabled Rules.correct (view p (p.device who)) a
  | .stop who => !(p.device who).stopped
  | .cutOff who => (p.device who).stopped && !(p.device who).cutOff
  | .takeBack who =>
    let d := p.device who
    !d.stopped && (p.device who.other).cutOff && !d.intended && d.call = .none &&
      (d.request = .none || d.request = .queued || d.request = .remains)

/-- Carry a device's step back: its row and call to the device, the
provider's grants to the pair. -/
def after (p : Pair) (who : Who) (w : World) : Pair :=
  let d := p.device who
  { p.set who { d with intended := w.intended, granted := w.granted, request := w.request,
                        call := w.call } with
    direct := w.direct, other := w.other }

def pairAct (p : Pair) : PairAct → Pair
  | .read who =>
    let d := p.device who
    match p.history[d.read]? with
    | some shared =>
      let p' := after p who (act Rules.correct (view p d) (.apply shared))
      p'.set who { p'.device who with read := d.read + 1 }
    | none => p
  | .on who a => after p who (act Rules.correct (view p (p.device who)) a)
  | .stop who => p.set who { p.device who with stopped := true }
  | .cutOff who => p.set who { p.device who with cutOff := true, call := .none }
  | .takeBack who => p.set who { p.device who with granted := none, request := .queued }

def pairTrace : Pair → List PairAct → Option Pair
  | p, [] => some p
  | p, a :: rest => if pairEnabled p a then pairTrace (pairAct p a) rest else none

/-- Every step a pair could take. -/
def pairActs : List PairAct :=
  [Who.first, Who.second].flatMap fun who =>
    [.read who, .stop who, .cutOff who, .takeBack who] ++
      [Act.fix, .retry, .send, .land, .landStray, .confirm, .remain, .drop, .unshare].map (.on who)

/-- Ana's phone and laptop both hold Household's account. The phone records
the share of an invite to Dan, so both devices read the account as intended
shared; the laptop takes the grant as made, and the phone, which needs the
invite's invitation, fixes and sends it. Before the grant lands, Ben removes
the phone, which ends the invite's share: the laptop reads that entry,
revokes Dan's account, and the provider confirms; nothing is left on the
laptop. Then the phone's grant lands, and the phone, reading its own
removal, stops for good without recording the reply. Dan's account is
shared against the sequence and nothing is listed on any device. The laptop
can only stop too, and the person's confirmation that the phone is cut off
is the only other step left: with no confirmation the account stays shared. -/
theorem gone_device_strands_share :
    ∃ p, pairTrace
        ⟨[true, false],
          ⟨true, some false, .queued, .none, 1, false, false⟩,
          ⟨true, some true, .none, .none, 1, false, false⟩, false, false⟩
        [.on .first .fix, .on .first .send, .read .second, .on .second .fix,
          .on .second .send, .on .second .land, .on .second .confirm, .on .first .land,
          .stop .first] = some p ∧
      p.direct = true ∧ p.second.intended = false ∧ p.second.request = .none ∧
      p.first.stopped = true ∧
      ∀ a ∈ pairActs, pairEnabled p a = true → a = .stop .second ∨ a = .cutOff .first := by
  refine ⟨_, rfl, rfl, rfl, rfl, rfl, ?_⟩
  decide

/-- The same story, ending as the person's confirmation ends it: she signs
the phone out at the provider and tells the laptop, which takes Dan's account
back; its revocation lands after the cut-off, and nothing is left shared. -/
theorem cut_off_ends_stranded_share :
    ∃ p, pairTrace
        ⟨[true, false],
          ⟨true, some false, .queued, .none, 1, false, false⟩,
          ⟨true, some true, .none, .none, 1, false, false⟩, false, false⟩
        [.on .first .fix, .on .first .send, .read .second, .on .second .fix,
          .on .second .send, .on .second .land, .on .second .confirm, .on .first .land,
          .stop .first, .cutOff .first, .takeBack .second, .on .second .fix,
          .on .second .send, .on .second .land, .on .second .confirm] = some p ∧
      p.direct = false ∧ p.second.intended = false ∧ p.second.granted = some false ∧
      p.second.request = .none := by
  exact ⟨_, rfl, rfl, rfl, rfl, rfl⟩

/-- A request the stopped device had sent that has not landed when the person
confirms never lands: Dan's account stays unshared. -/
theorem cut_off_stops_a_request_in_the_air :
    ∃ p, pairTrace
        ⟨[true, false],
          ⟨true, some false, .queued, .none, 1, false, false⟩,
          ⟨true, some true, .none, .none, 1, false, false⟩, false, false⟩
        [.on .first .fix, .on .first .send, .read .second, .on .second .fix,
          .on .second .send, .on .second .land, .on .second .confirm, .stop .first,
          .cutOff .first] = some p ∧
      p.direct = false ∧ p.first.call = .none ∧
      pairEnabled p (.on .first .land) = false := by
  exact ⟨_, rfl, rfl, rfl, rfl⟩

/-! ### The takeBack closes every account the survivor reads as unintended -/

/-- The survivor's own steps from a state, as the pair takes them: only the
survivor moves. -/
inductive PairMoves (who : Who) : Pair → Pair → Prop
  | refl (p : Pair) : PairMoves who p p
  | step {p p' : Pair} (a : Act) (own : own a = true)
      (e : pairEnabled p (.on who a) = true)
      (rest : PairMoves who (pairAct p (.on who a)) p') : PairMoves who p p'

/-- A device's view of the pair after it takes a step of its own: the view
that step gives. -/
theorem view_after (p : Pair) (who : Who) (w : World) (s : w.stray = none) :
    view (after p who w) ((after p who w).device who) = w := by
  cases who <;> obtain ⟨i, g, r, c, st, d, o⟩ := w <;> simp_all [view, after, Pair.device, Pair.set]

/-- The survivor's steps, taken from its view, are steps of the pair: they
reach the pair whose survivor is in the view they reach. -/
theorem moves_pair {who : Who} {w w' : World} (m : Moves w w') :
    Inv w → ∀ p : Pair, (p.device who).stopped = false → view p (p.device who) = w →
      ∃ p', PairMoves who p p' ∧ view p' (p'.device who) = w' := by
  induction m with
  | refl => exact fun _ p _ hv => ⟨p, .refl p, hv⟩
  | step a o e rest ih =>
    rename_i w w'
    intro inv p live hv
    subst hv
    have en : pairEnabled p (.on who a) = true := by
      have : entrySteps a = true := by cases a <;> simp_all [own, entrySteps]
      simp [pairEnabled, live, this, e]
    have s := (inv_step _ a inv e).stray
    have hv1 : view (pairAct p (.on who a)) ((pairAct p (.on who a)).device who) =
        act Rules.correct (view p (p.device who)) a := view_after p who _ s
    have live' : ((pairAct p (.on who a)).device who).stopped = false := by
      cases who <;> simpa [pairAct, after, Pair.device, Pair.set] using live
    obtain ⟨p', pm, vw⟩ := ih (inv_step _ a inv e) (pairAct p (.on who a)) live' hv1
    exact ⟨p', .step a o en pm, vw⟩

/-- **The confirmation closes the account.** Once the person has confirmed that
one owner device is cut off, and the other, still running, takes the account's
sharing back because its history no longer intends it, that device's own steps
end with nothing listed or running, and the provider holds no grant to the
account. Nothing else reaches the account meanwhile: the cut-off device lands
no request, and it is the only other device. -/
theorem take_back_closes (p : Pair) (who : Who)
    (e : pairEnabled p (.takeBack who) = true) :
    ∃ p', PairMoves who (pairAct p (.takeBack who)) p' ∧
      quiet (view p' (p'.device who)) = true ∧ p'.direct = false := by
  have live : ((pairAct p (.takeBack who)).device who).stopped = false := by
    cases who <;> simp_all [pairEnabled, pairAct, Pair.device, Pair.set]
  have unintended : ((pairAct p (.takeBack who)).device who).intended = false := by
    cases who <;> simp_all [pairEnabled, pairAct, Pair.device, Pair.set]
  have inv : Inv (view (pairAct p (.takeBack who)) ((pairAct p (.takeBack who)).device who)) := by
    simp only [pairEnabled, Bool.and_eq_true, Bool.or_eq_true, decide_eq_true_eq,
      Bool.not_eq_true'] at e
    obtain ⟨⟨⟨⟨-, -⟩, -⟩, hc⟩, -⟩ := e
    cases who <;> simp only [pairAct, Pair.device, Pair.set, view] at hc ⊢ <;>
      constructor <;> simp [hc]
  obtain ⟨w', m, q⟩ := settles_of_inv inv
  obtain ⟨p', pm, vw⟩ := moves_pair m inv _ live rfl
  refine ⟨p', pm, vw ▸ q, ?_⟩
  have exact := moves_quiet_exact inv m q
  have : view p' (p'.device who) = w' := vw
  have direct : p'.direct = w'.direct := by rw [← this]; rfl
  rw [direct, exact]
  exact unintended

end CovenStorage.ProviderAccess
