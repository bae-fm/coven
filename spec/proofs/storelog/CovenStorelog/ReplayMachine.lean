import Std

namespace CovenStorelog.ReplayMachine

structure Result (σ : Type) where
  state : σ
  kept : List Nat
  dropped : List Nat
  deriving DecidableEq, Repr

inductive Pass (σ : Type) where
  | complete (result : Result σ)
  | restart (dropped : List Nat)
  deriving DecidableEq, Repr

/-- Each pass follows timestamp order. Authority is first, including when
an effect is already present. A winning candidate drops all its opponents. -/
def scan (auth : Nat → Bool) (unchanged : σ → Nat → Bool)
    (realize : σ → Nat → Option σ) (conflict prefer : Nat → Nat → Bool)
    (todo : List Nat) (r : Result σ) : Pass σ :=
  match todo with
  | [] => .complete r
  | w :: ws =>
      if w ∈ r.dropped then scan auth unchanged realize conflict prefer ws r else
      if !auth w then
        scan auth unchanged realize conflict prefer ws { r with dropped := w :: r.dropped }
      else if unchanged r.state w then
        scan auth unchanged realize conflict prefer ws { r with kept := w :: r.kept }
      else match realize r.state w with
        | none => scan auth unchanged realize conflict prefer ws { r with dropped := w :: r.dropped }
        | some next =>
            let opponents := r.kept.filter (conflict w)
            if !opponents.all (prefer w) then
              scan auth unchanged realize conflict prefer ws { r with dropped := w :: r.dropped }
            else if opponents.isEmpty then
              scan auth unchanged realize conflict prefer ws { r with state := next, kept := w :: r.kept }
            else .restart (opponents ++ r.dropped)

end CovenStorelog.ReplayMachine
