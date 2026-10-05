import CovenStorelog.ExamplesVersions

namespace CovenStorelog

structure JoinRequest where
  invite : Nat
  member : Nat
  deviceName : String
  deriving DecidableEq, Repr

/-- A successful storage write returns the seal receipt. Its cryptographic
meaning is an assumption at the boundary, not a cryptography theorem. -/
structure SealReceipt where
  recipient : Nat
  key : Nat
  deriving DecidableEq, Repr

inductive JoinStage where
  | requested
  | approved
  | sealed (receipt : SealReceipt)
  | published (entry : Entry)
  | declined
  deriving DecidableEq, Repr

inductive JoinEvent where
  | approve
  | seal (receipt : SealReceipt)
  | publish
  | decline
  deriving DecidableEq, Repr

structure Admission where
  request : JoinRequest
  role : Role
  author : Nat
  device : Nat
  past : List Nat
  key : Nat
  deriving DecidableEq, Repr

def Admission.entry (a : Admission) : Entry :=
  ⟨a.author, a.device, a.past, .addMember a.request.member a.role⟩

/-- Only a seal for this person and this key allows publication. Requests,
approval, and sealing are outside the store log; publication creates its add.
The resolver still checks the approving author's authority. -/
def joinStep (a : Admission) : JoinStage → JoinEvent → Option JoinStage
  | .requested, .approve => some .approved
  | .approved, .seal receipt =>
      if receipt.recipient == a.request.member && receipt.key == a.key
      then some (.sealed receipt) else none
  | .sealed receipt, .publish =>
      if receipt.recipient == a.request.member && receipt.key == a.key
      then some (.published a.entry) else none
  | .requested, .decline | .approved, .decline | .sealed _, .decline => some .declined
  | _, _ => none

def joinRun (a : Admission) : List JoinEvent → JoinStage → Option JoinStage
  | [], s => some s
  | e :: es, s => (joinStep a s e).bind (joinRun a es)

theorem publication_requires_seal (a : Admission) (s : JoinStage) (e : Entry)
    (h : joinStep a s .publish = some (.published e)) :
    ∃ receipt, s = .sealed receipt ∧ receipt.recipient = a.request.member ∧
      receipt.key = a.key ∧ e = a.entry := by
  cases s <;> simp [joinStep] at h
  exact ⟨_, rfl, h.1.1, h.1.2, h.2.symm⟩

theorem sealing_requires_approval (a : Admission) (s : JoinStage) (r t : SealReceipt)
    (h : joinStep a s (.seal r) = some (.sealed t)) : s = .approved := by
  cases s <;> simp_all [joinStep]

inductive JoinOutcome where
  | waiting | joined | declined
  deriving DecidableEq, Repr

def joinOutcome (r : Result) (addition : Nat) : JoinOutcome :=
  if addition ∈ r.dropped then .declined else
  if addition ∈ r.kept then .joined else .waiting

theorem dropped_join_declined (r : Result) (addition : Nat) (h : addition ∈ r.dropped) :
    joinOutcome r addition = .declined := by simp [joinOutcome, h]

namespace Examples

def carolAdmission : Admission := ⟨⟨12, 2, "Carol's phone"⟩, .member, 0, 0, [0, 1, 2, 3], 1⟩

theorem seal_then_entry :
    joinRun carolAdmission [.approve, .publish] .requested = none ∧
    joinRun carolAdmission [.approve, .seal ⟨2, 1⟩, .publish] .requested =
      some (.published carolAdmission.entry) ∧
    joinRun carolAdmission [.approve, .seal ⟨3, 1⟩, .publish] .requested = none ∧
    joinRun carolAdmission [.approve, .seal ⟨2, 0⟩, .publish] .requested = none := by decide

theorem carol_join_declined : EveryOrder carol 6 [0, 1, 2, 3, 4, 5] (fun r =>
    joinOutcome r 4 = .declined ∧ reports carol r 0 = [4]) := by
  apply every_order
  decide

end Examples
end CovenStorelog
