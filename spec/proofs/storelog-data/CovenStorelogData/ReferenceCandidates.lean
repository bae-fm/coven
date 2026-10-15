import CovenStorelogData.RetentionSafety

/-! §16.5. Deleting a retained object makes the files its references name
candidates first. A candidate is only a reason to check a file; a deletion
that forgot one would leave that file unconsidered for good when no other
release names it.

This device's own writes and snapshots come from its own rows, whose
releases already made their files candidates, so their deletion needs no
read. Another uploader's write this device never checked is read before it
goes (coven `establish_references`), and so is one whose earlier verdict
was unproven once one of its parts opens or the schema range covers it: if
every part opens, its references are complete and join the candidates in
the deletion; otherwise its verdict stays unproven, and §16.5 leaves the
files such data names in storage. -/
namespace CovenStorelogData.RetentionSafety

/-- What this device keeps about one retained object's file references. -/
inductive Evidence where
  | unchecked
  /-- Read, but a part did not open here or its schema is out of range. -/
  | unproven
  | complete (files : List Nat)
  deriving DecidableEq, Repr

structure Retained where
  own : Bool
  /-- The files its authenticated body names. -/
  names : List Nat
  evidence : Evidence
  deriving DecidableEq, Repr

/-- Kept evidence is what a read of the authenticated body found. -/
def Retained.Truthful (o : Retained) : Prop :=
  ∀ files, o.evidence = .complete files → files = o.names

/-- Reading an object whose references are not complete here: complete with
exactly what it names when every part opens now (`opens`), unproven
otherwise. An unproven verdict is read again once one of its parts opens
or the schema range covers it, which is what `opens` says of it; `stale`
is the rule that reused any kept verdict. Complete evidence is reused. -/
def establish (stale : Bool := false) (opens : Bool) (o : Retained) : Retained :=
  match o.evidence with
  | .unchecked => { o with evidence := if opens then .complete o.names else .unproven }
  | .unproven =>
      if opens && !stale then { o with evidence := .complete o.names } else o
  | .complete _ => o

/-- One deletion: another uploader's object is read first; its complete
references join the candidates in the same step (coven `delete_retained`
after `establish_references`). `read` says whether the rule reads it. -/
def deleteObject (read opens : Bool) (candidates : List Nat) (o : Retained)
    (stale : Bool := false) : Retained × List Nat :=
  let o := if read && !o.own then establish stale opens o else o
  (o, match o.evidence with
    | .complete files => files ++ candidates
    | _ => candidates)

theorem establish_truthful (opens : Bool) (o : Retained) (t : o.Truthful) :
    (establish false opens o).Truthful := by
  obtain ⟨own, names, evidence⟩ := o
  intro files h
  cases evidence with
  | unchecked => cases opens <;> simp_all [establish]
  | unproven => cases opens <;> simp_all [establish]
  | complete old => exact t files (by simpa [establish] using h)

/-- **Another uploader's object is read before it goes**: the evidence its
deletion acts on is never `unchecked`. -/
theorem read_before_delete (opens : Bool) (candidates : List Nat) (o : Retained)
    (other : o.own = false) :
    (deleteObject true opens candidates o).1.evidence ≠ .unchecked := by
  obtain ⟨own, names, evidence⟩ := o
  subst other
  cases evidence <;> cases opens <;> simp [deleteObject, establish]

/-- **Every file a deleted object names is a candidate once it goes**, when
every part opens here, whether this device had checked it before, found it
unproven before, or not at all. -/
theorem names_become_candidates (opens : Bool) (candidates : List Nat) (o : Retained)
    (other : o.own = false) (t : o.Truthful)
    (readable : opens = true ∨ ∃ files, o.evidence = .complete files) :
    ∀ file ∈ o.names, file ∈ (deleteObject true opens candidates o).2 := by
  obtain ⟨own, names, evidence⟩ := o
  subst other
  intro file hf
  cases evidence with
  | unchecked =>
      rcases readable with rfl | ⟨_, hc⟩
      · simp [deleteObject, establish, hf]
      · cases hc
  | unproven =>
      rcases readable with rfl | ⟨_, hc⟩
      · simp [deleteObject, establish, hf]
      · cases hc
  | complete files =>
      have := t files rfl
      subst this
      simp [deleteObject, establish, hf]

/-- Reusing an unproven verdict: Ben found his removed phone's write
unproven while its Gifts key had not reached him. The key arrives, but
deleting the write reuses the old verdict, so the photo it names never
becomes a candidate. -/
theorem stale_unproven_counterexample :
    let o : Retained := ⟨false, [7], .unproven⟩
    7 ∈ o.names ∧ 7 ∉ (deleteObject true true [] o (stale := true)).2 ∧
      7 ∈ (deleteObject true true [] o).2 := by
  decide

/-- The rule T210 replaced deleted without reading: Ben deletes his removed
phone's write, which he never applied, and the photo it names never becomes
a candidate. -/
theorem unread_deletion_counterexample :
    let o : Retained := ⟨false, [7], .unchecked⟩
    7 ∈ o.names ∧ 7 ∉ (deleteObject false true [] o).2 := by
  decide

end CovenStorelogData.RetentionSafety
